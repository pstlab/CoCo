import socket
import ssl
import binascii
import hashlib
import random
import asyncio
import json
import select

from model import CocoClass, CocoObject


WS_CONNECT_TIMEOUT_S = 10
WS_IO_TIMEOUT_S = 20


def _set_timeout(sock, timeout_s: int):
    try:
        set_timeout = getattr(sock, "settimeout", None)
        if callable(set_timeout):
            set_timeout(timeout_s)
    except Exception:
        pass


def _handshake(host: str, token: str) -> socket.socket | ssl.SSLSocket | None:
    print("Performing WebSocket handshake...")
    path = "/ws?token=" + token
    random_key = bytes([random.getrandbits(8) for _ in range(16)])
    ws_key = binascii.b2a_base64(random_key).strip().decode()
    expected_accept = binascii.b2a_base64(
        hashlib.sha1(
            (ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
    ).strip().decode()

    tcp_sock = None
    tls_sock = None
    try:
        tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        _set_timeout(tcp_sock, WS_CONNECT_TIMEOUT_S)
        addr = socket.getaddrinfo(host, 443)[0][-1]
        tcp_sock.connect(addr)
        tls_sock = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT).wrap_socket(
            tcp_sock, server_hostname=host)
        _set_timeout(tls_sock, WS_IO_TIMEOUT_S)

        request = (
            "GET {} HTTP/1.1\r\n"
            "Host: {}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: {}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        ).format(path, host, ws_key)

        tls_sock.sendall(request.encode())
        response = b""
        while b"\r\n\r\n" not in response:
            chunk = tls_sock.recv(1024)
            if not chunk:
                break
            response += chunk

        response_text = response.decode("utf-8", "replace")
        status_line, _, header_block = response_text.partition("\r\n")
        headers = {}
        for line in header_block.split("\r\n"):
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()

        if status_line.startswith("HTTP/1.1 101") and headers.get("upgrade", "").lower() == "websocket" and headers.get("connection", "").lower() == "upgrade" and headers.get("sec-websocket-accept") == expected_accept:
            print("WebSocket handshake successful!")
            return tls_sock
        else:
            print("WebSocket handshake failed. Response:", response_text)
            tls_sock.close()
            tcp_sock.close()
            return None
    except Exception as e:
        print("An error occurred with WebSocket:", e)
        try:
            if tls_sock is not None:
                tls_sock.close()
        except Exception:
            pass
        try:
            if tcp_sock is not None:
                tcp_sock.close()
        except Exception:
            pass
        return None


def _read_exact(sock: socket.socket | ssl.SSLSocket, n: int) -> bytes | None:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def _ws_read_frame(sock: socket.socket | ssl.SSLSocket) -> tuple[int | None, bytes | None]:
    hdr = _read_exact(sock, 2)
    if not hdr:
        return None, None

    b0 = hdr[0]
    b1 = hdr[1]
    opcode = b0 & 0x0F
    masked = (b1 & 0x80) != 0
    plen = b1 & 0x7F

    if plen == 126:
        ext = _read_exact(sock, 2)
        if not ext:
            return None, None
        plen = (ext[0] << 8) | ext[1]
    elif plen == 127:
        ext = _read_exact(sock, 8)
        if not ext:
            return None, None
        plen = 0
        for b in ext:
            plen = (plen << 8) | b

    mask_key = b""
    if masked:
        mask_key = _read_exact(sock, 4)
        if not mask_key:
            return None, None

    payload = _read_exact(sock, plen) if plen else b""
    if payload is None:
        return None, None

    if masked:
        data = bytearray(payload)
        for i in range(len(data)):
            data[i] ^= mask_key[i % 4]
        payload = bytes(data)

    return opcode, payload


def _ws_send_pong(sock: socket.socket | ssl.SSLSocket, payload=b""):
    print("Sending pong frame to server")
    plen = len(payload)
    mask = bytes([random.getrandbits(8) for _ in range(4)])

    header = bytearray([0x8A])
    if plen < 126:
        header.append(0x80 | plen)
    elif plen < 65536:
        header.append(0x80 | 126)
        header.extend(((plen >> 8) & 0xFF, plen & 0xFF))
    else:
        header.append(0x80 | 127)
        for shift in (56, 48, 40, 32, 24, 16, 8, 0):
            header.append((plen >> shift) & 0xFF)

    header.extend(mask)
    masked = bytearray(plen)
    for i in range(plen):
        masked[i] = payload[i] ^ mask[i % 4]

    sock.sendall(header + masked)


def _decode_close_payload(payload: bytes) -> tuple[int | None, str]:
    if not payload or len(payload) < 2:
        return None, ""

    code = (payload[0] << 8) | payload[1]
    reason = ""
    if len(payload) > 2:
        try:
            reason = payload[2:].decode()
        except Exception:
            reason = str(payload[2:])
    return code, reason


async def ws_loop(host: str, token: str, on_new_class=None, on_new_object=None, on_classes_update=None, on_object_update=None, on_new_data=None):
    while True:
        ws_sock = _handshake(host, token)
        if ws_sock is None:
            print("Failed to establish WebSocket connection. Retrying in 5 seconds...")
            await asyncio.sleep(5)
            continue

        try:
            while True:
                selected = select.select((ws_sock,), (), (), 0)
                ready = ()
                if selected:
                    ready = selected[0]
                if not ready:
                    await asyncio.sleep(1/10)
                    continue

                try:
                    opcode, payload = _ws_read_frame(ws_sock)
                except OSError as e:
                    msg = str(e)
                    if "ETIMEDOUT" in msg or "timed out" in msg:
                        await asyncio.sleep(0)
                        continue
                    code = None
                    if hasattr(e, "args") and e.args:
                        code = e.args[0]
                    if code == 110:
                        await asyncio.sleep(0)
                        continue
                    raise
                if opcode is None:
                    raise OSError("WebSocket closed by peer")
                if payload is None:
                    payload = b""

                if opcode == 0x1:  # text
                    try:
                        print("Received message:", payload.decode())
                        data = json.loads(payload.decode())
                        msg_type = data["msg_type"]
                        if msg_type == "new-class" and callable(on_new_class):
                            on_new_class(CocoClass.from_json(data))
                        if msg_type == "new-object" and callable(on_new_object):
                            on_new_object(CocoObject.from_json(data))
                        if msg_type == "classes-update" and callable(on_classes_update):
                            on_classes_update(
                                data["object_id"], data["classes"])
                        if msg_type == "object-update" and callable(on_object_update):
                            on_object_update(
                                data["object_id"], data["properties"])
                        if msg_type == "new-data" and callable(on_new_data):
                            on_new_data(data["object_id"], data["data"])
                    except Exception:
                        print("Received binary-ish text payload:", payload)
                elif opcode == 0x9:  # ping
                    print("Received ping from server")
                    _ws_send_pong(ws_sock, payload)
                elif opcode == 0x8:  # close
                    code, reason = _decode_close_payload(payload)
                    print("Server close frame:", code, reason)
                    raise OSError("WebSocket closed by peer")
        except Exception as e:
            print("An error occurred during WebSocket communication:", e)
        finally:
            ws_sock.close()
            print("WebSocket connection closed. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)


def connect(host: str, token: str, on_new_class=None, on_new_object=None, on_classes_update=None, on_object_update=None, on_new_data=None):
    print("Connecting to WebSocket server...")
    asyncio.create_task(ws_loop(host, token, on_new_class, on_new_object,
                        on_classes_update, on_object_update, on_new_data))
