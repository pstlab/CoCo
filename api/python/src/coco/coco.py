from .model import *
import binascii
import hashlib
import random
import json
import asyncio
import socket
import ssl
import select
import requests


def _quote_plus(s: str) -> str:
    safe = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-~"
    result = []
    for c in s:
        if c in safe:
            result.append(c)
        elif c == " ":
            result.append("+")
        else:
            for b in c.encode("utf-8"):
                result.append("%{:02X}".format(b))
    return "".join(result)


def _urlencode(params: dict[str, Value]) -> str:
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append("{}={}".format(_quote_plus(str(k)), _quote_plus(str(v))))
    return "&".join(parts)


class CoCo:
    def __init__(self, host: str, verify_ssl=True):
        self.host = host
        self.verify_ssl = verify_ssl
        self.token: AuthTokens | None = None

    def login(self, username: str, password: str, timeout=5):
        url = f"https://{self.host}/login"
        payload = {"username": username, "password": password}

        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            self.token = AuthTokens.from_json(response.json())
        finally:
            response.close()

    def refresh_token(self, timeout=5):
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/refresh_token"
        payload = {"refresh_token": self.token.refresh_token}

        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            self.token = AuthTokens.from_json(response.json())
        finally:
            response.close()

    def get_classes(self, timeout=5) -> list[CoCoClass]:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/classes"

        def fetch_classes():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_classes()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_classes()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return [CoCoClass.from_json(cls) for cls in response.json()]
        finally:
            response.close()

    def get_class(self, class_id: str, timeout=5) -> CoCoClass:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/classes/{class_id}"

        def fetch_class():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_class()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_class()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return CoCoClass.from_json(response.json())
        finally:
            response.close()

    def create_class(self, cls: CoCoClass, timeout=5):
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/classes"
        payload = cls.to_json()

        def post_class():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.post(url, json=payload, headers=headers, timeout=timeout)

        response = post_class()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = post_class()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return CoCoClass.from_json(response.json())
        finally:
            response.close()

    def get_rules(self, timeout=5) -> list[CoCoRule]:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/rules"

        def fetch_rules():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_rules()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_rules()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return [CoCoRule.from_json(rule) for rule in response.json()]
        finally:
            response.close()

    def get_rule(self, name: str, timeout=5) -> CoCoRule:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/rules/{name}"

        def fetch_rule():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_rule()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_rule()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return CoCoRule.from_json(response.json())
        finally:
            response.close()

    def create_rule(self, rule: CoCoRule, timeout=5):
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/rules"
        payload = rule.to_json()

        def post_rule():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.post(url, json=payload, headers=headers, timeout=timeout)

        response = post_rule()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = post_rule()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return CoCoRule.from_json(response.json())
        finally:
            response.close()

    def get_objects(self, classes: set[str] = None, filters: dict[str, Value] = None, timeout=5) -> list[CoCoObject]:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects"
        params = {}
        if classes:
            params["classes"] = ",".join(classes)
        if filters:
            params.update(filters)
        query_string = _urlencode(params)
        if query_string:
            url += "?" + query_string

        def fetch_objects():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_objects()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_objects()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return [CoCoObject.from_json(obj) for obj in response.json()]
        finally:
            response.close()

    def get_object(self, object_id: str, timeout=5) -> CoCoObject:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects/{object_id}"

        def fetch_object():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_object()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_object()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return CoCoObject.from_json(response.json())
        finally:
            response.close()

    def create_object(self, obj: CoCoObject, timeout=5):
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects"
        payload = obj.to_json()

        def post_object():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.post(url, json=payload, headers=headers, timeout=timeout)

        response = post_object()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = post_object()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return CoCoObject.from_json(response.json())
        finally:
            response.close()

    def get_data(self, object_id: str, start: str = None, end: str = None, timeout=10) -> dict[str, list[Value]]:
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects/{object_id}/data"
        params = {}
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        query_string = _urlencode(params)
        if query_string:
            url += "?" + query_string

        def fetch_data():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.get(url, headers=headers, timeout=timeout)

        response = fetch_data()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = fetch_data()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return response.json()
        finally:
            response.close()

    def add_data(self, object_id: str, data: dict[str, Value], timestamp: str = None, timeout=5):
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects/{object_id}/data"
        params = {}
        if timestamp:
            params["timestamp"] = timestamp
        query_string = _urlencode(params)
        if query_string:
            url += "?" + query_string

        def post_data():
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return requests.post(url, json=data, headers=headers, timeout=timeout)

        response = post_data()
        if response.status_code in (401, 403):
            response.close()
            self.refresh_token(timeout=timeout)
            response = post_data()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        response.close()

    def _handshake(self, connect_timeout=5, io_timeout=20) -> ssl.SSLSocket:
        print("Performing WebSocket handshake...")
        random_key = bytes([random.getrandbits(8) for _ in range(16)])
        ws_key = binascii.b2a_base64(random_key).strip().decode()
        expected_accept = binascii.b2a_base64(hashlib.sha1((ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).strip().decode()

        def get_handshake(token: str) -> tuple[ssl.SSLSocket, str]:
            path = f"/ws?token={token}"
            tcp_sock = None
            tls_sock = None
            try:
                tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tcp_sock.settimeout(connect_timeout)
                addr = socket.getaddrinfo(self.host, 443)[0][-1]
                tcp_sock.connect(addr)
                ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

                if not self.verify_ssl:
                    ssl_ctx.check_hostname = False
                    ssl_ctx.verify_mode = ssl.CERT_NONE

                tls_sock = ssl_ctx.wrap_socket(tcp_sock, server_hostname=self.host)
                tls_sock.settimeout(io_timeout)

                request = (
                    "GET {} HTTP/1.1\r\n"
                    "Host: {}\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    "Sec-WebSocket-Key: {}\r\n"
                    "Sec-WebSocket-Version: 13\r\n"
                    "\r\n"
                ).format(path, self.host, ws_key)

                tls_sock.sendall(request.encode())
                response = b""
                while b"\r\n\r\n" not in response:
                    chunk = tls_sock.recv(1024)
                    if not chunk:
                        break
                    response += chunk

                return tls_sock, response.decode("utf-8", "replace")
            except Exception as e:
                print(f"An error occurred during WebSocket handshake: {e}")
                try:
                    if tls_sock is not None:
                        tls_sock.close()
                except Exception as e:
                    pass
                try:
                    if tcp_sock is not None:
                        tcp_sock.close()
                except Exception as e:
                    pass
                raise

        result = get_handshake(self.token.access_token)
        status_line, _, header_block = result[1].partition("\r\n")

        status_parts = status_line.split(" ")
        status_code = int(status_parts[1]) if len(status_parts) >= 2 and status_parts[1].isdigit() else 0

        if status_code in (401, 403):
            print(f"WebSocket auth failed with status: {status_code}")
            self.refresh_token()
            result = get_handshake(self.token.access_token)
            status_line, _, header_block = result[1].partition("\r\n")
            status_parts = status_line.split(" ")
            status_code = int(status_parts[1]) if len(status_parts) >= 2 and status_parts[1].isdigit() else 0

        headers = {}
        for line in header_block.split("\r\n"):
            if ":" not in line:
                continue
            name, value = line.split(":", 1)
            headers[name.strip().lower()] = value.strip()

        if status_line.startswith("HTTP/1.1 101") and headers.get("upgrade", "").lower() == "websocket" and headers.get("connection", "").lower() == "upgrade" and headers.get("sec-websocket-accept") == expected_accept:
            print("WebSocket handshake successful!")
            return result[0]
        else:
            print("WebSocket handshake failed. Response:", result[1])
            result[0].close()
            raise CoCoHTTPError(status_code)

    async def connect(self, on_new_class=None, on_new_rule=None, on_new_object=None, on_classes_update=None, on_object_update=None, on_new_data=None):
        if on_new_class is not None and not callable(on_new_class):
            raise ValueError("on_new_class must be a callable function or None.")
        if on_new_rule is not None and not callable(on_new_rule):
            raise ValueError("on_new_rule must be a callable function or None.")
        if on_new_object is not None and not callable(on_new_object):
            raise ValueError("on_new_object must be a callable function or None.")
        if on_classes_update is not None and not callable(on_classes_update):
            raise ValueError("on_classes_update must be a callable function or None.")
        if on_object_update is not None and not callable(on_object_update):
            raise ValueError("on_object_update must be a callable function or None.")
        if on_new_data is not None and not callable(on_new_data):
            raise ValueError("on_new_data must be a callable function or None.")
        if self.token is None:
            raise ValueError("No token available. Please login first.")
        while True:
            ws_sock = self._handshake()
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
                            text_payload = payload.decode("utf-8", "replace")
                            print("Received message:", text_payload)
                            data = json.loads(text_payload)
                            msg_type = data["msg_type"]
                            if msg_type == "new-class" and on_new_class is not None:
                                on_new_class(CoCoClass.from_json(data))
                            if msg_type == "new-rule" and on_new_rule is not None:
                                on_new_rule(CoCoRule.from_json(data))
                            if msg_type == "new-object" and on_new_object is not None:
                                on_new_object(CoCoObject.from_json(data))
                            if msg_type == "classes-update" and on_classes_update is not None:
                                on_classes_update(data["object_id"], data["classes"])
                            if msg_type == "properties-updated" and on_object_update is not None:
                                on_object_update(data["object_id"], data["properties"])
                            if msg_type == "values-added" and on_new_data is not None:
                                on_new_data(data["object_id"], data["values"], data["timestamp"])
                        except Exception as e:
                            print("Error occurred while processing message:", e)
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


def _read_exact(sock: ssl.SSLSocket, n: int) -> bytes | None:
    data = b""
    while len(data) < n:
        chunk = sock.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def _ws_read_frame(sock: ssl.SSLSocket) -> tuple[int | None, bytes | None]:
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


def _ws_send_pong(sock: ssl.SSLSocket, payload=b""):
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
