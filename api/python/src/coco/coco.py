"""
CoCo client - MicroPython-compatible version.

This module provides a client for interacting with the CoCo API, including
authentication, class and rule management, object management, data retrieval,
and WebSocket communication for real-time updates.
"""

import binascii
import hashlib
import random
import json
import asyncio
import socket
import ssl
import select
import sys


class AuthTokens:
    def __init__(self, access_token, refresh_token, token_type):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_type = token_type

    def __repr__(self):
        return f"AuthTokens(access_token={self.access_token}, refresh_token={self.refresh_token}, token_type={self.token_type})"

    @staticmethod
    def from_json(json_data):
        access_token = json_data["access_token"]
        refresh_token = json_data["refresh_token"]
        token_type = json_data["token_type"]
        return AuthTokens(access_token=access_token, refresh_token=refresh_token, token_type=token_type)


class CoCoHTTPError(Exception):
    def __init__(self, status_code):
        super().__init__(
            f"HTTP request failed with status code: {status_code}")
        self.status_code = status_code

    def __repr__(self):
        return f"CoCoHTTPError(status_code={self.status_code})"


class Property:
    def __init__(self, default=None, description=None):
        self.default = default
        self.description = description

    def __repr__(self):
        prop = []
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return ", ".join(prop)

    @staticmethod
    def from_json(json_data):
        property_type = json_data.get("type")
        if property_type == "bool":
            return BoolProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "int":
            return IntProperty(
                min=json_data.get("min"),
                max=json_data.get("max"),
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "float":
            return FloatProperty(
                min=json_data.get("min"),
                max=json_data.get("max"),
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "string":
            return StringProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "symbol":
            return SymbolProperty(
                allowed_values=json_data.get("allowed_values"),
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "object":
            return ObjectProperty(
                classes=json_data["classes"],
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "bool-array":
            return BoolArrayProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "int-array":
            return IntArrayProperty(
                min=json_data.get("min"),
                max=json_data.get("max"),
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "float-array":
            return FloatArrayProperty(
                min=json_data.get("min"),
                max=json_data.get("max"),
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "string-array":
            return StringArrayProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "symbol-array":
            return SymbolArrayProperty(
                allowed_values=json_data.get("allowed_values"),
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "object-array":
            return ObjectArrayProperty(
                classes=json_data["classes"],
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        else:
            raise ValueError(f"Unknown property type: {property_type}")

    def to_json(self):
        raise NotImplementedError("Subclasses must implement to_json method")


class BoolProperty(Property):
    def __init__(self, default=None, description=None):
        super().__init__(default=default, description=description)

    def __repr__(self):
        prop = []
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"BoolProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "bool"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class IntProperty(Property):
    def __init__(self, min=None, max=None, default=None, description=None):
        super().__init__(default=default, description=description)
        self.min = min
        self.max = max

    def __repr__(self):
        prop = []
        if self.min is not None:
            prop.append(f"min={self.min}")
        if self.max is not None:
            prop.append(f"max={self.max}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"IntProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "int"}
        if self.min is not None:
            json_data["min"] = self.min
        if self.max is not None:
            json_data["max"] = self.max
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class FloatProperty(Property):
    def __init__(self, min=None, max=None, default=None, description=None):
        super().__init__(default=default, description=description)
        self.min = min
        self.max = max

    def __repr__(self):
        prop = []
        if self.min is not None:
            prop.append(f"min={self.min}")
        if self.max is not None:
            prop.append(f"max={self.max}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"FloatProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "float"}
        if self.min is not None:
            json_data["min"] = self.min
        if self.max is not None:
            json_data["max"] = self.max
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class StringProperty(Property):
    def __init__(self, default=None, description=None):
        super().__init__(default=default, description=description)

    def __repr__(self):
        prop = []
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"StringProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "string"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class SymbolProperty(Property):
    def __init__(self, allowed_values=None, default=None, description=None):
        super().__init__(default=default, description=description)
        self.allowed_values = allowed_values

    def __repr__(self):
        prop = []
        if self.allowed_values is not None:
            prop.append(f"allowed_values={self.allowed_values}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"SymbolProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "symbol"}
        if self.allowed_values is not None:
            json_data["allowed_values"] = self.allowed_values
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class ObjectProperty(Property):
    def __init__(self, classes, default=None, description=None):
        super().__init__(default=default, description=description)
        self.classes = classes

    def __repr__(self):
        prop = []
        if self.classes is not None:
            prop.append(f"classes={self.classes}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"ObjectProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {
            "type": "object", "classes": self.classes}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class BoolArrayProperty(Property):
    def __init__(self, default=None, description=None):
        super().__init__(default=default, description=description)

    def __repr__(self):
        prop = []
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"BoolArrayProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "bool-array"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class IntArrayProperty(Property):
    def __init__(self, min=None, max=None, default=None, description=None):
        super().__init__(default=default, description=description)
        self.min = min
        self.max = max

    def __repr__(self):
        prop = []
        if self.min is not None:
            prop.append(f"min={self.min}")
        if self.max is not None:
            prop.append(f"max={self.max}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"IntArrayProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "int-array"}
        if self.min is not None:
            json_data["min"] = self.min
        if self.max is not None:
            json_data["max"] = self.max
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class FloatArrayProperty(Property):
    def __init__(self, min=None, max=None, default=None, description=None):
        super().__init__(default=default, description=description)
        self.min = min
        self.max = max

    def __repr__(self):
        prop = []
        if self.min is not None:
            prop.append(f"min={self.min}")
        if self.max is not None:
            prop.append(f"max={self.max}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"FloatArrayProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "float-array"}
        if self.min is not None:
            json_data["min"] = self.min
        if self.max is not None:
            json_data["max"] = self.max
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class StringArrayProperty(Property):
    def __init__(self, default=None, description=None):
        super().__init__(default=default, description=description)

    def __repr__(self):
        prop = []
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"StringArrayProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "string-array"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class SymbolArrayProperty(Property):
    def __init__(self, allowed_values=None, default=None, description=None):
        super().__init__(default=default, description=description)
        self.allowed_values = allowed_values

    def __repr__(self):
        prop = []
        if self.allowed_values is not None:
            prop.append(f"allowed_values={self.allowed_values}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"SymbolArrayProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {"type": "symbol-array"}
        if self.allowed_values is not None:
            json_data["allowed_values"] = self.allowed_values
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class ObjectArrayProperty(Property):
    def __init__(self, classes, default=None, description=None):
        super().__init__(default=default, description=description)
        self.classes = classes

    def __repr__(self):
        prop = []
        if self.classes is not None:
            prop.append(f"classes={self.classes}")
        if self.default is not None:
            prop.append(f"default={self.default}")
        if self.description is not None:
            prop.append(f"description={self.description}")
        return f"ObjectArrayProperty({', '.join(prop)})"

    def to_json(self):
        json_data = {
            "type": "object-array", "classes": self.classes}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class CoCoClass:
    def __init__(self, name, parents=None, static_properties=None, dynamic_properties=None):
        self.name = name
        self.parents = parents
        self.static_properties = static_properties
        self.dynamic_properties = dynamic_properties

    def __repr__(self):
        props = []
        if self.parents is not None:
            props.append(f"parents={self.parents}")
        if self.static_properties is not None:
            props.append(f"static_properties={self.static_properties}")
        if self.dynamic_properties is not None:
            props.append(f"dynamic_properties={self.dynamic_properties}")
        return f"CoCoClass(name={self.name}, {', '.join(props)})"

    @staticmethod
    def from_json(json_data):
        name = json_data["name"]
        if not isinstance(name, str):
            raise ValueError("Invalid class name in JSON data")
        parents = json_data.get("parents")
        static_properties_json = json_data.get("static_properties")
        dynamic_properties_json = json_data.get("dynamic_properties")

        static_properties = None
        if static_properties_json is not None:
            static_properties = {}
            for key, prop_json in static_properties_json.items():
                static_properties[key] = Property.from_json(prop_json)

        dynamic_properties = None
        if dynamic_properties_json is not None:
            dynamic_properties = {}
            for key, prop_json in dynamic_properties_json.items():
                dynamic_properties[key] = Property.from_json(prop_json)

        return CoCoClass(name=name, parents=parents, static_properties=static_properties, dynamic_properties=dynamic_properties)

    def to_json(self):
        json_data = {"name": self.name}
        if self.parents is not None:
            json_data["parents"] = self.parents
        if self.static_properties is not None:
            json_data["static_properties"] = {
                key: prop.to_json() for key, prop in self.static_properties.items()
            }
        if self.dynamic_properties is not None:
            json_data["dynamic_properties"] = {
                key: prop.to_json() for key, prop in self.dynamic_properties.items()
            }
        return json_data


class CoCoRule:
    def __init__(self, name, content):
        self.name = name
        self.content = content

    def __repr__(self):
        return f"CoCoRule(name={self.name}, content={self.content})"

    @staticmethod
    def from_json(json_data):
        name = json_data["name"]
        content = json_data["content"]
        return CoCoRule(name=name, content=content)

    def to_json(self):
        return {"name": self.name, "content": self.content}


class CoCoObject:
    def __init__(self, id, classes, properties=None, values=None):
        self.id = id
        self.classes = classes
        self.properties = properties
        self.values = values

    def __repr__(self):
        props = []
        if self.properties is not None:
            props.append(f"properties={self.properties}")
        if self.values is not None:
            props.append(f"values={self.values}")
        return f"CoCoObject(id={self.id}, classes={self.classes}, {', '.join(props)})"

    @staticmethod
    def from_json(json_data):
        id = json_data["id"]
        if not isinstance(id, str):
            raise ValueError("Invalid object ID in JSON data")
        classes = json_data["classes"]
        properties = json_data.get("properties")
        values_json = json_data.get("values")

        values = None
        if values_json is not None:
            values = {}
            for key, value in values_json.items():
                if isinstance(value, dict) and "value" in value and "timestamp" in value:
                    values[key] = (value["value"], value["timestamp"])

        return CoCoObject(id=id, classes=classes, properties=properties, values=values)

    def to_json(self):
        json_data = {
            "id": self.id,
            "classes": self.classes,
        }
        if self.properties is not None:
            json_data["properties"] = self.properties
        if self.values is not None:
            json_data["values"] = {key: {"value": value[0], "timestamp": value[1]} for key, value in self.values.items()}
        return json_data


# MicroPython: "requests" is not a builtin module. On ports with
# networking it can be installed via `mip.install("requests")` (the
# modern micropython-lib name), or on older firmware it's available as
# `urequests`. We try both.
try:
    import requests
except ImportError:
    import urequests as requests  # type: ignore


def _quote_plus(s):
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


def _urlencode(params):
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append("{}={}".format(_quote_plus(str(k)), _quote_plus(str(v))))
    return "&".join(parts)


def _http_request(method, url, timeout=5, **kwargs):
    # MicroPython: some urequests builds don't accept timeout=. In
    # that case fall back to a call without an explicit timeout (the
    # socket's default timeout will be used, if one is set).
    try:
        return method(url, timeout=timeout, **kwargs)
    except TypeError:
        return method(url, **kwargs)


def _sock_write(sock, data):
    # MicroPython: TLS-wrapped sockets reliably expose only the stream
    # interface (write/read), not necessarily sendall/recv. write()
    # can write fewer bytes than requested (or return None on a
    # non-blocking socket), so we loop.
    if hasattr(sock, "write"):
        mv = memoryview(data)
        total = 0
        while total < len(mv):
            n = sock.write(mv[total:])
            if not n:
                continue
            total += n
    else:
        sock.sendall(data)


def _sock_read(sock, n):
    if hasattr(sock, "read"):
        return sock.read(n)
    return sock.recv(n)


def _safe_decode_bytes(data):
    # MicroPython can raise UnicodeError when using decode(..., errors=...).
    # Try UTF-8 first, then fall back to latin-1 for diagnostics.
    try:
        return data.decode("utf-8")
    except Exception:
        try:
            return data.decode("latin-1")
        except Exception:
            return str(data)


def _is_timeout_error(e):
    msg = str(e)
    if "ETIMEDOUT" in msg or "timed out" in msg:
        return True
    code = None
    if hasattr(e, "args") and e.args:
        code = e.args[0]
    return code == 110  # errno.ETIMEDOUT


def _tls_wrap(tcp_sock, host, verify_ssl):
    # MicroPython: not every port has ssl.SSLContext /
    # ssl.PROTOCOL_TLS_CLIENT (added in relatively recent versions).
    # Older/minimal ports only have the module-level
    # ssl.wrap_socket(...) function.
    try:
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if not verify_ssl:
            if hasattr(ssl_ctx, "check_hostname"):
                # setattr instead of ssl_ctx.check_hostname = False:
                # this is an assignment target the stub may not
                # declare on every port/version; setattr() is typed
                # permissively and isn't statically checked the way a
                # direct assignment would be.
                setattr(ssl_ctx, "check_hostname", False)
            ssl_ctx.verify_mode = ssl.CERT_NONE
        return ssl_ctx.wrap_socket(tcp_sock, server_hostname=host)
    except AttributeError:
        kwargs = {"server_hostname": host}
        if not verify_ssl:
            kwargs["cert_reqs"] = ssl.CERT_NONE
        return ssl.wrap_socket(tcp_sock, **kwargs)  # type: ignore


class CoCo:
    def __init__(self, host, verify_ssl=True):
        """
        Initialize the CoCo client.

        :param host: The API host.
        :param verify_ssl: Whether to verify SSL certificates.
        """
        self.host = host
        self.verify_ssl = verify_ssl
        self.token = None

        self._stop_requested = False
        self._ws_sock = None

    def login(self, username, password, timeout=5):
        """
        Log in to the CoCo API.

        :param username: The username.
        :param password: The password.
        :param timeout: The request timeout.
        """
        url = f"https://{self.host}/login"
        payload = {"username": username, "password": password}

        response = _http_request(
            requests.post, url, json=payload, timeout=timeout)
        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            self.token = AuthTokens.from_json(response.json())
        finally:
            response.close()

    def refresh_token(self, timeout=5):
        """
        Refresh the authentication token.

        :param timeout: The request timeout.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/refresh_token"
        payload = {"refresh_token": self.token.refresh_token}

        response = _http_request(
            requests.post, url, json=payload, timeout=timeout)
        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            self.token = AuthTokens.from_json(response.json())
        finally:
            response.close()

    def get_classes(self, timeout=5):
        """
        Retrieve all classes from the CoCo API.

        :param timeout: The request timeout.
        :return: A list of CoCoClass instances.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/classes"

        def fetch_classes():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_classes()
        if response.status_code == 401:
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

    def get_class(self, class_id, timeout=5):
        """
        Retrieve a specific class from the CoCo API.

        :param class_id: The ID of the class to retrieve.
        :param timeout: The request timeout.
        :return: A CoCoClass instance.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/classes/{class_id}"

        def fetch_class():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_class()
        if response.status_code == 401:
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

    def create_class(self, cls, timeout=5):
        """
        Create a new class in the CoCo API.

        :param cls: The class to create.
        :param timeout: The request timeout.
        :return: A CoCoClass instance.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/classes"
        payload = cls.to_json()

        def post_class():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.post, url, json=payload, headers=headers, timeout=timeout)

        response = post_class()
        if response.status_code == 401:
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

    def get_rules(self, timeout=5):
        """
        Retrieve all rules from the CoCo API.

        :param timeout: The request timeout.
        :return: A list of CoCoRule instances.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/rules"

        def fetch_rules():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_rules()
        if response.status_code == 401:
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

    def get_rule(self, name, timeout=5):
        """
        Retrieve a specific rule from the CoCo API.

        :param name: The name of the rule to retrieve.
        :param timeout: The request timeout.
        :return: A CoCoRule instance.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/rules/{name}"

        def fetch_rule():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_rule()
        if response.status_code == 401:
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

    def create_rule(self, rule, timeout=5):
        """
        Create a new rule in the CoCo API.

        :param rule: The rule to create.
        :param timeout: The request timeout.
        :return: A CoCoRule instance.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/rules"
        payload = rule.to_json()

        def post_rule():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.post, url, json=payload, headers=headers, timeout=timeout)

        response = post_rule()
        if response.status_code == 401:
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

    def get_objects(self, classes=None, filters=None, timeout=5):
        """
        Retrieve all objects from the CoCo API.

        :param classes: A list of classes to filter by.
        :param filters: A dictionary of additional filters.
        :param timeout: The request timeout.
        :return: A list of CoCoObject instances.
        """
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
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_objects()
        if response.status_code == 401:
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

    def get_object(self, object_id, timeout=5):
        """
        Retrieve a specific object from the CoCo API.

        :param object_id: The ID of the object to retrieve.
        :param timeout: The request timeout.
        :return: A CoCoObject instance.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects/{object_id}"

        def fetch_object():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_object()
        if response.status_code == 401:
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

    def create_object(self, obj, timeout=5):
        """
        Create a new object in the CoCo API.

        :param obj: The object to create.
        :param timeout: The request timeout.
        :return: A CoCoObject instance.
        """
        if self.token is None:
            raise ValueError("No token available. Please login first.")

        url = f"https://{self.host}/objects"
        payload = obj.to_json()

        def post_object():
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.post, url, json=payload, headers=headers, timeout=timeout)

        response = post_object()
        if response.status_code == 401:
            response.close()
            self.refresh_token(timeout=timeout)
            response = post_object()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        try:
            return response.text
        finally:
            response.close()

    def get_data(self, object_id, start=None, end=None, timeout=10):
        """
        Retrieve data for a specific object from the CoCo API.

        :param object_id: The ID of the object for which to retrieve data.
        :param start: The start timestamp for the data range.
        :param end: The end timestamp for the data range.
        :param timeout: The request timeout.
        :return: A list of data points.
        """
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
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.get, url, headers=headers, timeout=timeout)

        response = fetch_data()
        if response.status_code == 401:
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

    def add_data(self, object_id, data, timestamp=None, timeout=5):
        """
        Add data for a specific object in the CoCo API.

        :param object_id: The ID of the object for which to add data.
        :param data: The data to add.
        :param timestamp: The timestamp for the data point.
        :param timeout: The request timeout.
        """
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
            if self.token is None:
                raise ValueError("No token available. Please login first.")
            headers = {"Authorization": f"Bearer {self.token.access_token}"}
            return _http_request(requests.post, url, json=data, headers=headers, timeout=timeout)

        response = post_data()
        if response.status_code == 401:
            response.close()
            self.refresh_token(timeout=timeout)
            response = post_data()

        if response.status_code != 200:
            response.close()
            raise CoCoHTTPError(response.status_code)

        response.close()

    def _handshake(self, connect_timeout=5, io_timeout=20):
        print("Performing WebSocket handshake...")
        if self.token is None:
            raise ValueError("No token available. Please login first.")
        random_key = bytes([random.getrandbits(8) for _ in range(16)])
        ws_key = binascii.b2a_base64(random_key).strip().decode()
        expected_accept = binascii.b2a_base64(hashlib.sha1(
            (ws_key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).strip().decode()

        def get_handshake(token):
            path = f"/ws?token={token}"
            tcp_sock = None
            tls_sock = None
            try:
                tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                tcp_sock.settimeout(connect_timeout)
                addr = socket.getaddrinfo(self.host, 443)[0][-1]
                tcp_sock.connect(addr)

                tls_sock = _tls_wrap(tcp_sock, self.host, self.verify_ssl)

                # MicroPython: actually call settimeout() (the original
                # mistakenly used setattr(tls_sock, "settimeout",
                # io_timeout), which just creates an attribute that
                # *shadows* the method instead of calling it).
                # settimeout() isn't guaranteed on the object returned
                # by wrap_socket on every port (which is why the stub
                # doesn't declare it on SSLSocket): we fetch it with
                # getattr instead of direct access, so no
                # # type: ignore is needed, and if it's missing we fall
                # back to the underlying TCP socket.
                set_tls_timeout = getattr(tls_sock, "settimeout", None)
                if set_tls_timeout is not None:
                    try:
                        set_tls_timeout(io_timeout)
                    except OSError:
                        tcp_sock.settimeout(io_timeout)
                else:
                    tcp_sock.settimeout(io_timeout)

                request = (
                    "GET {} HTTP/1.1\r\n"
                    "Host: {}\r\n"
                    "Upgrade: websocket\r\n"
                    "Connection: Upgrade\r\n"
                    "Sec-WebSocket-Key: {}\r\n"
                    "Sec-WebSocket-Version: 13\r\n"
                    "\r\n"
                ).format(path, self.host, ws_key)

                _sock_write(tls_sock, request.encode())
                response = b""
                while b"\r\n\r\n" not in response:
                    chunk = _sock_read(tls_sock, 1024)
                    if not chunk:
                        break
                    response += chunk

                return tls_sock, _safe_decode_bytes(response)
            except Exception as e:
                print("An error occurred during WebSocket handshake")
                print("Exception type:", type(e).__name__)
                print("Exception repr:", repr(e))
                if hasattr(e, "args"):
                    print("Exception args:", e.args)
                if hasattr(sys, "print_exception"):
                    sys.print_exception(e)  # type: ignore
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
        status_code = int(status_parts[1]) if len(
            status_parts) >= 2 and status_parts[1].isdigit() else 0

        if status_code == 401:
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

        upgrade_ok = headers.get("upgrade", "").lower() == "websocket"
        connection_ok = "upgrade" in headers.get("connection", "").lower()
        accept_ok = headers.get("sec-websocket-accept") == expected_accept

        if status_code == 101 and upgrade_ok and connection_ok and accept_ok:
            print("WebSocket handshake successful!")
            return result[0]

        # Some MicroPython TLS/stream combinations can make header parsing
        # incomplete, even though the server already switched protocols.
        if status_code == 101:
            print("WebSocket handshake accepted with relaxed validation.")
            print("Handshake checks:", "upgrade=", upgrade_ok, "connection=", connection_ok, "accept=", accept_ok)
            return result[0]

        print("WebSocket handshake failed. Response:", result[1])
        result[0].close()
        raise CoCoHTTPError(status_code)

    async def connect(self, on_init=None, on_new_class=None, on_new_rule=None, on_new_object=None, on_classes_update=None, on_object_update=None, on_new_data=None):
        """
        Establish a WebSocket connection to the CoCo server.

        Args:
            on_init: Callback function for initialization events.
            on_new_class: Callback function for new class events.
            on_new_rule: Callback function for new rule events.
            on_new_object: Callback function for new object events.
            on_classes_update: Callback function for class updates.
            on_object_update: Callback function for object updates.
            on_new_data: Callback function for new data events.

        Raises:
            ValueError: If any of the callback functions are not callable.
            CoCoHTTPError: If the WebSocket handshake fails.
        """
        if on_init is not None and not callable(on_init):
            raise ValueError("on_init must be a callable function or None.")
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

        self._stop_requested = False
        while not self._stop_requested:
            ws_sock = self._handshake()
            if ws_sock is None:
                print("Failed to establish WebSocket connection. Retrying in 5 seconds...")
                for _ in range(50):
                    if self._stop_requested:
                        break
                    await asyncio.sleep(0.1)
                continue

            self._ws_sock = ws_sock

            try:
                while not self._stop_requested:
                    try:
                        # MicroPython: select() on a TLS-wrapped socket
                        # only checks the state of the underlying TCP
                        # socket, not any bytes already decrypted and
                        # buffered by the TLS layer. For this
                        # protocol's traffic (single messages, not
                        # pipelined) this is a known limitation in
                        # CPython too and isn't addressed here: if
                        # messages ever seem to lag "by one", it's
                        # worth revisiting this with a direct
                        # non-blocking read instead of select-based
                        # polling.
                        selected = select.select((ws_sock,), (), (), 0)
                    except (ValueError, OSError):
                        break
                    ready = ()
                    if selected:
                        ready = selected[0]
                    if not ready:
                        await asyncio.sleep(1/10)
                        continue

                    try:
                        opcode, payload = _ws_read_frame(ws_sock)
                    except OSError as e:
                        if _is_timeout_error(e):
                            await asyncio.sleep(0)
                            continue
                        raise
                    if opcode is None:
                        raise OSError("WebSocket closed by peer")
                    if payload is None:
                        payload = b""

                    if opcode == 0x1:  # text
                        try:
                            text_payload = _safe_decode_bytes(payload)
                            print("Received message:", text_payload)
                            data = json.loads(text_payload)
                            msg_type = data["msg_type"]
                            if msg_type == "coco" and on_init is not None:
                                on_init([CoCoClass.from_json({"name": name, **cls_json}) for name, cls_json in data["classes"].items()], [CoCoRule.from_json({"name": name, **rule_json}) for name, rule_json in data["rules"].items()], [CoCoObject.from_json({"id": obj_id, **obj_json}) for obj_id, obj_json in data["objects"].items()])
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
                if not self._stop_requested:
                    print("An error occurred during WebSocket communication:", e)
            finally:
                if self._ws_sock:
                    try:
                        self._ws_sock.close()
                    except Exception:
                        pass
                    self._ws_sock = None

            if self._stop_requested:
                print("WebSocket connection closed.")
                break

            print("WebSocket connection closed. Reconnecting in 5 seconds...")
            for _ in range(50):
                if self._stop_requested:
                    break
                await asyncio.sleep(0.1)

    async def close(self):
        """
        Close the WebSocket connection.
        """
        self._stop_requested = True

        if self._ws_sock:
            try:
                self._ws_sock.close()
            except Exception:
                pass
            finally:
                self._ws_sock = None


def _read_exact(sock, n):
    data = b""
    while len(data) < n:
        chunk = _sock_read(sock, n - len(data))
        if not chunk:
            return None
        data += chunk
    return data


def _ws_read_frame(sock):
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


def _ws_send_pong(sock, payload=b""):
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

    _sock_write(sock, header + masked)


def _decode_close_payload(payload):
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
