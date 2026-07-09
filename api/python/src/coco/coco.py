from model import *
import requests


def urlencode(params: dict[str, Value]) -> str:
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append("{}={}".format(k, v))
    return "&".join(parts)


class CoCo:
    def __init__(self, host: str):
        self.host = host
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
        query_string = urlencode(params)
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
        query_string = urlencode(params)
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
        query_string = urlencode(params)
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
