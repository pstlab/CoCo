from model import AuthTokens, CoCoHTTPError, CoCoClass, CoCoObject
import requests


def urlencode(params: dict[str, str | int | float | bool]) -> str:
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append("{}={}".format(k, v))
    return "&".join(parts)


def login(host: str, username: str, password: str, timeout=5) -> AuthTokens:
    url = "https://{}/login".format(host)
    payload = {"username": username, "password": password}

    response = requests.post(url, json=payload, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Login successful!")
    data = response.json()
    response.close()
    return AuthTokens.from_json(data)


def refresh_token(host: str, token: AuthTokens, timeout=5) -> AuthTokens:
    url = "https://{}/refresh_token".format(host)
    payload = {"refresh_token": token.refresh_token}

    response = requests.post(url, json=payload, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Token refreshed successfully!")
    data = response.json()
    response.close()
    return AuthTokens.from_json(data)


def get_classes(host: str, token: AuthTokens, timeout=5) -> list[CoCoClass]:
    url = "https://{}/classes".format(host)
    headers = {"Authorization": "Bearer {}".format(token.access_token)}

    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Classes retrieved successfully!")
    data = response.json()
    response.close()
    return [CoCoClass.from_json(cls_data) for cls_data in data]


def get_class(host: str, token: AuthTokens, class_id: str, timeout=5) -> CoCoClass:
    url = "https://{}/classes/{}".format(host, class_id)
    headers = {"Authorization": "Bearer {}".format(token.access_token)}

    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Class retrieved successfully!")
    data = response.json()
    response.close()
    return CoCoClass.from_json(data)


def create_class(host: str, token: AuthTokens, cls: CoCoClass, timeout=5):
    url = "https://{}/classes".format(host)
    headers = {"Authorization": "Bearer {}".format(token.access_token)}
    payload = cls.to_json()

    response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Class created successfully!")
    response.close()


def get_objects(host: str, token: AuthTokens, classes=None, filters=None, timeout=5) -> list[CoCoObject]:
    params: dict[str, str | int | float | bool] = {}
    if classes:
        params["classes"] = ",".join(classes)
    if filters:
        params.update(filters)

    query = "?" + urlencode(params) if params else ""
    url = "https://{}/objects{}".format(host, query)

    headers = {"Authorization": "Bearer {}".format(token.access_token)}

    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Objects retrieved successfully!")
    data = response.json()
    response.close()
    return [CoCoObject.from_json(obj_data) for obj_data in data]


def get_object(host: str, token: AuthTokens, object_id: str, timeout=5) -> CoCoObject:
    url = "https://{}/objects/{}".format(host, object_id)
    headers = {"Authorization": "Bearer {}".format(token.access_token)}

    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Object retrieved successfully!")
    data = response.json()
    response.close()
    return CoCoObject.from_json(data)


def create_object(host: str, token: AuthTokens, obj: CoCoObject, timeout=5) -> str:
    url = "https://{}/objects".format(host)
    headers = {"Authorization": "Bearer {}".format(token.access_token)}
    payload = obj.to_json()

    response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Object created successfully!")
    data = response.text
    response.close()
    return data


def get_data(host: str, token: AuthTokens, object_id: str, start: str | None = None, end: str | None = None, timeout=10) -> dict[str, list[tuple[str | int | float | bool, str]]] | None:
    params: dict[str, str | int | float | bool] = {}
    if start:
        params["start"] = start
    if end:
        params["end"] = end

    query = "?" + urlencode(params) if params else ""
    url = "https://{}/objects/{}/data{}".format(host, object_id, query)

    headers = {"Authorization": "Bearer {}".format(token.access_token)}

    response = requests.get(url, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Data retrieved successfully!")
    data = response.json()
    response.close()
    return data


def add_data(host: str, token: AuthTokens, object_id: str, data: dict[str, str | int | float | bool], timeout=5):
    url = "https://{}/objects/{}/data".format(host, object_id)
    headers = {"Authorization": "Bearer {}".format(token.access_token)}

    response = requests.post(url, json=data, headers=headers, timeout=timeout)
    if response.status_code != 200:
        response.close()
        raise CoCoHTTPError(response.status_code)

    print("Data added successfully!")
    response.close()
