from model import CocoClass, CocoObject
import requests


def urlencode(params: dict[str, str | int | float | bool]) -> str:
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append("{}={}".format(k, v))
    return "&".join(parts)


def login(host: str, username: str, password: str) -> dict | None:
    url = "https://{}/login".format(host)
    payload = {"username": username, "password": password}

    try:
        response = requests.post(url, json=payload)
        if response.status_code == 200:
            print("Login successful!")
            data = response.json()
            response.close()
            return data
        else:
            print("Login failed:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error during login:", e)
        return None


def get_classes(host: str, token: str) -> list[CocoClass] | None:
    url = "https://{}/classes".format(host)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Classes retrieved successfully!")
            data = response.json()
            response.close()
            return [CocoClass.from_json(cls_data) for cls_data in data]
        else:
            print("Failed to retrieve classes:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving classes:", e)
        return None


def get_class(host: str, token: str, class_id: str) -> CocoClass | None:
    url = "https://{}/classes/{}".format(host, class_id)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Class retrieved successfully!")
            data = response.json()
            response.close()
            return CocoClass.from_json(data)
        else:
            print("Failed to retrieve class:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving class:", e)
        return None


def create_class(host: str, token: str, cls: CocoClass) -> bool:
    url = "https://{}/classes".format(host)
    headers = {"Authorization": "Bearer {}".format(token)}
    payload = cls.to_json()

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            print("Class created successfully!")
            response.close()
            return True
        else:
            print("Failed to create class:", response.status_code)
            response.close()
            return False
    except Exception as e:
        print("Error creating class:", e)
        return False


def get_objects(host: str, token: str, classes=None, filters=None) -> list[CocoObject] | None:
    params: dict[str, str | int | float | bool] = {}
    if classes:
        params["classes"] = ",".join(classes)
    if filters:
        params.update(filters)

    query = "?" + urlencode(params) if params else ""
    url = "https://{}/objects{}".format(host, query)

    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Objects retrieved successfully!")
            data = response.json()
            response.close()
            return [CocoObject.from_json(obj_data) for obj_data in data]
        else:
            print("Failed to retrieve objects:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving objects:", e)
        return None


def get_object(host: str, token: str, object_id: str) -> CocoObject | None:
    url = "https://{}/objects/{}".format(host, object_id)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Object retrieved successfully!")
            data = response.json()
            response.close()
            return CocoObject.from_json(data)
        else:
            print("Failed to retrieve object:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving object:", e)
        return None


def create_object(host: str, token: str, obj: CocoObject) -> str | None:
    url = "https://{}/objects".format(host)
    headers = {"Authorization": "Bearer {}".format(token)}
    payload = obj.to_json()

    try:
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            print("Object created successfully!")
            response.close()
            return response.json().get("id")
        else:
            print("Failed to create object:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error creating object:", e)
        return None


def add_data(host: str, token: str, object_id: str, data: dict[str, str | int | float | bool]) -> bool:
    url = "https://{}/objects/{}/data".format(host, object_id)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code == 200:
            print("Data added successfully!")
            response.close()
            return True
        else:
            print("Failed to add data:", response.status_code)
            response.close()
            return False
    except Exception as e:
        print("Error adding data:", e)
        return False
