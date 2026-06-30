import json
import threading
from typing import Any, Callable
from coco.model import CocoClass, CocoObject
import requests
import websocket
from datetime import datetime


def urlencode(params: dict[str, Any]) -> str:
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


def get_classes(host: str, token: str) -> list | None:
    url = "https://{}/classes".format(host)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Classes retrieved successfully!")
            data = response.json()
            response.close()
            return data
        else:
            print("Failed to retrieve classes:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving classes:", e)
        return None


def get_class(host: str, token: str, class_id: str) -> dict | None:
    url = "https://{}/classes/{}".format(host, class_id)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Class retrieved successfully!")
            data = response.json()
            response.close()
            return data
        else:
            print("Failed to retrieve class:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving class:", e)
        return None


def get_objects(host: str, token: str, classes=None, filters=None) -> list | None:
    params: dict[str, Any] = {}
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
            return data
        else:
            print("Failed to retrieve objects:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving objects:", e)
        return None


def get_object(host: str, token: str, object_id: str) -> dict | None:
    url = "https://{}/objects/{}".format(host, object_id)
    headers = {"Authorization": "Bearer {}".format(token)}

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("Object retrieved successfully!")
            data = response.json()
            response.close()
            return data
        else:
            print("Failed to retrieve object:", response.status_code)
            response.close()
            return None
    except Exception as e:
        print("Error retrieving object:", e)
        return None


def add_data(host: str, token: str, object_id: str, data: dict) -> bool:
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


OnNewClassCallback = Callable[[CocoClass], None]
OnNewObjectCallback = Callable[[CocoObject], None]
OnClassesUpdateCallback = Callable[[str, set[str]], None]
OnPropertiesUpdateCallback = Callable[[str, dict[str, Any]], None]
OnNewDataCallback = Callable[[str, dict[str, tuple[Any, datetime]]], None]


def connect(host: str, token: str, on_new_class: OnNewClassCallback | None = None, on_new_object: OnNewObjectCallback | None = None, on_classes_update: OnClassesUpdateCallback | None = None, on_object_update: OnPropertiesUpdateCallback | None = None, on_new_data: OnNewDataCallback | None = None) -> websocket.WebSocketApp:

    def on_open(ws: websocket.WebSocketApp) -> None:
        print("WebSocket connection opened")

    def on_message(ws: websocket.WebSocketApp, message: Any) -> None:
        print("Received raw message:", message)
        try:
            data: dict[str, Any] = json.loads(message)

            if on_new_class and data.get("msg_type") == "new-class":
                on_new_class(CocoClass.from_json(data))

            if on_new_object and data.get("msg_type") == "new-object":
                on_new_object(CocoObject.from_json(data))

            if on_classes_update and data.get("msg_type") == "classes-updated":
                on_classes_update(data.get("object_id"), data.get("classes"))

            if on_object_update and data.get("msg_type") == "properties-updated":
                on_object_update(data.get("object_id"), data.get("properties"))

            if on_new_data and data.get("msg_type") == "values-added":
                on_new_data(data.get("object_id"), data.get("values"))

        except json.JSONDecodeError:
            print("Failed to decode JSON message:", message)
        except Exception as e:
            print("Error processing message:", e)

    def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
        print("WebSocket error:", error)

    def on_close(ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
        print("WebSocket closed:", close_status_code, close_msg)

    url = "wss://{}/ws?token={}".format(host, token)
    ws = websocket.WebSocketApp(url,
                                on_open=on_open,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)

    wst = threading.Thread(target=ws.run_forever, daemon=True)
    wst.start()

    return ws
