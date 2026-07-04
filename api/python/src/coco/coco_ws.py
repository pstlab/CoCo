import json
import websocket
import threading
from coco.model import CocoClass, CocoObject
from typing import Any, Callable

OnNewClassCallback = Callable[[CocoClass], None]
OnNewObjectCallback = Callable[[CocoObject], None]
OnClassesUpdateCallback = Callable[[str, set[str]], None]
OnPropertiesUpdateCallback = Callable[[
    str, dict[str, str | int | float | bool]], None]
OnNewDataCallback = Callable[[
    str, dict[str, tuple[str | int | float | bool, str]]], None]


def connect(host: str, token: str, on_new_class: OnNewClassCallback | None = None, on_new_object: OnNewObjectCallback | None = None, on_classes_update: OnClassesUpdateCallback | None = None, on_object_update: OnPropertiesUpdateCallback | None = None, on_new_data: OnNewDataCallback | None = None) -> websocket.WebSocketApp:
    stop_event = threading.Event()
    current_ws: dict[str, websocket.WebSocketApp | None] = {"ws": None}

    def build_ws() -> websocket.WebSocketApp:
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
                    object_id = data.get("object_id")
                    classes = data.get("classes")
                    if isinstance(object_id, str) and isinstance(classes, set):
                        on_classes_update(object_id, classes)

                if on_object_update and data.get("msg_type") == "properties-updated":
                    object_id = data.get("object_id")
                    properties = data.get("properties")
                    if isinstance(object_id, str) and isinstance(properties, dict):
                        on_object_update(object_id, properties)

                if on_new_data and data.get("msg_type") == "values-added":
                    object_id = data.get("object_id")
                    values = data.get("values")
                    if isinstance(object_id, str) and isinstance(values, dict):
                        on_new_data(object_id, values)

            except json.JSONDecodeError:
                print("Failed to decode JSON message:", message)
            except Exception as e:
                print("Error processing message:", e)

        def on_error(ws: websocket.WebSocketApp, error: Exception) -> None:
            print("WebSocket error:", error)

        def on_close(ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
            print("WebSocket closed:", close_status_code, close_msg)

        ws = websocket.WebSocketApp(
            "wss://{}/ws?token={}".format(host, token),
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )

        original_close = ws.close

        def close(*args: Any, **kwargs: Any) -> None:
            stop_event.set()
            active_ws = current_ws.get("ws")
            if active_ws is not None and active_ws is not ws:
                active_ws.close(*args, **kwargs)
            original_close(*args, **kwargs)

        ws.close = close
        current_ws["ws"] = ws
        return ws

    def run() -> None:
        reconnect_delay = 1.0

        while not stop_event.is_set():
            ws = build_ws()
            ws.run_forever()

            if stop_event.is_set() or not ws.keep_running:
                break

            print("WebSocket disconnected, reconnecting in {} seconds...".format(
                reconnect_delay))
            stop_event.wait(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 30.0)

    ws = build_ws()
    wst = threading.Thread(target=run, daemon=True)
    wst.start()

    return ws
