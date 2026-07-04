from typing import Any
from datetime import datetime


class Property:
    @staticmethod
    def from_json(json_data: dict) -> Property:
        property_type = json_data.get("type")
        if property_type == "bool":
            return BoolProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "int":
            return IntProperty(
                default=json_data.get("default"),
                min=json_data.get("min"),
                max=json_data.get("max"),
                description=json_data.get("description"),
            )
        elif property_type == "float":
            return FloatProperty(
                default=json_data.get("default"),
                min=json_data.get("min"),
                max=json_data.get("max"),
                description=json_data.get("description"),
            )
        elif property_type == "string":
            return StringProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "symbol":
            return SymbolProperty(
                default=json_data.get("default"),
                allowed_values=json_data.get("allowed_values"),
                description=json_data.get("description"),
            )
        elif property_type == "object":
            return ObjectProperty(
                default=json_data.get("default"),
                classes=json_data.get("classes"),
                description=json_data.get("description"),
            )
        elif property_type == "bool-array":
            return BoolArrayProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "int-array":
            return IntArrayProperty(
                default=json_data.get("default"),
                description=json_data.get("description"),
            )
        elif property_type == "float-array":
            return FloatArrayProperty(
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
                default=json_data.get("default"),
                allowed_values=json_data.get("allowed_values"),
                description=json_data.get("description"),
            )
        elif property_type == "object-array":
            return ObjectArrayProperty(
                default=json_data.get("default"),
                classes=json_data.get("classes"),
                description=json_data.get("description"),
            )
        else:
            raise ValueError(f"Unknown property type: {property_type}")


class BoolProperty(Property):
    def __init__(self, default: bool | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.description = description


class IntProperty(Property):
    def __init__(self, default: int | None = None, min: int | None = None, max: int | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.min = min
        self.max = max
        self.description = description


class FloatProperty(Property):
    def __init__(self, default: float | None = None, min: float | None = None, max: float | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.min = min
        self.max = max
        self.description = description


class StringProperty(Property):
    def __init__(self, default: str | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.description = description


class SymbolProperty(Property):
    def __init__(self, default: str | None = None, allowed_values: list[str] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.allowed_values = allowed_values
        self.description = description


class ObjectProperty(Property):
    def __init__(self, default: str | None = None, classes: list[str] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.classes = classes
        self.description = description


class BoolArrayProperty(Property):
    def __init__(self, default: list[bool] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.description = description


class IntArrayProperty(Property):
    def __init__(self, default: list[int] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.description = description


class FloatArrayProperty(Property):
    def __init__(self, default: list[float] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.description = description


class StringArrayProperty(Property):
    def __init__(self, default: list[str] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.description = description


class SymbolArrayProperty(Property):
    def __init__(self, default: list[str] | None = None, allowed_values: list[str] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.allowed_values = allowed_values
        self.description = description


class ObjectArrayProperty(Property):
    def __init__(self, default: list[str] | None = None, classes: list[str] | None = None, description: str | None = None):
        super().__init__()
        self.default = default
        self.classes = classes
        self.description = description


class CocoClass:
    def __init__(self, name: str, static_properties: dict[str, Property], dynamic_properties: dict[str, Property]):
        self.name = name
        self.static_properties = static_properties
        self.dynamic_properties = dynamic_properties

    @staticmethod
    def from_json(json_data: dict) -> CocoClass:
        name = json_data.get("name")
        if not isinstance(name, str):
            raise ValueError("Invalid class name in JSON data")
        static_properties_json = json_data.get("static_properties", {})
        dynamic_properties_json = json_data.get("dynamic_properties", {})

        static_properties = {key: Property.from_json(
            value) for key, value in static_properties_json.items()}
        dynamic_properties = {key: Property.from_json(
            value) for key, value in dynamic_properties_json.items()}

        return CocoClass(name=name, static_properties=static_properties, dynamic_properties=dynamic_properties)


class CocoObject:
    def __init__(self, id: str, classes: list[str], properties: dict[str, Any], values: dict[str, tuple[Any, datetime]]):
        super().__init__()
        self.id = id
        self.classes = classes
        self.properties = properties
        self.values = values

    @staticmethod
    def from_json(json_data: dict) -> CocoObject:
        id = json_data.get("id")
        if not isinstance(id, str):
            raise ValueError("Invalid object ID in JSON data")
        classes = json_data.get("classes", [])
        properties = json_data.get("properties", {})
        values_json = json_data.get("values", {})

        values = {}
        for key, value in values_json.items():
            if isinstance(value, dict) and "value" in value and "timestamp" in value:
                timestamp_str = value["timestamp"]
                try:
                    timestamp = datetime.fromisoformat(timestamp_str)
                except ValueError:
                    timestamp = None
                values[key] = (value["value"], timestamp)

        return CocoObject(id=id, classes=classes, properties=properties, values=values)
