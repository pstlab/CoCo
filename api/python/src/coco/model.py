class AuthTokens:
    def __init__(self, access_token, refresh_token, token_type):
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.token_type = token_type

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


class Property:
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
        super().__init__()
        self.default = default
        self.description = description

    def to_json(self):
        json_data = {"type": "bool"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class IntProperty(Property):
    def __init__(self, min=None, max=None, default=None, description=None):
        super().__init__()
        self.min = min
        self.max = max
        self.default = default
        self.description = description

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
        super().__init__()
        self.min = min
        self.max = max
        self.default = default
        self.description = description

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
        super().__init__()
        self.default = default
        self.description = description

    def to_json(self):
        json_data = {"type": "string"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class SymbolProperty(Property):
    def __init__(self, allowed_values=None, default=None, description=None):
        super().__init__()
        self.allowed_values = allowed_values
        self.default = default
        self.description = description

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
        super().__init__()
        self.default = default
        self.classes = classes
        self.description = description

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
        super().__init__()
        self.default = default
        self.description = description

    def to_json(self):
        json_data = {"type": "bool-array"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class IntArrayProperty(Property):
    def __init__(self, min=None, max=None, default=None, description=None):
        super().__init__()
        self.min = min
        self.max = max
        self.default = default
        self.description = description

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
        super().__init__()
        self.min = min
        self.max = max
        self.default = default
        self.description = description

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
        super().__init__()
        self.default = default
        self.description = description

    def to_json(self):
        json_data = {"type": "string-array"}
        if self.default is not None:
            json_data["default"] = self.default
        if self.description is not None:
            json_data["description"] = self.description
        return json_data


class SymbolArrayProperty(Property):
    def __init__(self, allowed_values=None, default=None, description=None):
        super().__init__()
        self.allowed_values = allowed_values
        self.default = default
        self.description = description

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
        super().__init__()
        self.default = default
        self.classes = classes
        self.description = description

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
            json_data["values"] = {key: {
                "value": value[0], "timestamp": value[1]} for key, value in self.values.items()}
        return json_data
