import requests


def urlencode(params):
    if not params:
        return ""
    parts = []
    for k, v in params.items():
        parts.append("{}={}".format(k, v))
    return "&".join(parts)


def login(host: str, username: str, password: str):
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


def get_classes(host: str, token: str):
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


def get_objects(host: str, token: str, classes=None, filters=None):
    params = {}
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


def add_data(host: str, token: str, object_id: str, data: dict):
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
