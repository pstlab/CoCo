import os
import pytest
from dotenv import load_dotenv
from coco import CoCo
import asyncio

load_dotenv()


@pytest.fixture
def credentials():
    host = os.getenv("COCO_TEST_HOST")
    username = os.getenv("COCO_TEST_USERNAME")
    password = os.getenv("COCO_TEST_PASSWORD")

    if not all([host, username, password]):
        pytest.fail("Variabili d'ambiente mancanti nel file .env")

    return {
        "host": host,
        "username": username,
        "password": password
    }


@pytest.fixture
def coco(credentials):
    return CoCo(host=credentials["host"], verify_ssl=False)


def test_login(coco, credentials):
    coco.login(credentials["username"], credentials["password"])
    assert coco.token is not None
    assert coco.token.access_token is not None


def test_get_classes(coco, credentials):
    coco.login(credentials["username"], credentials["password"])
    classes = coco.get_classes()
    assert isinstance(classes, list)


async def test_ws(coco, credentials):
    coco.login(credentials["username"], credentials["password"])
    connect_task = asyncio.create_task(coco.connect())
    await asyncio.sleep(5)
    await coco.close()
    await connect_task
