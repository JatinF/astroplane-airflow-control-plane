import pytest
from fastapi.testclient import TestClient

from astroplane.api.app import create_app
from astroplane.governance.auth import PlatformStore


@pytest.fixture
def store() -> PlatformStore:
    return PlatformStore()


@pytest.fixture
def client(store: PlatformStore) -> TestClient:
    return TestClient(create_app(store))


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
