from fastapi.testclient import TestClient

from astroplane.api.app import create_app
from astroplane.governance.auth import PlatformStore
from astroplane.workflow import install, open_store, upgrade


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _principal(store: PlatformStore) -> dict:
    principal = store.principal("dev-operator-a")
    assert principal is not None
    return principal


def test_sqlite_upgrade_survives_a_new_store(tmp_path):
    path = tmp_path / "state.sqlite"
    store = PlatformStore(path=path)
    installed = install(store, _principal(store), "syn-cluster-01")
    assert installed["cluster"]["finalizers"] == ["astroplane.example/finalizer"]
    assert installed["cluster"]["release"] == "r1"
    store.close()

    revived = open_store(path)
    upgraded = upgrade(revived, _principal(revived), "syn-cluster-01", "r2")
    image = upgraded["cluster"]["deployments"][0]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert image == "registry.local/astroplane/airflow:2.11.0-r2"
    assert upgraded["cluster"]["applied"] is False
    assert upgraded["cluster"]["phase"] == "Planned"
    assert upgraded["cluster"]["deployments"][0]["status"]["phase"] == "Pending"
    metadata = upgraded["cluster"]["deployments"][4]["spec"]["template"]["spec"]["containers"][0]["image"]
    assert metadata == "registry.local/astroplane/postgres:16"
    revived.close()

    checked = open_store(path)
    assert checked.clusters["syn-cluster-01"]["observed_release"] == "r2"
    checked.close()


def test_recover_finishes_a_deletion_saved_before_stop(tmp_path):
    path = tmp_path / "state.sqlite"
    store = PlatformStore(path=path)
    install(store, _principal(store), "syn-cluster-01")
    store.clusters["syn-cluster-01"]["deletion_timestamp"] = "2026-10-21T00:00:00+00:00"
    store.clusters["syn-cluster-01"]["phase"] = "Deleting"
    store.persist()
    store.close()

    revived = open_store(path)
    assert "syn-cluster-01" not in revived.clusters
    assert revived.audits[-1]["action"] == "recover"
    assert revived.audits[-1]["finalizer_completed"] is True
    assert revived.metrics["components_desired"] == 0
    assert revived.metrics["components_running"] == 0
    revived.close()


def test_api_upgrade_and_delete(tmp_path):
    store = PlatformStore(path=tmp_path / "state.sqlite")
    client = TestClient(create_app(store))
    headers = bearer("dev-operator-a")
    viewer = bearer("dev-viewer-a")
    other = bearer("dev-operator-b")
    installed = client.post("/v1/clusters/syn-cluster-01/install", headers=headers, json={})
    assert installed.status_code == 200
    denied = client.post(
        "/v1/clusters/syn-cluster-01/upgrade",
        headers=viewer,
        json={"release": "r2"},
    )
    assert denied.status_code == 403
    hidden = client.delete("/v1/clusters/syn-cluster-01", headers=other)
    assert hidden.status_code == 404
    bad = client.post("/v1/clusters/syn-cluster-01/upgrade", headers=headers, json={"release": "latest"})
    assert bad.status_code == 400
    upgraded = client.post("/v1/clusters/syn-cluster-01/upgrade", headers=headers, json={"release": "r2"})
    assert upgraded.status_code == 200
    assert upgraded.json()["applied"] is False
    assert upgraded.json()["release"] == "r2"
    assert upgraded.json()["components_running"] == 0
    removed = client.delete("/v1/clusters/syn-cluster-01", headers=headers)
    assert removed.status_code == 200
    assert removed.json()["phase"] == "Deleted"
    assert removed.json()["finalizer_completed"] is True
    assert client.get("/v1/clusters/syn-cluster-01", headers=headers).status_code == 404
    assert client.delete("/v1/clusters/syn-cluster-01", headers=headers).status_code == 404
    egress = client.post("/v1/clusters/syn-cluster-egress/install", headers=headers, json={})
    assert egress.json()["phase"] == "Denied"
    blocked = client.post("/v1/clusters/syn-cluster-egress/upgrade", headers=headers, json={"release": "r2"})
    assert blocked.status_code == 400
    store.close()
