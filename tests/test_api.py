def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


OPERATOR_A = bearer("dev-operator-a")
VIEWER_A = bearer("dev-viewer-a")
OPERATOR_B = bearer("dev-operator-b")


def _install(client, cluster_id: str, headers: dict):
    response = client.post(f"/v1/clusters/{cluster_id}/install", headers=headers, json={})
    assert response.status_code == 200, response.text
    return response.json()


def test_healthz_reports_the_stand_in():
    from astroplane.api.app import create_app
    from fastapi.testclient import TestClient

    body = TestClient(create_app()).get("/healthz").json()
    assert body["status"] == "ok"
    assert body["mode"] == "control-plane-stand-in"
    assert body["airflow_distribution"] == "not-installed"
    assert body["kube_apiserver"] == "not-run"
    assert body["real_cluster"] == 0
    assert body["components_running"] == 0


def test_missing_or_unknown_bearer_is_rejected(client):
    assert client.post("/v1/clusters/syn-cluster-01/install", json={}).status_code == 401
    assert client.get("/api/v1/health", headers=bearer("nope")).status_code == 401


def test_viewer_cannot_install(client):
    response = client.post("/v1/clusters/syn-cluster-01/install", headers=VIEWER_A, json={})
    assert response.status_code == 403


def test_extra_install_field_is_rejected(client):
    response = client.post(
        "/v1/clusters/syn-cluster-01/install",
        headers=OPERATOR_A,
        json={"note": "skip"},
    )
    assert response.status_code == 422


def test_planned_install_stores_six_pending_deployments(client):
    body = _install(client, "syn-cluster-01", OPERATOR_A)
    assert body["phase"] == "Planned"
    assert body["applied"] is False
    assert body["deployment_count"] == 6
    assert body["components_desired"] == 6
    assert body["components_running"] == 0
    assert body["egress_allowed"] is True
    record = client.get("/v1/clusters/syn-cluster-01", headers=OPERATOR_A).json()
    names = [item["metadata"]["name"] for item in record["deployments"]]
    assert names == [
        "syn-cluster-01-scheduler",
        "syn-cluster-01-webserver",
        "syn-cluster-01-dag-processor",
        "syn-cluster-01-triggerer",
        "syn-cluster-01-metadata",
        "syn-cluster-01-statsd",
    ]
    assert {item["status"]["phase"] for item in record["deployments"]} == {"Pending"}
    assert all(item["status"]["applied"] is False for item in record["deployments"])
    assert all(item["status"]["reason"] == "kubelet not-run" for item in record["deployments"])
    assert all(item["status"]["readyReplicas"] == 0 for item in record["deployments"])
    assert all(item["status"]["image_digest"] == "not-pulled" for item in record["deployments"])
    assert all(
        item["spec"]["template"]["spec"]["containers"][0]["image"].startswith("registry.local/astroplane/")
        for item in record["deployments"]
    )
    assert all(
        item["spec"]["template"]["spec"]["containers"][0]["imagePullPolicy"] == "Never"
        for item in record["deployments"]
    )
    conditions = {item["type"]: item for item in record["conditions"]}
    assert conditions["Egress"]["status"] == "True"
    assert conditions["PackScanned"]["status"] == "True"
    assert conditions["Ready"] == {"type": "Ready", "status": "False", "reason": "kube-apiserver not-run"}
    again = _install(client, "syn-cluster-01", OPERATOR_A)
    assert again["audit_id"] == "SYN-AUD-0002"
    assert again["deployment_count"] == 6
    reread = client.get("/v1/clusters/syn-cluster-01", headers=OPERATOR_A).json()
    assert len(reread["deployments"]) == 6


def test_denied_install_stores_no_deployments(client):
    body = _install(client, "syn-cluster-egress", OPERATOR_A)
    assert body["phase"] == "Denied"
    assert body["deployment_count"] == 0
    assert body["components_desired"] == 0
    assert body["components_running"] == 0
    assert body["reasons"] == ["image-registry:airflow"]
    record = client.get("/v1/clusters/syn-cluster-egress", headers=OPERATOR_A).json()
    assert record["deployments"] == []
    ready = next(item for item in record["conditions"] if item["type"] == "Egress")
    assert ready["status"] == "False"
    assert "image-registry:airflow" in ready["reason"]


def test_cross_tenant_cluster_is_hidden(client):
    _install(client, "syn-cluster-01", OPERATOR_A)
    assert client.get("/v1/clusters/syn-cluster-01", headers=OPERATOR_B).status_code == 404
    hidden = client.post("/v1/clusters/syn-cluster-01/install", headers=OPERATOR_B, json={})
    assert hidden.status_code == 404
    audit = client.get("/v1/audits/SYN-AUD-0001", headers=OPERATOR_B)
    assert audit.status_code == 404
    visible = client.get("/v1/audits/SYN-AUD-0001", headers=VIEWER_A)
    assert visible.status_code == 200
    assert visible.json()["phase"] == "Planned"


def test_airflow_health_and_version_stay_uninstalled(client):
    health = client.get("/api/v1/health", headers=OPERATOR_A).json()
    for name in ("metadatabase", "scheduler", "triggerer", "dag_processor"):
        assert health[name]["status"] == "unhealthy"
    assert health["scheduler"]["latest_scheduler_heartbeat"] is None
    assert health["triggerer"]["latest_triggerer_heartbeat"] is None
    assert health["dag_processor"]["latest_dag_processor_heartbeat"] is None
    assert client.get("/api/v1/version", headers=OPERATOR_A).json() == {
        "version": "2.11.0",
        "git_version": None,
    }


def test_dag_collection_and_refused_run(client):
    _install(client, "syn-cluster-01", OPERATOR_A)
    missing = client.get("/api/v1/dags", headers=OPERATOR_A)
    assert missing.status_code == 400
    listed = client.get(
        "/api/v1/dags",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
    ).json()
    assert listed["total_entries"] == 1
    dag = listed["dags"][0]
    assert dag["dag_id"] == "SYN-DAG-ETL"
    assert dag["is_active"] is False
    assert dag["is_paused"] is False
    assert dag["next_dagrun"] is None
    assert dag["timetable_description"] == "none"
    assert dag["tags"] == [{"name": "offline"}]
    assert dag["max_active_tasks"] == 16
    assert dag["max_active_runs"] == 1
    assert dag["last_parsed_time"]
    assert "file_token" not in dag
    page = client.get(
        "/api/v1/dags",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01", "limit": 1, "offset": 1},
    ).json()
    assert page["total_entries"] == 1
    assert page["dags"] == []
    one = client.get(
        "/api/v1/dags/SYN-DAG-ETL",
        headers=VIEWER_A,
        params={"cluster_id": "syn-cluster-01"},
    ).json()
    assert one["dag_id"] == "SYN-DAG-ETL"
    errors = client.get(
        "/api/v1/importErrors",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
    ).json()
    assert errors["total_entries"] == 1
    assert errors["import_errors"][0]["import_error_id"] == 1
    assert errors["import_errors"][0]["filename"] == "datasets/offline/dags/syn_dag_leak.py"
    assert "Traceback" not in errors["import_errors"][0]["stack_trace"]
    created = client.post(
        "/api/v1/dags/SYN-DAG-ETL/dagRuns",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
        json={"dag_run_id": "manual"},
    )
    assert created.status_code == 503
    assert created.json()["detail"] == "scheduler process is not-run"
    runs = client.get(
        "/api/v1/dags/SYN-DAG-ETL/dagRuns",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
    ).json()
    assert runs == {"dag_runs": [], "total_entries": 0}
    unknown = client.post(
        "/api/v1/dags/SYN-DAG-LEAK/dagRuns",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
        json={},
    )
    assert unknown.status_code == 404
    extra = client.post(
        "/api/v1/dags/SYN-DAG-ETL/dagRuns",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
        json={"unexpected": True},
    )
    assert extra.status_code == 422


def test_tenant_dag_lists_stay_separate(client):
    _install(client, "syn-cluster-01", OPERATOR_A)
    _install(client, "syn-cluster-b", OPERATOR_B)
    dags_a = client.get(
        "/api/v1/dags",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-01"},
    ).json()
    dags_b = client.get(
        "/api/v1/dags",
        headers=OPERATOR_B,
        params={"cluster_id": "syn-cluster-b"},
    ).json()
    assert [item["dag_id"] for item in dags_a["dags"]] == ["SYN-DAG-ETL"]
    assert [item["dag_id"] for item in dags_b["dags"]] == ["SYN-DAG-OTHER"]
    assert client.get(
        "/api/v1/dags",
        headers=OPERATOR_A,
        params={"cluster_id": "syn-cluster-b"},
    ).status_code == 404
    text = client.get("/metrics", headers=OPERATOR_A).text
    assert 'astroplane_reconcile_total{result="planned"} 2' in text
    assert 'astroplane_reconcile_total{result="denied"} 0' in text
    assert "astroplane_components_desired 12" in text
    assert "astroplane_components_running 0" in text
    assert "astroplane_dags_accepted 2" in text
    assert "astroplane_import_errors 1" in text
    assert "astroplane_components_ready" not in text
