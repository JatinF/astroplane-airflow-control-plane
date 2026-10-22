from astroplane.egress.policy import evaluate_egress
from astroplane.workflow import admit_spec, load_cluster


def _spec() -> dict:
    return load_cluster("syn-cluster-01")["spec"]


def test_local_images_and_deny_all_are_admitted():
    decision = evaluate_egress(_spec())
    assert decision == {"allowed": True, "reasons": []}


def test_public_registry_is_denied():
    decision = evaluate_egress(load_cluster("syn-cluster-egress")["spec"])
    assert decision["allowed"] is False
    assert decision["reasons"] == ["image-registry:airflow"]


def test_egress_mode_and_allow_host():
    spec = _spec()
    spec["egress"] = {"mode": "allow-all", "allow": ["https://pypi.org", "10.0.0.8"]}
    decision = evaluate_egress(spec)
    assert decision["allowed"] is False
    assert decision["reasons"] == ["egress-mode", "allow-host"]


def test_cluster_local_service_host_is_allowed():
    spec = _spec()
    spec["egress"] = {"mode": "deny-all", "allow": ["metadata.astroplane.svc"]}
    assert evaluate_egress(spec)["allowed"] is True


def test_pack_path_and_version_join_the_egress_reasons():
    spec = _spec()
    spec["offlineDagPack"] = "/tmp/other"
    spec["airflowVersion"] = "3.0.0"
    decision = admit_spec(spec)
    assert decision["allowed"] is False
    assert decision["reasons"] == ["pack-path", "airflow-version"]
