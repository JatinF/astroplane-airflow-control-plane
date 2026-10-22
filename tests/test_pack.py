import hashlib
import importlib.util
from pathlib import Path

from astroplane.supply.pack import manifest, scan_pack


def test_airflow_package_is_absent():
    assert importlib.util.find_spec("airflow") is None


def test_scan_does_not_import_pack_modules():
    import sys

    scan_pack("tenant-a")
    assert "syn_dag_etl" not in sys.modules


def test_committed_pack_filters_tenant_before_urls():
    tenant_a = scan_pack("tenant-a")
    assert [item["dag_id"] for item in tenant_a["accepted"]] == ["SYN-DAG-ETL"]
    assert tenant_a["accepted"][0]["fileloc"] == "datasets/offline/dags/syn_dag_etl.py"
    assert tenant_a["errors"][0]["reason"] == "external-url"
    assert tenant_a["errors"][0]["filename"] == "datasets/offline/dags/syn_dag_leak.py"
    assert "Traceback" not in tenant_a["errors"][0]["stack_trace"]
    tenant_b = scan_pack("tenant-b")
    assert [item["dag_id"] for item in tenant_b["accepted"]] == ["SYN-DAG-OTHER"]
    assert tenant_b["errors"] == []


def test_other_tenant_url_is_omitted(tmp_path: Path):
    (tmp_path / "hidden.py").write_text(
        '# tenant: tenant-b\n# dag_id: SYN-DAG-HIDDEN\nINDEX = "https://example.invalid/index"\n',
        encoding="utf-8",
    )
    assert scan_pack("tenant-a", tmp_path) == {"accepted": [], "errors": []}


def test_missing_dag_id_is_an_import_error(tmp_path: Path):
    (tmp_path / "bad.py").write_text("# tenant: tenant-a\n# description: no id\n", encoding="utf-8")
    result = scan_pack("tenant-a", tmp_path)
    assert result["accepted"] == []
    assert result["errors"][0]["reason"] == "missing-header"
    assert result["errors"][0]["filename"] == "datasets/offline/dags/bad.py"


def test_manifest_hash_matches_the_file_bytes():
    body = manifest()
    assert body["images_pulled"] is False
    assert body["airflow_distribution"] == "not-installed"
    assert body["wheelhouse"] == "not-vendored"
    assert body["evidence_class"] == "SPEC"
    assert all(image["digest"] == "not-pulled" for image in body["images"])
    entry = next(item for item in body["files"] if item["path"] == "datasets/offline/dags/syn_dag_etl.py")
    raw = Path("datasets/offline/dags/syn_dag_etl.py").read_bytes()
    assert entry["sha256"] == hashlib.sha256(raw).hexdigest()
    assert entry["bytes"] == len(raw)
