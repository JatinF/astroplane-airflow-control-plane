import json
from pathlib import Path

from benchmarks.analyze_results import nearest_rank

REPORT = Path("benchmarks/results/benchmark_report.json")
README = Path("README.md")


def test_nearest_rank_uses_ceil():
    values = [float(item) for item in range(1, 41)]
    assert nearest_rank(values, 50) == 20
    assert nearest_rank(values, 95) == 38


def test_committed_report_matches_its_samples():
    report = json.loads(REPORT.read_text(encoding="utf-8"))
    samples = report["install_latency_ms"]
    assert report["evidence_class"] == "MEASURED"
    assert report["decision"] == "NO-GO"
    assert report["cost"] == "no price applied"
    assert report["sample_size"] == 40
    assert len(samples) == 40
    assert report["denied_in_percentile_sample"] is False
    assert report["p50_ms"] == round(nearest_rank(samples, 50), 4)
    assert report["p95_ms"] == round(nearest_rank(samples, 95), 4)
    planned = report["after_planned_sample"]
    assert planned["phase"] == "Planned"
    assert planned["applied"] is False
    assert planned["deployment_count"] == 6
    assert planned["deployment_phases"] == ["Pending"]
    assert planned["ready_replicas"] == 0
    assert planned["components_desired"] == 6
    assert planned["components_running"] == 0
    assert planned["dags_accepted"] == 1
    assert planned["import_errors"] == 1
    assert planned["reconcile_planned"] == 40
    assert planned["reconcile_denied"] == 0
    assert planned["accepted_dag_ids"] == ["SYN-DAG-ETL"]
    denied = report["after_denied_install"]
    assert denied["phase"] == "Denied"
    assert denied["deployment_count"] == 0
    assert denied["reasons"] == ["image-registry:airflow"]
    assert denied["components_desired"] == 6
    assert denied["components_running"] == 0
    assert denied["reconcile_planned"] == 40
    assert denied["reconcile_denied"] == 1
    assert denied["egress_denied"] == 1
    assert report["airflow_distribution"] == "not-installed"
    assert report["images_pulled"] is False
    assert report["wheelhouse"] == "not-vendored"
    assert set(report["gates"].values()) == {"not-run"}
    readme = README.read_text(encoding="utf-8")
    sentence = f"P50 {report['p50_ms']:.4f} ms, P95 {report['p95_ms']:.4f} ms"
    assert sentence in readme
    assert report["machine"]["platform"] in readme
    assert "NO-GO" in readme
    assert "scheduler process is not-run" in readme
