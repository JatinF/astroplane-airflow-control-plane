"""Record 40 planned installs and one denied install outside that sample."""

from __future__ import annotations

import json
import os
import platform
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from benchmarks.analyze_results import nearest_rank
from astroplane.governance.auth import PlatformStore
from astroplane.supply.pack import manifest
from astroplane.workflow import install
RESULTS = ROOT / "benchmarks" / "results"
GATES = {
    "kube-apiserver": "not-run",
    "kind-cluster": "not-run",
    "helm": "not-run",
    "airflow-scheduler-process": "not-run",
    "image-pull": "not-run",
    "postgres": "not-run",
    "zero-egress-network-namespace": "not-run",
    "wheelhouse-vendoring": "not-run",
}


def _snapshot(store: PlatformStore) -> dict:
    return {
        "components_desired": int(store.metrics["components_desired"]),
        "components_running": int(store.metrics["components_running"]),
        "dags_accepted": int(store.metrics["dags_accepted"]),
        "import_errors": int(store.metrics["import_errors"]),
        "reconcile_planned": int(store.metrics["reconcile_planned"]),
        "reconcile_denied": int(store.metrics["reconcile_denied"]),
        "egress_denied": int(store.metrics["egress_denied"]),
    }


def main() -> None:
    store = PlatformStore()
    principal = store.principal("dev-operator-a")
    assert principal is not None
    samples: list[float] = []
    planned = None
    for _ in range(40):
        started = time.perf_counter()
        planned = install(store, principal, "syn-cluster-01")
        samples.append((time.perf_counter() - started) * 1000.0)
    assert planned is not None
    cluster = planned["cluster"]
    after_planned = {
        "phase": cluster["phase"],
        "applied": cluster["applied"],
        "deployment_count": len(cluster["deployments"]),
        "deployment_phases": sorted({item["status"]["phase"] for item in cluster["deployments"]}),
        "ready_replicas": sum(item["status"]["readyReplicas"] for item in cluster["deployments"]),
        "accepted_dag_ids": [item["dag_id"] for item in cluster["accepted"]],
        "import_error_filenames": [item["filename"] for item in cluster["errors"]],
        **_snapshot(store),
    }
    started = time.perf_counter()
    denied = install(store, principal, "syn-cluster-egress")
    denied_ms = (time.perf_counter() - started) * 1000.0
    denied_cluster = denied["cluster"]
    supply = manifest()
    report = {
        "evidence_class": "MEASURED",
        "decision": "NO-GO",
        "cost": "no price applied",
        "scenario": (
            "40 in-process installs of syn-cluster-01. "
            "One denied install of syn-cluster-egress is excluded from the percentile sample."
        ),
        "sample_size": len(samples),
        "install_latency_ms": samples,
        "p50_ms": round(nearest_rank(samples, 50), 4),
        "p95_ms": round(nearest_rank(samples, 95), 4),
        "denied_install_ms": round(denied_ms, 4),
        "denied_in_percentile_sample": False,
        "after_planned_sample": after_planned,
        "after_denied_install": {
            "phase": denied_cluster["phase"],
            "applied": denied_cluster["applied"],
            "deployment_count": len(denied_cluster["deployments"]),
            "reasons": denied_cluster["egress"]["reasons"],
            **_snapshot(store),
        },
        "airflow_distribution": supply["airflow_distribution"],
        "images_pulled": supply["images_pulled"],
        "wheelhouse": supply["wheelhouse"],
        "machine": {
            "platform": platform.platform(),
            "processor": platform.processor(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
        },
        "gates": GATES,
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "benchmark_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    lines = [
        "# AstroPlane control-plane benchmark",
        "",
        f"Evidence class: {report['evidence_class']}. Decision: {report['decision']}.",
        "",
        report["scenario"],
        "",
        f"P50 {report['p50_ms']:.4f} ms, P95 {report['p95_ms']:.4f} ms.",
        f"Denied install latency {report['denied_install_ms']:.4f} ms is outside the percentile sample.",
        f"Machine: {report['machine']['platform']}. Processor: {report['machine']['processor']}. "
        f"cpu_count {report['machine']['cpu_count']}. Python {report['machine']['python']}.",
        "",
        f"After the planned sample, phase {after_planned['phase']}, "
        f"deployments {after_planned['deployment_count']}, "
        f"components_running {after_planned['components_running']}, "
        f"accepted DAGs {after_planned['accepted_dag_ids']}.",
        f"After the denied install, phase {report['after_denied_install']['phase']}, "
        f"deployments {report['after_denied_install']['deployment_count']}, "
        f"reasons {report['after_denied_install']['reasons']}.",
        "",
        "Cost: no price applied.",
        "Airflow distribution: not-installed. Images pulled: false. Wheelhouse: not-vendored.",
        "",
        "Gates:",
    ]
    for name, state in GATES.items():
        lines.append(f"- {name}: {state}")
    lines.append("")
    (RESULTS / "benchmark_report.md").write_text("\n".join(lines), encoding="utf-8")


if __name__ == "__main__":
    main()
