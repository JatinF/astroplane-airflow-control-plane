"""Install path for one AirflowCluster custom resource."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import yaml

from astroplane.compat.airflow_api import COMPAT_VERSION
from astroplane.egress.policy import evaluate_egress
from astroplane.governance.auth import PlatformStore, can_install, can_read
from astroplane.operator.reconcile import reconcile
from astroplane.provision.planner import AIRFLOW_COMPONENTS
from astroplane.paths import dataset_dir
from astroplane.supply.pack import PACK_RELATIVE, scan_pack

CLUSTER_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
RELEASE = re.compile(r"r[1-9][0-9]{0,3}")
FINALIZER = "astroplane.example/finalizer"
DEFAULT_RELEASE = "r1"


def refresh_gauges(store: PlatformStore) -> None:
    desired = 0
    running = 0
    dags = 0
    errors = 0
    for record in store.clusters.values():
        deployments = record["deployments"]
        desired += len(deployments)
        running += sum(1 for item in deployments if item["status"]["readyReplicas"] > 0)
        dags += len(record["accepted"])
        errors += len(record["errors"])
    store.metrics["components_desired"] = float(desired)
    store.metrics["components_running"] = float(running)
    store.metrics["dags_accepted"] = float(dags)
    store.metrics["import_errors"] = float(errors)


def admit_spec(spec: dict) -> dict:
    decision = evaluate_egress(spec)
    reasons = list(decision["reasons"])
    if spec.get("offlineDagPack") != PACK_RELATIVE:
        reasons.append("pack-path")
    if str(spec.get("airflowVersion")) != COMPAT_VERSION:
        reasons.append("airflow-version")
    return {"allowed": not reasons, "reasons": reasons}


def load_cluster(cluster_id: str) -> dict:
    if CLUSTER_ID.fullmatch(cluster_id) is None:
        raise LookupError("cluster not found")
    path = dataset_dir() / "clusters" / f"{cluster_id}.yaml"
    if not path.is_file():
        raise LookupError("cluster not found")
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise LookupError("cluster not found")
    if document.get("metadata", {}).get("name") != cluster_id:
        raise LookupError("cluster not found")
    return document


def install(store: PlatformStore, principal: dict, cluster_id: str) -> dict:
    if not can_install(principal):
        raise PermissionError("install requires the operator role")
    document = load_cluster(cluster_id)
    tenant = document["spec"]["tenant"]
    if tenant != principal["tenant"]:
        raise LookupError("cluster not found")
    admission = admit_spec(document["spec"])
    pack = scan_pack(tenant)
    parsed_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    status = reconcile(document, admission, pack, parsed_at)
    record = {
        "cluster_id": cluster_id,
        "tenant": tenant,
        "airflow_version_compat": COMPAT_VERSION,
        "executor": document["spec"].get("executor"),
        "parsed_at": parsed_at,
        "finalizers": [FINALIZER] if status["phase"] == "Planned" else [],
        "release": DEFAULT_RELEASE,
        "observed_release": DEFAULT_RELEASE if status["phase"] == "Planned" else None,
        "base_airflow_image": str(document["spec"]["images"]["airflow"]),
        "deletion_timestamp": None,
        "generation": 1,
        **status,
    }
    store.clusters[cluster_id] = record
    if record["phase"] == "Planned":
        store.metrics["reconcile_planned"] += 1.0
    else:
        store.metrics["reconcile_denied"] += 1.0
    if not admission["allowed"]:
        store.metrics["egress_denied"] += 1.0
    refresh_gauges(store)
    audit = store.append_audit(
        {
            "audit_id": store.next_id("SYN-AUD"),
            "cluster_id": cluster_id,
            "tenant": tenant,
            "subject": principal["subject"],
            "action": "install",
            "phase": record["phase"],
            "applied": False,
            "egress_allowed": admission["allowed"],
            "reasons": list(admission["reasons"]),
        }
    )
    store.persist()
    return {"cluster": record, "audit": audit}


def open_store(path: str | Path) -> PlatformStore:
    store = PlatformStore(path=path)
    recover(store)
    return store


def _owned(store: PlatformStore, principal: dict, cluster_id: str) -> dict:
    if not can_install(principal):
        raise PermissionError("the operator role is required")
    record = store.clusters.get(cluster_id)
    if record is None or record["tenant"] != principal["tenant"]:
        raise LookupError("cluster not found")
    return record


def upgrade(store: PlatformStore, principal: dict, cluster_id: str, release: str) -> dict:
    if RELEASE.fullmatch(release) is None:
        raise ValueError("release must look like r2")
    record = _owned(store, principal, cluster_id)
    if record["phase"] != "Planned":
        raise ValueError("upgrade requires phase Planned")
    image = record["base_airflow_image"] if release == DEFAULT_RELEASE else f"{record['base_airflow_image']}-{release}"
    if not image.startswith("registry.local/astroplane/"):
        raise ValueError("release image leaves the local registry")
    for deployment in record["deployments"]:
        container = deployment["spec"]["template"]["spec"]["containers"][0]
        if container["name"] in AIRFLOW_COMPONENTS:
            container["image"] = image
        deployment["status"]["applied"] = False
        deployment["status"]["phase"] = "Pending"
        deployment["status"]["reason"] = "kubelet not-run"
        deployment["status"]["image_digest"] = "not-pulled"
        deployment["status"]["readyReplicas"] = 0
    record["release"] = release
    record["observed_release"] = release
    record["applied"] = False
    record["generation"] = int(record.get("generation", 1)) + 1
    audit = store.append_audit(
        {
            "audit_id": store.next_id("SYN-AUD"),
            "cluster_id": cluster_id,
            "tenant": record["tenant"],
            "subject": principal["subject"],
            "action": "upgrade",
            "phase": "Planned",
            "applied": False,
            "release": release,
            "reasons": [],
        }
    )
    store.persist()
    return {"cluster": record, "audit": audit}


def delete(store: PlatformStore, principal: dict, cluster_id: str) -> dict:
    record = _owned(store, principal, cluster_id)
    record["deletion_timestamp"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    finished = _finish_deletion(store, principal, cluster_id, record, action="delete")
    store.persist()
    return finished


def recover(store: PlatformStore) -> list[str]:
    """Finish deletions left behind by a stopped process."""
    finished: list[str] = []
    for cluster_id, record in list(store.clusters.items()):
        if not record.get("deletion_timestamp"):
            continue
        principal = {
            "tenant": record["tenant"],
            "subject": "controller-recover",
            "role": "operator",
        }
        _finish_deletion(store, principal, cluster_id, record, action="recover")
        finished.append(cluster_id)
    if finished:
        store.persist()
    return finished


def _finish_deletion(store: PlatformStore, principal: dict, cluster_id: str, record: dict, action: str) -> dict:
    had_finalizer = FINALIZER in record.get("finalizers", [])
    record["deployments"] = []
    record["finalizers"] = []
    record["phase"] = "Deleted"
    record["applied"] = False
    store.clusters.pop(cluster_id, None)
    refresh_gauges(store)
    audit = store.append_audit(
        {
            "audit_id": store.next_id("SYN-AUD"),
            "cluster_id": cluster_id,
            "tenant": record["tenant"],
            "subject": principal["subject"],
            "action": action,
            "phase": "Deleted",
            "applied": False,
            "finalizer": FINALIZER if had_finalizer else None,
            "finalizer_completed": had_finalizer,
            "reasons": [],
        }
    )
    return {"cluster_id": cluster_id, "phase": "Deleted", "audit": audit, "finalizer_completed": had_finalizer}


def read_cluster(store: PlatformStore, principal: dict, cluster_id: str) -> dict:
    if not can_read(principal):
        raise PermissionError("read requires the operator or viewer role")
    record = store.clusters.get(cluster_id)
    if record is None or record["tenant"] != principal["tenant"]:
        raise LookupError("cluster not found")
    return record


def read_audit(store: PlatformStore, principal: dict, audit_id: str) -> dict:
    if not can_read(principal):
        raise PermissionError("read requires the operator or viewer role")
    for record in store.audits:
        if record["audit_id"] == audit_id and record["tenant"] == principal["tenant"]:
            return record
    raise LookupError("audit not found")
