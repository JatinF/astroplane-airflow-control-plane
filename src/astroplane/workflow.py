"""Install path for one AirflowCluster custom resource."""

from __future__ import annotations

import re
from datetime import datetime, timezone

import yaml

from astroplane.compat.airflow_api import COMPAT_VERSION
from astroplane.egress.policy import evaluate_egress
from astroplane.governance.auth import PlatformStore, can_install, can_read
from astroplane.operator.reconcile import reconcile
from astroplane.paths import dataset_dir
from astroplane.supply.pack import PACK_RELATIVE, scan_pack

CLUSTER_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")


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
    return {"cluster": record, "audit": audit}


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
