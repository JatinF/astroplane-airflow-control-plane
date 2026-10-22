"""Reconcile one AirflowCluster document in memory.

A second call replaces the object list. It does not append a second copy.
No object is applied to a Kubernetes API server.
"""

from __future__ import annotations

from astroplane.provision.planner import plan_deployments


def reconcile(document: dict, admission: dict, pack: dict, parsed_at: str) -> dict:
    allowed = bool(admission["allowed"])
    deployments = plan_deployments(document) if allowed else []
    accepted = [{**item, "last_parsed_time": parsed_at} for item in pack["accepted"]]
    if allowed:
        ready_reason = "kube-apiserver not-run"
        phase = "Planned"
    else:
        ready_reason = "egress-denied"
        phase = "Denied"
    return {
        "phase": phase,
        "applied": False,
        "conditions": [
            {
                "type": "Egress",
                "status": "True" if allowed else "False",
                "reason": "deny-all" if allowed else ",".join(admission["reasons"]),
            },
            {
                "type": "PackScanned",
                "status": "True",
                "reason": "text-scan",
            },
            {
                "type": "Ready",
                "status": "False",
                "reason": ready_reason,
            },
        ],
        "deployments": deployments,
        "accepted": accepted,
        "errors": list(pack["errors"]),
        "egress": admission,
    }
