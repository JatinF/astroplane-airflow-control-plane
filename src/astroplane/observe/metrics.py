"""Render Prometheus text. Gauges count stored objects, not live pods."""

from __future__ import annotations

from astroplane.governance.auth import PlatformStore


def render_metrics(store: PlatformStore) -> str:
    planned = int(store.metrics["reconcile_planned"])
    denied = int(store.metrics["reconcile_denied"])
    egress_denied = int(store.metrics["egress_denied"])
    desired = int(store.metrics["components_desired"])
    running = int(store.metrics["components_running"])
    dags = int(store.metrics["dags_accepted"])
    errors = int(store.metrics["import_errors"])
    lines = [
        "# HELP astroplane_reconcile_total Reconcile calls in this process by result.",
        "# TYPE astroplane_reconcile_total counter",
        f'astroplane_reconcile_total{{result="planned"}} {planned}',
        f'astroplane_reconcile_total{{result="denied"}} {denied}',
        "# HELP astroplane_egress_denied_total Installs refused by the egress check.",
        "# TYPE astroplane_egress_denied_total counter",
        f"astroplane_egress_denied_total {egress_denied}",
        "# HELP astroplane_components_desired Deployment objects stored in this process.",
        "# TYPE astroplane_components_desired gauge",
        f"astroplane_components_desired {desired}",
        "# HELP astroplane_components_running Deployments with a ready replica. Stays 0 while kubelet is not-run.",
        "# TYPE astroplane_components_running gauge",
        f"astroplane_components_running {running}",
        "# HELP astroplane_dags_accepted Accepted pack members stored on clusters in this process.",
        "# TYPE astroplane_dags_accepted gauge",
        f"astroplane_dags_accepted {dags}",
        "# HELP astroplane_import_errors Rejected pack members stored on clusters in this process.",
        "# TYPE astroplane_import_errors gauge",
        f"astroplane_import_errors {errors}",
    ]
    return "\n".join(lines) + "\n"
