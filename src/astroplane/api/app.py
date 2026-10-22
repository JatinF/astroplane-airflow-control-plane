"""FastAPI control plane.

Airflow-shaped routes use the 2.11 paths. `cluster_id` is an AstroPlane
query parameter because one process can plan more than one cluster.
Airflow's own OpenAPI does not include that parameter.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, ConfigDict

from astroplane.compat.airflow_api import (
    dag_collection,
    empty_dag_runs,
    health_payload,
    import_error_collection,
    version_payload,
)
from astroplane.governance.auth import PlatformStore
from astroplane.observe.metrics import render_metrics
from astroplane.supply.pack import manifest
from astroplane.workflow import install, read_audit, read_cluster

SCHEDULER_DETAIL = "scheduler process is not-run"


class InstallBody(BaseModel):
    model_config = ConfigDict(extra="forbid")


class DagRunBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dag_run_id: str | None = None
    logical_date: str | None = None
    execution_date: str | None = None
    conf: dict | None = None
    note: str | None = None


def create_app(store: PlatformStore | None = None) -> FastAPI:
    app = FastAPI(title="AstroPlane", version="0.1.0")
    app.state.store = store or PlatformStore()

    def principal_from(request: Request) -> dict:
        header = request.headers.get("authorization", "")
        token = header[7:].strip() if header.lower().startswith("bearer ") else None
        found = app.state.store.principal(token)
        if found is None:
            raise HTTPException(status_code=401, detail="bearer token is required")
        return found

    def call(fn):
        try:
            return fn()
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    def require_cluster_id(cluster_id: str | None) -> str:
        if not cluster_id:
            raise HTTPException(status_code=400, detail="cluster_id is required")
        return cluster_id

    @app.get("/healthz")
    def healthz() -> dict:
        return {
            "status": "ok",
            "mode": "control-plane-stand-in",
            "airflow_distribution": "not-installed",
            "kube_apiserver": "not-run",
            "real_cluster": 0,
            "components_running": 0,
        }

    @app.get("/api/v1/health")
    def airflow_health(request: Request) -> dict:
        principal_from(request)
        return health_payload()

    @app.get("/api/v1/version")
    def airflow_version(request: Request) -> dict:
        principal_from(request)
        return version_payload()

    @app.get("/api/v1/dags")
    def list_dags(
        request: Request,
        cluster_id: str | None = None,
        limit: int = Query(default=100, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        principal = principal_from(request)
        chosen = require_cluster_id(cluster_id)
        record = call(lambda: read_cluster(app.state.store, principal, chosen))
        return dag_collection(record["accepted"], limit, offset)

    @app.get("/api/v1/dags/{dag_id}")
    def get_dag(dag_id: str, request: Request, cluster_id: str | None = None) -> dict:
        principal = principal_from(request)
        chosen = require_cluster_id(cluster_id)
        record = call(lambda: read_cluster(app.state.store, principal, chosen))
        collection = dag_collection(record["accepted"], limit=10_000, offset=0)
        for item in collection["dags"]:
            if item["dag_id"] == dag_id:
                return item
        raise HTTPException(status_code=404, detail="dag not found")

    @app.get("/api/v1/importErrors")
    def list_import_errors(
        request: Request,
        cluster_id: str | None = None,
        limit: int = Query(default=100, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> dict:
        principal = principal_from(request)
        chosen = require_cluster_id(cluster_id)
        record = call(lambda: read_cluster(app.state.store, principal, chosen))
        return import_error_collection(record["errors"], record["parsed_at"], limit, offset)

    @app.get("/api/v1/importErrors/{import_error_id}")
    def get_import_error(import_error_id: int, request: Request, cluster_id: str | None = None) -> dict:
        principal = principal_from(request)
        chosen = require_cluster_id(cluster_id)
        record = call(lambda: read_cluster(app.state.store, principal, chosen))
        collection = import_error_collection(record["errors"], record["parsed_at"], limit=10_000, offset=0)
        for item in collection["import_errors"]:
            if item["import_error_id"] == import_error_id:
                return item
        raise HTTPException(status_code=404, detail="import error not found")

    @app.get("/api/v1/dags/{dag_id}/dagRuns")
    def list_dag_runs(dag_id: str, request: Request, cluster_id: str | None = None) -> dict:
        principal = principal_from(request)
        chosen = require_cluster_id(cluster_id)
        record = call(lambda: read_cluster(app.state.store, principal, chosen))
        if not any(item["dag_id"] == dag_id for item in record["accepted"]):
            raise HTTPException(status_code=404, detail="dag not found")
        return empty_dag_runs()

    @app.post("/api/v1/dags/{dag_id}/dagRuns")
    def create_dag_run(
        dag_id: str,
        body: DagRunBody,
        request: Request,
        cluster_id: str | None = None,
    ) -> dict:
        del body
        principal = principal_from(request)
        chosen = require_cluster_id(cluster_id)
        record = call(lambda: read_cluster(app.state.store, principal, chosen))
        if not any(item["dag_id"] == dag_id for item in record["accepted"]):
            raise HTTPException(status_code=404, detail="dag not found")
        raise HTTPException(status_code=503, detail=SCHEDULER_DETAIL)

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics(request: Request) -> PlainTextResponse:
        principal_from(request)
        return PlainTextResponse(
            render_metrics(app.state.store),
            media_type="text/plain; version=0.0.4",
        )

    @app.get("/v1/supply/manifest")
    def supply_manifest(request: Request) -> dict:
        principal_from(request)
        return manifest()

    @app.post("/v1/clusters/{cluster_id}/install")
    def install_cluster(cluster_id: str, body: InstallBody, request: Request) -> dict:
        del body
        principal = principal_from(request)
        result = call(lambda: install(app.state.store, principal, cluster_id))
        cluster = result["cluster"]
        return {
            "cluster_id": cluster["cluster_id"],
            "phase": cluster["phase"],
            "applied": False,
            "audit_id": result["audit"]["audit_id"],
            "deployment_count": len(cluster["deployments"]),
            "components_desired": int(app.state.store.metrics["components_desired"]),
            "components_running": int(app.state.store.metrics["components_running"]),
            "egress_allowed": cluster["egress"]["allowed"],
            "reasons": cluster["egress"]["reasons"],
        }

    @app.get("/v1/clusters/{cluster_id}")
    def get_cluster(cluster_id: str, request: Request) -> dict:
        principal = principal_from(request)
        return call(lambda: read_cluster(app.state.store, principal, cluster_id))

    @app.get("/v1/audits/{audit_id}")
    def get_audit(audit_id: str, request: Request) -> dict:
        principal = principal_from(request)
        return call(lambda: read_audit(app.state.store, principal, audit_id))

    return app
