"""Airflow 2.11.0 API shapes.

`version` is the pinned compatibility target. It is not `airflow.__version__`.
The Apache Airflow distribution is not installed in this project.

Health follows HealthInfo from Airflow 2.11, including `dag_processor`
(added in 2.6.3) and `triggerer` (added in 2.6.2). Every status stays
`unhealthy` because SchedulerJob, TriggererJob, DagProcessorJob, and
Postgres were not started. `is_active` stays false because Airflow defines
it as whether a scheduler currently sees the DAG.
"""

from __future__ import annotations

COMPAT_VERSION = "2.11.0"


def health_payload() -> dict:
    return {
        "metadatabase": {"status": "unhealthy"},
        "scheduler": {"status": "unhealthy", "latest_scheduler_heartbeat": None},
        "triggerer": {"status": "unhealthy", "latest_triggerer_heartbeat": None},
        "dag_processor": {"status": "unhealthy", "latest_dag_processor_heartbeat": None},
    }


def version_payload() -> dict:
    return {"version": COMPAT_VERSION, "git_version": None}


def dag_payload(item: dict) -> dict:
    return {
        "dag_id": item["dag_id"],
        "dag_display_name": item["dag_id"],
        "root_dag_id": None,
        "is_paused": False,
        "is_active": False,
        "is_subdag": False,
        "description": item["description"],
        "fileloc": item["fileloc"],
        "owners": ["astroplane"],
        "schedule_interval": None,
        "timetable_description": "none",
        "tags": [{"name": "offline"}],
        "max_active_tasks": item["max_active_tasks"],
        "max_active_runs": item["max_active_runs"],
        "has_import_errors": False,
        "next_dagrun": None,
        "last_parsed_time": item["last_parsed_time"],
    }


def dag_collection(items: list[dict], limit: int, offset: int) -> dict:
    page = items[offset : offset + limit]
    return {"dags": [dag_payload(item) for item in page], "total_entries": len(items)}


def import_error_payload(index: int, item: dict, timestamp: str) -> dict:
    return {
        "import_error_id": index,
        "timestamp": timestamp,
        "filename": item["filename"],
        "stack_trace": item["stack_trace"],
    }


def import_error_collection(errors: list[dict], timestamp: str, limit: int, offset: int) -> dict:
    rows = [import_error_payload(index, item, timestamp) for index, item in enumerate(errors, start=1)]
    page = rows[offset : offset + limit]
    return {"import_errors": page, "total_entries": len(rows)}


def empty_dag_runs() -> dict:
    return {"dag_runs": [], "total_entries": 0}
