"""Fixture tokens. They authorize nothing outside this process."""

from __future__ import annotations

DEV_TOKENS = {
    "dev-operator-a": {"tenant": "tenant-a", "role": "operator", "subject": "operator-a"},
    "dev-viewer-a": {"tenant": "tenant-a", "role": "viewer", "subject": "viewer-a"},
    "dev-operator-b": {"tenant": "tenant-b", "role": "operator", "subject": "operator-b"},
}


class PlatformStore:
    def __init__(self) -> None:
        self.clusters: dict[str, dict] = {}
        self.audits: list[dict] = []
        self.metrics: dict[str, float] = {
            "reconcile_planned": 0.0,
            "reconcile_denied": 0.0,
            "egress_denied": 0.0,
            "components_desired": 0.0,
            "components_running": 0.0,
            "dags_accepted": 0.0,
            "import_errors": 0.0,
        }
        self._sequence = 0

    def next_id(self, prefix: str) -> str:
        self._sequence += 1
        return f"{prefix}-{self._sequence:04d}"

    def principal(self, token: str | None) -> dict | None:
        if not token:
            return None
        record = DEV_TOKENS.get(token)
        if record is None:
            return None
        return {"token": token, **record}

    def append_audit(self, record: dict) -> dict:
        self.audits.append(record)
        return record


def can_read(principal: dict) -> bool:
    return principal["role"] in {"operator", "viewer"}


def can_install(principal: dict) -> bool:
    return principal["role"] == "operator"
