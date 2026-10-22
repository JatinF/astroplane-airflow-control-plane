"""Fixture tokens and the optional SQLite journal.

The journal is the persistent state for the local API. An empty path keeps
the original in-memory store. Tokens authorize nothing outside this process.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

DEV_TOKENS = {
    "dev-operator-a": {"tenant": "tenant-a", "role": "operator", "subject": "operator-a"},
    "dev-viewer-a": {"tenant": "tenant-a", "role": "viewer", "subject": "viewer-a"},
    "dev-operator-b": {"tenant": "tenant-b", "role": "operator", "subject": "operator-b"},
}


class PlatformStore:
    def __init__(self, path: str | Path | None = None) -> None:
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
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        if path is not None:
            self._conn = sqlite3.connect(path, check_same_thread=False)
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS kv (
                    bucket TEXT NOT NULL,
                    item_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY (bucket, item_key)
                )
                """
            )
            self._load()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

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

    def persist(self) -> None:
        if self._conn is None:
            return
        with self._lock:
            self._write()

    def _write(self) -> None:
        assert self._conn is not None
        self._conn.execute("DELETE FROM kv")
        for cluster_id, record in self.clusters.items():
            self._conn.execute(
                "INSERT INTO kv(bucket, item_key, payload) VALUES (?, ?, ?)",
                ("cluster", cluster_id, json.dumps(record)),
            )
        for record in self.audits:
            self._conn.execute(
                "INSERT INTO kv(bucket, item_key, payload) VALUES (?, ?, ?)",
                ("audit", record["audit_id"], json.dumps(record)),
            )
        self._conn.execute(
            "INSERT INTO kv(bucket, item_key, payload) VALUES (?, ?, ?)",
            (
                "meta",
                "state",
                json.dumps({"sequence": self._sequence, "metrics": self.metrics}),
            ),
        )
        self._conn.commit()

    def _load(self) -> None:
        assert self._conn is not None
        rows = list(self._conn.execute("SELECT bucket, item_key, payload FROM kv"))
        audits: list[dict] = []
        for bucket, key, payload in rows:
            data = json.loads(payload)
            if bucket == "cluster":
                self.clusters[key] = data
            elif bucket == "audit":
                audits.append(data)
            elif bucket == "meta" and key == "state":
                self._sequence = int(data["sequence"])
                self.metrics.update({name: float(value) for name, value in data["metrics"].items()})
        self.audits = sorted(audits, key=lambda item: item["audit_id"])


def can_read(principal: dict) -> bool:
    return principal["role"] in {"operator", "viewer"}


def can_install(principal: dict) -> bool:
    return principal["role"] == "operator"
