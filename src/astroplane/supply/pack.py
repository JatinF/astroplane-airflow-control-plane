"""Read the committed DAG pack. The files are not imported or executed."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from astroplane.paths import dataset_dir, repo_root

HEADER = re.compile(r"^#\s+([a-z_]+):\s*(.*)$", re.M)
URL = re.compile(r"https?://", re.I)
PACK_RELATIVE = "datasets/offline/dags"


def pack_dir() -> Path:
    return dataset_dir() / "offline" / "dags"


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 16), b""):
            digest.update(chunk)
    return digest.hexdigest()


def member_path(path: Path) -> str:
    return f"{PACK_RELATIVE}/{path.name}"


def _headers(text: str) -> dict[str, str]:
    return {match.group(1): match.group(2).strip() for match in HEADER.finditer(text)}


def scan_pack(tenant: str, root: Path | None = None) -> dict:
    """Accept pack members for one tenant.

    The tenant header is applied before the URL scan. Another tenant's file
    is omitted. It is not reported as an import error.
    """
    folder = pack_dir() if root is None else root
    accepted: list[dict] = []
    errors: list[dict] = []
    for path in sorted(folder.glob("*.py")):
        text = path.read_text(encoding="utf-8")
        headers = _headers(text)
        file_tenant = headers.get("tenant")
        if file_tenant != tenant:
            continue
        filename = member_path(path)
        if URL.search(text):
            errors.append(
                {
                    "filename": filename,
                    "reason": "external-url",
                    "stack_trace": "external-url: the pack member contains an http or https URL",
                }
            )
            continue
        if "dag_id" not in headers:
            errors.append(
                {
                    "filename": filename,
                    "reason": "missing-header",
                    "stack_trace": "missing-header: dag_id is required",
                }
            )
            continue
        accepted.append(
            {
                "dag_id": headers["dag_id"],
                "tenant": file_tenant,
                "description": headers.get("description", ""),
                "max_active_tasks": int(headers.get("max_active_tasks", "16")),
                "max_active_runs": int(headers.get("max_active_runs", "1")),
                "fileloc": filename,
                "sha256": file_sha256(path),
            }
        )
    return {"accepted": accepted, "errors": errors}


def manifest() -> dict:
    folder = dataset_dir() / "offline"
    files = sorted(path for path in folder.rglob("*") if path.is_file())
    entries = [
        {
            "path": path.relative_to(repo_root()).as_posix(),
            "sha256": file_sha256(path),
            "bytes": path.stat().st_size,
        }
        for path in files
    ]
    lock = json.loads((folder / "images.lock.json").read_text(encoding="utf-8"))
    return {
        "files": entries,
        "images": lock["images"],
        "images_pulled": lock["pulled"],
        "wheelhouse": "not-vendored",
        "airflow_distribution": "not-installed",
        "evidence_class": lock["evidence_class"],
    }
