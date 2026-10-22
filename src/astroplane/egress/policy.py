"""Admit images and endpoints that stay on the local registry."""

from __future__ import annotations

LOCAL_PREFIX = "registry.local/astroplane/"


def evaluate_egress(spec: dict) -> dict:
    reasons: list[str] = []
    egress = spec.get("egress") or {}
    if egress.get("mode") != "deny-all":
        reasons.append("egress-mode")
    for host in egress.get("allow") or []:
        text = str(host)
        if "://" in text or not text.endswith(".svc"):
            reasons.append("allow-host")
            break
    images = spec.get("images") or {}
    for name, image in images.items():
        if not str(image).startswith(LOCAL_PREFIX):
            reasons.append(f"image-registry:{name}")
    return {"allowed": not reasons, "reasons": reasons}
