"""Nearest-rank percentile. Rank is ceil(p/100 * n), then the value at that rank."""

from __future__ import annotations

import math


def nearest_rank(values: list[float], percentile: float) -> float:
    if not values:
        raise ValueError("nearest-rank needs at least one sample")
    ordered = sorted(values)
    rank = math.ceil((percentile / 100.0) * len(ordered))
    return ordered[max(rank, 1) - 1]
