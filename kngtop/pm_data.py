"""Polymarket Data API helpers for user position reconciliation."""

from __future__ import annotations

import logging
from typing import Any

import requests

LOGGER = logging.getLogger("kngtop")

DATA_API_URL = "https://data-api.polymarket.com"


def fetch_user_positions(*, user: str, timeout: float, limit: int = 500) -> list[dict[str, Any]]:
    """Return current user positions from the Polymarket Data API."""
    addr = (user or "").strip()
    if not addr:
        return []
    try:
        response = requests.get(
            f"{DATA_API_URL}/positions",
            params={
                "user": addr,
                "sizeThreshold": 0,
                "limit": max(1, int(limit)),
            },
            timeout=timeout,
        )
        response.raise_for_status()
        rows = response.json()
    except (requests.RequestException, TypeError, ValueError) as exc:
        LOGGER.debug("Data API positions failed for %s: %s", addr[:10], exc)
        return []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]
