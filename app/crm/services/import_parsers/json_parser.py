"""JSON parser helpers for import sources."""

from __future__ import annotations

import json
from pathlib import Path


def _normalize_json_rows(payload: object) -> list[dict[str, str]]:
    """Normalize supported JSON payloads into row dictionaries."""
    rows = payload
    if isinstance(payload, dict):
        rows = payload.get("rows")

    if not isinstance(rows, list):
        raise ValueError("JSON import expects a top-level list or an object with a 'rows' list.")

    normalized_rows: list[dict[str, str]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("JSON import rows must be objects.")
        normalized_rows.append(
            {str(key): "" if value is None else str(value) for key, value in row.items()}
        )

    return normalized_rows


def parse_json_file(json_path: str | Path) -> list[dict[str, str]]:
    """Parse a JSON import file into row dictionaries."""
    path = Path(json_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return _normalize_json_rows(payload)
