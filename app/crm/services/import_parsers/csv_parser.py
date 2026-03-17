"""CSV parser helpers for import sources."""

from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path


def _normalize_cell(value: object) -> str:
    """Normalize parser output to string values."""
    return "" if value is None else str(value)


def derive_csv_headers(rows: list[dict[str, str]]) -> list[str]:
    """Return CSV headers derived from row dictionaries in first-seen order."""
    headers: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)

    return headers


def serialize_rows_to_csv_content(rows: list[dict[str, str]]) -> str:
    """Serialize row dictionaries into CSV text."""
    if not rows:
        raise ValueError("Cannot generate CSV content from an empty row set.")

    headers = derive_csv_headers(rows)
    if not headers:
        raise ValueError("Cannot generate CSV content because no headers were found.")

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow({header: _normalize_cell(row.get(header, "")) for header in headers})
    return buffer.getvalue()


def detect_csv_headers(csv_path: str | Path) -> list[str]:
    """Read and return normalized CSV headers from a file path."""
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return [header.strip() for header in (reader.fieldnames or []) if header and header.strip()]


def parse_csv_file(csv_path: str | Path) -> list[dict[str, str]]:
    """Parse a CSV file into a list of row dictionaries."""
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []

        headers = [header.strip() for header in reader.fieldnames if header and header.strip()]
        return [
            {header: _normalize_cell(row.get(header, "")) for header in headers}
            for row in reader
        ]
