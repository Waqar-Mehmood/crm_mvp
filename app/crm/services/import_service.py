"""Adapters for feeding external row data into the existing CSV import flow."""

from __future__ import annotations

import csv
from io import StringIO
from pathlib import Path
from tempfile import NamedTemporaryFile

from django.core.files.uploadedfile import SimpleUploadedFile


def get_row_headers(rows: list[dict[str, str]]) -> list[str]:
    """Return CSV headers derived from a list of row dictionaries.

    Headers are collected in first-seen order across all rows so the generated
    CSV remains stable even when later rows contain additional keys.

    Args:
        rows: Parsed row dictionaries, typically from a Google Sheet.

    Returns:
        A list of header names in write order.
    """
    headers: list[str] = []
    seen: set[str] = set()

    for row in rows:
        for key in row.keys():
            if key not in seen:
                seen.add(key)
                headers.append(key)

    return headers


def _validate_rows(rows: list[dict[str, str]]) -> list[str]:
    """Validate row input and return the derived CSV headers."""
    if not rows:
        raise ValueError("Cannot generate a CSV file from an empty row set.")

    headers = get_row_headers(rows)
    if not headers:
        raise ValueError("Cannot generate a CSV file because no headers were found.")

    return headers


def _serialize_rows_to_csv_content(rows: list[dict[str, str]]) -> str:
    """Serialize row dictionaries into CSV text."""
    headers = _validate_rows(rows)
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=headers)
    writer.writeheader()
    for row in rows:
        writer.writerow(
            {header: "" if row.get(header) is None else row.get(header, "") for header in headers}
        )
    return buffer.getvalue()


def rows_to_temporary_csv(rows: list[dict[str, str]]) -> str:
    """Write row dictionaries to a temporary CSV file and return its path.

    This adapter is intended for preview/import workflows that already operate
    on CSV files and should not be rewritten to accept raw row dictionaries.

    Args:
        rows: Parsed row dictionaries to serialize as CSV.

    Returns:
        The filesystem path to the generated temporary CSV file.

    Raises:
        ValueError: If no rows are provided or no headers can be derived.
        RuntimeError: If the temporary CSV cannot be written.
    """
    csv_content = _serialize_rows_to_csv_content(rows)

    try:
        with NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            suffix=".csv",
            prefix="google_sheets_",
            delete=False,
        ) as temp_file:
            temp_file.write(csv_content)
            return str(Path(temp_file.name))
    except OSError as exc:
        raise RuntimeError(f"Failed to write temporary CSV file: {exc}") from exc


def rows_to_uploaded_csv(
    rows: list[dict[str, str]],
    filename: str = "google_sheet_import.csv",
) -> SimpleUploadedFile:
    """Convert row dictionaries into a Django uploaded CSV file object.

    This allows external row sources, such as Google Sheets, to be adapted into
    the existing import workflow that expects a file in ``request.FILES``.

    Args:
        rows: Parsed row dictionaries to serialize as CSV.
        filename: The uploaded filename to expose to the existing import flow.

    Returns:
        A ``SimpleUploadedFile`` containing CSV data.

    Raises:
        ValueError: If no rows are provided, no headers are found, or the
            filename is blank.
    """
    clean_filename = filename.strip()
    if not clean_filename:
        raise ValueError("Uploaded CSV filename must be a non-empty string.")

    csv_content = _serialize_rows_to_csv_content(rows)
    return SimpleUploadedFile(
        clean_filename,
        csv_content.encode("utf-8"),
        content_type="text/csv",
    )
