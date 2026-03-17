"""Adapters for feeding external row data into the existing CSV import flow."""

from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Callable

from django.core.files.uploadedfile import SimpleUploadedFile

from crm.services.import_parsers import (
    derive_csv_headers,
    parse_csv_file,
    parse_google_sheet,
    parse_json_file,
    parse_xlsx_file,
    serialize_rows_to_csv_content,
)
from crm.services.google_sheets import extract_sheet_id


ImportParser = Callable[[str | Path], list[dict[str, str]]]

_PARSER_BY_SOURCE_TYPE: dict[str, ImportParser] = {
    "csv": parse_csv_file,
    "xlsx": parse_xlsx_file,
    "json": parse_json_file,
    "google_sheets": parse_google_sheet,
}

_SOURCE_TYPE_BY_SUFFIX = {
    ".csv": "csv",
    ".xlsx": "xlsx",
    ".json": "json",
}


def _looks_like_google_sheets_source(source: str | Path | None) -> bool:
    """Return True when the source looks like a Google Sheets URL."""
    if source is None:
        return False

    try:
        extract_sheet_id(str(source))
    except ValueError:
        return False
    return True


def get_row_headers(rows: list[dict[str, str]]) -> list[str]:
    """Return CSV headers derived from row dictionaries."""
    return derive_csv_headers(rows)


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
    _validate_rows(rows)
    return serialize_rows_to_csv_content(rows)


def detect_import_source_type(
    source: str | Path | None = None,
    source_type: str | None = None,
    filename: str | None = None,
) -> str:
    """Resolve the normalized source type from a source hint or filename."""
    resolved_source_type = (source_type or "").strip().lower()
    if not resolved_source_type and _looks_like_google_sheets_source(source):
        resolved_source_type = "google_sheets"
    if not resolved_source_type and source is not None:
        resolved_source_type = _SOURCE_TYPE_BY_SUFFIX.get(Path(str(source)).suffix.lower(), "")
    if not resolved_source_type and filename:
        resolved_source_type = _SOURCE_TYPE_BY_SUFFIX.get(Path(filename).suffix.lower(), "")

    return resolved_source_type


def select_import_parser(
    source: str | Path | None = None,
    source_type: str | None = None,
    filename: str | None = None,
) -> ImportParser:
    """Return the parser function that matches a source type or filename."""
    resolved_source_type = detect_import_source_type(
        source=source,
        source_type=source_type,
        filename=filename,
    )

    parser = _PARSER_BY_SOURCE_TYPE.get(resolved_source_type)
    if parser is None:
        raise ValueError(
            "Unsupported import source type. Expected one of: csv, xlsx, json, google_sheets."
        )
    return parser


def parse_rows_from_source(
    source: str | Path,
    *,
    source_type: str | None = None,
    filename: str | None = None,
) -> list[dict[str, str]]:
    """Parse an import source into row dictionaries via the parser registry."""
    parser = select_import_parser(source=source, source_type=source_type, filename=filename)
    return parser(source)


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
