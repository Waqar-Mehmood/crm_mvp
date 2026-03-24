"""Reusable upload preprocessing for import sources."""

from __future__ import annotations

from pathlib import Path
import re
from tempfile import NamedTemporaryFile
from typing import Any

from crm.services import import_service
from crm.services.import_components.data_cleaner import DataCleaner


class UploadHandler:
    """Validate and normalize uploaded import sources into parsed row payloads."""

    MAX_FILE_SIZE = 10 * 1024 * 1024
    ALLOWED_FILE_EXTENSIONS = {".csv", ".xlsx", ".json"}
    GOOGLE_SHEETS_SOURCE_TYPE = "google_sheets"

    @classmethod
    def detect_file_format(cls, filename: str) -> str:
        """Detect the normalized source type from a filename or Google Sheets URL."""
        detected = import_service.detect_import_source_type(source=filename, filename=filename)
        if not detected:
            raise ValueError(
                "Unsupported import source type. Expected one of: csv, xlsx, json, google_sheets."
            )
        return detected

    @classmethod
    def validate_file(cls, uploaded_file) -> tuple[bool, str]:
        """Validate source type and file size for an uploaded file or sheet URL."""
        if uploaded_file is None:
            return False, "Choose an import file or enter a Google Sheets URL."

        if isinstance(uploaded_file, str):
            source = DataCleaner.clean(uploaded_file)
            if not source:
                return False, "Choose an import file or enter a Google Sheets URL."
            try:
                source_type = cls.detect_file_format(source)
            except ValueError as exc:
                return False, str(exc)
            if source_type != cls.GOOGLE_SHEETS_SOURCE_TYPE:
                return False, "Only Google Sheets URLs are supported as string sources."
            return True, ""

        raw_name = Path(getattr(uploaded_file, "name", "")).name
        if not raw_name:
            return False, "Uploaded file must include a filename."

        suffix = Path(raw_name).suffix.lower()
        if suffix not in cls.ALLOWED_FILE_EXTENSIONS:
            return (
                False,
                "Unsupported import source type. Expected one of: csv, xlsx, json, google_sheets.",
            )

        file_size = getattr(uploaded_file, "size", None)
        if file_size is not None and int(file_size) > cls.MAX_FILE_SIZE:
            return False, f"File exceeds the maximum allowed size of {cls.MAX_FILE_SIZE} bytes."

        return True, ""

    @classmethod
    def process_uploaded_file(cls, uploaded_file) -> dict:
        """Parse an uploaded source into standardized row payload metadata."""
        is_valid, error_message = cls.validate_file(uploaded_file)
        if not is_valid:
            raise ValueError(error_message)

        source_type = cls._resolve_source_type(uploaded_file)
        parse_source = uploaded_file
        temp_path: Path | None = None

        try:
            if source_type != cls.GOOGLE_SHEETS_SOURCE_TYPE:
                temp_path = cls._write_uploaded_file_to_temp_path(uploaded_file)
                parse_source = temp_path

            rows = import_service.parse_rows_from_source(
                parse_source,
                source_type=source_type,
                filename=cls._raw_file_name(uploaded_file),
            )
        except Exception as exc:
            raise ValueError(f"Failed to process import source: {exc}") from exc
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

        return {
            "rows": rows,
            "file_name": cls._build_output_filename(uploaded_file, source_type),
            "source_type": source_type,
            "row_count": len(rows),
        }

    @classmethod
    def _resolve_source_type(cls, uploaded_file) -> str:
        if isinstance(uploaded_file, str):
            return cls.detect_file_format(uploaded_file)
        return cls.detect_file_format(cls._raw_file_name(uploaded_file))

    @staticmethod
    def _raw_file_name(uploaded_file) -> str:
        if isinstance(uploaded_file, str):
            return uploaded_file
        return Path(getattr(uploaded_file, "name", "import.csv")).name

    @classmethod
    def _build_output_filename(cls, uploaded_file, source_type: str) -> str:
        raw_name = cls._raw_file_name(uploaded_file)
        if source_type == cls.GOOGLE_SHEETS_SOURCE_TYPE:
            stem = "google_sheets_import"
        else:
            stem = Path(raw_name).stem or "import"

        cleaned_stem = DataCleaner.clean(stem)
        cleaned_stem = re.sub(r"[^A-Za-z0-9._ -]+", "-", cleaned_stem).strip(" ._-")
        return f"{cleaned_stem or 'import'}.csv"

    @staticmethod
    def _read_uploaded_file_bytes(uploaded_file) -> bytes:
        if hasattr(uploaded_file, "chunks"):
            return b"".join(chunk for chunk in uploaded_file.chunks())
        if hasattr(uploaded_file, "read"):
            data = uploaded_file.read()
            if isinstance(data, str):
                return data.encode("utf-8")
            return data
        raise ValueError("Uploaded source must be a file-like object or Google Sheets URL.")

    @classmethod
    def _write_uploaded_file_to_temp_path(cls, uploaded_file) -> Path:
        raw_name = cls._raw_file_name(uploaded_file)
        suffix = Path(raw_name).suffix or ".tmp"
        file_bytes = cls._read_uploaded_file_bytes(uploaded_file)

        try:
            with NamedTemporaryFile(suffix=suffix, delete=False) as handle:
                handle.write(file_bytes)
                return Path(handle.name)
        except OSError as exc:
            raise RuntimeError(f"Failed to stage uploaded file for parsing: {exc}") from exc


__all__ = [
    "UploadHandler",
]
