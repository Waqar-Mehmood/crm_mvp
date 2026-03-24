"""Reusable file helpers for import staging and cleanup."""

from __future__ import annotations

import csv
from pathlib import Path
from tempfile import NamedTemporaryFile

from crm.services.import_components.data_cleaner import DataCleaner


class FileManager:
    """Manage temporary CSV files and safe import filenames."""

    @staticmethod
    def _derive_headers(rows: list[dict]) -> list[str]:
        headers: list[str] = []
        seen = set()
        for row in rows:
            for key in row.keys():
                header = DataCleaner.clean(key)
                if not header or header in seen:
                    continue
                seen.add(header)
                headers.append(header)
        return headers

    @classmethod
    def create_temp_csv(cls, rows: list[dict]) -> str:
        """Write row dictionaries to a temporary CSV file and return its path."""
        if not rows:
            raise ValueError("Cannot create a temporary CSV from an empty row set.")

        headers = cls._derive_headers(rows)
        if not headers:
            raise ValueError("Cannot create a temporary CSV because no headers were found.")

        try:
            with NamedTemporaryFile(
                mode="w",
                newline="",
                encoding="utf-8",
                suffix=".csv",
                prefix="import_",
                delete=False,
            ) as handle:
                writer = csv.DictWriter(handle, fieldnames=headers)
                writer.writeheader()
                for row in rows:
                    writer.writerow(
                        {
                            header: DataCleaner.clean(row.get(header, ""))
                            for header in headers
                        }
                    )
                return str(Path(handle.name))
        except OSError as exc:
            raise RuntimeError(f"Failed to create temporary CSV file: {exc}") from exc

    @staticmethod
    def cleanup_temp_file(file_path: str) -> None:
        """Delete a temporary file if it exists."""
        if not file_path:
            return
        try:
            Path(file_path).unlink(missing_ok=True)
        except OSError:
            return

    @staticmethod
    def validate_filename(filename: str) -> str:
        """Return a safe CSV filename for user-facing import workflows."""
        cleaned = DataCleaner.clean(filename)
        if not cleaned:
            cleaned = "import"

        safe_characters = []
        for char in cleaned:
            if char.isalnum() or char in {"-", "_", " ", "."}:
                safe_characters.append(char)

        normalized = "".join(safe_characters).strip(" .")
        normalized = normalized.replace(" ", "_")
        normalized = "_".join(part for part in normalized.split("_") if part)
        if not normalized:
            normalized = "import"

        stem = Path(normalized).stem or "import"
        return f"{stem}.csv"


__all__ = [
    "FileManager",
]
