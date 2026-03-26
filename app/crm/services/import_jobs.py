"""Background job helpers for frontend import processing."""

from __future__ import annotations

import csv
from pathlib import Path

from django.db import transaction
from django.utils import timezone

from crm.models import ImportFile
from crm.services.import_workflow import (
    build_import_result_summary,
    import_csv_with_mapping,
)

IMPORT_PROGRESS_INTERVAL = 25


def count_csv_rows(csv_path: str | Path) -> int:
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return sum(1 for _ in reader)


def queue_import_job(
    *,
    file_name: str,
    source_path: str | Path,
    mapping: dict[str, str],
    total_rows: int,
    original_source_path: str | Path | None = None,
    original_source_name: str = "",
) -> ImportFile:
    normalized_source_path = str(Path(source_path))
    import_file, _ = ImportFile.objects.get_or_create(
        file_name=file_name,
        defaults={"source_path": normalized_source_path},
    )
    normalized_original_source_path = (
        str(Path(original_source_path)) if original_source_path else ""
    )
    import_file.source_path = normalized_source_path
    import_file.original_source_path = normalized_original_source_path
    import_file.original_source_name = original_source_name or ""
    import_file.status = ImportFile.Status.QUEUED
    import_file.mapping = mapping
    import_file.total_rows = total_rows
    import_file.processed_rows = 0
    import_file.result_summary = {}
    import_file.error_message = ""
    import_file.started_at = None
    import_file.completed_at = None
    import_file.save(
        update_fields=[
            "source_path",
            "original_source_path",
            "original_source_name",
            "status",
            "mapping",
            "total_rows",
            "processed_rows",
            "result_summary",
            "error_message",
            "started_at",
            "completed_at",
            "updated_at",
        ]
    )
    return import_file


@transaction.atomic
def claim_next_import_job() -> ImportFile | None:
    import_file = (
        ImportFile.objects.select_for_update(skip_locked=True)
        .filter(status=ImportFile.Status.QUEUED)
        .order_by("updated_at", "id")
        .first()
    )
    if not import_file:
        return None

    import_file.status = ImportFile.Status.RUNNING
    import_file.started_at = timezone.now()
    import_file.completed_at = None
    import_file.error_message = ""
    import_file.processed_rows = 0
    import_file.result_summary = {}
    import_file.save(
        update_fields=[
            "status",
            "started_at",
            "completed_at",
            "error_message",
            "processed_rows",
            "result_summary",
            "updated_at",
        ]
    )
    return import_file


def persist_import_job_progress(
    import_file: ImportFile,
    stats: dict[str, object],
    *,
    final: bool = False,
) -> None:
    processed_rows = int(stats.get("rows_processed", 0) or 0)
    if not final and processed_rows == 0:
        return

    ImportFile.objects.filter(pk=import_file.pk).update(
        processed_rows=processed_rows,
        updated_at=timezone.now(),
    )


def complete_import_job(import_file: ImportFile, stats: dict[str, object]) -> dict[str, object]:
    summary = build_import_result_summary(stats)
    processed_rows = int(summary.get("rows_processed", 0) or 0)
    ImportFile.objects.filter(pk=import_file.pk).update(
        status=ImportFile.Status.COMPLETED,
        processed_rows=processed_rows,
        total_rows=max(import_file.total_rows, processed_rows),
        result_summary=summary,
        error_message="",
        completed_at=timezone.now(),
        updated_at=timezone.now(),
    )
    return summary


def fail_import_job(import_file: ImportFile, exc: Exception) -> None:
    ImportFile.objects.filter(pk=import_file.pk).update(
        status=ImportFile.Status.FAILED,
        error_message=str(exc),
        completed_at=timezone.now(),
        updated_at=timezone.now(),
    )


def process_import_job(import_file: ImportFile) -> dict[str, object]:
    csv_path = Path(import_file.source_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Queued import source path does not exist: {csv_path}")

    _, stats = import_csv_with_mapping(
        csv_path=csv_path,
        file_name=import_file.file_name,
        mapping=import_file.mapping or {},
        source_path=import_file.source_path,
        import_file=import_file,
        progress_callback=persist_import_job_progress,
        progress_interval=IMPORT_PROGRESS_INTERVAL,
    )
    complete_import_job(import_file, stats)
    return stats
