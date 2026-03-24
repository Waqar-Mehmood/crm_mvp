"""Import workflow views."""

from __future__ import annotations

from pathlib import Path
import re

from django.contrib import messages
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.files.uploadedfile import UploadedFile
from django.db.models import Count
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required
from crm.models import ImportFile
from crm.services.import_components import (
    FileManager,
    ImportSessionManager,
    MappingBuilder,
    UploadHandler,
)
from crm.services.import_jobs import count_csv_rows, queue_import_job
from crm.services.import_workflow import (
    TARGET_FIELDS,
)
from crm.services.import_service import (
    detect_import_source_type,
    get_row_headers,
)
from crm.upload_storage import save_import_upload
from ._shared import (
    PAGE_SIZE,
    _add_active_filter,
    _clean_text,
    _page_query,
    _paginate,
    _parse_date_value,
    _query_items,
    _query_string,
)

SOURCE_TYPE_LABELS = {
    "csv": "CSV file",
    "xlsx": "Excel workbook",
    "json": "JSON file",
    "google_sheets": "Google Sheets",
}

FIELD_REQUIREMENTS = {
    "company_name": "Needed to create or update company records.",
    "contact_name": "Needed for contacts unless both first and last name are mapped.",
    "contact_first_name": "Map this together with Last Name when there is no full-name column.",
    "contact_last_name": "Map this together with First Name when there is no full-name column.",
    "email": "Optional, but useful for matching or enriching contacts.",
    "phone": "Optional, but useful for matching or enriching contacts.",
}

STAGED_IMPORTS_SESSION_KEY = "import_staged_sources"
ACTIVE_IMPORT_JOB_SESSION_KEY = "import_active_job_id"
IMPORT_FILTER_KEYS = frozenset({"q", "status", "updated_from", "updated_to"})
IMPORT_STATUS_LABELS = dict(ImportFile.Status.choices)
LEGACY_STAGED_SOURCE_KEYS = (
    "import_csv_temp_path",
    "import_csv_original_name",
    "import_csv_headers",
    "import_source_type",
)

DISPLAY_NAME_PREFIX_PATTERN = re.compile(
    r"^(?:\d+\s*[._-]+\s*)?(?:template|worksheet|sheet)(?=$|[\s._-])[\s._-]*",
    re.IGNORECASE,
)
DISPLAY_NAME_SUFFIX_PATTERN = re.compile(
    r"[\s._-]*\d{8}[_-]\d{6}$",
)


def _clean_import_status(value: str | None) -> str:
    value = _clean_text(value).lower()
    return value if value in IMPORT_STATUS_LABELS else ""


def _default_import_display_name(original_name: str) -> str:
    """Derive a human-friendly default display name from an uploaded file name."""
    stem = Path(original_name or "").stem.strip()
    if not stem:
        return "Import file"

    cleaned = DISPLAY_NAME_SUFFIX_PATTERN.sub("", stem)
    cleaned = DISPLAY_NAME_PREFIX_PATTERN.sub("", cleaned)
    cleaned = cleaned.strip(" ._-")
    cleaned = cleaned.replace("_", " ")
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or stem


def _delete_staged_paths(paths: list[str | Path]) -> None:
    for raw_path in {str(path) for path in paths if path}:
        FileManager.cleanup_temp_file(raw_path)


def _cleanup_staged_entries(entries: list[dict[str, object]]) -> None:
    _delete_staged_paths(
        path
        for entry in entries
        for path in entry.get("cleanup_paths", [])
    )


def _clear_legacy_staged_source(request: HttpRequest) -> None:
    for key in LEGACY_STAGED_SOURCE_KEYS:
        request.session.pop(key, None)


def _sync_legacy_staged_source(
    request: HttpRequest,
    entry: dict[str, object],
) -> None:
    request.session["import_csv_temp_path"] = entry["temp_path"]
    request.session["import_csv_original_name"] = entry["original_name"]
    request.session["import_csv_headers"] = entry["headers"]
    request.session["import_source_type"] = entry["source_type"]


def _legacy_staged_entry_from_session(
    request: HttpRequest,
) -> dict[str, object] | None:
    temp_path = request.session.get("import_csv_temp_path")
    headers = request.session.get("import_csv_headers", [])
    if not temp_path or not headers:
        return None

    original_name = request.session.get("import_csv_original_name", "")
    source_type = request.session.get("import_source_type") or detect_import_source_type(
        filename=original_name
    )
    return {
        "temp_path": temp_path,
        "original_name": original_name,
        "headers": headers,
        "source_type": source_type,
        "queue_position": 1,
        "queue_total": 1,
        "cleanup_paths": [temp_path],
    }


def _get_staged_queue(request: HttpRequest) -> list[dict[str, object]]:
    queue = ImportSessionManager.get_staged_queue(request)
    if queue:
        return queue

    legacy_entry = _legacy_staged_entry_from_session(request)
    if not legacy_entry:
        return []

    request.session[STAGED_IMPORTS_SESSION_KEY] = [legacy_entry]
    request.session.modified = True
    return [legacy_entry]


def _set_staged_queue(
    request: HttpRequest,
    entries: list[dict[str, object]],
) -> None:
    ImportSessionManager.clear_queue(request)
    if entries:
        for entry in entries:
            ImportSessionManager.add_to_queue(request, entry)
        _sync_legacy_staged_source(request, entries[0])
    else:
        _clear_legacy_staged_source(request)


def _clear_staged_queue(request: HttpRequest, *, cleanup: bool = False) -> None:
    queue = _get_staged_queue(request)
    if cleanup and queue:
        _cleanup_staged_entries(queue)
    ImportSessionManager.clear_queue(request)
    _clear_legacy_staged_source(request)
    request.session.modified = True


def _clear_active_import_job(request: HttpRequest) -> None:
    ImportSessionManager.mark_job_complete(request)


def _set_active_import_job(request: HttpRequest, import_file: ImportFile) -> None:
    ImportSessionManager.set_active_job(request, str(import_file.id))


def _build_staged_entry_from_rows(
    rows: list[dict[str, str]],
    original_name: str,
    *,
    source_type: str,
) -> dict[str, object]:
    headers = get_row_headers(rows)
    if not headers:
        raise ValueError("The uploaded file did not contain any headers or data rows.")

    temp_csv_path = FileManager.create_temp_csv(rows)
    try:
        staged_filename = FileManager.validate_filename(original_name)
        with Path(temp_csv_path).open("rb") as handle:
            staged_upload = SimpleUploadedFile(
                staged_filename,
                handle.read(),
                content_type="text/csv",
            )
        source_path = save_import_upload(staged_upload)
    finally:
        FileManager.cleanup_temp_file(temp_csv_path)

    return {
        "temp_path": str(source_path),
        "original_name": original_name,
        "headers": headers,
        "source_type": source_type,
        "cleanup_paths": [str(source_path)],
    }


def _build_staged_upload_entry(
    uploaded: UploadedFile,
    *,
    source_type_override: str | None = None,
) -> dict[str, object]:
    original_name = Path(getattr(uploaded, "name", "import.csv")).name
    processed = UploadHandler.process_uploaded_file(uploaded)
    return _build_staged_entry_from_rows(
        processed["rows"],
        original_name,
        source_type=source_type_override or processed["source_type"],
    )


def _stage_entries_for_mapping(
    request: HttpRequest,
    entries: list[dict[str, object]],
) -> HttpResponse:
    total = len(entries)
    staged_entries = []
    for index, entry in enumerate(entries, start=1):
        staged_entry = dict(entry)
        staged_entry["queue_position"] = index
        staged_entry["queue_total"] = total
        staged_entries.append(staged_entry)

    _set_staged_queue(request, staged_entries)
    return redirect("import_map_headers")


def _build_staged_entries_from_uploads(
    uploaded_files: list[UploadedFile],
) -> list[dict[str, object]]:
    original_names = [Path(getattr(uploaded, "name", "import.csv")).name for uploaded in uploaded_files]
    if len(original_names) != len(set(original_names)):
        raise ValueError("Each selected file must have a unique file name.")

    staged_entries: list[dict[str, object]] = []
    try:
        for uploaded in uploaded_files:
            staged_entries.append(_build_staged_upload_entry(uploaded))
    except Exception:
        _cleanup_staged_entries(staged_entries)
        raise

    return staged_entries


def _stage_import_upload(
    request: HttpRequest,
    uploaded: UploadedFile,
    *,
    source_type_override: str | None = None,
) -> HttpResponse:
    """Persist an uploaded import source and stage a CSV for the mapping flow."""
    entry = _build_staged_upload_entry(uploaded, source_type_override=source_type_override)
    return _stage_entries_for_mapping(request, [entry])


def _stage_parsed_rows_for_mapping(
    request: HttpRequest,
    rows: list[dict[str, str]],
    filename: str,
    *,
    source_type: str,
) -> HttpResponse:
    """Normalize parsed rows into the existing staged CSV mapping flow."""
    entry = _build_staged_entry_from_rows(rows, filename, source_type=source_type)
    return _stage_entries_for_mapping(request, [entry])


def _build_mapping_fields(headers: list[str]) -> list[dict[str, str | bool]]:
    """Build mapping field metadata with reusable suggestion state."""
    mapping_fields = [
        {
            "key": field["target_field"],
            "label": field["label"],
            "suggested": field["suggested_column"],
            "selected": field["suggested_column"],
            "requirement": FIELD_REQUIREMENTS.get(field["target_field"], "Optional field."),
            "status_label": "Suggested" if field["suggested_column"] else "Review",
            "status_tone": "suggested" if field["suggested_column"] else "review",
        }
        for field in MappingBuilder.build_mapping_fields(headers)
    ]
    return mapping_fields


def _apply_selected_mapping(
    mapping_fields: list[dict[str, str | bool]],
    mapping: dict[str, str],
) -> list[dict[str, str | bool]]:
    updated_fields = []
    for field in mapping_fields:
        selected = (mapping.get(field["key"], "") or "").strip()
        updated_field = dict(field)
        updated_field["selected"] = selected
        updated_field["status_label"] = "Suggested" if selected else "Review"
        updated_field["status_tone"] = "suggested" if selected else "review"
        updated_fields.append(updated_field)
    return updated_fields


def _build_preview_rows(
    rows: list[dict[str, str]], limit: int = PAGE_SIZE
) -> tuple[list[str], list[dict[str, str]]]:
    if not rows:
        return [], []

    headers = list(rows[0].keys())
    preview_rows = [
        {header: row.get(header, "") for header in headers}
        for row in rows[:limit]
    ]
    return headers, preview_rows


def _import_file_list_state(request: HttpRequest) -> dict[str, object]:
    import_filters = {
        "q": _clean_text(request.GET.get("q")),
        "status": _clean_import_status(request.GET.get("status")),
        "updated_from": _clean_text(request.GET.get("updated_from")),
        "updated_to": _clean_text(request.GET.get("updated_to")),
    }
    total_imports = ImportFile.objects.count()
    has_import_records = total_imports > 0

    import_files_qs = (
        ImportFile.objects
        .annotate(stored_rows=Count("rows"))
        .order_by("-updated_at", "-id")
    )

    if import_filters["q"]:
        import_files_qs = import_files_qs.filter(
            file_name__icontains=import_filters["q"]
        )
    if import_filters["status"]:
        import_files_qs = import_files_qs.filter(status=import_filters["status"])

    updated_from = _parse_date_value(import_filters["updated_from"])
    updated_to = _parse_date_value(import_filters["updated_to"])
    if updated_from:
        import_files_qs = import_files_qs.filter(updated_at__date__gte=updated_from)
    if updated_to:
        import_files_qs = import_files_qs.filter(updated_at__date__lte=updated_to)

    active_filters = []
    _add_active_filter(active_filters, "Search", import_filters["q"])
    _add_active_filter(
        active_filters,
        "Status",
        IMPORT_STATUS_LABELS.get(import_filters["status"], ""),
    )
    if updated_from:
        _add_active_filter(
            active_filters,
            "Updated from",
            import_filters["updated_from"],
        )
    if updated_to:
        _add_active_filter(
            active_filters,
            "Updated to",
            import_filters["updated_to"],
        )

    filter_reset_query = _query_string(
        request,
        remove_keys=IMPORT_FILTER_KEYS | {"page"},
    )
    filter_reset_url = reverse("import_file_list")
    if filter_reset_query:
        filter_reset_url = f"{filter_reset_url}?{filter_reset_query}"

    return {
        "queryset": import_files_qs,
        "filters": import_filters,
        "filters_active": bool(active_filters),
        "active_filters": active_filters,
        "total_imports": total_imports,
        "has_import_records": has_import_records,
        "status_options": ImportFile.Status.choices,
        "filter_form_hidden_items": _query_items(
            request,
            remove_keys=IMPORT_FILTER_KEYS | {"page"},
        ),
        "filter_reset_url": filter_reset_url,
    }


@crm_role_required(ROLE_STAFF)
def import_file_list(request):
    state = _import_file_list_state(request)
    page_obj = _paginate(request, state["queryset"])
    return render(
        request,
        "crm/imports/import_file_list.html",
        {
            "import_files": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            **state,
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def import_google_sheets_preview(request: HttpRequest) -> HttpResponse:
    sheet_url = ""
    headers: list[str] = []
    preview_rows: list[dict[str, str]] = []
    total_rows = 0
    error = ""

    if request.method == "POST":
        action = _clean_text(request.POST.get("action")) or "preview"
        sheet_url = _clean_text(request.POST.get("sheet_url"))
        if not sheet_url:
            error = "Please enter a Google Sheets URL."
        else:
            try:
                from crm.services.google_sheets import extract_sheet_id

                is_valid, error_message = UploadHandler.validate_file(sheet_url)
                if not is_valid:
                    raise ValueError(error_message)

                processed = UploadHandler.process_uploaded_file(sheet_url)
                rows = processed["rows"]
                total_rows = processed["row_count"]
                headers, preview_rows = _build_preview_rows(rows)
                if action == "import":
                    sheet_id = extract_sheet_id(sheet_url)
                    filename = f"Google Sheet - {sheet_id}.csv"
                    return _stage_parsed_rows_for_mapping(
                        request,
                        rows,
                        filename,
                        source_type="google_sheets",
                    )
            except (ValueError, RuntimeError) as exc:
                error = str(exc)

    return render(
        request,
        "crm/imports/import_google_sheets.html",
        {
            "headers": headers,
            "preview_rows": preview_rows,
            "total_rows": total_rows,
            "sheet_url": sheet_url,
            "error": error,
            "preview_limit": PAGE_SIZE,
        },
    )


@crm_role_required(ROLE_STAFF)
def import_file_detail(request, file_id):
    import_file = get_object_or_404(ImportFile, pk=file_id)
    staged_queue = _get_staged_queue(request)
    active_import_job_id = ImportSessionManager.get_active_job(request)

    if active_import_job_id == str(import_file.id):
        if import_file.status == ImportFile.Status.COMPLETED:
            _clear_active_import_job(request)
            if staged_queue:
                next_entry = staged_queue[0]
                messages.success(
                    request,
                    (
                        f"Imported {import_file.file_name}. Continue with file "
                        f"{next_entry.get('queue_position', 1)} of {next_entry.get('queue_total', 1)}."
                    ),
                )
                return redirect("import_map_headers")
        elif import_file.status == ImportFile.Status.FAILED:
            _clear_active_import_job(request)

    rows_qs = (
        import_file.rows
        .select_related("company", "contact")
        .order_by("row_number")
    )
    page_obj = _paginate(request, rows_qs)
    return render(
        request,
        "crm/imports/import_file_detail.html",
        {
            "import_file": import_file,
            "import_result": import_file.result_summary or None,
            "rows": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "should_auto_refresh": import_file.status in {ImportFile.Status.QUEUED, ImportFile.Status.RUNNING},
            "staged_queue_remaining": len(staged_queue),
            "is_active_import_job": active_import_job_id == str(import_file.id),
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def import_upload(request):
    if request.method == "GET" and request.GET.get("reset_queue"):
        _clear_staged_queue(request, cleanup=True)
        _clear_active_import_job(request)

    if request.method == "POST":
        uploaded_files = [
            uploaded
            for uploaded in request.FILES.getlist("csv_file")
            if getattr(uploaded, "name", "")
        ]
        sheet_url = _clean_text(request.POST.get("sheet_url"))

        if not uploaded_files and not sheet_url:
            return render(
                request,
                "crm/imports/import_upload.html",
                {
                    "error": "Choose an import file or enter a Google Sheets URL.",
                    "sheet_url": sheet_url,
                },
            )
        if uploaded_files and sheet_url:
            return render(
                request,
                "crm/imports/import_upload.html",
                {
                    "error": "Choose either an import file or a Google Sheets URL, not both.",
                    "sheet_url": sheet_url,
                },
            )
        try:
            if uploaded_files:
                staged_entries = _build_staged_entries_from_uploads(uploaded_files)
                return _stage_entries_for_mapping(request, staged_entries)

            from crm.services.google_sheets import extract_sheet_id

            is_valid, error_message = UploadHandler.validate_file(sheet_url)
            if not is_valid:
                raise ValueError(error_message)

            processed = UploadHandler.process_uploaded_file(sheet_url)
            rows = processed["rows"]
            sheet_id = extract_sheet_id(sheet_url)
            filename = f"Google Sheet - {sheet_id}.csv"
            return _stage_parsed_rows_for_mapping(
                request,
                rows,
                filename,
                source_type="google_sheets",
            )
        except (ValueError, RuntimeError, UnicodeDecodeError) as exc:
            return render(
                request,
                "crm/imports/import_upload.html",
                {
                    "error": str(exc),
                    "sheet_url": sheet_url,
                },
            )

    return render(request, "crm/imports/import_upload.html")


@crm_role_required(ROLE_TEAM_LEAD)
def import_map_headers(request):
    staged_queue = _get_staged_queue(request)
    if not staged_queue:
        return redirect("import_upload")

    current_entry = staged_queue[0]
    temp_path = current_entry["temp_path"]
    original_name = current_entry["original_name"]
    display_name = _default_import_display_name(original_name)
    headers = current_entry["headers"]
    source_type = current_entry["source_type"]
    queue_position = current_entry.get("queue_position", 1)
    queue_total = current_entry.get("queue_total", 1)

    mapping_fields = _build_mapping_fields(headers)
    if request.method == "POST":
        file_name = (request.POST.get("file_name") or display_name).strip() or display_name
        mapping = {}
        for key in TARGET_FIELDS:
            selected = (request.POST.get(f"map_{key}") or "").strip()
            mapping[key] = selected

        is_valid, error_message = MappingBuilder.validate_user_mapping(mapping)
        if not is_valid:
            return render(
                request,
                "crm/imports/import_map_headers.html",
                {
                    "original_name": original_name,
                    "display_name": file_name,
                    "headers": headers,
                    "mapping_fields": _apply_selected_mapping(mapping_fields, mapping),
                    "source_type_label": SOURCE_TYPE_LABELS.get(source_type, "Import source"),
                    "queue_position": queue_position,
                    "queue_total": queue_total,
                    "error": error_message,
                },
            )

        import_file = queue_import_job(
            file_name=file_name,
            source_path=temp_path,
            mapping=mapping,
            total_rows=count_csv_rows(temp_path),
        )
        _set_active_import_job(request, import_file)

        remaining_queue = staged_queue[1:]
        _set_staged_queue(request, remaining_queue)
        if remaining_queue:
            messages.success(
                request,
                (
                    f"Queued {file_name} for background import. "
                    "The next staged file will open after this import completes."
                ),
            )
        else:
            messages.success(request, f"Queued {file_name} for background import.")
        return redirect("import_file_detail", file_id=import_file.id)

    return render(
        request,
        "crm/imports/import_map_headers.html",
        {
            "original_name": original_name,
            "display_name": display_name,
            "headers": headers,
            "mapping_fields": mapping_fields,
            "source_type_label": SOURCE_TYPE_LABELS.get(source_type, "Import source"),
            "queue_position": queue_position,
            "queue_total": queue_total,
        },
    )
