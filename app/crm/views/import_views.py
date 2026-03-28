"""Import workflow views."""

from __future__ import annotations

import base64
import json
from mimetypes import guess_type
from pathlib import Path
import re

from django.contrib import messages
from django.core.files.uploadedfile import SimpleUploadedFile
from django.core.files.uploadedfile import UploadedFile
from django.db.models import Count, Q
from django.http import FileResponse, Http404, HttpRequest, HttpResponse
from django.core.paginator import Paginator
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse
from django.utils import timezone

from crm.auth import (
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    crm_role_required,
    user_has_minimum_crm_role,
)
from crm.models import ImportFile
from crm.services.import_components import (
    FileManager,
    ImportSessionManager,
    MappingBuilder,
    UploadHandler,
)
from crm.services.import_jobs import count_csv_rows, queue_import_job
from crm.services.import_rows import annotate_import_row_payload_values, import_row_annotation_name
from crm.services.import_source_preview import (
    build_json_preview,
    build_tabular_preview,
    filter_tabular_preview_rows,
    resolve_preview_source,
)
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
    PAGE_SIZE_OPTIONS,
    _add_active_filter,
    _clean_export_format,
    _clean_per_page,
    _clean_text,
    _export_query,
    _export_response,
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
GOOGLE_SHEETS_PREVIEW_SESSION_KEY = "import_google_sheets_preview"
IMPORT_FILTER_KEYS = frozenset({"q", "status", "updated_from", "updated_to"})
RAW_PREVIEW_FILTER_KEYS = frozenset({"q"})
TABULAR_PREVIEW_SORT_DEFAULT = "col_0"
MAPPING_FILTER_KEYS = frozenset({"q", "mapping_state", "requirement"})
MAPPING_SORT_KEYS = frozenset({"crm_field", "status", "source_column"})
MAPPING_DEFAULT_SORT = "crm_field"
MAPPING_DEFAULT_DIRECTION = "asc"
FAILED_ROWS_FILTER_KEYS = frozenset({"failed_q"})
FAILED_ROWS_SORT_KEYS = frozenset({"row_number", "reason"})
FAILED_ROWS_DEFAULT_SORT = "row_number"
FAILED_ROWS_DEFAULT_DIRECTION = "asc"
CAPTURED_ROWS_FILTER_KEYS = frozenset({"rows_q"})
CAPTURED_ROWS_SORT_KEYS = frozenset(
    {
        "row_number",
        "company_name",
        "website",
        "contact_name",
        "contact_title",
        "email_address",
        "phone_number",
        "person_source",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
    }
)
CAPTURED_ROWS_DEFAULT_SORT = "row_number"
CAPTURED_ROWS_DEFAULT_DIRECTION = "asc"
IMPORT_STATUS_LABELS = dict(ImportFile.Status.choices)
IMPORT_SORT_KEYS = frozenset({"file_name", "status", "stored_rows", "updated_at"})
IMPORT_DEFAULT_SORT = "updated_at"
IMPORT_DEFAULT_DIRECTION = "desc"
IMPORT_TABLE_CELL_TEMPLATES = {
    "row": "crm/components/list_workspace/cells/text.html",
    "file": "crm/components/import_list/cells/file.html",
    "status": "crm/components/import_list/cells/status.html",
    "stored_rows": "crm/components/list_workspace/cells/text.html",
    "updated_at": "crm/components/list_workspace/cells/text.html",
    "actions": "crm/components/list_workspace/cells/action_buttons.html",
}
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


def _clean_import_sort(value: str | None) -> str:
    value = _clean_text(value)
    return value if value in IMPORT_SORT_KEYS else IMPORT_DEFAULT_SORT


def _clean_sort_direction(value: str | None, default: str = IMPORT_DEFAULT_DIRECTION) -> str:
    value = _clean_text(value).lower()
    return value if value in {"asc", "desc"} else default


def _import_ordering(sort_key: str, direction: str) -> tuple[str, ...]:
    primary = f"-{sort_key}" if direction == "desc" else sort_key
    if sort_key == "updated_at":
        return (primary, "-id")
    return (primary, "-updated_at", "-id")


def _import_action(label: str, href: str, variant: str = "secondary") -> dict[str, str]:
    return {
        "label": label,
        "href": href,
        "variant": variant,
    }


def _status_display_label(import_file: object) -> str:
    label = getattr(import_file, "get_status_display", "")
    return label() if callable(label) else str(label)


def _format_import_timestamp(value) -> str:
    if not value:
        return ""
    return timezone.localtime(value).strftime("%Y-%m-%d %H:%M")


def _safe_count(value, default: int = 0) -> int:
    if value is None:
        return default
    count_attr = getattr(value, "count", None)
    if isinstance(count_attr, int):
        return count_attr
    count_method = count_attr
    if callable(count_method):
        try:
            result = count_method()
        except TypeError:
            result = None
        if isinstance(result, int):
            return result
    if isinstance(value, int):
        return value
    try:
        return len(value)
    except (TypeError, AttributeError):
        return default


def _encode_action_payload(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


def _button_action(
    *,
    label: str,
    icon: str,
    action_name: str,
    title: str | None = None,
    disabled: bool = False,
    data_attrs: list[tuple[str, str]] | None = None,
) -> dict[str, object]:
    payload = {
        "label": label,
        "title": title or label,
        "icon": icon,
    }
    if disabled:
        payload["disabled"] = True
        return payload
    payload["data_attrs"] = [
        {"name": "data-table-action", "value": action_name},
        *[
            {"name": name, "value": value}
            for name, value in (data_attrs or [])
            if value not in (None, "")
        ],
    ]
    return payload


def _copy_row_actions(row: dict[str, object], headers: list[str]) -> list[dict[str, object]]:
    json_payload = json.dumps(row, ensure_ascii=False, indent=2)
    tsv_payload = "\t".join(str(row.get(header, "") or "") for header in headers)
    return [
        _button_action(
            label="Copy row as JSON",
            title="Copy row as JSON",
            icon="copy",
            action_name="copy-base64",
            data_attrs=[("data-copy-base64", _encode_action_payload(json_payload))],
        ),
        _button_action(
            label="Copy row as TSV",
            title="Copy row as TSV",
            icon="copy",
            action_name="copy-base64",
            data_attrs=[("data-copy-base64", _encode_action_payload(tsv_payload))],
        ),
    ]


def _minimum_filter_panel(
    *,
    visible: bool,
    open_state: bool,
    hidden_items: list[tuple[str, str]],
    fields: list[dict[str, object]],
    active_filters: list[dict[str, str]],
    matching_count: int,
    total_count: int,
    reset_url: str,
) -> dict[str, object]:
    return {
        "visible": visible,
        "open": open_state,
        "hidden_items": hidden_items,
        "fields": fields,
        "active_filters": active_filters,
        "matching_count": matching_count,
        "total_count": total_count,
        "reset_url": reset_url,
    }


def _minimum_filter_ui(*, title: str, id_prefix: str, results_label: str, empty_results_subject: str) -> dict[str, str]:
    return {
        "kicker": "Advanced filters",
        "title": title,
        "closed_label": "Show filters",
        "open_label": "Hide filters",
        "fields_template": "crm/components/list_workspace/filter_fields.html",
        "id_prefix": id_prefix,
        "results_label": results_label,
        "empty_results_subject": empty_results_subject,
    }


def _single_search_filter_fields(*, value: str, label: str, placeholder: str) -> list[dict[str, str]]:
    return [
        {
            "name": "q",
            "label": label,
            "type": "text",
            "value": value,
            "placeholder": placeholder,
            "wrapper_class": "md:col-span-2 xl:col-span-4",
        }
    ]


def _normalize_tabular_columns(headers: list[str]) -> list[dict[str, str]]:
    return [
        {
            "key": f"col_{index}",
            "source_key": header,
            "label": header or f"Column {index + 1}",
        }
        for index, header in enumerate(headers)
    ]


def _clean_tabular_sort(value: str | None, columns: list[dict[str, str]]) -> str:
    allowed = {column["key"] for column in columns}
    value = _clean_text(value)
    if value in allowed:
        return value
    return columns[0]["key"] if columns else TABULAR_PREVIEW_SORT_DEFAULT


def _sort_tabular_rows(rows: list[dict[str, object]], sort_key: str, direction: str) -> list[dict[str, object]]:
    if not sort_key:
        return list(rows)

    def sort_value(row: dict[str, object]) -> tuple[bool, str]:
        value = row.get(sort_key, "")
        return (value in (None, ""), str(value or "").casefold())

    return sorted(rows, key=sort_value, reverse=direction == "desc")


def _tabular_table_headers(
    request: HttpRequest,
    *,
    columns: list[dict[str, str]],
    current_sort: str,
    current_direction: str,
    base_url: str,
) -> list[dict[str, object]]:
    headers = [
        {
            "key": column["key"],
            "label": column["label"],
            "is_sortable": True,
            "is_active": current_sort == column["key"],
            "direction": current_direction if current_sort == column["key"] else "",
            "aria_sort": (
                "ascending"
                if current_sort == column["key"] and current_direction == "asc"
                else "descending"
                if current_sort == column["key"]
                else "none"
            ),
            "action_label": (
                f"Sort by {column['label']} "
                f"{'descending' if not (current_sort == column['key'] and current_direction == 'asc') else 'ascending'}"
            ),
            "url": "",
        }
        for column in columns
    ]
    for header in headers:
        next_direction = "desc" if header["is_active"] and current_direction == "asc" else "asc"
        query_string = _query_string(
            request,
            remove_keys={"page", "export"},
            extra={"sort": header["key"], "direction": next_direction},
        )
        header["url"] = f"{base_url}?{query_string}" if query_string else base_url
    headers.append(
        {
            "key": "actions",
            "label": "Actions",
            "is_sortable": False,
            "aria_sort": "",
            "header_class": "w-[1%] whitespace-nowrap",
        }
    )
    return headers


def _build_import_table_headers(
    request: HttpRequest,
    current_sort: str,
    current_direction: str,
) -> list[dict[str, object]]:
    import_list_url = reverse("import_file_list")
    headers: list[dict[str, object]] = [
        {
            "key": "row",
            "label": "#",
            "is_sortable": False,
            "aria_sort": "",
            "header_class": "w-[1%] whitespace-nowrap",
        }
    ]
    sortable_headers = (
        ("file", "file_name", "File"),
        ("status", "status", "Status"),
        ("stored_rows", "stored_rows", "Rows"),
        ("updated_at", "updated_at", "Updated"),
    )
    for key, sort_key, label in sortable_headers:
        is_active = current_sort == sort_key
        next_direction = "desc" if is_active and current_direction == "asc" else "asc"
        query_string = _query_string(
            request,
            remove_keys={"page", "export"},
            extra={"sort": sort_key, "direction": next_direction},
        )
        headers.append(
            {
                "key": key,
                "label": label,
                "is_sortable": True,
                "is_active": is_active,
                "direction": current_direction if is_active else "",
                "aria_sort": (
                    "ascending"
                    if is_active and current_direction == "asc"
                    else "descending"
                    if is_active
                    else "none"
                ),
                "action_label": f"Sort by {label} { 'descending' if next_direction == 'desc' else 'ascending'}",
                "url": f"{import_list_url}?{query_string}" if query_string else import_list_url,
            }
        )
    headers.append(
        {
            "key": "actions",
            "label": "Actions",
            "is_sortable": False,
            "aria_sort": "",
            "header_class": "w-[1%] whitespace-nowrap",
        }
    )
    return headers


def _build_import_hero_metrics(page_obj, *, total_imports, filters_active):
    visible_range = "0-0"
    if page_obj.paginator.count:
        visible_range = f"{page_obj.start_index()}-{page_obj.end_index()}"

    return [
        {
            "label": "Matching imports" if filters_active else "Import sets",
            "value": page_obj.paginator.count,
            "subtext": f"of {total_imports} total" if filters_active else "",
        },
        {
            "label": "Visible rows",
            "value": visible_range,
            "subtext": "",
            "mono": True,
        },
    ]


def _build_import_hero_actions(can_import):
    actions = []
    if can_import:
        actions.append(_import_action("Upload", reverse("import_upload"), "primary"))
    actions.append(_import_action("Browse records", reverse("company_list")))
    return actions


def _build_import_filter_panel(
    *,
    has_import_records,
    filters_active,
    filters,
    filter_form_hidden_items,
    active_filters,
    total_imports,
    matching_count,
    filter_reset_url,
):
    return {
        "visible": has_import_records or filters_active,
        "open": filters_active,
        "hidden_items": filter_form_hidden_items,
        "filters": filters,
        "fields": [],
        "status_options": ImportFile.Status.choices,
        "active_filters": active_filters,
        "matching_count": matching_count,
        "total_count": total_imports,
        "reset_url": filter_reset_url,
    }


def _build_import_filter_ui():
    return {
        "kicker": "Advanced filters",
        "title": "Refine import history",
        "closed_label": "Show filters",
        "open_label": "Hide filters",
        "fields_template": "crm/components/import_list/filter_fields.html",
        "id_prefix": "import",
        "results_label": "matching imports",
        "empty_results_subject": "imports in the ledger",
    }


def _build_import_toolbar_menus(*, per_page, per_page_menu_options):
    return [
        {
            "kind": "rows",
            "label": f"Rows: {per_page}",
            "options": per_page_menu_options,
        }
    ]


def _build_import_table_ui():
    return {
        "toolbar_kicker": "Import table",
        "toolbar_title": "Stored file batches",
        "row_template": "crm/components/list_workspace/table_row.html",
        "table_class": "w-full min-w-[72rem] border-collapse",
        "scroll_shell_class": "overflow-x-auto rounded-[1.6rem] border border-brand-surface-borderSoft bg-white/78 shadow-brand-surface-inset-strong",
    }


def _build_import_row_actions(import_file: object) -> list[dict[str, object]]:
    actions: list[dict[str, object]] = []
    preview_source = getattr(import_file, "preview_source", {}) or {}
    if preview_source.get("available"):
        actions.append(
            {
                "label": "Download source file",
                "title": "Download source file",
                "href": reverse("import_file_download", args=[import_file.id]),
                "icon": "download",
            }
        )
        actions.append(
            {
                "label": "Open raw source",
                "title": "Open raw source",
                "href": reverse("import_file_raw_source", args=[import_file.id]),
                "icon": "code",
            }
        )
    else:
        actions.extend(
            [
                {
                    "label": "Stored source unavailable",
                    "title": "Stored source unavailable",
                    "icon": "download",
                    "disabled": True,
                },
                {
                    "label": "Stored source unavailable",
                    "title": "Stored source unavailable",
                    "icon": "code",
                    "disabled": True,
                },
            ]
        )

    actions.append(
        {
            "label": "View imported data",
            "title": "View imported data",
            "href": reverse("import_file_detail", args=[import_file.id]),
            "icon": "table",
        }
    )
    return actions


def _build_import_table_rows(import_files, table_headers, row_number_offset):
    rows = []
    for index, import_file in enumerate(import_files, start=row_number_offset + 1):
        progress = ""
        if getattr(import_file, "status", "") != "completed":
            progress = f"{getattr(import_file, 'processed_rows', 0)} / {getattr(import_file, 'total_rows', 0)}"

        cells = {
            "row": {
                "template": IMPORT_TABLE_CELL_TEMPLATES["row"],
                "text": index,
            },
            "file": {
                "template": IMPORT_TABLE_CELL_TEMPLATES["file"],
                "label": import_file.file_name,
            },
            "status": {
                "template": IMPORT_TABLE_CELL_TEMPLATES["status"],
                "label": _status_display_label(import_file),
                "subtext": progress,
            },
            "stored_rows": {
                "template": IMPORT_TABLE_CELL_TEMPLATES["stored_rows"],
                "text": getattr(import_file, "stored_rows", ""),
            },
            "updated_at": {
                "template": IMPORT_TABLE_CELL_TEMPLATES["updated_at"],
                "text": _format_import_timestamp(getattr(import_file, "updated_at", None)),
            },
            "actions": {
                "template": IMPORT_TABLE_CELL_TEMPLATES["actions"],
                "actions": _build_import_row_actions(import_file),
            },
        }

        rows.append(
            {
                "cells": [
                    {"key": header["key"], **cells[header["key"]]}
                    for header in table_headers
                ]
            }
        )

    return rows


def _build_import_empty_state(
    *,
    filters_active,
    has_import_records,
    active_filters,
    filter_reset_url,
    can_import,
):
    if filters_active and has_import_records:
        return {
            "kicker": "No filtered imports",
            "title": "No imports matched the current filters.",
            "description": "Adjust the active filters or clear them to return to the full import ledger.",
            "active_filters": active_filters,
            "actions": [_import_action("Clear filters", filter_reset_url, "primary")],
        }

    actions = []
    if can_import:
        actions.append(_import_action("Upload your first file", reverse("import_upload"), "primary"))
    return {
        "kicker": "No uploads yet",
        "title": "Your import ledger is still empty.",
        "description": "Bring in the first CSV to unlock the mapping workflow and start filling companies, contacts, and source rows.",
        "active_filters": [],
        "actions": actions,
    }


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
    original_source_path: str | None = None,
    original_source_name: str = "",
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

    cleanup_paths = [str(source_path)]
    if original_source_path:
        cleanup_paths.append(str(original_source_path))

    return {
        "temp_path": str(source_path),
        "original_name": original_name,
        "headers": headers,
        "source_type": source_type,
        "original_source_path": str(original_source_path) if original_source_path else "",
        "original_source_name": original_source_name or "",
        "cleanup_paths": cleanup_paths,
    }


def _build_staged_upload_entry(
    uploaded: UploadedFile,
    *,
    source_type_override: str | None = None,
) -> dict[str, object]:
    original_name = Path(getattr(uploaded, "name", "import.csv")).name
    uploaded_bytes = uploaded.read()
    if hasattr(uploaded, "seek"):
        uploaded.seek(0)

    upload_content_type = getattr(uploaded, "content_type", "application/octet-stream")
    original_source_path: Path | None = None
    try:
        original_upload = SimpleUploadedFile(
            original_name,
            uploaded_bytes,
            content_type=upload_content_type,
        )
        parse_upload = SimpleUploadedFile(
            original_name,
            uploaded_bytes,
            content_type=upload_content_type,
        )
        original_source_path = save_import_upload(original_upload)
        processed = UploadHandler.process_uploaded_file(parse_upload)
        return _build_staged_entry_from_rows(
            processed["rows"],
            original_name,
            source_type=source_type_override or processed["source_type"],
            original_source_path=str(original_source_path),
            original_source_name=original_name,
        )
    except Exception:
        if original_source_path:
            FileManager.cleanup_temp_file(original_source_path)
        raise


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
            "required": bool(field.get("required")),
            "requirement": FIELD_REQUIREMENTS.get(field["target_field"], "Optional field."),
            "requirement_key": "required" if field.get("required") else "optional",
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
        status_label, status_tone = _mapping_status_meta(updated_field)
        updated_field["status_label"] = status_label
        updated_field["status_tone"] = status_tone
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


def _set_google_sheets_preview(request: HttpRequest, *, sheet_url: str, rows: list[dict[str, str]]) -> None:
    request.session[GOOGLE_SHEETS_PREVIEW_SESSION_KEY] = {
        "sheet_url": sheet_url,
        "rows": rows,
    }
    request.session.modified = True


def _get_google_sheets_preview(request: HttpRequest) -> dict[str, object]:
    state = request.session.get(GOOGLE_SHEETS_PREVIEW_SESSION_KEY) or {}
    rows = state.get("rows")
    if not isinstance(rows, list):
        return {}
    return {
        "sheet_url": _clean_text(state.get("sheet_url")),
        "rows": rows,
        "headers": get_row_headers(rows),
        "total_rows": len(rows),
    }


def _clear_google_sheets_preview(request: HttpRequest) -> None:
    request.session.pop(GOOGLE_SHEETS_PREVIEW_SESSION_KEY, None)
    request.session.modified = True


def _mapping_state_for_field(field: dict[str, object]) -> str:
    selected = _clean_text(field.get("selected"))
    suggested = _clean_text(field.get("suggested"))
    if selected and suggested and selected == suggested:
        return "suggested"
    if selected:
        return "mapped"
    return "unmapped"


def _mapping_status_meta(field: dict[str, object]) -> tuple[str, str]:
    mapping_state = _mapping_state_for_field(field)
    if mapping_state == "suggested":
        return ("Suggested", "suggested")
    if mapping_state == "mapped":
        return ("Mapped", "mapped")
    return ("Review", "review")


def _clean_mapping_state_filter(value: str | None) -> str:
    value = _clean_text(value).lower()
    return value if value in {"mapped", "unmapped", "suggested"} else ""


def _clean_mapping_requirement_filter(value: str | None) -> str:
    value = _clean_text(value).lower()
    return value if value in {"required", "optional"} else ""


def _clean_mapping_sort(value: str | None) -> str:
    value = _clean_text(value)
    return value if value in MAPPING_SORT_KEYS else MAPPING_DEFAULT_SORT


def _import_file_list_state(request: HttpRequest) -> dict[str, object]:
    import_filters = {
        "q": _clean_text(request.GET.get("q")),
        "status": _clean_import_status(request.GET.get("status")),
        "updated_from": _clean_text(request.GET.get("updated_from")),
        "updated_to": _clean_text(request.GET.get("updated_to")),
    }
    per_page = _clean_per_page(request.GET.get("per_page"))
    sort = _clean_import_sort(request.GET.get("sort"))
    direction = _clean_sort_direction(request.GET.get("direction"))
    total_imports = ImportFile.objects.count()
    has_import_records = total_imports > 0

    import_files_qs = (
        ImportFile.objects
        .annotate(stored_rows=Count("rows"))
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

    import_files_qs = import_files_qs.order_by(*_import_ordering(sort, direction))

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
        "per_page": per_page,
        "sort": sort,
        "direction": direction,
        "filter_form_hidden_items": _query_items(
            request,
            remove_keys=IMPORT_FILTER_KEYS | {"page"},
        ),
        "filter_reset_url": filter_reset_url,
    }


def _attach_import_source_state(import_files: list[object]) -> list[object]:
    for import_file in import_files:
        setattr(import_file, "preview_source", resolve_preview_source(import_file))
    return import_files


def _raw_preview_export_columns(headers: list[str]) -> tuple[tuple[str, str], ...]:
    return tuple(
        (header, header or f"Column {index}")
        for index, header in enumerate(headers, start=1)
    )


def _raw_preview_export_base_name(
    import_file: ImportFile,
    preview_source: dict[str, object],
    *,
    selected_sheet: str = "",
) -> str:
    base_name = Path(str(preview_source.get("file_name") or import_file.file_name or "import-preview")).stem
    base_name = re.sub(r"[^A-Za-z0-9._-]+", "-", base_name).strip("-._") or "import-preview"
    if selected_sheet:
        sheet_slug = re.sub(r"[^A-Za-z0-9._-]+", "-", selected_sheet).strip("-._")
        if sheet_slug:
            return f"{base_name}-{sheet_slug}"
    return base_name


def _build_per_page_menu_options(
    request: HttpRequest,
    *,
    base_url: str,
    current_per_page: int,
    page_key: str = "page",
    extra_remove_keys: set[str] | None = None,
) -> list[dict[str, object]]:
    remove_keys = {page_key, "export"}
    if extra_remove_keys:
        remove_keys.update(extra_remove_keys)

    options = []
    for option in PAGE_SIZE_OPTIONS:
        query_string = _query_string(
            request,
            remove_keys=remove_keys,
            extra={"per_page": option},
        )
        options.append(
            {
                "value": option,
                "label": str(option),
                "is_active": option == current_per_page,
                "url": f"{base_url}?{query_string}" if query_string else base_url,
            }
        )
    return options


def _build_rows_export_toolbar(
    *,
    per_page: int,
    per_page_menu_options: list[dict[str, object]],
    export_links: list[dict[str, str]] | None = None,
) -> list[dict[str, object]]:
    menus: list[dict[str, object]] = [
        {
            "kind": "rows",
            "label": f"Rows: {per_page}",
            "options": per_page_menu_options,
        }
    ]
    if export_links:
        menus.append(
            {
                "kind": "export",
                "label": "Export",
                "links": export_links,
            }
        )
    return menus


def _build_tabular_preview_rows(
    page_rows: list[dict[str, object]],
    *,
    columns: list[dict[str, str]],
    table_headers: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows = []
    for row in page_rows:
        cells = {
            column["key"]: {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.get(column["key"], "") or "-",
            }
            for column in columns
        }
        source_row = {
            column["label"]: row.get(column["key"], "") or ""
            for column in columns
        }
        cells["actions"] = {
            "template": "crm/components/list_workspace/cells/action_buttons.html",
            "actions": _copy_row_actions(source_row, [column["label"] for column in columns]),
        }
        rows.append(
            {
                "cells": [
                    {"key": header["key"], **cells[header["key"]]}
                    for header in table_headers
                ]
            }
        )
    return rows


def _build_tabular_preview_empty_state(
    *,
    filters_active: bool,
    filter_reset_url: str,
) -> dict[str, object]:
    if filters_active:
        return {
            "kicker": "No matching rows",
            "title": "No preview rows matched the current filters.",
            "description": "Change the search filters or clear them to return to the full preview set.",
            "active_filters": [],
            "actions": [_import_action("Clear filters", filter_reset_url, "primary")],
        }
    return {
        "kicker": "No preview rows",
        "title": "No source rows are available for preview.",
        "description": "This source did not produce any table rows for the current selection.",
        "active_filters": [],
        "actions": [],
    }


@crm_role_required(ROLE_STAFF)
def import_file_list(request):
    state = _import_file_list_state(request)
    import_list_url = reverse("import_file_list")
    can_import = user_has_minimum_crm_role(request.user, ROLE_TEAM_LEAD)
    per_page_menu_options = []
    for option in PAGE_SIZE_OPTIONS:
        query_string = _query_string(
            request,
            remove_keys={"page", "export"},
            extra={"per_page": option},
        )
        per_page_menu_options.append(
            {
                "value": option,
                "label": str(option),
                "is_active": option == state["per_page"],
                "url": f"{import_list_url}?{query_string}" if query_string else import_list_url,
            }
        )

    page_obj = _paginate(request, state["queryset"], per_page=state["per_page"])
    import_files = _attach_import_source_state(list(page_obj.object_list))
    row_number_offset = page_obj.start_index() - 1 if page_obj.paginator.count else 0
    hero_metrics = _build_import_hero_metrics(
        page_obj,
        total_imports=state["total_imports"],
        filters_active=state["filters_active"],
    )
    hero_actions = _build_import_hero_actions(can_import)
    filter_panel = _build_import_filter_panel(
        has_import_records=state["has_import_records"],
        filters_active=state["filters_active"],
        filters=state["filters"],
        filter_form_hidden_items=state["filter_form_hidden_items"],
        active_filters=state["active_filters"],
        total_imports=state["total_imports"],
        matching_count=page_obj.paginator.count,
        filter_reset_url=state["filter_reset_url"],
    )
    filter_ui = _build_import_filter_ui()
    toolbar_menus = _build_import_toolbar_menus(
        per_page=state["per_page"],
        per_page_menu_options=per_page_menu_options,
    )
    table_headers = _build_import_table_headers(
        request,
        state["sort"],
        state["direction"],
    )
    table_ui = _build_import_table_ui()
    table_rows = _build_import_table_rows(import_files, table_headers, row_number_offset)
    empty_state = _build_import_empty_state(
        filters_active=state["filters_active"],
        has_import_records=state["has_import_records"],
        active_filters=state["active_filters"],
        filter_reset_url=state["filter_reset_url"],
        can_import=can_import,
    )
    table_workspace = {
        "filter_panel": filter_panel,
        "filter_ui": filter_ui,
        "toolbar_menus": toolbar_menus,
        "table_ui": table_ui,
        "table_headers": table_headers,
        "table_rows": table_rows,
        "page_obj": page_obj,
        "page_query": _page_query(request),
        "empty_state": empty_state,
        "empty_actions_template": "crm/components/list_workspace/action_stack.html",
        "empty_action_tone": "surface",
    }
    return render(
        request,
        "crm/imports/import_file_list.html",
        {
            "import_files": import_files,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "row_number_offset": row_number_offset,
            "crm_can_import": can_import,
            "hero_metrics": hero_metrics,
            "hero_actions": hero_actions,
            "filter_panel": filter_panel,
            "filter_ui": filter_ui,
            "toolbar_menus": toolbar_menus,
            "table_ui": table_ui,
            "table_rows": table_rows,
            "empty_state": empty_state,
            "table_workspace": table_workspace,
            "per_page_menu_options": per_page_menu_options,
            "table_headers": table_headers,
            **state,
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def import_google_sheets_preview(request: HttpRequest) -> HttpResponse:
    preview_state = _get_google_sheets_preview(request)
    sheet_url = str(preview_state.get("sheet_url") or "")
    headers: list[str] = list(preview_state.get("headers") or [])
    preview_rows: list[dict[str, str]] = list(preview_state.get("rows") or [])[:PAGE_SIZE]
    total_rows = int(preview_state.get("total_rows") or 0)
    error = ""
    preview_workspace: dict[str, object] | None = None

    if request.method == "POST":
        action = _clean_text(request.POST.get("action")) or "preview"
        if action == "import":
            stored_preview = _get_google_sheets_preview(request)
            rows = list(stored_preview.get("rows") or [])
            sheet_url = str(stored_preview.get("sheet_url") or _clean_text(request.POST.get("sheet_url")))
            if not sheet_url or not rows:
                error = "Preview the sheet before continuing to mapping."
            else:
                from crm.services.google_sheets import extract_sheet_id

                sheet_id = extract_sheet_id(sheet_url)
                filename = f"Google Sheet - {sheet_id}.csv"
                _clear_google_sheets_preview(request)
                return _stage_parsed_rows_for_mapping(
                    request,
                    rows,
                    filename,
                    source_type="google_sheets",
                )
        else:
            sheet_url = _clean_text(request.POST.get("sheet_url"))
            if not sheet_url:
                error = "Please enter a Google Sheets URL."
                _clear_google_sheets_preview(request)
            else:
                try:
                    is_valid, error_message = UploadHandler.validate_file(sheet_url)
                    if not is_valid:
                        raise ValueError(error_message)

                    processed = UploadHandler.process_uploaded_file(sheet_url)
                    rows = processed["rows"]
                    _set_google_sheets_preview(request, sheet_url=sheet_url, rows=rows)
                    return redirect("import_google_sheets")
                except (ValueError, RuntimeError) as exc:
                    error = str(exc)
                    _clear_google_sheets_preview(request)

    preview_state = _get_google_sheets_preview(request)
    if preview_state:
        sheet_url = str(preview_state.get("sheet_url") or "")
        headers = list(preview_state.get("headers") or [])
        total_rows = int(preview_state.get("total_rows") or 0)

        filters = {"q": _clean_text(request.GET.get("q"))}
        per_page = _clean_per_page(request.GET.get("per_page"))
        export_format = _clean_export_format(request.GET.get("export"))
        columns = _normalize_tabular_columns(headers)
        sort = _clean_tabular_sort(request.GET.get("sort"), columns)
        direction = _clean_sort_direction(request.GET.get("direction"), "asc")
        selected_column = next(
            (column for column in columns if column["key"] == sort),
            columns[0] if columns else None,
        )
        filtered_rows = filter_tabular_preview_rows(preview_state["rows"], headers, filters["q"])

        def source_row_sort_value(row: dict[str, object]) -> tuple[bool, str]:
            if not selected_column:
                return (False, "")
            source_key = selected_column["source_key"]
            value = row.get(source_key, "")
            return (value in (None, ""), str(value or "").casefold())

        sorted_rows = sorted(
            filtered_rows,
            key=source_row_sort_value,
            reverse=direction == "desc",
        )
        if export_format:
            return _export_response(
                export_format,
                "google-sheets-preview",
                "Google Sheets Preview",
                _raw_preview_export_columns(headers),
                sorted_rows,
            )

        paginator = Paginator(sorted_rows, per_page)
        page_obj = paginator.get_page(request.GET.get("page"))
        page_rows = list(page_obj.object_list)
        preview_rows = page_rows
        active_filters: list[dict[str, str]] = []
        _add_active_filter(active_filters, "Search", filters["q"])
        filter_form_hidden_items = _query_items(
            request,
            remove_keys=RAW_PREVIEW_FILTER_KEYS | {"page", "export"},
        )
        filter_reset_query = _query_string(
            request,
            remove_keys=RAW_PREVIEW_FILTER_KEYS | {"page", "export"},
        )
        preview_url = reverse("import_google_sheets")
        filter_reset_url = preview_url if not filter_reset_query else f"{preview_url}?{filter_reset_query}"
        per_page_menu_options = _build_per_page_menu_options(
            request,
            base_url=preview_url,
            current_per_page=per_page,
        )
        table_headers = _tabular_table_headers(
            request,
            columns=columns,
            current_sort=sort,
            current_direction=direction,
            base_url=preview_url,
        )
        normalized_page_rows = [
            {
                column["key"]: row.get(column["source_key"], "")
                for column in columns
            }
            for row in page_rows
        ]
        table_rows = _build_tabular_preview_rows(
            normalized_page_rows,
            columns=columns,
            table_headers=table_headers,
        )
        preview_workspace = {
            "filter_panel": _minimum_filter_panel(
                visible=bool(total_rows),
                open_state=bool(filters["q"]),
                hidden_items=filter_form_hidden_items,
                fields=_single_search_filter_fields(
                    value=filters["q"],
                    label="Search visible cells",
                    placeholder="Search any visible cell",
                ),
                active_filters=active_filters,
                matching_count=len(sorted_rows),
                total_count=total_rows,
                reset_url=filter_reset_url,
            ),
            "filter_ui": _minimum_filter_ui(
                title="Search this preview",
                id_prefix="sheet-preview",
                results_label="matching rows",
                empty_results_subject="rows in this Google Sheets preview",
            ),
            "toolbar_menus": _build_rows_export_toolbar(
                per_page=per_page,
                per_page_menu_options=per_page_menu_options,
                export_links=[
                    {"label": "Export filtered preview as CSV", "url": f"?{_export_query(request, 'csv')}"},
                    {"label": "Export filtered preview as Excel", "url": f"?{_export_query(request, 'xlsx')}"},
                ],
            ),
            "table_ui": {
                "toolbar_kicker": "Preview table",
                "toolbar_title": "Google Sheets rows",
                "row_template": "crm/components/list_workspace/table_row.html",
                "table_class": "w-full min-w-[54rem] border-collapse",
            },
            "table_headers": table_headers,
            "table_rows": table_rows,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "empty_state": _build_tabular_preview_empty_state(
                filters_active=bool(filters["q"]),
                filter_reset_url=filter_reset_url,
            ),
            "empty_actions_template": "crm/components/list_workspace/action_stack.html",
            "empty_action_tone": "surface",
        }

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
            "preview_workspace": preview_workspace,
        },
    )


def _clean_failed_rows_sort(value: str | None) -> str:
    value = _clean_text(value)
    return value if value in FAILED_ROWS_SORT_KEYS else FAILED_ROWS_DEFAULT_SORT


def _build_failed_rows_headers(
    request: HttpRequest,
    *,
    current_sort: str,
    current_direction: str,
    base_url: str,
) -> list[dict[str, object]]:
    headers = []
    for key, label in (("row_number", "Row"), ("reason", "Reason")):
        is_active = current_sort == key
        next_direction = "desc" if is_active and current_direction == "asc" else "asc"
        query_string = _query_string(
            request,
            remove_keys={"failed_page"},
            extra={"failed_sort": key, "failed_direction": next_direction},
        )
        headers.append(
            {
                "key": key,
                "label": label,
                "is_sortable": True,
                "is_active": is_active,
                "direction": current_direction if is_active else "",
                "aria_sort": (
                    "ascending"
                    if is_active and current_direction == "asc"
                    else "descending"
                    if is_active
                    else "none"
                ),
                "action_label": f"Sort by {label} {'descending' if next_direction == 'desc' else 'ascending'}",
                "url": f"{base_url}?{query_string}" if query_string else base_url,
            }
        )
    headers.append(
        {
            "key": "actions",
            "label": "Actions",
            "is_sortable": False,
            "aria_sort": "",
            "header_class": "w-[1%] whitespace-nowrap",
        }
    )
    return headers


def _build_failed_rows_table_rows(
    failed_rows: list[dict[str, object]],
    table_headers: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows = []
    for failed_row in failed_rows:
        copy_text = f"Row {failed_row['row_number']}: {failed_row['reason']}"
        cells = {
            "row_number": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": failed_row["row_number"],
            },
            "reason": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": failed_row["reason"],
            },
            "actions": {
                "template": "crm/components/list_workspace/cells/action_buttons.html",
                "actions": [
                    _button_action(
                        label="Copy failure details",
                        title="Copy failure details",
                        icon="copy",
                        action_name="copy-base64",
                        data_attrs=[("data-copy-base64", _encode_action_payload(copy_text))],
                    )
                ],
            },
        }
        rows.append(
            {
                "cells": [
                    {"key": header["key"], **cells[header["key"]]}
                    for header in table_headers
                ]
            }
        )
    return rows


def _clean_captured_rows_sort(value: str | None) -> str:
    value = _clean_text(value)
    return value if value in CAPTURED_ROWS_SORT_KEYS else CAPTURED_ROWS_DEFAULT_SORT


def _captured_rows_ordering(sort_key: str, direction: str) -> tuple[str, ...]:
    ordering_key = sort_key if sort_key == "row_number" else import_row_annotation_name(sort_key)
    primary = f"-{ordering_key}" if direction == "desc" else ordering_key
    if sort_key == "row_number":
        return (primary,)
    return (primary, "row_number")


def _build_captured_rows_headers(
    request: HttpRequest,
    *,
    current_sort: str,
    current_direction: str,
    base_url: str,
) -> list[dict[str, object]]:
    sortable_headers = (
        ("row_number", "Row"),
        ("company_name", "Company"),
        ("website", "Website"),
        ("contact_name", "Contact"),
        ("contact_title", "Title"),
        ("email_address", "Email"),
        ("phone_number", "Phone"),
        ("person_source", "Person Source"),
        ("address", "Address"),
        ("city", "City"),
        ("state", "State"),
        ("zip_code", "Zip Code"),
        ("country", "Country"),
    )
    headers: list[dict[str, object]] = []
    for key, label in sortable_headers:
        is_active = current_sort == key
        next_direction = "desc" if is_active and current_direction == "asc" else "asc"
        query_string = _query_string(
            request,
            remove_keys={"rows_page"},
            extra={"rows_sort": key, "rows_direction": next_direction},
        )
        headers.append(
            {
                "key": key,
                "label": label,
                "is_sortable": True,
                "is_active": is_active,
                "direction": current_direction if is_active else "",
                "aria_sort": (
                    "ascending"
                    if is_active and current_direction == "asc"
                    else "descending"
                    if is_active
                    else "none"
                ),
                "action_label": f"Sort by {label} {'descending' if next_direction == 'desc' else 'ascending'}",
                "url": f"{base_url}?{query_string}" if query_string else base_url,
            }
        )
    headers.append(
        {
            "key": "actions",
            "label": "Actions",
            "is_sortable": False,
            "aria_sort": "",
            "header_class": "w-[1%] whitespace-nowrap",
        }
    )
    return headers


def _build_captured_rows_table_rows(
    rows_qs,
    table_headers: list[dict[str, object]],
) -> list[dict[str, object]]:
    rows = []
    for row in rows_qs:
        cells = {
            "row_number": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.row_number,
            },
            "company_name": {
                "template": "crm/components/import_list/cells/link_or_text.html",
                "label": row.company_name or "-",
                "href": reverse("company_detail", args=[row.company_id]) if row.company_id else "",
                "external": False,
            },
            "website": {
                "template": "crm/components/import_list/cells/link_or_text.html",
                "label": row.website or "-",
                "href": row.website or "",
                "external": True,
            },
            "contact_name": {
                "template": "crm/components/import_list/cells/link_or_text.html",
                "label": row.contact_name or "-",
                "href": reverse("contact_detail", args=[row.contact_id]) if row.contact_id else "",
                "external": False,
            },
            "contact_title": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.contact_title or "-",
            },
            "email_address": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.email_address or "-",
            },
            "phone_number": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.phone_number or "-",
            },
            "person_source": {
                "template": "crm/components/import_list/cells/link_or_text.html",
                "label": "Open source" if row.person_source else "-",
                "href": row.person_source or "",
                "external": True,
            },
            "address": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.address or "-",
            },
            "city": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.city or "-",
            },
            "state": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.state or "-",
            },
            "zip_code": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.zip_code or "-",
            },
            "country": {
                "template": "crm/components/list_workspace/cells/text.html",
                "text": row.country or "-",
            },
            "actions": {
                "template": "crm/components/list_workspace/cells/action_buttons.html",
                "actions": [
                    {
                        "label": "View company",
                        "title": "View company",
                        "href": reverse("company_detail", args=[row.company_id]),
                        "icon": "view",
                    } if row.company_id else {
                        "label": "Company not linked",
                        "title": "Company not linked",
                        "icon": "view",
                        "disabled": True,
                    },
                    {
                        "label": "View contact",
                        "title": "View contact",
                        "href": reverse("contact_detail", args=[row.contact_id]),
                        "icon": "view",
                    } if row.contact_id else {
                        "label": "Contact not linked",
                        "title": "Contact not linked",
                        "icon": "view",
                        "disabled": True,
                    },
                ],
            },
        }
        rows.append(
            {
                "cells": [
                    {"key": header["key"], **cells[header["key"]]}
                    for header in table_headers
                ]
            }
        )
    return rows


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

    detail_url = reverse("import_file_detail", args=[import_file.id])
    import_result = import_file.result_summary or None
    failed_rows_workspace = None
    captured_rows_workspace = None

    if import_result is not None:
        failed_filters = {
            "failed_q": _clean_text(request.GET.get("failed_q")),
        }
        failed_sort = _clean_failed_rows_sort(request.GET.get("failed_sort"))
        failed_direction = _clean_sort_direction(request.GET.get("failed_direction"), FAILED_ROWS_DEFAULT_DIRECTION)
        failed_rows = list(import_result.get("failed_rows") or [])
        if failed_filters["failed_q"]:
            needle = failed_filters["failed_q"].casefold()
            failed_rows = [
                row for row in failed_rows
                if needle in str(row.get("row_number", "")).casefold()
                or needle in str(row.get("reason", "")).casefold()
            ]
        failed_rows = sorted(
            failed_rows,
            key=(
                (lambda row: int(row.get("row_number") or 0))
                if failed_sort == "row_number"
                else (lambda row: str(row.get("reason") or "").casefold())
            ),
            reverse=failed_direction == "desc",
        )
        failed_page_obj = _paginate(
            request,
            failed_rows,
            per_page=PAGE_SIZE,
            page_key="failed_page",
        )
        failed_active_filters: list[dict[str, str]] = []
        _add_active_filter(failed_active_filters, "Search", failed_filters["failed_q"])
        failed_filter_reset_query = _query_string(
            request,
            remove_keys=FAILED_ROWS_FILTER_KEYS | {"failed_page"},
        )
        failed_filter_reset_url = detail_url if not failed_filter_reset_query else f"{detail_url}?{failed_filter_reset_query}"
        failed_headers = _build_failed_rows_headers(
            request,
            current_sort=failed_sort,
            current_direction=failed_direction,
            base_url=detail_url,
        )
        failed_rows_workspace = {
            "filter_panel": _minimum_filter_panel(
                visible=bool(import_result.get("failed_rows_count")) or bool(failed_active_filters),
                open_state=bool(failed_active_filters),
                hidden_items=_query_items(
                    request,
                    remove_keys=FAILED_ROWS_FILTER_KEYS | {"failed_page"},
                ),
                fields=[
                    {
                        "name": "failed_q",
                        "label": "Search failed rows",
                        "type": "text",
                        "value": failed_filters["failed_q"],
                        "placeholder": "Search row number or reason",
                        "wrapper_class": "md:col-span-2 xl:col-span-4",
                    }
                ],
                active_filters=failed_active_filters,
                matching_count=len(failed_rows),
                total_count=int(import_result.get("failed_rows_count") or 0),
                reset_url=failed_filter_reset_url,
            ),
            "filter_ui": _minimum_filter_ui(
                title="Search failed rows",
                id_prefix="failed",
                results_label="matching failed rows",
                empty_results_subject="failed rows in this import",
            ),
            "toolbar_menus": [],
            "table_ui": {
                "toolbar_kicker": "Exceptions",
                "toolbar_title": "Failed rows",
                "row_template": "crm/components/list_workspace/table_row.html",
                "page_param": "failed_page",
            },
            "table_headers": failed_headers,
            "table_rows": _build_failed_rows_table_rows(list(failed_page_obj.object_list), failed_headers),
            "page_obj": failed_page_obj,
            "page_query": _page_query(request, page_key="failed_page"),
            "empty_state": {
                "kicker": "No failed rows",
                "title": "No failed rows matched the current filters." if failed_active_filters else "No failed rows were recorded for this import.",
                "description": (
                    "Change the failed-row filters or clear them to review the full failure list again."
                    if failed_active_filters
                    else "This import completed without any stored failed-row exceptions."
                ),
                "active_filters": failed_active_filters,
                "actions": (
                    [_import_action("Clear filters", failed_filter_reset_url, "primary")]
                    if failed_active_filters
                    else []
                ),
            },
            "empty_actions_template": "crm/components/list_workspace/action_stack.html",
            "empty_action_tone": "surface",
        }

    rows_filters = {
        "rows_q": _clean_text(request.GET.get("rows_q")),
    }
    rows_sort = _clean_captured_rows_sort(request.GET.get("rows_sort"))
    rows_direction = _clean_sort_direction(request.GET.get("rows_direction"), CAPTURED_ROWS_DEFAULT_DIRECTION)
    rows_qs = annotate_import_row_payload_values(
        import_file.rows.select_related("company", "contact"),
        CAPTURED_ROWS_SORT_KEYS,
    )
    if rows_filters["rows_q"]:
        query = rows_filters["rows_q"]
        query_filter = Q()
        for field_name in CAPTURED_ROWS_SORT_KEYS:
            if field_name == "row_number":
                continue
            query_filter |= Q(**{f"{import_row_annotation_name(field_name)}__icontains": query})
        rows_qs = rows_qs.filter(query_filter)
    rows_qs = rows_qs.order_by(*_captured_rows_ordering(rows_sort, rows_direction))
    rows_page_obj = _paginate(request, rows_qs, page_key="rows_page")
    rows_active_filters: list[dict[str, str]] = []
    _add_active_filter(rows_active_filters, "Search", rows_filters["rows_q"])
    rows_filter_reset_query = _query_string(
        request,
        remove_keys=CAPTURED_ROWS_FILTER_KEYS | {"rows_page"},
    )
    rows_filter_reset_url = detail_url if not rows_filter_reset_query else f"{detail_url}?{rows_filter_reset_query}"
    captured_headers = _build_captured_rows_headers(
        request,
        current_sort=rows_sort,
        current_direction=rows_direction,
        base_url=detail_url,
    )
    captured_rows_workspace = {
        "filter_panel": _minimum_filter_panel(
            visible=import_file.status == ImportFile.Status.COMPLETED or bool(rows_active_filters),
            open_state=bool(rows_active_filters),
            hidden_items=_query_items(
                request,
                remove_keys=CAPTURED_ROWS_FILTER_KEYS | {"rows_page"},
            ),
            fields=[
                {
                    "name": "rows_q",
                    "label": "Search stored rows",
                    "type": "text",
                    "value": rows_filters["rows_q"],
                    "placeholder": "Search visible row values",
                    "wrapper_class": "md:col-span-2 xl:col-span-4",
                }
            ],
            active_filters=rows_active_filters,
            matching_count=_safe_count(getattr(rows_page_obj, "paginator", None), default=len(rows_page_obj.object_list)),
            total_count=_safe_count(getattr(import_file, "rows", None), default=_safe_count(getattr(rows_page_obj, "paginator", None), default=len(rows_page_obj.object_list))),
            reset_url=rows_filter_reset_url,
        ),
        "filter_ui": _minimum_filter_ui(
            title="Search captured rows",
            id_prefix="rows",
            results_label="matching stored rows",
            empty_results_subject="stored rows in this import",
        ),
        "toolbar_menus": [],
        "table_ui": {
            "toolbar_kicker": "Rows",
            "toolbar_title": "Captured import rows",
            "row_template": "crm/components/list_workspace/table_row.html",
            "table_class": "w-full min-w-[80rem] border-collapse",
            "page_param": "rows_page",
        },
        "table_headers": captured_headers,
        "table_rows": _build_captured_rows_table_rows(rows_page_obj.object_list, captured_headers),
        "page_obj": rows_page_obj,
        "page_query": _page_query(request, page_key="rows_page"),
        "empty_state": {
            "kicker": "No stored rows",
            "title": "No stored rows matched the current filters." if rows_active_filters else "This file has no persisted row data.",
            "description": (
                "Change the stored-row filters or clear them to review the full row set again."
                if rows_active_filters
                else "Try another import or revisit the upload flow to populate mapped rows for this file."
            ),
            "active_filters": rows_active_filters,
            "actions": (
                [_import_action("Clear filters", rows_filter_reset_url, "primary")]
                if rows_active_filters
                else []
            ),
        },
        "empty_actions_template": "crm/components/list_workspace/action_stack.html",
        "empty_action_tone": "surface",
    }
    return render(
        request,
        "crm/imports/import_file_detail.html",
        {
            "import_file": import_file,
            "import_result": import_result,
            "rows": rows_page_obj.object_list,
            "page_obj": rows_page_obj,
            "page_query": _page_query(request, page_key="rows_page"),
            "should_auto_refresh": import_file.status in {ImportFile.Status.QUEUED, ImportFile.Status.RUNNING},
            "staged_queue_remaining": len(staged_queue),
            "is_active_import_job": active_import_job_id == str(import_file.id),
            "failed_rows_workspace": failed_rows_workspace,
            "captured_rows_workspace": captured_rows_workspace,
        },
    )


@crm_role_required(ROLE_STAFF)
def import_file_raw_source(request, file_id):
    import_file = get_object_or_404(ImportFile, pk=file_id)
    preview_source = resolve_preview_source(import_file)
    preview_context: dict[str, object] = {}
    preview_workspace: dict[str, object] | None = None
    page_query = ""

    if preview_source["available"]:
        if preview_source["source_type"] == "json":
            preview_context = build_json_preview(preview_source["path"])
            preview_context["is_tabular"] = False
        else:
            filters = {
                "q": _clean_text(request.GET.get("q")),
            }
            per_page = _clean_per_page(request.GET.get("per_page"))
            export_format = _clean_export_format(request.GET.get("export"))
            selected_sheet = (
                (request.GET.get("sheet") or "").strip() or None
                if preview_source["source_type"] == "xlsx"
                else None
            )

            preview_context = build_tabular_preview(
                preview_source["path"],
                source_type=str(preview_source["source_type"]),
                sheet_name=selected_sheet,
            )
            filtered_rows = filter_tabular_preview_rows(
                preview_context["rows"],
                preview_context["headers"],
                filters["q"],
            )
            columns = _normalize_tabular_columns(preview_context["headers"])
            sort = _clean_tabular_sort(request.GET.get("sort"), columns)
            direction = _clean_sort_direction(request.GET.get("direction"), "asc")
            selected_column = next(
                (column for column in columns if column["key"] == sort),
                columns[0] if columns else None,
            )

            def source_row_sort_value(row: dict[str, object]) -> tuple[bool, str]:
                if not selected_column:
                    return (False, "")
                source_key = selected_column["source_key"]
                value = row.get(source_key, "")
                return (value in (None, ""), str(value or "").casefold())

            sorted_rows = sorted(
                filtered_rows,
                key=source_row_sort_value,
                reverse=direction == "desc",
            )
            export_columns = _raw_preview_export_columns(preview_context["headers"])
            if export_format:
                return _export_response(
                    export_format,
                    _raw_preview_export_base_name(
                        import_file,
                        preview_source,
                        selected_sheet=str(preview_context.get("selected_sheet") or ""),
                    ),
                    str(preview_context.get("selected_sheet") or "Preview"),
                    export_columns,
                    sorted_rows,
                )

            paginator = Paginator(sorted_rows, per_page)
            preview_context["page_obj"] = paginator.get_page(request.GET.get("page"))
            preview_context["page_rows"] = list(preview_context["page_obj"].object_list)
            preview_context["page_row_count"] = len(preview_context["page_obj"].object_list)
            preview_context["filtered_row_count"] = len(sorted_rows)
            preview_context["total_row_count"] = preview_context["row_count"]
            preview_context["is_tabular"] = True
            preview_context["filters"] = filters
            preview_context["filters_active"] = bool(filters["q"])
            active_filters: list[dict[str, str]] = []
            _add_active_filter(active_filters, "Search", filters["q"])
            preview_context["active_filters"] = active_filters
            preview_context["per_page"] = per_page
            preview_context["filter_form_hidden_items"] = _query_items(
                request,
                remove_keys=RAW_PREVIEW_FILTER_KEYS | {"page", "export"},
            )
            preview_context["sheet_tabs"] = []

            raw_source_url = reverse("import_file_raw_source", args=[import_file.id])
            preview_context["per_page_menu_options"] = _build_per_page_menu_options(
                request,
                base_url=raw_source_url,
                current_per_page=per_page,
            )

            if preview_context["sheet_names"]:
                for sheet_name in preview_context["sheet_names"]:
                    query_string = _query_string(
                        request,
                        remove_keys={"page", "export"},
                        extra={"sheet": sheet_name},
                    )
                    preview_context["sheet_tabs"].append(
                        {
                            "name": sheet_name,
                            "is_active": preview_context["selected_sheet"] == sheet_name,
                            "url": f"{raw_source_url}?{query_string}" if query_string else raw_source_url,
                        },
                    )

            filter_reset_query = _query_string(
                request,
                remove_keys=RAW_PREVIEW_FILTER_KEYS | {"page", "export"},
            )
            preview_context["filter_reset_url"] = raw_source_url
            if filter_reset_query:
                preview_context["filter_reset_url"] = f"{raw_source_url}?{filter_reset_query}"

            preview_context["export_csv_query"] = _export_query(request, "csv")
            preview_context["export_xlsx_query"] = _export_query(request, "xlsx")
            preview_context["sort"] = sort
            preview_context["direction"] = direction

            table_headers = _tabular_table_headers(
                request,
                columns=columns,
                current_sort=sort,
                current_direction=direction,
                base_url=raw_source_url,
            )
            normalized_page_rows = [
                {
                    column["key"]: row.get(column["source_key"], "")
                    for column in columns
                }
                for row in preview_context["page_obj"].object_list
            ]
            table_rows = _build_tabular_preview_rows(
                normalized_page_rows,
                columns=columns,
                table_headers=table_headers,
            )
            filter_panel = _minimum_filter_panel(
                visible=bool(preview_context["row_count"]) or preview_context["filters_active"],
                open_state=preview_context["filters_active"],
                hidden_items=preview_context["filter_form_hidden_items"],
                fields=_single_search_filter_fields(
                    value=filters["q"],
                    label="Search visible cells",
                    placeholder="Search any visible cell",
                ),
                active_filters=active_filters,
                matching_count=preview_context["filtered_row_count"],
                total_count=preview_context["total_row_count"],
                reset_url=preview_context["filter_reset_url"],
            )
            filter_ui = _minimum_filter_ui(
                title="Search this preview",
                id_prefix="preview",
                results_label="matching rows",
                empty_results_subject="source rows in this preview",
            )
            toolbar_menus = _build_rows_export_toolbar(
                per_page=per_page,
                per_page_menu_options=preview_context["per_page_menu_options"],
                export_links=[
                    {"label": "Export filtered preview as CSV", "url": f"?{preview_context['export_csv_query']}"},
                    {"label": "Export filtered preview as Excel", "url": f"?{preview_context['export_xlsx_query']}"},
                    {"label": "Download stored source file", "url": reverse("import_file_download", args=[import_file.id])},
                ],
            )
            table_ui = {
                "toolbar_kicker": "Preview table",
                "toolbar_title": (
                    str(preview_context["selected_sheet"])
                    if preview_context.get("selected_sheet")
                    else "Source rows"
                ),
                "row_template": "crm/components/list_workspace/table_row.html",
                "table_class": "w-full min-w-[54rem] border-collapse",
                "scroll_shell_class": "overflow-x-auto rounded-[1.6rem] border border-brand-surface-borderSoft bg-white/78 shadow-brand-surface-inset-strong [-webkit-overflow-scrolling:touch]",
            }
            empty_state = _build_tabular_preview_empty_state(
                filters_active=preview_context["filters_active"],
                filter_reset_url=preview_context["filter_reset_url"],
            )
            page_query = _page_query(request)
            preview_workspace = {
                "filter_panel": filter_panel,
                "filter_ui": filter_ui,
                "toolbar_menus": toolbar_menus,
                "table_ui": table_ui,
                "table_headers": table_headers,
                "table_rows": table_rows,
                "page_obj": preview_context["page_obj"],
                "page_query": page_query,
                "empty_state": empty_state,
                "empty_actions_template": "crm/components/list_workspace/action_stack.html",
                "empty_action_tone": "surface",
            }

    return render(
        request,
        "crm/imports/import_file_raw.html",
        {
            "import_file": import_file,
            "preview_source": preview_source,
            "preview_context": preview_context,
            "preview_workspace": preview_workspace,
            "page_query": page_query,
        },
    )


@crm_role_required(ROLE_STAFF)
def import_file_download(request, file_id):
    import_file = get_object_or_404(ImportFile, pk=file_id)
    preview_source = resolve_preview_source(import_file)
    if not preview_source["available"] or not preview_source["path"]:
        raise Http404("No stored source file is available for this import.")

    download_name = str(preview_source["file_name"])
    content_type, _encoding = guess_type(download_name)
    return FileResponse(
        Path(preview_source["path"]).open("rb"),
        as_attachment=True,
        filename=download_name,
        content_type=content_type or "application/octet-stream",
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


def _mapping_filter_fields(filters: dict[str, str]) -> list[dict[str, object]]:
    return [
        {
            "name": "q",
            "label": "Search",
            "type": "text",
            "value": filters["q"],
            "placeholder": "Search CRM fields or selected source columns",
            "wrapper_class": "md:col-span-2",
        },
        {
            "name": "mapping_state",
            "label": "Mapping state",
            "type": "select",
            "options": [
                {"value": "", "label": "Any state", "selected": filters["mapping_state"] == ""},
                {"value": "suggested", "label": "Suggested", "selected": filters["mapping_state"] == "suggested"},
                {"value": "mapped", "label": "Mapped", "selected": filters["mapping_state"] == "mapped"},
                {"value": "unmapped", "label": "Unmapped", "selected": filters["mapping_state"] == "unmapped"},
            ],
        },
        {
            "name": "requirement",
            "label": "Requirement",
            "type": "select",
            "options": [
                {"value": "", "label": "Any requirement", "selected": filters["requirement"] == ""},
                {"value": "required", "label": "Required", "selected": filters["requirement"] == "required"},
                {"value": "optional", "label": "Optional", "selected": filters["requirement"] == "optional"},
            ],
        },
    ]


def _build_mapping_table_headers(
    request: HttpRequest,
    *,
    current_sort: str,
    current_direction: str,
    base_url: str,
) -> list[dict[str, object]]:
    sortable_headers = (
        ("crm_field", "CRM field"),
        ("status", "Status"),
        ("source_column", "Source column"),
    )
    headers: list[dict[str, object]] = []
    for key, label in sortable_headers:
        is_active = current_sort == key
        next_direction = "desc" if is_active and current_direction == "asc" else "asc"
        query_string = _query_string(
            request,
            remove_keys={"page", "export"},
            extra={"sort": key, "direction": next_direction},
        )
        headers.append(
            {
                "key": key,
                "label": label,
                "is_sortable": True,
                "is_active": is_active,
                "direction": current_direction if is_active else "",
                "aria_sort": (
                    "ascending"
                    if is_active and current_direction == "asc"
                    else "descending"
                    if is_active
                    else "none"
                ),
                "action_label": f"Sort by {label} {'descending' if next_direction == 'desc' else 'ascending'}",
                "url": f"{base_url}?{query_string}" if query_string else base_url,
            }
        )
    headers.append(
        {
            "key": "actions",
            "label": "Actions",
            "is_sortable": False,
            "aria_sort": "",
            "header_class": "w-[1%] whitespace-nowrap",
        }
    )
    return headers


def _mapping_status_sort_rank(field: dict[str, object]) -> int:
    return {
        "suggested": 0,
        "mapped": 1,
        "unmapped": 2,
    }.get(_mapping_state_for_field(field), 3)


def _sort_mapping_fields(
    fields: list[dict[str, object]],
    *,
    sort_key: str,
    direction: str,
) -> list[dict[str, object]]:
    def sort_value(field: dict[str, object]) -> tuple[object, ...]:
        if sort_key == "status":
            return (_mapping_status_sort_rank(field), str(field.get("label") or "").casefold())
        if sort_key == "source_column":
            return (
                _clean_text(field.get("selected") or field.get("suggested")).casefold(),
                str(field.get("label") or "").casefold(),
            )
        return (str(field.get("label") or "").casefold(),)

    return sorted(fields, key=sort_value, reverse=direction == "desc")


def _mapping_status_classes(tone: str) -> str:
    if tone == "suggested":
        return "inline-flex rounded-full bg-accent/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-accent"
    if tone == "mapped":
        return "inline-flex rounded-full bg-brand-teal/10 px-3 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-brand-teal"
    return "inline-flex rounded-full bg-slate-200/70 px-3 py-1 text-xs font-semibold uppercase tracking-[0.12em] text-brand-text-soft"


def _build_mapping_row_actions(field: dict[str, object]) -> list[dict[str, object]]:
    actions = [
        _button_action(
            label="Clear mapping",
            title="Clear mapping",
            icon="clear",
            action_name="set-select-value",
            disabled=not _clean_text(field.get("selected")),
            data_attrs=[
                ("data-target-id", f"mapping-select-{field['key']}"),
                ("data-target-value", ""),
                ("data-status-target-id", f"mapping-status-{field['key']}"),
                ("data-status-label", "Review"),
                ("data-status-tone", "review"),
            ],
        )
    ]
    actions.append(
        _button_action(
            label="Restore suggested match",
            title="Restore suggested match",
            icon="restore",
            action_name="set-select-value",
            disabled=not _clean_text(field.get("suggested")),
            data_attrs=[
                ("data-target-id", f"mapping-select-{field['key']}"),
                ("data-target-value", _clean_text(field.get("suggested"))),
                ("data-status-target-id", f"mapping-status-{field['key']}"),
                ("data-status-label", "Suggested"),
                ("data-status-tone", "suggested"),
            ],
        )
    )
    return actions


def _build_mapping_table_rows(
    mapping_fields: list[dict[str, object]],
    table_headers: list[dict[str, object]],
    headers: list[str],
) -> list[dict[str, object]]:
    rows = []
    for field in mapping_fields:
        status_label, status_tone = _mapping_status_meta(field)
        cells = {
            "crm_field": {
                "template": "crm/components/import_list/cells/mapping_field.html",
                "label": field["label"],
                "requirement": field["requirement"],
            },
            "status": {
                "template": "crm/components/import_list/cells/mapping_status.html",
                "label": status_label,
                "tone": status_tone,
                "badge_id": f"mapping-status-{field['key']}",
                "review_class": _mapping_status_classes("review"),
                "suggested_class": _mapping_status_classes("suggested"),
            },
            "source_column": {
                "template": "crm/components/import_list/cells/mapping_select.html",
                "field_id": f"mapping-select-{field['key']}",
                "name": f"map_{field['key']}",
                "selected": _clean_text(field.get("selected")),
                "options": headers,
            },
            "actions": {
                "template": "crm/components/list_workspace/cells/action_buttons.html",
                "actions": _build_mapping_row_actions(field),
            },
        }
        rows.append(
            {
                "cells": [
                    {"key": header["key"], **cells[header["key"]]}
                    for header in table_headers
                ]
            }
        )
    return rows


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

    base_mapping_fields = _build_mapping_fields(headers)

    def render_mapping_page(*, display_name_value: str, mapping_fields: list[dict[str, object]], error_message: str = ""):
        filters = {
            "q": _clean_text(request.GET.get("q")),
            "mapping_state": _clean_mapping_state_filter(request.GET.get("mapping_state")),
            "requirement": _clean_mapping_requirement_filter(request.GET.get("requirement")),
        }
        sort = _clean_mapping_sort(request.GET.get("sort"))
        direction = _clean_sort_direction(request.GET.get("direction"), MAPPING_DEFAULT_DIRECTION)
        filtered_fields = list(mapping_fields)
        if filters["q"]:
            query = filters["q"].casefold()
            filtered_fields = [
                field
                for field in filtered_fields
                if query in str(field.get("label") or "").casefold()
                or query in str(field.get("requirement") or "").casefold()
                or query in _clean_text(field.get("selected")).casefold()
                or query in _clean_text(field.get("suggested")).casefold()
            ]
        if filters["mapping_state"]:
            filtered_fields = [
                field for field in filtered_fields
                if _mapping_state_for_field(field) == filters["mapping_state"]
            ]
        if filters["requirement"]:
            filtered_fields = [
                field for field in filtered_fields
                if field.get("requirement_key") == filters["requirement"]
            ]

        filtered_fields = _sort_mapping_fields(
            filtered_fields,
            sort_key=sort,
            direction=direction,
        )
        page_obj = Paginator(filtered_fields, max(len(filtered_fields), 1)).get_page(1)
        mapping_url = reverse("import_map_headers")
        filter_form_hidden_items = _query_items(
            request,
            remove_keys=MAPPING_FILTER_KEYS | {"page", "export"},
        )
        filter_reset_query = _query_string(
            request,
            remove_keys=MAPPING_FILTER_KEYS | {"page", "export"},
        )
        filter_reset_url = mapping_url if not filter_reset_query else f"{mapping_url}?{filter_reset_query}"
        active_filters: list[dict[str, str]] = []
        _add_active_filter(active_filters, "Search", filters["q"])
        _add_active_filter(active_filters, "Mapping state", filters["mapping_state"].title())
        _add_active_filter(active_filters, "Requirement", filters["requirement"].title())
        table_headers = _build_mapping_table_headers(
            request,
            current_sort=sort,
            current_direction=direction,
            base_url=mapping_url,
        )
        table_rows = _build_mapping_table_rows(filtered_fields, table_headers, headers)
        table_workspace = {
            "filter_panel": _minimum_filter_panel(
                visible=True,
                open_state=bool(active_filters),
                hidden_items=filter_form_hidden_items,
                fields=_mapping_filter_fields(filters),
                active_filters=active_filters,
                matching_count=len(filtered_fields),
                total_count=len(mapping_fields),
                reset_url=filter_reset_url,
            ),
            "filter_ui": _minimum_filter_ui(
                title="Refine the mapping matrix",
                id_prefix="mapping",
                results_label="matching fields",
                empty_results_subject="mapping fields in this import",
            ),
            "toolbar_menus": [],
            "table_ui": {
                "toolbar_kicker": "Mapping table",
                "toolbar_title": "Column pairing",
                "row_template": "crm/components/list_workspace/table_row.html",
                "table_class": "w-full min-w-[52rem] border-collapse",
            },
            "table_headers": table_headers,
            "table_rows": table_rows,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "empty_state": {
                "kicker": "No matching fields",
                "title": "No mapping fields matched the current filters.",
                "description": "Change the active mapping filters or clear them to review the full matrix again.",
                "active_filters": active_filters,
                "actions": [_import_action("Clear filters", filter_reset_url, "primary")],
            },
            "empty_actions_template": "crm/components/list_workspace/action_stack.html",
            "empty_action_tone": "surface",
        }
        return render(
            request,
            "crm/imports/import_map_headers.html",
            {
                "original_name": original_name,
                "display_name": display_name_value,
                "headers": headers,
                "mapping_fields": mapping_fields,
                "mapping_table_workspace": table_workspace,
                "source_type_label": SOURCE_TYPE_LABELS.get(source_type, "Import source"),
                "queue_position": queue_position,
                "queue_total": queue_total,
                "error": error_message,
            },
        )

    if request.method == "POST":
        file_name = (request.POST.get("file_name") or display_name).strip() or display_name
        mapping = {}
        for key in TARGET_FIELDS:
            selected = (request.POST.get(f"map_{key}") or "").strip()
            mapping[key] = selected

        is_valid, error_message = MappingBuilder.validate_user_mapping(mapping)
        if not is_valid:
            return render_mapping_page(
                display_name_value=file_name,
                mapping_fields=_apply_selected_mapping(base_mapping_fields, mapping),
                error_message=error_message,
            )

        import_file = queue_import_job(
            file_name=file_name,
            source_path=temp_path,
            mapping=mapping,
            total_rows=count_csv_rows(temp_path),
            original_source_path=current_entry.get("original_source_path") or None,
            original_source_name=current_entry.get("original_source_name", ""),
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

    return render_mapping_page(
        display_name_value=display_name,
        mapping_fields=base_mapping_fields,
    )
