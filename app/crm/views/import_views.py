"""Import workflow views."""

from __future__ import annotations

from pathlib import Path

from django.core.files.uploadedfile import UploadedFile
from django.db.models import Count
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required
from crm.import_utils import (
    TARGET_FIELDS,
    build_import_result_summary,
    import_csv_with_mapping,
    suggest_mapping,
)
from crm.models import ImportFile
from crm.services.import_parsers import detect_csv_headers, parse_csv_file
from crm.services.import_service import (
    detect_import_source_type,
    get_row_headers,
    parse_rows_from_source,
    rows_to_uploaded_csv,
    select_import_parser,
)
from crm.upload_storage import save_import_upload
from .misc_views import PAGE_SIZE, _clean_text, _page_query, _paginate

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


def _stage_import_upload(
    request: HttpRequest,
    uploaded: UploadedFile,
    *,
    source_type_override: str | None = None,
) -> HttpResponse:
    """Persist an uploaded import source and stage a CSV for the mapping flow."""
    original_name = Path(getattr(uploaded, "name", "import.csv")).name
    source_path = save_import_upload(uploaded)
    resolved_source_type = detect_import_source_type(
        source=source_path,
        source_type=source_type_override,
        filename=original_name,
    )
    parser = select_import_parser(
        source=source_path,
        source_type=source_type_override,
        filename=original_name,
    )

    if parser is parse_csv_file:
        temp_path = source_path
        headers = detect_csv_headers(temp_path)
    else:
        rows = parse_rows_from_source(source_path, filename=original_name)
        headers = get_row_headers(rows)
        if not headers:
            raise ValueError("The uploaded file did not contain any headers or data rows.")

        staged_csv_name = f"{Path(original_name).stem}.csv"
        staged_upload = rows_to_uploaded_csv(rows, filename=staged_csv_name)
        temp_path = save_import_upload(staged_upload)

    if not headers:
        raise ValueError("The uploaded file did not contain any headers.")

    request.session["import_csv_temp_path"] = str(temp_path)
    request.session["import_csv_original_name"] = original_name
    request.session["import_csv_headers"] = headers
    request.session["import_source_type"] = resolved_source_type
    return redirect("import_map_headers")


def _stage_parsed_rows_for_mapping(
    request: HttpRequest,
    rows: list[dict[str, str]],
    filename: str,
    *,
    source_type: str,
) -> HttpResponse:
    """Normalize parsed rows into the existing staged CSV mapping flow."""
    uploaded = rows_to_uploaded_csv(rows, filename=filename)
    return _stage_import_upload(request, uploaded, source_type_override=source_type)


def _build_mapping_fields(headers: list[str]) -> list[dict[str, str | bool]]:
    """Build mapping field metadata with reusable suggestion state."""
    target_labels = {
        "company_name": "Company Name",
        "industry": "Industry / Business Type",
        "company_size": "Company Size",
        "revenue": "Revenue",
        "website": "Website / Company URL",
        "contact_name": "Contact Full Name",
        "contact_first_name": "Contact First Name",
        "contact_last_name": "Contact Last Name",
        "contact_title": "Contact Title",
        "email": "Email",
        "phone": "Phone",
        "person_source": "Person Source / Profile URL",
        "address": "Address / Location",
        "city": "City",
        "state": "State",
        "zip_code": "Zip Code",
        "country": "Country",
    }
    suggested = suggest_mapping(headers)

    mapping_fields = [
        {
            "key": key,
            "label": target_labels.get(key, key),
            "suggested": suggested.get(key, ""),
            "selected": suggested.get(key, ""),
            "requirement": FIELD_REQUIREMENTS.get(key, "Optional field."),
            "status_label": "Suggested" if suggested.get(key, "") else "Review",
            "status_tone": "suggested" if suggested.get(key, "") else "review",
        }
        for key in TARGET_FIELDS
    ]
    return sorted(mapping_fields, key=lambda field: (field["selected"] == "",))


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


@crm_role_required(ROLE_STAFF)
def import_file_list(request):
    import_files_qs = (
        ImportFile.objects
        .annotate(total_rows=Count("rows"))
        .order_by("-updated_at", "-id")
    )
    page_obj = _paginate(request, import_files_qs)
    return render(
        request,
        "crm/imports/import_file_list.html",
        {
            "import_files": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
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

                rows = parse_rows_from_source(sheet_url)
                total_rows = len(rows)
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
    import_result = request.session.get("import_result_summary")
    if import_result and import_result.get("import_file_id") != import_file.id:
        import_result = None
    elif import_result:
        request.session.pop("import_result_summary", None)

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
            "import_result": import_result,
            "rows": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def import_upload(request):
    if request.method == "POST":
        uploaded = request.FILES.get("csv_file")
        sheet_url = _clean_text(request.POST.get("sheet_url"))

        if not uploaded and not sheet_url:
            return render(
                request,
                "crm/imports/import_upload.html",
                {
                    "error": "Choose an import file or enter a Google Sheets URL.",
                    "sheet_url": sheet_url,
                },
            )
        if uploaded and sheet_url:
            return render(
                request,
                "crm/imports/import_upload.html",
                {
                    "error": "Choose either an import file or a Google Sheets URL, not both.",
                    "sheet_url": sheet_url,
                },
            )
        try:
            if uploaded:
                return _stage_import_upload(request, uploaded)

            from crm.services.google_sheets import extract_sheet_id

            rows = parse_rows_from_source(sheet_url)
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
    temp_path = request.session.get("import_csv_temp_path")
    original_name = request.session.get("import_csv_original_name", "")
    headers = request.session.get("import_csv_headers", [])
    source_type = request.session.get("import_source_type") or detect_import_source_type(
        filename=original_name
    )
    if not temp_path or not headers:
        return redirect("import_upload")

    mapping_fields = _build_mapping_fields(headers)
    if request.method == "POST":
        file_name = (request.POST.get("file_name") or original_name).strip() or original_name
        mapping = {}
        for key in TARGET_FIELDS:
            selected = (request.POST.get(f"map_{key}") or "").strip()
            mapping[key] = selected

        import_file, stats = import_csv_with_mapping(
            csv_path=temp_path,
            file_name=file_name,
            mapping=mapping,
            source_path=temp_path,
        )
        request.session["import_result_summary"] = {
            **build_import_result_summary(stats),
            "import_file_id": import_file.id,
        }

        request.session.pop("import_csv_temp_path", None)
        request.session.pop("import_csv_original_name", None)
        request.session.pop("import_csv_headers", None)
        request.session.pop("import_source_type", None)
        return redirect("import_file_detail", file_id=import_file.id)

    return render(
        request,
        "crm/imports/import_map_headers.html",
        {
            "original_name": original_name,
            "headers": headers,
            "mapping_fields": mapping_fields,
            "source_type_label": SOURCE_TYPE_LABELS.get(source_type, "Import source"),
        },
    )
