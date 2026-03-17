"""Import workflow views."""

from __future__ import annotations

import csv

from django.core.files.uploadedfile import UploadedFile
from django.db.models import Count
from django.http import HttpRequest, HttpResponse
from django.shortcuts import get_object_or_404, redirect, render

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required
from crm.import_utils import TARGET_FIELDS, import_csv_with_mapping, suggest_mapping
from crm.models import ImportFile
from crm.services.import_service import rows_to_uploaded_csv
from crm.upload_storage import save_import_upload
from .misc_views import PAGE_SIZE, _clean_text, _page_query, _paginate


def _stage_import_upload(request: HttpRequest, uploaded: UploadedFile) -> HttpResponse:
    """Persist an uploaded CSV file and stage it for the existing mapping flow."""
    temp_path = save_import_upload(uploaded)

    with temp_path.open(newline="", encoding="utf-8-sig") as csv_file:
        reader = csv.DictReader(csv_file)
        headers = [header.strip() for header in (reader.fieldnames or []) if header and header.strip()]

    request.session["import_csv_temp_path"] = str(temp_path)
    request.session["import_csv_original_name"] = uploaded.name
    request.session["import_csv_headers"] = headers
    return redirect("import_map_headers")


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
        "crm/import_file_list.html",
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
                from crm.services.google_sheets import extract_sheet_id, fetch_google_sheet_rows
            except ModuleNotFoundError as exc:
                if exc.name == "requests":
                    error = (
                        "Google Sheets preview is unavailable because the requests "
                        "package is not installed."
                    )
                else:
                    error = "Google Sheets preview is temporarily unavailable."
            else:
                try:
                    rows = fetch_google_sheet_rows(sheet_url)
                    total_rows = len(rows)
                    headers, preview_rows = _build_preview_rows(rows)
                    if action == "import":
                        sheet_id = extract_sheet_id(sheet_url)
                        filename = f"Google Sheet - {sheet_id}.csv"
                        uploaded = rows_to_uploaded_csv(rows, filename=filename)
                        return _stage_import_upload(request, uploaded)
                except (ValueError, RuntimeError) as exc:
                    error = str(exc)

    return render(
        request,
        "crm/import_google_sheets.html",
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
    rows_qs = (
        import_file.rows
        .select_related("company", "contact")
        .order_by("row_number")
    )
    page_obj = _paginate(request, rows_qs)
    return render(
        request,
        "crm/import_file_detail.html",
        {
            "import_file": import_file,
            "rows": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def import_upload(request):
    if request.method == "POST":
        uploaded = request.FILES.get("csv_file")
        if not uploaded:
            return render(
                request,
                "crm/import_upload.html",
                {"error": "Please choose a CSV file."},
            )
        return _stage_import_upload(request, uploaded)

    return render(request, "crm/import_upload.html")


@crm_role_required(ROLE_TEAM_LEAD)
def import_map_headers(request):
    temp_path = request.session.get("import_csv_temp_path")
    original_name = request.session.get("import_csv_original_name", "")
    headers = request.session.get("import_csv_headers", [])
    if not temp_path or not headers:
        return redirect("import_upload")

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
        }
        for key in TARGET_FIELDS
    ]
    mapping_fields = sorted(mapping_fields, key=lambda field: (field["suggested"] == "",))
    if request.method == "POST":
        file_name = (request.POST.get("file_name") or original_name).strip() or original_name
        mapping = {}
        for key in TARGET_FIELDS:
            selected = (request.POST.get(f"map_{key}") or "").strip()
            mapping[key] = selected

        import_file, _ = import_csv_with_mapping(
            csv_path=temp_path,
            file_name=file_name,
            mapping=mapping,
            source_path=temp_path,
        )

        request.session.pop("import_csv_temp_path", None)
        request.session.pop("import_csv_original_name", None)
        request.session.pop("import_csv_headers", None)
        return redirect("import_file_detail", file_id=import_file.id)

    return render(
        request,
        "crm/import_map_headers.html",
        {
            "original_name": original_name,
            "headers": headers,
            "mapping_fields": mapping_fields,
        },
    )
