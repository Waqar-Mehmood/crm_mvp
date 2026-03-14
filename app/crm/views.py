import csv
import uuid
from pathlib import Path

from django.conf import settings
from django.core.paginator import Paginator
from django.db.models import Count
from django.shortcuts import get_object_or_404, redirect, render
from .models import Company, Contact, ImportFile
from .import_utils import TARGET_FIELDS, import_csv_with_mapping, suggest_mapping

PAGE_SIZE = 10


def _paginate(request, queryset, per_page=PAGE_SIZE):
    paginator = Paginator(queryset, per_page)
    return paginator.get_page(request.GET.get("page"))


def company_list(request):
    # include related phone/email/social records for efficiency
    companies_qs = (
        Company.objects
        .prefetch_related("phones", "emails", "social_links")
        .order_by("name")
    )
    page_obj = _paginate(request, companies_qs)
    return render(
        request,
        "crm/company_list.html",
        {
            "companies": page_obj.object_list,
            "page_obj": page_obj,
        },
    )


def contact_list(request):
    contacts_qs = (
        Contact.objects
        .prefetch_related("companies", "phones", "emails", "social_links")
        .order_by("full_name")
    )
    page_obj = _paginate(request, contacts_qs)
    return render(
        request,
        "crm/contact_list.html",
        {
            "contacts": page_obj.object_list,
            "page_obj": page_obj,
        },
    )


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
        },
    )


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
        },
    )


def import_upload(request):
    if request.method == "POST":
        uploaded = request.FILES.get("csv_file")
        if not uploaded:
            return render(
                request,
                "crm/import_upload.html",
                {"error": "Please choose a CSV file."},
            )

        uploads_dir = Path(settings.BASE_DIR) / "data" / "uploads"
        uploads_dir.mkdir(parents=True, exist_ok=True)
        temp_name = f"{uuid.uuid4().hex}_{uploaded.name}"
        temp_path = uploads_dir / temp_name
        with temp_path.open("wb") as out:
            for chunk in uploaded.chunks():
                out.write(chunk)

        with temp_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = [h.strip() for h in (reader.fieldnames or []) if h and h.strip()]

        request.session["import_csv_temp_path"] = str(temp_path)
        request.session["import_csv_original_name"] = uploaded.name
        request.session["import_csv_headers"] = headers
        return redirect("import_map_headers")

    return render(request, "crm/import_upload.html")


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
