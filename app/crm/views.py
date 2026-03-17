import csv

from django.core.paginator import Paginator
from django.db.models import (
    BooleanField,
    Case,
    Count,
    Exists,
    IntegerField,
    OuterRef,
    Q,
    Value,
    When,
)
from django.db.models.functions import Cast
from django.shortcuts import get_object_or_404, redirect, render
from django.utils.dateparse import parse_date

from .auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required
from .export_utils import (
    COMPANY_EXPORT_COLUMNS,
    CONTACT_EXPORT_COLUMNS,
    export_rows_to_csv_response,
    export_rows_to_xlsx_response,
    serialize_company_export_row,
    serialize_contact_export_row,
)
from .import_utils import TARGET_FIELDS, import_csv_with_mapping, suggest_mapping
from .models import (
    Company,
    CompanyEmail,
    CompanyPhone,
    CompanySocialLink,
    Contact,
    ContactEmail,
    ContactPhone,
    ContactSocialLink,
    ImportFile,
)
from .upload_storage import save_import_upload

PAGE_SIZE = 10
BOOLEAN_FILTER_LABELS = {
    "yes": "Yes",
    "no": "No",
}


def _paginate(request, queryset, per_page=PAGE_SIZE):
    paginator = Paginator(queryset, per_page)
    return paginator.get_page(request.GET.get("page"))


def _query_string(request, remove_keys=None, extra=None):
    query = request.GET.copy()
    for key in remove_keys or ():
        query.pop(key, None)
    for key, value in (extra or {}).items():
        if value in (None, ""):
            query.pop(key, None)
        else:
            query[key] = value
    return query.urlencode()


def _page_query(request):
    return _query_string(request, remove_keys={"page", "export"})


def _export_query(request, export_format):
    return _query_string(
        request,
        remove_keys={"page", "export"},
        extra={"export": export_format},
    )


def _clean_text(value):
    return (value or "").strip()


def _clean_export_format(value):
    value = _clean_text(value).lower()
    return value if value in {"csv", "xlsx"} else ""


def _clean_toggle(value):
    value = _clean_text(value).lower()
    return value if value in BOOLEAN_FILTER_LABELS else ""


def _parse_int(value):
    value = _clean_text(value)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_date_value(value):
    return parse_date(_clean_text(value))


def _distinct_nonempty_values(queryset, field_name):
    return list(
        queryset.exclude(**{field_name: ""})
        .order_by(field_name)
        .values_list(field_name, flat=True)
        .distinct()
    )


def _add_active_filter(active_filters, label, value):
    if value:
        active_filters.append({"label": label, "value": value})


def _apply_toggle_filter(queryset, field_name, toggle_value):
    if toggle_value == "yes":
        return queryset.filter(**{field_name: True})
    if toggle_value == "no":
        return queryset.filter(**{field_name: False})
    return queryset


def _export_response(export_format, base_name, sheet_name, columns, rows):
    if export_format == "csv":
        return export_rows_to_csv_response(base_name, columns, rows)
    return export_rows_to_xlsx_response(base_name, sheet_name, columns, rows)


def _company_list_state(request):
    company_filters = {
        "q": _clean_text(request.GET.get("q")),
        "industry": _clean_text(request.GET.get("industry")),
        "state": _clean_text(request.GET.get("state")),
        "country": _clean_text(request.GET.get("country")),
        "city": _clean_text(request.GET.get("city")),
        "size_min": _clean_text(request.GET.get("size_min")),
        "size_max": _clean_text(request.GET.get("size_max")),
        "revenue": _clean_text(request.GET.get("revenue")),
        "has_revenue": _clean_toggle(request.GET.get("has_revenue")),
        "has_phone": _clean_toggle(request.GET.get("has_phone")),
        "has_email": _clean_toggle(request.GET.get("has_email")),
        "has_profile": _clean_toggle(request.GET.get("has_profile")),
        "created_from": _clean_text(request.GET.get("created_from")),
        "created_to": _clean_text(request.GET.get("created_to")),
    }
    total_companies = Company.objects.count()
    has_company_records = total_companies > 0

    companies_qs = (
        Company.objects.prefetch_related("phones", "emails", "social_links")
        .annotate(
            has_phone_data=Exists(
                CompanyPhone.objects.filter(company_id=OuterRef("pk"))
            ),
            has_email_data=Exists(
                CompanyEmail.objects.filter(company_id=OuterRef("pk"))
            ),
            has_profile_data=Exists(
                CompanySocialLink.objects.filter(company_id=OuterRef("pk"))
            ),
            has_revenue_data=Case(
                When(revenue__gt="", then=Value(True)),
                default=Value(False),
                output_field=BooleanField(),
            ),
            size_number=Case(
                When(company_size__regex=r"^\d+$", then=Cast("company_size", IntegerField())),
                default=None,
                output_field=IntegerField(),
            ),
        )
        .order_by("name")
    )

    query = company_filters["q"]
    if query:
        companies_qs = companies_qs.filter(
            Q(name__icontains=query)
            | Q(industry__icontains=query)
            | Q(address__icontains=query)
            | Q(city__icontains=query)
            | Q(state__icontains=query)
            | Q(country__icontains=query)
            | Q(notes__icontains=query)
        )

    if company_filters["industry"]:
        companies_qs = companies_qs.filter(industry=company_filters["industry"])
    if company_filters["state"]:
        companies_qs = companies_qs.filter(state=company_filters["state"])
    if company_filters["country"]:
        companies_qs = companies_qs.filter(country=company_filters["country"])
    if company_filters["city"]:
        companies_qs = companies_qs.filter(city__icontains=company_filters["city"])
    if company_filters["revenue"]:
        companies_qs = companies_qs.filter(revenue=company_filters["revenue"])

    size_min = _parse_int(company_filters["size_min"])
    size_max = _parse_int(company_filters["size_max"])
    if size_min is not None:
        companies_qs = companies_qs.filter(size_number__gte=size_min)
    if size_max is not None:
        companies_qs = companies_qs.filter(size_number__lte=size_max)

    created_from = _parse_date_value(company_filters["created_from"])
    created_to = _parse_date_value(company_filters["created_to"])
    if created_from:
        companies_qs = companies_qs.filter(created_at__date__gte=created_from)
    if created_to:
        companies_qs = companies_qs.filter(created_at__date__lte=created_to)

    companies_qs = _apply_toggle_filter(
        companies_qs, "has_revenue_data", company_filters["has_revenue"]
    )
    companies_qs = _apply_toggle_filter(
        companies_qs, "has_phone_data", company_filters["has_phone"]
    )
    companies_qs = _apply_toggle_filter(
        companies_qs, "has_email_data", company_filters["has_email"]
    )
    companies_qs = _apply_toggle_filter(
        companies_qs, "has_profile_data", company_filters["has_profile"]
    )

    active_filters = []
    _add_active_filter(active_filters, "Search", company_filters["q"])
    _add_active_filter(active_filters, "Industry", company_filters["industry"])
    _add_active_filter(active_filters, "State", company_filters["state"])
    _add_active_filter(active_filters, "Country", company_filters["country"])
    _add_active_filter(active_filters, "City", company_filters["city"])
    if size_min is not None:
        _add_active_filter(active_filters, "Size from", size_min)
    if size_max is not None:
        _add_active_filter(active_filters, "Size to", size_max)
    _add_active_filter(active_filters, "Revenue", company_filters["revenue"])
    _add_active_filter(
        active_filters,
        "Has revenue",
        BOOLEAN_FILTER_LABELS.get(company_filters["has_revenue"], ""),
    )
    _add_active_filter(
        active_filters,
        "Has phone",
        BOOLEAN_FILTER_LABELS.get(company_filters["has_phone"], ""),
    )
    _add_active_filter(
        active_filters,
        "Has email",
        BOOLEAN_FILTER_LABELS.get(company_filters["has_email"], ""),
    )
    _add_active_filter(
        active_filters,
        "Has profile",
        BOOLEAN_FILTER_LABELS.get(company_filters["has_profile"], ""),
    )
    if created_from:
        _add_active_filter(active_filters, "Created from", company_filters["created_from"])
    if created_to:
        _add_active_filter(active_filters, "Created to", company_filters["created_to"])

    return {
        "queryset": companies_qs,
        "filters": company_filters,
        "filters_active": bool(active_filters),
        "active_filters": active_filters,
        "total_companies": total_companies,
        "has_company_records": has_company_records,
        "industry_options": _distinct_nonempty_values(Company.objects, "industry"),
        "state_options": _distinct_nonempty_values(Company.objects, "state"),
        "country_options": _distinct_nonempty_values(Company.objects, "country"),
        "revenue_options": _distinct_nonempty_values(Company.objects, "revenue"),
    }


def _contact_list_state(request):
    contact_filters = {
        "q": _clean_text(request.GET.get("q")),
        "title": _clean_text(request.GET.get("title")),
        "company": _clean_text(request.GET.get("company")),
        "has_email": _clean_toggle(request.GET.get("has_email")),
        "has_phone": _clean_toggle(request.GET.get("has_phone")),
        "has_company": _clean_toggle(request.GET.get("has_company")),
        "has_profile": _clean_toggle(request.GET.get("has_profile")),
        "created_from": _clean_text(request.GET.get("created_from")),
        "created_to": _clean_text(request.GET.get("created_to")),
    }
    total_contacts = Contact.objects.count()
    has_contact_records = total_contacts > 0
    company_link_model = Contact.companies.through
    contacts_qs = (
        Contact.objects.prefetch_related("companies", "phones", "emails", "social_links")
        .annotate(
            has_related_email=Exists(
                ContactEmail.objects.filter(contact_id=OuterRef("pk"))
            ),
            has_related_phone=Exists(
                ContactPhone.objects.filter(contact_id=OuterRef("pk"))
            ),
            has_company_data=Exists(
                company_link_model.objects.filter(contact_id=OuterRef("pk"))
            ),
            has_profile_data=Exists(
                ContactSocialLink.objects.filter(contact_id=OuterRef("pk"))
            ),
        )
        .annotate(
            has_email_data=Case(
                When(email__gt="", then=Value(True)),
                When(has_related_email=True, then=Value(True)),
                default=Value(False),
                output_field=BooleanField(),
            ),
            has_phone_data=Case(
                When(phone__gt="", then=Value(True)),
                When(has_related_phone=True, then=Value(True)),
                default=Value(False),
                output_field=BooleanField(),
            ),
        )
    )

    needs_distinct = False
    query = contact_filters["q"]
    if query:
        contacts_qs = contacts_qs.filter(
            Q(full_name__icontains=query)
            | Q(title__icontains=query)
            | Q(notes__icontains=query)
            | Q(email__icontains=query)
            | Q(phone__icontains=query)
            | Q(companies__name__icontains=query)
            | Q(emails__email__icontains=query)
            | Q(phones__phone__icontains=query)
        )
        needs_distinct = True

    if contact_filters["title"]:
        contacts_qs = contacts_qs.filter(title__icontains=contact_filters["title"])
    if contact_filters["company"]:
        contacts_qs = contacts_qs.filter(companies__name__icontains=contact_filters["company"])
        needs_distinct = True

    created_from = _parse_date_value(contact_filters["created_from"])
    created_to = _parse_date_value(contact_filters["created_to"])
    if created_from:
        contacts_qs = contacts_qs.filter(created_at__date__gte=created_from)
    if created_to:
        contacts_qs = contacts_qs.filter(created_at__date__lte=created_to)

    contacts_qs = _apply_toggle_filter(
        contacts_qs, "has_email_data", contact_filters["has_email"]
    )
    contacts_qs = _apply_toggle_filter(
        contacts_qs, "has_phone_data", contact_filters["has_phone"]
    )
    contacts_qs = _apply_toggle_filter(
        contacts_qs, "has_company_data", contact_filters["has_company"]
    )
    contacts_qs = _apply_toggle_filter(
        contacts_qs, "has_profile_data", contact_filters["has_profile"]
    )

    if needs_distinct:
        contacts_qs = contacts_qs.distinct()

    contacts_qs = contacts_qs.order_by("full_name")
    active_filters = []
    _add_active_filter(active_filters, "Search", contact_filters["q"])
    _add_active_filter(active_filters, "Title", contact_filters["title"])
    _add_active_filter(active_filters, "Company", contact_filters["company"])
    _add_active_filter(
        active_filters,
        "Has email",
        BOOLEAN_FILTER_LABELS.get(contact_filters["has_email"], ""),
    )
    _add_active_filter(
        active_filters,
        "Has phone",
        BOOLEAN_FILTER_LABELS.get(contact_filters["has_phone"], ""),
    )
    _add_active_filter(
        active_filters,
        "Has company",
        BOOLEAN_FILTER_LABELS.get(contact_filters["has_company"], ""),
    )
    _add_active_filter(
        active_filters,
        "Has profile",
        BOOLEAN_FILTER_LABELS.get(contact_filters["has_profile"], ""),
    )
    if created_from:
        _add_active_filter(active_filters, "Created from", contact_filters["created_from"])
    if created_to:
        _add_active_filter(active_filters, "Created to", contact_filters["created_to"])

    return {
        "queryset": contacts_qs,
        "filters": contact_filters,
        "filters_active": bool(active_filters),
        "active_filters": active_filters,
        "total_contacts": total_contacts,
        "has_contact_records": has_contact_records,
    }


@crm_role_required(ROLE_STAFF)
def company_list(request):
    state = _company_list_state(request)
    export_format = _clean_export_format(request.GET.get("export"))
    if export_format:
        rows = [serialize_company_export_row(company) for company in state["queryset"]]
        return _export_response(
            export_format,
            "companies",
            "Companies",
            COMPANY_EXPORT_COLUMNS,
            rows,
        )

    page_obj = _paginate(request, state["queryset"])
    return render(
        request,
        "crm/company_list.html",
        {
            **state,
            "companies": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "export_csv_query": _export_query(request, "csv"),
            "export_xlsx_query": _export_query(request, "xlsx"),
        },
    )


@crm_role_required(ROLE_STAFF)
def contact_list(request):
    state = _contact_list_state(request)
    export_format = _clean_export_format(request.GET.get("export"))
    if export_format:
        rows = [serialize_contact_export_row(contact) for contact in state["queryset"]]
        return _export_response(
            export_format,
            "contacts",
            "Contacts",
            CONTACT_EXPORT_COLUMNS,
            rows,
        )

    page_obj = _paginate(request, state["queryset"])
    return render(
        request,
        "crm/contact_list.html",
        {
            **state,
            "contacts": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "export_csv_query": _export_query(request, "csv"),
            "export_xlsx_query": _export_query(request, "xlsx"),
        },
    )


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

        temp_path = save_import_upload(uploaded)

        with temp_path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            headers = [h.strip() for h in (reader.fieldnames or []) if h and h.strip()]

        request.session["import_csv_temp_path"] = str(temp_path)
        request.session["import_csv_original_name"] = uploaded.name
        request.session["import_csv_headers"] = headers
        return redirect("import_map_headers")

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
