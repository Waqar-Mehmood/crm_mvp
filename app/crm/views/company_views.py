"""Company list, detail, and form views."""

from __future__ import annotations

from django.contrib import messages
from django.http import JsonResponse
from django.db.models import (
    BooleanField,
    Case,
    Exists,
    IntegerField,
    OuterRef,
    Prefetch,
    Q,
    Value,
    When,
)
from django.db.models.functions import Cast
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required
from crm.forms.companies import (
    CompanyEmailFormSet,
    CompanyForm,
    CompanyPhoneFormSet,
    CompanySocialLinkFormSet,
)
from crm.models import Company, CompanyEmail, CompanyPhone, CompanySocialLink, Contact
from crm.services.companies import save_company_bundle
from crm.services.export_service import (
    COMPANY_EXPORT_COLUMNS,
    serialize_company_export_row,
)
from ._shared import (
    BOOLEAN_FILTER_LABELS,
    _add_active_filter,
    _apply_toggle_filter,
    _clean_column_list,
    _clean_export_format,
    _clean_per_page,
    _clean_text,
    _clean_toggle,
    _distinct_nonempty_values,
    _export_query,
    _export_response,
    _page_query,
    _paginate,
    _parse_date_value,
    _parse_int,
    _query_items,
    _query_string,
    PAGE_SIZE_OPTIONS,
)

RELATION_SEARCH_LIMIT = 10
COMPANY_COLUMN_OPTIONS = (
    ("row", "Row number"),
    ("company", "Company"),
    ("industry", "Industry"),
    ("address", "Address"),
    ("size", "Size"),
    ("revenue", "Revenue"),
    ("location", "Location"),
    ("phones", "Phones"),
    ("emails", "Emails"),
    ("profiles", "Profiles"),
)
DEFAULT_COMPANY_COLUMNS = [key for key, _label in COMPANY_COLUMN_OPTIONS]
COMPANY_FILTER_KEYS = frozenset(
    {
        "q",
        "industry",
        "state",
        "country",
        "city",
        "size_min",
        "size_max",
        "revenue",
        "has_revenue",
        "has_phone",
        "has_email",
        "has_profile",
        "created_from",
        "created_to",
    }
)


def _company_detail_queryset():
    return Company.objects.prefetch_related(
        Prefetch("contacts", queryset=Contact.objects.order_by("full_name")),
        "phones",
        "emails",
        "social_links",
    )


def _company_form_bundle(request, company):
    data = request.POST if request.method == "POST" else None
    return {
        "form": CompanyForm(data=data, instance=company),
        "phone_formset": CompanyPhoneFormSet(
            data=data,
            instance=company,
            prefix="phones",
        ),
        "email_formset": CompanyEmailFormSet(
            data=data,
            instance=company,
            prefix="emails",
        ),
        "social_link_formset": CompanySocialLinkFormSet(
            data=data,
            instance=company,
            prefix="social_links",
        ),
    }


def _company_form_context(company, bundle, is_edit_mode):
    return {
        "company": company,
        "form": bundle["form"],
        "selected_contacts": list(bundle["form"].fields["contacts"].queryset),
        "phone_formset": bundle["phone_formset"],
        "email_formset": bundle["email_formset"],
        "social_link_formset": bundle["social_link_formset"],
        "is_edit_mode": is_edit_mode,
        "form_title": "Edit company" if is_edit_mode else "New company",
        "form_description": (
            "Update the company profile, related channels, and linked contacts from one page."
            if is_edit_mode
            else "Create a company record and capture the contacts and channels already known."
        ),
        "submit_label": "Save changes" if is_edit_mode else "Create company",
        "cancel_url": (
            reverse("company_detail", args=[company.pk])
            if is_edit_mode
            else reverse("company_list")
        ),
    }


@crm_role_required(ROLE_TEAM_LEAD)
def company_contact_search(request):
    query = _clean_text(request.GET.get("q"))
    if len(query) < 2:
        return JsonResponse({"results": []})

    contacts = (
        Contact.objects.filter(
            Q(full_name__icontains=query)
            | Q(email__icontains=query)
            | Q(title__icontains=query)
            | Q(phone__icontains=query)
        )
        .order_by("full_name")[:RELATION_SEARCH_LIMIT]
    )
    return JsonResponse(
        {
            "results": [
                {
                    "id": contact.pk,
                    "label": contact.full_name,
                    "meta": " | ".join(
                        value
                        for value in (contact.title, contact.email, contact.phone)
                        if value
                    ),
                }
                for contact in contacts
            ]
        }
    )


@crm_role_required(ROLE_TEAM_LEAD)
def company_industry_search(request):
    query = _clean_text(request.GET.get("q"))
    if len(query) < 2:
        return JsonResponse({"results": []})

    industries = list(
        Company.objects.exclude(industry="")
        .filter(industry__icontains=query)
        .order_by("industry")
        .values_list("industry", flat=True)
        .distinct()[:RELATION_SEARCH_LIMIT]
    )
    return JsonResponse(
        {
            "results": [
                {
                    "value": industry,
                    "label": industry,
                }
                for industry in industries
            ]
        }
    )


def _company_list_state(request):
    per_page = _clean_per_page(request.GET.get("per_page"))
    visible_columns = _clean_column_list(
        request.GET.get("columns"),
        [key for key, _label in COMPANY_COLUMN_OPTIONS],
        DEFAULT_COMPANY_COLUMNS,
    )
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
        "visible_columns": visible_columns,
        "selected_columns_query": ",".join(visible_columns),
        "column_options": [
            {"key": key, "label": label} for key, label in COMPANY_COLUMN_OPTIONS
        ],
        "per_page": per_page,
        "per_page_options": PAGE_SIZE_OPTIONS,
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

    filter_reset_query = _query_string(
        request,
        remove_keys=COMPANY_FILTER_KEYS | {"page", "export"},
    )
    per_page_menu_options = []
    company_list_url = reverse("company_list")
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
                "url": f"{company_list_url}?{query_string}" if query_string else company_list_url,
            }
        )

    filter_reset_url = reverse("company_list")
    if filter_reset_query:
        filter_reset_url = f"{filter_reset_url}?{filter_reset_query}"

    page_obj = _paginate(request, state["queryset"], per_page=state["per_page"])
    return render(
        request,
        "crm/companies/company_list.html",
        {
            **state,
            "companies": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "export_csv_query": _export_query(request, "csv"),
            "export_xlsx_query": _export_query(request, "xlsx"),
            "row_number_offset": page_obj.start_index() - 1 if page_obj.paginator.count else 0,
            "per_page_menu_options": per_page_menu_options,
            "filter_form_hidden_items": _query_items(
                request,
                remove_keys=COMPANY_FILTER_KEYS | {"page", "export"},
            ),
            "column_picker_hidden_items": _query_items(
                request,
                remove_keys={"columns", "page", "export"},
            ),
            "per_page_hidden_items": _query_items(
                request,
                remove_keys={"per_page", "page", "export"},
            ),
            "filter_reset_url": filter_reset_url,
        },
    )


@crm_role_required(ROLE_STAFF)
def company_detail(request, company_id):
    company = get_object_or_404(_company_detail_queryset(), pk=company_id)
    return render(
        request,
        "crm/companies/company_detail.html",
        {
            "company": company,
            "contact_count": company.contacts.count(),
            "phone_count": company.phones.count(),
            "email_count": company.emails.count(),
            "profile_count": company.social_links.count(),
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def company_create(request):
    company = Company()
    bundle = _company_form_bundle(request, company)
    if request.method == "POST":
        saved_company = save_company_bundle(
            bundle["form"],
            bundle["phone_formset"],
            bundle["email_formset"],
            bundle["social_link_formset"],
        )
        if saved_company is not None:
            messages.success(request, "Company created.")
            return redirect("company_detail", company_id=saved_company.pk)

    return render(
        request,
        "crm/companies/company_form.html",
        _company_form_context(company, bundle, is_edit_mode=False),
    )


@crm_role_required(ROLE_TEAM_LEAD)
def company_edit(request, company_id):
    company = get_object_or_404(_company_detail_queryset(), pk=company_id)
    bundle = _company_form_bundle(request, company)
    if request.method == "POST":
        saved_company = save_company_bundle(
            bundle["form"],
            bundle["phone_formset"],
            bundle["email_formset"],
            bundle["social_link_formset"],
        )
        if saved_company is not None:
            messages.success(request, "Company updated.")
            return redirect("company_detail", company_id=saved_company.pk)

    return render(
        request,
        "crm/companies/company_form.html",
        _company_form_context(company, bundle, is_edit_mode=True),
    )
