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

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required, user_has_minimum_crm_role
from crm.channel_choices import humanize_channel_value
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
COMPANY_TABLE_CELL_TEMPLATES = {
    "row": "crm/components/list_workspace/cells/text.html",
    "company": "crm/components/list_workspace/cells/single_link.html",
    "industry": "crm/components/list_workspace/cells/text.html",
    "address": "crm/components/list_workspace/cells/text.html",
    "size": "crm/components/list_workspace/cells/text.html",
    "revenue": "crm/components/list_workspace/cells/text.html",
    "location": "crm/components/company_list/cells/location.html",
    "phones": "crm/components/company_list/cells/channel_list.html",
    "emails": "crm/components/company_list/cells/channel_list.html",
    "profiles": "crm/components/list_workspace/cells/stacked_links.html",
}
DETAIL_CARDS_TEMPLATE = "crm/components/content_panels/body_detail_cards.html"
DETAIL_FIELD_GRID_TEMPLATE = "crm/components/record_detail/body_field_grid.html"
DETAIL_CHANNEL_LIST_TEMPLATE = "crm/components/record_detail/body_channel_list.html"


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
    selected_contacts = list(bundle["form"].fields["contacts"].queryset)
    return {
        "company": company,
        "form": bundle["form"],
        "selected_contacts": [
            {
                "id": contact.id,
                "label": contact.full_name,
                "meta": " | ".join(
                    value for value in (contact.title, contact.email, contact.phone) if value
                ),
            }
            for contact in selected_contacts
        ],
        "phone_formset": bundle["phone_formset"],
        "email_formset": bundle["email_formset"],
        "social_link_formset": bundle["social_link_formset"],
        "company_industry_search_url": reverse("company_industry_search"),
        "company_contact_search_url": reverse("company_contact_search"),
        "hero_metrics": [
            {
                "label": "Mode",
                "value": "Edit" if is_edit_mode else "Create",
                "subtext": "",
                "mono": True,
            },
            {
                "label": "Related forms",
                "value": 3,
                "subtext": "",
            },
        ],
        "hero_actions": [
            _company_action(
                "Back to company" if is_edit_mode else "Back to companies",
                reverse("company_detail", args=[company.pk]) if is_edit_mode else reverse("company_list"),
                "primary",
            ),
            *(
                [_company_action("View detail", reverse("company_detail", args=[company.pk]))]
                if is_edit_mode
                else []
            ),
            _company_action("Open contacts", reverse("contact_list")),
        ],
        "channel_sections": [
            {
                "formset": bundle["phone_formset"],
                "title": "Phone lines",
                "row_kicker": "Phone row",
                "existing_label": "Existing line",
                "new_label": "New line",
                "remove_label": "phone row",
                "empty_text": "No phone lines added yet.",
                "add_label": "Add phone",
            },
            {
                "formset": bundle["email_formset"],
                "title": "Email inboxes",
                "row_kicker": "Email row",
                "existing_label": "Existing inbox",
                "new_label": "New inbox",
                "remove_label": "email row",
                "empty_text": "No email inboxes added yet.",
                "add_label": "Add email",
            },
            {
                "formset": bundle["social_link_formset"],
                "title": "Public profiles",
                "row_kicker": "Profile row",
                "existing_label": "Existing profile",
                "new_label": "New profile",
                "remove_label": "profile row",
                "empty_text": "No public profiles added yet.",
                "add_label": "Add profile",
            },
        ],
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


def _build_company_detail_context(company, *, can_manage_records):
    contacts = list(company.contacts.all())
    phones = list(company.phones.all())
    emails = list(company.emails.all())
    profiles = list(company.social_links.all())

    return {
        "hero_metrics": [
            {"label": "Linked contacts", "value": len(contacts), "subtext": ""},
            {
                "label": "Channels",
                "value": len(phones) + len(emails) + len(profiles),
                "subtext": "",
            },
        ],
        "hero_actions": [
            _company_action("Back to companies", reverse("company_list"), "primary"),
            *(
                [_company_action("Edit company", reverse("company_edit", args=[company.id]))]
                if can_manage_records
                else []
            ),
            _company_action("Open contacts", reverse("contact_list")),
        ],
        "top_panels": [
            {
                "kicker": "Profile summary",
                "title": "Company snapshot",
                "body_template": DETAIL_FIELD_GRID_TEMPLATE,
                "fields": [
                    {"label": "Industry", "value": company.industry or "Not tagged"},
                    {"label": "Company size", "value": company.company_size or "Unknown"},
                    {"label": "Revenue", "value": company.revenue or "Undisclosed"},
                    {"label": "Created", "value": company.created_at.strftime("%Y-%m-%d %H:%M")},
                    {"label": "City", "value": company.city or "No city"},
                    {"label": "State", "value": company.state or "No state"},
                    {"label": "Zip code", "value": company.zip_code or "No zip code"},
                    {"label": "Country", "value": company.country or "No country"},
                ],
                "note_kicker": "Street address",
                "note": company.address,
                "note_empty_text": "No street address stored.",
            },
            {
                "kicker": "Relationships",
                "title": "Linked contacts",
                "body_template": DETAIL_CARDS_TEMPLATE,
                "items": [
                    {
                        "kicker": "Contact",
                        "title": contact.full_name,
                        "meta": contact.title or "No title captured",
                        "body": " · ".join(value for value in (contact.email, contact.phone) if value)
                        or "No primary email or phone stored.",
                        "link": {
                            "href": reverse("contact_detail", args=[contact.id]),
                            "label": "Open contact",
                        },
                    }
                    for contact in contacts
                ],
                "empty_text": "No contacts are linked to this company yet.",
            },
        ],
        "bottom_panels": [
            {
                "kicker": "Channels",
                "title": "Phone lines",
                "body_template": DETAIL_CHANNEL_LIST_TEMPLATE,
                "lines": [
                    {
                        "label": humanize_channel_value((phone.label or "").strip().lower()) or "Direct",
                        "value": phone.phone,
                    }
                    for phone in phones
                ],
                "empty_text": "No company phone numbers are stored yet.",
            },
            {
                "kicker": "Channels",
                "title": "Email inboxes",
                "body_template": DETAIL_CHANNEL_LIST_TEMPLATE,
                "lines": [
                    {
                        "label": humanize_channel_value((email.label or "").strip().lower()) or "Inbox",
                        "value": email.email,
                    }
                    for email in emails
                ],
                "empty_text": "No company email addresses are stored yet.",
            },
        ],
        "profile_panel": {
            "kicker": "Channels",
            "title": "Public profiles",
            "body_template": DETAIL_CARDS_TEMPLATE,
            "items": [
                {
                    "kicker": humanize_channel_value((link.platform or "").strip().lower()) or "Website",
                    "title": link.url,
                    "link": {
                        "href": link.url,
                        "label": "Visit profile",
                    },
                }
                for link in profiles
            ],
            "empty_text": "No public profile links are stored yet.",
        },
    }


def _company_action(label, href, variant="secondary"):
    return {
        "label": label,
        "href": href,
        "variant": variant,
    }


def _company_select_options(options, selected_value, blank_label):
    return [
        {"value": "", "label": blank_label, "selected": selected_value == ""},
        *[
            {
                "value": option,
                "label": option,
                "selected": selected_value == option,
            }
            for option in options
        ],
    ]


def _company_toggle_options(value):
    return [
        {"value": "", "label": "Any", "selected": value == ""},
        {"value": "yes", "label": "Yes", "selected": value == "yes"},
        {"value": "no", "label": "No", "selected": value == "no"},
    ]


def _company_filter_fields(filters, *, industry_options, state_options, country_options, revenue_options):
    return [
        {
            "name": "q",
            "label": "Search",
            "type": "text",
            "value": filters["q"],
            "placeholder": "Search name, industry, city, state, notes",
            "wrapper_class": "md:col-span-2",
        },
        {
            "name": "industry",
            "label": "Industry",
            "type": "select",
            "options": _company_select_options(
                industry_options,
                filters["industry"],
                "All industries",
            ),
        },
        {
            "name": "state",
            "label": "State",
            "type": "select",
            "options": _company_select_options(
                state_options,
                filters["state"],
                "All states",
            ),
        },
        {
            "name": "country",
            "label": "Country",
            "type": "select",
            "options": _company_select_options(
                country_options,
                filters["country"],
                "All countries",
            ),
        },
        {
            "name": "city",
            "label": "City",
            "type": "text",
            "value": filters["city"],
            "placeholder": "Contains city name",
        },
        {
            "name": "size_min",
            "label": "Size from",
            "type": "number",
            "value": filters["size_min"],
            "placeholder": "Min",
        },
        {
            "name": "size_max",
            "label": "Size to",
            "type": "number",
            "value": filters["size_max"],
            "placeholder": "Max",
        },
        {
            "name": "revenue",
            "label": "Revenue",
            "type": "select",
            "options": _company_select_options(
                revenue_options,
                filters["revenue"],
                "All revenue values",
            ),
        },
        {
            "name": "has_revenue",
            "label": "Has revenue",
            "type": "select",
            "options": _company_toggle_options(filters["has_revenue"]),
        },
        {
            "name": "has_phone",
            "label": "Has phone",
            "type": "select",
            "options": _company_toggle_options(filters["has_phone"]),
        },
        {
            "name": "has_email",
            "label": "Has email",
            "type": "select",
            "options": _company_toggle_options(filters["has_email"]),
        },
        {
            "name": "has_profile",
            "label": "Has profile",
            "type": "select",
            "options": _company_toggle_options(filters["has_profile"]),
        },
        {
            "name": "created_from",
            "label": "Created from",
            "type": "date",
            "value": filters["created_from"],
        },
        {
            "name": "created_to",
            "label": "Created to",
            "type": "date",
            "value": filters["created_to"],
        },
    ]


def _build_company_hero_metrics(page_obj, *, total_companies, filters_active):
    visible_range = "0-0"
    if page_obj.paginator.count:
        visible_range = f"{page_obj.start_index()}-{page_obj.end_index()}"

    return [
        {
            "label": "Matching companies" if filters_active else "Companies",
            "value": page_obj.paginator.count,
            "subtext": f"of {total_companies} total" if filters_active else "",
        },
        {
            "label": "Visible rows",
            "value": visible_range,
            "subtext": "",
            "mono": True,
        },
    ]


def _build_company_hero_actions(can_manage_records):
    actions = []
    if can_manage_records:
        actions.append(_company_action("New company", reverse("company_create"), "primary"))
    actions.append(_company_action("Open contacts", reverse("contact_list"), "primary"))
    actions.append(_company_action("Review imports", reverse("import_file_list")))
    return actions


def _build_company_filter_panel(
    *,
    has_company_records,
    filters_active,
    filters,
    filter_form_hidden_items,
    active_filters,
    total_companies,
    matching_count,
    filter_reset_url,
    industry_options,
    state_options,
    country_options,
    revenue_options,
):
    return {
        "visible": has_company_records or filters_active,
        "open": filters_active,
        "hidden_items": filter_form_hidden_items,
        "fields": _company_filter_fields(
            filters,
            industry_options=industry_options,
            state_options=state_options,
            country_options=country_options,
            revenue_options=revenue_options,
        ),
        "active_filters": active_filters,
        "matching_count": matching_count,
        "total_count": total_companies,
        "reset_url": filter_reset_url,
    }


def _build_company_filter_ui():
    return {
        "kicker": "Advanced filters",
        "title": "Refine company records",
        "closed_label": "Show filters",
        "open_label": "Hide filters",
        "fields_template": "crm/components/list_workspace/filter_fields.html",
        "id_prefix": "company",
        "results_label": "matching companies",
        "empty_results_subject": "companies in the CRM directory",
    }


def _build_company_toolbar_menus(
    *,
    per_page,
    per_page_menu_options,
    column_picker_hidden_items,
    selected_columns_query,
    column_options,
    visible_columns,
    export_csv_query,
    export_xlsx_query,
):
    return [
        {
            "kind": "rows",
            "label": f"Rows: {per_page}",
            "options": per_page_menu_options,
        },
        {
            "kind": "columns",
            "label": "Columns",
            "hidden_items": column_picker_hidden_items,
            "output_value": selected_columns_query,
            "options": [
                {
                    "key": option["key"],
                    "label": option["label"],
                    "checked": option["key"] in visible_columns,
                }
                for option in column_options
            ],
        },
        {
            "kind": "export",
            "label": "Export",
            "links": [
                {"label": "Export as CSV", "url": f"?{export_csv_query}"},
                {"label": "Export as Excel", "url": f"?{export_xlsx_query}"},
            ],
        },
    ]


def _build_company_table_ui():
    return {
        "toolbar_kicker": "Directory table",
        "toolbar_title": "Company records",
        "row_template": "crm/components/list_workspace/table_row.html",
    }


def _normalize_company_channels(items, *, value_attr, fallback_label):
    normalized = []
    for item in items:
        raw_label = (getattr(item, "label", "") or "").strip().lower()
        normalized.append(
            {
                "label": humanize_channel_value(raw_label) or fallback_label,
                "value": getattr(item, value_attr),
            }
        )
    return normalized


def _normalize_company_links(items, *, label_attr, href_attr, fallback_label):
    return [
        {
            "label": humanize_channel_value(getattr(item, label_attr)) or fallback_label,
            "href": getattr(item, href_attr),
            "external": True,
        }
        for item in items
    ]


def _build_company_table_headers(visible_columns):
    return [
        {
            "key": key,
            "label": "#" if key == "row" else label,
        }
        for key, label in COMPANY_COLUMN_OPTIONS
        if key in visible_columns
    ]


def _build_company_table_rows(companies, table_headers, row_number_offset):
    rows = []
    for index, company in enumerate(companies, start=row_number_offset + 1):
        phones = list(company.phones.all())
        emails = list(company.emails.all())
        social_links = list(company.social_links.all())

        location_primary = ""
        if company.city or company.state:
            location_primary = f"{company.city or ''}{', ' if company.city and company.state else ''}{company.state or ''}"

        location_secondary = ""
        if company.zip_code or company.country:
            location_secondary = f"{company.zip_code or ''}{' ' if company.zip_code and company.country else ''}{company.country or ''}"

        cells = {
            "row": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["row"],
                "text": index,
            },
            "company": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["company"],
                "href": reverse("company_detail", args=[company.id]),
                "label": company.name,
                "external": False,
            },
            "industry": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["industry"],
                "text": company.industry,
            },
            "address": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["address"],
                "text": company.address,
            },
            "size": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["size"],
                "text": company.company_size,
            },
            "revenue": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["revenue"],
                "text": company.revenue,
            },
            "location": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["location"],
                "primary": location_primary,
                "secondary": location_secondary,
            },
            "phones": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["phones"],
                "channels": _normalize_company_channels(
                    phones,
                    value_attr="phone",
                    fallback_label="Direct",
                ),
            },
            "emails": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["emails"],
                "channels": _normalize_company_channels(
                    emails,
                    value_attr="email",
                    fallback_label="Inbox",
                ),
            },
            "profiles": {
                "template": COMPANY_TABLE_CELL_TEMPLATES["profiles"],
                "links": _normalize_company_links(
                    social_links,
                    label_attr="platform",
                    href_attr="url",
                    fallback_label="Website",
                ),
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


def _build_company_empty_state(
    *,
    filters_active,
    has_company_records,
    active_filters,
    filter_reset_url,
    can_import,
):
    if filters_active and has_company_records:
        return {
            "kicker": "No matching results",
            "title": "No companies matched the current filters.",
            "description": "Adjust the filter combination or clear everything to return to the full directory.",
            "active_filters": active_filters,
            "actions": [
                _company_action("Clear filters", filter_reset_url, "primary"),
                _company_action("Open contacts", reverse("contact_list")),
            ],
        }

    actions = []
    if can_import:
        actions.append(_company_action("Upload a CSV", reverse("import_upload"), "primary"))
    actions.append(_company_action("Review imports", reverse("import_file_list")))
    return {
        "kicker": "No records yet",
        "title": "No companies have landed in the ledger.",
        "description": "Import a CSV to start turning raw lead data into a searchable portfolio of companies and relationships.",
        "active_filters": [],
        "actions": actions,
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
    row_number_offset = page_obj.start_index() - 1 if page_obj.paginator.count else 0
    table_headers = _build_company_table_headers(state["visible_columns"])
    table_rows = _build_company_table_rows(page_obj.object_list, table_headers, row_number_offset)
    can_manage_records = user_has_minimum_crm_role(request.user, ROLE_TEAM_LEAD)
    hero_metrics = _build_company_hero_metrics(
        page_obj,
        total_companies=state["total_companies"],
        filters_active=state["filters_active"],
    )
    hero_actions = _build_company_hero_actions(can_manage_records)
    filter_form_hidden_items = _query_items(
        request,
        remove_keys=COMPANY_FILTER_KEYS | {"page", "export"},
    )
    column_picker_hidden_items = _query_items(
        request,
        remove_keys={"columns", "page", "export"},
    )
    per_page_hidden_items = _query_items(
        request,
        remove_keys={"per_page", "page", "export"},
    )
    filter_panel = _build_company_filter_panel(
        has_company_records=state["has_company_records"],
        filters_active=state["filters_active"],
        filters=state["filters"],
        filter_form_hidden_items=filter_form_hidden_items,
        active_filters=state["active_filters"],
        total_companies=state["total_companies"],
        matching_count=page_obj.paginator.count,
        filter_reset_url=filter_reset_url,
        industry_options=state["industry_options"],
        state_options=state["state_options"],
        country_options=state["country_options"],
        revenue_options=state["revenue_options"],
    )
    filter_ui = _build_company_filter_ui()
    toolbar_menus = _build_company_toolbar_menus(
        per_page=state["per_page"],
        per_page_menu_options=per_page_menu_options,
        column_picker_hidden_items=column_picker_hidden_items,
        selected_columns_query=state["selected_columns_query"],
        column_options=state["column_options"],
        visible_columns=state["visible_columns"],
        export_csv_query=_export_query(request, "csv"),
        export_xlsx_query=_export_query(request, "xlsx"),
    )
    table_ui = _build_company_table_ui()
    empty_state = _build_company_empty_state(
        filters_active=state["filters_active"],
        has_company_records=state["has_company_records"],
        active_filters=state["active_filters"],
        filter_reset_url=filter_reset_url,
        can_import=can_manage_records,
    )
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
            "row_number_offset": row_number_offset,
            "per_page_menu_options": per_page_menu_options,
            "filter_form_hidden_items": filter_form_hidden_items,
            "column_picker_hidden_items": column_picker_hidden_items,
            "per_page_hidden_items": per_page_hidden_items,
            "filter_reset_url": filter_reset_url,
            "hero_metrics": hero_metrics,
            "hero_actions": hero_actions,
            "filter_panel": filter_panel,
            "filter_ui": filter_ui,
            "toolbar_menus": toolbar_menus,
            "table_ui": table_ui,
            "table_headers": table_headers,
            "table_rows": table_rows,
            "empty_state": empty_state,
        },
    )


@crm_role_required(ROLE_STAFF)
def company_detail(request, company_id):
    company = get_object_or_404(_company_detail_queryset(), pk=company_id)
    can_manage_records = user_has_minimum_crm_role(request.user, ROLE_TEAM_LEAD)
    return render(
        request,
        "crm/companies/company_detail.html",
        {
            "company": company,
            **_build_company_detail_context(company, can_manage_records=can_manage_records),
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
