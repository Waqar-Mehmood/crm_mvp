"""Contact list, detail, and form views."""

from __future__ import annotations

from django.contrib import messages
from django.http import JsonResponse
from django.db.models import BooleanField, Case, Exists, OuterRef, Prefetch, Q, Value, When
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required, user_has_minimum_crm_role
from crm.channel_choices import humanize_channel_value
from crm.forms.contacts import (
    ContactEmailFormSet,
    ContactForm,
    ContactPhoneFormSet,
    ContactSocialLinkFormSet,
)
from crm.models import Company, Contact, ContactEmail, ContactPhone, ContactSocialLink
from crm.services.contacts import save_contact_bundle
from crm.services.export_service import (
    CONTACT_EXPORT_COLUMNS,
    serialize_contact_export_row,
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
    _export_query,
    _export_response,
    _page_query,
    _paginate,
    _parse_date_value,
    _query_items,
    _query_string,
    PAGE_SIZE_OPTIONS,
)

RELATION_SEARCH_LIMIT = 10
CONTACT_COLUMN_OPTIONS = (
    ("row", "Row number"),
    ("contact", "Contact"),
    ("title", "Title"),
    ("email", "Email"),
    ("phone", "Phone"),
    ("companies", "Companies"),
    ("profiles", "Profiles"),
)
DEFAULT_CONTACT_COLUMNS = [key for key, _label in CONTACT_COLUMN_OPTIONS]
CONTACT_FILTER_KEYS = frozenset(
    {
        "q",
        "title",
        "company",
        "has_email",
        "has_phone",
        "has_company",
        "has_profile",
        "created_from",
        "created_to",
    }
)
CONTACT_TABLE_CELL_TEMPLATES = {
    "row": "crm/components/list_workspace/cells/text.html",
    "contact": "crm/components/list_workspace/cells/single_link.html",
    "title": "crm/components/list_workspace/cells/text.html",
    "email": "crm/components/contact_list/cells/channels.html",
    "phone": "crm/components/contact_list/cells/channels.html",
    "companies": "crm/components/list_workspace/cells/stacked_links.html",
    "profiles": "crm/components/list_workspace/cells/stacked_links.html",
}
DETAIL_CARDS_TEMPLATE = "crm/components/content_panels/body_detail_cards.html"
DETAIL_FIELD_GRID_TEMPLATE = "crm/components/record_detail/body_field_grid.html"
DETAIL_CHANNEL_LIST_TEMPLATE = "crm/components/record_detail/body_channel_list.html"


def _contact_detail_queryset():
    return Contact.objects.prefetch_related(
        Prefetch("companies", queryset=Company.objects.order_by("name")),
        "phones",
        "emails",
        "social_links",
    )


def _contact_action(label, href, variant="secondary"):
    return {
        "label": label,
        "href": href,
        "variant": variant,
    }


def _contact_toggle_options(value):
    return [
        {"value": "", "label": "Any", "selected": value == ""},
        {"value": "yes", "label": "Yes", "selected": value == "yes"},
        {"value": "no", "label": "No", "selected": value == "no"},
    ]


def _contact_filter_fields(filters):
    return [
        {
            "name": "q",
            "label": "Search",
            "type": "text",
            "value": filters["q"],
            "placeholder": "Search names, titles, company, email, phone",
            "wrapper_class": "md:col-span-2",
        },
        {
            "name": "title",
            "label": "Title",
            "type": "text",
            "value": filters["title"],
            "placeholder": "Contains title",
        },
        {
            "name": "company",
            "label": "Company",
            "type": "text",
            "value": filters["company"],
            "placeholder": "Contains company name",
        },
        {
            "name": "has_email",
            "label": "Has email",
            "type": "select",
            "options": _contact_toggle_options(filters["has_email"]),
        },
        {
            "name": "has_phone",
            "label": "Has phone",
            "type": "select",
            "options": _contact_toggle_options(filters["has_phone"]),
        },
        {
            "name": "has_company",
            "label": "Has company",
            "type": "select",
            "options": _contact_toggle_options(filters["has_company"]),
        },
        {
            "name": "has_profile",
            "label": "Has profile",
            "type": "select",
            "options": _contact_toggle_options(filters["has_profile"]),
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


def _build_contact_hero_metrics(page_obj, *, total_contacts, filters_active):
    visible_range = "0-0"
    if page_obj.paginator.count:
        visible_range = f"{page_obj.start_index()}-{page_obj.end_index()}"

    metrics = [
        {
            "label": "Matching contacts" if filters_active else "Contacts",
            "value": page_obj.paginator.count,
            "subtext": f"of {total_contacts} total" if filters_active else "",
        },
        {
            "label": "Visible rows",
            "value": visible_range,
            "subtext": "",
            "mono": True,
        },
    ]
    return metrics


def _build_contact_hero_actions(can_manage_records):
    actions = []
    if can_manage_records:
        actions.append(_contact_action("New contact", reverse("contact_create"), "primary"))
    actions.append(_contact_action("Open companies", reverse("company_list"), "primary"))
    actions.append(_contact_action("Import ledger", reverse("import_file_list")))
    return actions


def _build_contact_filter_panel(
    *,
    has_contact_records,
    filters_active,
    filters,
    filter_form_hidden_items,
    active_filters,
    total_contacts,
    matching_count,
    filter_reset_url,
):
    return {
        "visible": has_contact_records or filters_active,
        "open": filters_active,
        "hidden_items": filter_form_hidden_items,
        "fields": _contact_filter_fields(filters),
        "active_filters": active_filters,
        "matching_count": matching_count,
        "total_count": total_contacts,
        "reset_url": filter_reset_url,
    }


def _build_contact_filter_ui():
    return {
        "kicker": "Advanced filters",
        "title": "Refine contact records",
        "closed_label": "Show filters",
        "open_label": "Hide filters",
        "fields_template": "crm/components/list_workspace/filter_fields.html",
        "id_prefix": "contact",
        "results_label": "matching contacts",
        "empty_results_subject": "contacts in the CRM roster",
    }


def _build_contact_toolbar_menus(
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


def _build_contact_table_ui():
    return {
        "toolbar_kicker": "Roster table",
        "toolbar_title": "Contact records",
        "row_template": "crm/components/list_workspace/table_row.html",
    }


def _normalize_contact_channels(items, *, fallback_value, value_attr, fallback_label):
    normalized = []
    items = list(items)
    if items:
        for item in items:
            raw_label = (getattr(item, "label", "") or "").strip().lower()
            normalized.append(
                {
                    "label": humanize_channel_value(raw_label) or fallback_label,
                    "value": getattr(item, value_attr),
                    "tone": raw_label if raw_label in {"work", "personal"} else "default",
                }
            )
        return normalized

    if fallback_value:
        return [
            {
                "label": fallback_label,
                "value": fallback_value,
                "tone": "default",
            }
        ]
    return []


def _normalize_contact_links(items, *, label_attr, href_attr, fallback_label, internal=False):
    normalized = []
    for item in items:
        label_value = getattr(item, label_attr)
        normalized.append(
            {
                "label": humanize_channel_value(label_value) or fallback_label,
                "href": getattr(item, href_attr),
                "external": not internal,
            }
        )
    return normalized


def _build_contact_table_headers(visible_columns):
    return [
        {
            "key": key,
            "label": "#" if key == "row" else label,
        }
        for key, label in CONTACT_COLUMN_OPTIONS
        if key in visible_columns
    ]


def _build_contact_table_rows(contacts, table_headers, row_number_offset):
    rows = []
    for index, contact in enumerate(contacts, start=row_number_offset + 1):
        companies = list(contact.companies.all())
        emails = list(contact.emails.all())
        phones = list(contact.phones.all())
        social_links = list(contact.social_links.all())

        cells = {
            "row": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["row"],
                "text": index,
            },
            "contact": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["contact"],
                "label": contact.full_name,
                "href": reverse("contact_detail", args=[contact.id]),
                "notes": contact.notes,
            },
            "title": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["title"],
                "text": contact.title,
            },
            "email": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["email"],
                "channels": _normalize_contact_channels(
                    emails,
                    fallback_value=contact.email,
                    value_attr="email",
                    fallback_label="Inbox",
                ),
            },
            "phone": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["phone"],
                "channels": _normalize_contact_channels(
                    phones,
                    fallback_value=contact.phone,
                    value_attr="phone",
                    fallback_label="Line",
                ),
            },
            "companies": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["companies"],
                "links": [
                    {
                        "label": company.name,
                        "href": reverse("company_detail", args=[company.id]),
                        "external": False,
                    }
                    for company in companies
                ],
            },
            "profiles": {
                "template": CONTACT_TABLE_CELL_TEMPLATES["profiles"],
                "links": _normalize_contact_links(
                    social_links,
                    label_attr="platform",
                    href_attr="url",
                    fallback_label="Open profile",
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


def _build_contact_empty_state(*, filters_active, has_contact_records, active_filters, filter_reset_url, can_import):
    if filters_active and has_contact_records:
        return {
            "kicker": "No matching results",
            "title": "No contacts matched the current filters.",
            "description": "Change the active filters or clear them to return to the full contact roster.",
            "active_filters": active_filters,
            "actions": [
                _contact_action("Clear filters", filter_reset_url, "primary"),
                _contact_action("Open companies", reverse("company_list")),
            ],
        }

    actions = []
    if can_import:
        actions.append(_contact_action("Import contacts", reverse("import_upload"), "primary"))
    actions.append(_contact_action("Review imports", reverse("import_file_list")))
    return {
        "kicker": "Nothing to review",
        "title": "No contacts are available yet.",
        "description": "Once a CSV import lands, this roster becomes a polished people directory with contact channels and company relationships.",
        "active_filters": [],
        "actions": actions,
    }


def _contact_form_bundle(request, contact):
    data = request.POST if request.method == "POST" else None
    return {
        "form": ContactForm(data=data, instance=contact),
        "phone_formset": ContactPhoneFormSet(
            data=data,
            instance=contact,
            prefix="phones",
        ),
        "email_formset": ContactEmailFormSet(
            data=data,
            instance=contact,
            prefix="emails",
        ),
        "social_link_formset": ContactSocialLinkFormSet(
            data=data,
            instance=contact,
            prefix="social_links",
        ),
    }


def _contact_form_context(contact, bundle, is_edit_mode):
    selected_companies = list(bundle["form"].fields["companies"].queryset)
    return {
        "contact": contact,
        "form": bundle["form"],
        "selected_companies": [
            {
                "id": company.id,
                "label": company.name,
                "meta": " | ".join(
                    value
                    for value in (
                        company.industry,
                        ", ".join(
                            value for value in (company.city, company.state, company.country) if value
                        ),
                    )
                    if value
                ),
            }
            for company in selected_companies
        ],
        "phone_formset": bundle["phone_formset"],
        "email_formset": bundle["email_formset"],
        "social_link_formset": bundle["social_link_formset"],
        "contact_company_search_url": reverse("contact_company_search"),
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
            _contact_action(
                "Back to contact" if is_edit_mode else "Back to contacts",
                reverse("contact_detail", args=[contact.pk]) if is_edit_mode else reverse("contact_list"),
                "primary",
            ),
            *(
                [_contact_action("View detail", reverse("contact_detail", args=[contact.pk]))]
                if is_edit_mode
                else []
            ),
            _contact_action("Open companies", reverse("company_list")),
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
                "title": "Email addresses",
                "row_kicker": "Email row",
                "existing_label": "Existing inbox",
                "new_label": "New inbox",
                "remove_label": "email row",
                "empty_text": "No email addresses added yet.",
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
        "form_title": "Edit contact" if is_edit_mode else "New contact",
        "form_description": (
            "Update the contact profile, related channels, and linked companies from one page."
            if is_edit_mode
            else "Create a contact record and capture the companies and channels already known."
        ),
        "submit_label": "Save changes" if is_edit_mode else "Create contact",
        "cancel_url": (
            reverse("contact_detail", args=[contact.pk])
            if is_edit_mode
            else reverse("contact_list")
        ),
    }


def _build_contact_detail_context(contact, *, can_manage_records):
    companies = list(contact.companies.all())
    phones = list(contact.phones.all())
    emails = list(contact.emails.all())
    profiles = list(contact.social_links.all())

    return {
        "hero_metrics": [
            {"label": "Linked companies", "value": len(companies), "subtext": ""},
            {
                "label": "Channels",
                "value": len(phones) + len(emails) + len(profiles),
                "subtext": "",
            },
        ],
        "hero_actions": [
            _contact_action("Back to contacts", reverse("contact_list"), "primary"),
            *(
                [_contact_action("Edit contact", reverse("contact_edit", args=[contact.id]))]
                if can_manage_records
                else []
            ),
            _contact_action("Open companies", reverse("company_list")),
        ],
        "top_panels": [
            {
                "kicker": "Profile summary",
                "title": "Contact snapshot",
                "body_template": DETAIL_FIELD_GRID_TEMPLATE,
                "fields": [
                    {
                        "label": "Title",
                        "value": contact.title or "No title captured",
                        "span": 2,
                        "nowrap": True,
                        "title": contact.title or "No title captured",
                    },
                    {
                        "label": "Primary email",
                        "value": contact.email or "No primary email",
                        "span": 2,
                        "nowrap": True,
                        "title": contact.email or "No primary email",
                    },
                    {"label": "Primary phone", "value": contact.phone or "No primary phone"},
                    {"label": "Created", "value": contact.created_at.strftime("%Y-%m-%d %H:%M")},
                ],
                "note_kicker": "Internal notes",
                "note": contact.notes,
                "note_empty_text": "No internal notes are stored for this contact yet.",
            },
            {
                "kicker": "Relationships",
                "title": "Linked companies",
                "body_template": DETAIL_CARDS_TEMPLATE,
                "items": [
                    {
                        "kicker": "Company",
                        "title": company.name,
                        "meta": company.industry or "No industry tagged",
                        "body": ", ".join(value for value in (company.city, company.state, company.country) if value)
                        or "No location captured",
                        "link": {
                            "href": reverse("company_detail", args=[company.id]),
                            "label": "Open company",
                        },
                    }
                    for company in companies
                ],
                "empty_text": "No companies are linked to this contact yet.",
            },
        ],
        "bottom_panels": [
            {
                "kicker": "Channels",
                "title": "Email addresses",
                "body_template": DETAIL_CHANNEL_LIST_TEMPLATE,
                "lines": [
                    {
                        "label": humanize_channel_value((email.label or "").strip().lower()) or "Inbox",
                        "value": email.email,
                    }
                    for email in emails
                ],
                "empty_text": "No additional email addresses are stored yet.",
            },
            {
                "kicker": "Channels",
                "title": "Phone lines",
                "body_template": DETAIL_CHANNEL_LIST_TEMPLATE,
                "lines": [
                    {
                        "label": humanize_channel_value((phone.label or "").strip().lower()) or "Line",
                        "value": phone.phone,
                    }
                    for phone in phones
                ],
                "empty_text": "No additional phone numbers are stored yet.",
            },
        ],
        "profile_panel": {
            "kicker": "Channels",
            "title": "Public profiles",
            "body_template": DETAIL_CARDS_TEMPLATE,
            "items": [
                {
                    "kicker": humanize_channel_value((link.platform or "").strip().lower()) or "Profile",
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


@crm_role_required(ROLE_TEAM_LEAD)
def contact_company_search(request):
    query = _clean_text(request.GET.get("q"))
    if len(query) < 2:
        return JsonResponse({"results": []})

    companies = (
        Company.objects.filter(
            Q(name__icontains=query)
            | Q(industry__icontains=query)
            | Q(city__icontains=query)
            | Q(state__icontains=query)
            | Q(country__icontains=query)
        )
        .order_by("name")[:RELATION_SEARCH_LIMIT]
    )
    return JsonResponse(
        {
            "results": [
                {
                    "id": company.pk,
                    "label": company.name,
                    "meta": " | ".join(
                        value
                        for value in (
                            company.industry,
                            ", ".join(
                                value for value in (company.city, company.state, company.country) if value
                            ),
                        )
                        if value
                    ),
                }
                for company in companies
            ]
        }
    )


def _contact_list_state(request):
    per_page = _clean_per_page(request.GET.get("per_page"))
    visible_columns = _clean_column_list(
        request.GET.get("columns"),
        [key for key, _label in CONTACT_COLUMN_OPTIONS],
        DEFAULT_CONTACT_COLUMNS,
    )
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
        "visible_columns": visible_columns,
        "selected_columns_query": ",".join(visible_columns),
        "column_options": [
            {"key": key, "label": label} for key, label in CONTACT_COLUMN_OPTIONS
        ],
        "per_page": per_page,
        "per_page_options": PAGE_SIZE_OPTIONS,
    }


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

    filter_reset_query = _query_string(
        request,
        remove_keys=CONTACT_FILTER_KEYS | {"page", "export"},
    )
    per_page_menu_options = []
    contact_list_url = reverse("contact_list")
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
                "url": f"{contact_list_url}?{query_string}" if query_string else contact_list_url,
            }
        )

    filter_reset_url = reverse("contact_list")
    if filter_reset_query:
        filter_reset_url = f"{filter_reset_url}?{filter_reset_query}"

    page_obj = _paginate(request, state["queryset"], per_page=state["per_page"])
    row_number_offset = page_obj.start_index() - 1 if page_obj.paginator.count else 0
    table_headers = _build_contact_table_headers(state["visible_columns"])
    table_rows = _build_contact_table_rows(page_obj.object_list, table_headers, row_number_offset)
    hero_metrics = _build_contact_hero_metrics(
        page_obj,
        total_contacts=state["total_contacts"],
        filters_active=state["filters_active"],
    )
    can_manage_records = user_has_minimum_crm_role(request.user, ROLE_TEAM_LEAD)
    hero_actions = _build_contact_hero_actions(can_manage_records)
    filter_form_hidden_items = _query_items(
        request,
        remove_keys=CONTACT_FILTER_KEYS | {"page", "export"},
    )
    column_picker_hidden_items = _query_items(
        request,
        remove_keys={"columns", "page", "export"},
    )
    per_page_hidden_items = _query_items(
        request,
        remove_keys={"per_page", "page", "export"},
    )
    filter_panel = _build_contact_filter_panel(
        has_contact_records=state["has_contact_records"],
        filters_active=state["filters_active"],
        filters=state["filters"],
        filter_form_hidden_items=filter_form_hidden_items,
        active_filters=state["active_filters"],
        total_contacts=state["total_contacts"],
        matching_count=page_obj.paginator.count,
        filter_reset_url=filter_reset_url,
    )
    filter_ui = _build_contact_filter_ui()
    toolbar_menus = _build_contact_toolbar_menus(
        per_page=state["per_page"],
        per_page_menu_options=per_page_menu_options,
        column_picker_hidden_items=column_picker_hidden_items,
        selected_columns_query=state["selected_columns_query"],
        column_options=state["column_options"],
        visible_columns=state["visible_columns"],
        export_csv_query=_export_query(request, "csv"),
        export_xlsx_query=_export_query(request, "xlsx"),
    )
    table_ui = _build_contact_table_ui()
    empty_state = _build_contact_empty_state(
        filters_active=state["filters_active"],
        has_contact_records=state["has_contact_records"],
        active_filters=state["active_filters"],
        filter_reset_url=filter_reset_url,
        can_import=can_manage_records,
    )
    return render(
        request,
        "crm/contacts/contact_list.html",
        {
            **state,
            "contacts": page_obj.object_list,
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
def contact_detail(request, contact_id):
    contact = get_object_or_404(_contact_detail_queryset(), pk=contact_id)
    can_manage_records = user_has_minimum_crm_role(request.user, ROLE_TEAM_LEAD)
    return render(
        request,
        "crm/contacts/contact_detail.html",
        {
            "contact": contact,
            **_build_contact_detail_context(contact, can_manage_records=can_manage_records),
        },
    )


@crm_role_required(ROLE_TEAM_LEAD)
def contact_create(request):
    contact = Contact()
    bundle = _contact_form_bundle(request, contact)
    if request.method == "POST":
        saved_contact = save_contact_bundle(
            bundle["form"],
            bundle["phone_formset"],
            bundle["email_formset"],
            bundle["social_link_formset"],
        )
        if saved_contact is not None:
            messages.success(request, "Contact created.")
            return redirect("contact_detail", contact_id=saved_contact.pk)

    return render(
        request,
        "crm/contacts/contact_form.html",
        _contact_form_context(contact, bundle, is_edit_mode=False),
    )


@crm_role_required(ROLE_TEAM_LEAD)
def contact_edit(request, contact_id):
    contact = get_object_or_404(_contact_detail_queryset(), pk=contact_id)
    bundle = _contact_form_bundle(request, contact)
    if request.method == "POST":
        saved_contact = save_contact_bundle(
            bundle["form"],
            bundle["phone_formset"],
            bundle["email_formset"],
            bundle["social_link_formset"],
        )
        if saved_contact is not None:
            messages.success(request, "Contact updated.")
            return redirect("contact_detail", contact_id=saved_contact.pk)

    return render(
        request,
        "crm/contacts/contact_form.html",
        _contact_form_context(contact, bundle, is_edit_mode=True),
    )
