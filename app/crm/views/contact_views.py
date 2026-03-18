"""Contact list and filtering views."""

from __future__ import annotations

from django.db.models import BooleanField, Case, Exists, OuterRef, Q, Value, When
from django.shortcuts import render

from crm.auth import ROLE_STAFF, crm_role_required
from crm.export_utils import CONTACT_EXPORT_COLUMNS, serialize_contact_export_row
from crm.models import Contact, ContactEmail, ContactPhone, ContactSocialLink
from ._shared import (
    BOOLEAN_FILTER_LABELS,
    _add_active_filter,
    _apply_toggle_filter,
    _clean_export_format,
    _clean_text,
    _clean_toggle,
    _export_query,
    _export_response,
    _page_query,
    _paginate,
    _parse_date_value,
)


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
        "crm/contacts/contact_list.html",
        {
            **state,
            "contacts": page_obj.object_list,
            "page_obj": page_obj,
            "page_query": _page_query(request),
            "export_csv_query": _export_query(request, "csv"),
            "export_xlsx_query": _export_query(request, "xlsx"),
        },
    )
