"""Contact list, detail, and form views."""

from __future__ import annotations

from django.contrib import messages
from django.http import JsonResponse
from django.db.models import BooleanField, Case, Exists, OuterRef, Prefetch, Q, Value, When
from django.shortcuts import get_object_or_404, redirect, render
from django.urls import reverse

from crm.auth import ROLE_STAFF, ROLE_TEAM_LEAD, crm_role_required
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


def _contact_detail_queryset():
    return Contact.objects.prefetch_related(
        Prefetch("companies", queryset=Company.objects.order_by("name")),
        "phones",
        "emails",
        "social_links",
    )


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
    return {
        "contact": contact,
        "form": bundle["form"],
        "selected_companies": list(bundle["form"].fields["companies"].queryset),
        "phone_formset": bundle["phone_formset"],
        "email_formset": bundle["email_formset"],
        "social_link_formset": bundle["social_link_formset"],
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
            "row_number_offset": page_obj.start_index() - 1 if page_obj.paginator.count else 0,
            "per_page_menu_options": per_page_menu_options,
            "filter_form_hidden_items": _query_items(
                request,
                remove_keys=CONTACT_FILTER_KEYS | {"page", "export"},
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
def contact_detail(request, contact_id):
    contact = get_object_or_404(_contact_detail_queryset(), pk=contact_id)
    return render(
        request,
        "crm/contacts/contact_detail.html",
        {
            "contact": contact,
            "company_count": len(contact.companies.all()),
            "phone_count": len(contact.phones.all()),
            "email_count": len(contact.emails.all()),
            "profile_count": len(contact.social_links.all()),
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
