"""Contact-focused service helpers."""

from __future__ import annotations

from django.db import transaction
from django.db.models import CharField, OuterRef, QuerySet, Subquery, Value
from django.db.models.functions import Coalesce

from crm.models import ContactEmail, ContactPhone


def annotate_contact_primary_channels(queryset: QuerySet) -> QuerySet:
    email_rows = ContactEmail.objects.filter(contact_id=OuterRef("pk")).order_by("-is_primary", "id")
    phone_rows = ContactPhone.objects.filter(contact_id=OuterRef("pk")).order_by("-is_primary", "id")
    return queryset.annotate(
        primary_email_value=Coalesce(
            Subquery(email_rows.values("email")[:1], output_field=CharField()),
            Value(""),
        ),
        primary_phone_value=Coalesce(
            Subquery(phone_rows.values("phone")[:1], output_field=CharField()),
            Value(""),
        ),
    )


def _normalize_label(label: str, default_label: str) -> str:
    value = (label or "").strip().lower()
    return value or default_label


def _existing_primary_row(manager):
    return manager.filter(is_primary=True).order_by("id").first()


def _normalize_primary_rows(manager, *, preferred_row=None):
    rows = list(manager.order_by("id"))
    if not rows:
        return None

    chosen_row = preferred_row
    if chosen_row is None:
        chosen_row = next((row for row in rows if row.is_primary), rows[0])

    for row in rows:
        should_be_primary = row.pk == chosen_row.pk
        if row.is_primary != should_be_primary:
            row.is_primary = should_be_primary
            row.save(update_fields=["is_primary"])
    return chosen_row


def _upsert_primary_channel(manager, model, value_field: str, value: str, default_label: str):
    value = (value or "").strip()
    if not value:
        return _normalize_primary_rows(manager)

    existing_row = manager.filter(**{value_field: value}).order_by("-is_primary", "id").first()
    current_primary = _existing_primary_row(manager)
    if existing_row is None:
        if current_primary is not None:
            current_primary_updates = []
            if getattr(current_primary, value_field) != value:
                setattr(current_primary, value_field, value)
                current_primary_updates.append(value_field)
            if not (current_primary.label or "").strip():
                current_primary.label = default_label
                current_primary_updates.append("label")
            if current_primary_updates:
                current_primary.save(update_fields=current_primary_updates)
            existing_row = current_primary
        else:
            existing_row = model.objects.create(
                contact=manager.instance,
                **{
                    value_field: value,
                    "label": default_label,
                    "is_primary": False,
                },
            )
    elif not (existing_row.label or "").strip():
        existing_row.label = default_label
        existing_row.save(update_fields=["label"])

    return _normalize_primary_rows(manager, preferred_row=existing_row)


def _merge_import_channel(manager, model, value_field: str, value: str, default_label: str):
    value = (value or "").strip()
    if not value:
        return None

    existing_row = manager.filter(**{value_field: value}).order_by("-is_primary", "id").first()
    current_primary = _existing_primary_row(manager)
    if existing_row is not None:
        if current_primary is None:
            _normalize_primary_rows(manager, preferred_row=existing_row)
        return existing_row

    row = model.objects.create(
        contact=manager.instance,
        **{
            value_field: value,
            "label": default_label,
            "is_primary": current_primary is None,
        },
    )
    if current_primary is None:
        _normalize_primary_rows(manager, preferred_row=row)
    return row


def get_primary_contact_email(contact) -> str:
    return contact.email


def get_primary_contact_phone(contact) -> str:
    return contact.phone


def sync_primary_contact_channels(contact, *, email: str = "", phone: str = "") -> None:
    _upsert_primary_channel(
        contact.emails,
        ContactEmail,
        "email",
        (email or "").strip(),
        "work",
    )
    _upsert_primary_channel(
        contact.phones,
        ContactPhone,
        "phone",
        (phone or "").strip(),
        "work",
    )


def merge_import_contact_channels(contact, *, email: str = "", phone: str = "") -> None:
    _merge_import_channel(
        contact.emails,
        ContactEmail,
        "email",
        (email or "").strip(),
        "work",
    )
    _merge_import_channel(
        contact.phones,
        ContactPhone,
        "phone",
        (phone or "").strip(),
        "work",
    )


def save_contact_bundle(
    form,
    phone_formset,
    email_formset,
    social_link_formset,
):
    is_valid = (
        form.is_valid()
        and phone_formset.is_valid()
        and email_formset.is_valid()
        and social_link_formset.is_valid()
    )
    if not is_valid:
        return None

    with transaction.atomic():
        contact = form.save()
        contact.companies.set(form.cleaned_data["companies"])

        phone_formset.instance = contact
        email_formset.instance = contact
        social_link_formset.instance = contact

        phone_formset.save()
        email_formset.save()
        social_link_formset.save()

        sync_primary_contact_channels(
            contact,
            email=form.cleaned_data.get("email", ""),
            phone=form.cleaned_data.get("phone", ""),
        )

    return contact


__all__ = [
    "annotate_contact_primary_channels",
    "get_primary_contact_email",
    "get_primary_contact_phone",
    "merge_import_contact_channels",
    "save_contact_bundle",
    "sync_primary_contact_channels",
]
