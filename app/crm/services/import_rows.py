"""Helpers for the payload-backed ImportRow model."""

from __future__ import annotations

from django.db.models import CharField, F, QuerySet, Value
from django.db.models.functions import Cast, Coalesce


IMPORT_ROW_PAYLOAD_KEY_MAP = {
    "company_name": "company_name",
    "industry": "industry",
    "company_size": "company_size",
    "revenue": "revenue",
    "website": "website",
    "contact_name": "contact_name",
    "contact_first_name": "contact_first_name",
    "contact_last_name": "contact_last_name",
    "contact_title": "contact_title",
    "email_address": "email",
    "phone_number": "phone",
    "person_source": "person_source",
    "address": "address",
    "city": "city",
    "state": "state",
    "zip_code": "zip_code",
    "country": "country",
}

IMPORT_ROW_PAYLOAD_KEYS = (
    "company_name",
    "industry",
    "company_size",
    "revenue",
    "website",
    "contact_name",
    "contact_first_name",
    "contact_last_name",
    "contact_title",
    "email",
    "phone",
    "person_source",
    "address",
    "city",
    "state",
    "zip_code",
    "country",
)


def payload_key_for_import_row_field(field_name: str) -> str:
    return IMPORT_ROW_PAYLOAD_KEY_MAP.get(field_name, field_name)


def build_import_row_payload(row_values: dict[str, str]) -> dict[str, str]:
    payload = {}
    for payload_key in IMPORT_ROW_PAYLOAD_KEYS:
        value = (row_values.get(payload_key) or "").strip()
        if value:
            payload[payload_key] = value
    return payload


def get_import_row_field_value(import_row, field_name: str) -> str:
    payload = getattr(import_row, "mapped_payload", {}) or {}
    value = payload.get(payload_key_for_import_row_field(field_name), "")
    return "" if value is None else str(value)


def import_row_annotation_name(field_name: str) -> str:
    return f"payload_{field_name}"


def annotate_import_row_payload_values(queryset: QuerySet, field_names) -> QuerySet:
    annotations = {}
    for field_name in field_names:
        if field_name == "row_number":
            continue
        annotations[import_row_annotation_name(field_name)] = Coalesce(
            Cast(F(f"mapped_payload__{payload_key_for_import_row_field(field_name)}"), CharField()),
            Value(""),
        )
    if not annotations:
        return queryset
    return queryset.annotate(**annotations)


__all__ = [
    "IMPORT_ROW_PAYLOAD_KEYS",
    "build_import_row_payload",
    "get_import_row_field_value",
    "annotate_import_row_payload_values",
    "import_row_annotation_name",
    "payload_key_for_import_row_field",
]
