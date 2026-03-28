"""Entity creation helpers for the import workflow."""

from django.db import transaction

from crm.models import Company, Contact
from crm.services.import_components.data_cleaner import DataCleaner


class EntityCreator:
    """Create or hydrate CRM entities from mapped import data."""

    _COMPANY_FIELD_MAP = {
        "industry": "industry",
        "company_size": "company_size",
        "revenue": "revenue",
        "address": "address",
        "city": "city",
        "state": "state",
        "zip_code": "zip_code",
        "country": "country",
    }
    _CONTACT_FIELD_MAP = {
        "title": "title",
    }

    @classmethod
    def _clean_kwargs(cls, field_map: dict[str, str], fields: dict) -> dict[str, str]:
        cleaned = {}
        for key, field_name in field_map.items():
            cleaned[key] = DataCleaner.clean_for_model_field(field_name, fields.get(key, ""))
        return cleaned

    @staticmethod
    def _build_contact_name(first_name: str, last_name: str) -> str:
        return DataCleaner.clean_for_model_field("full_name", f"{first_name} {last_name}")

    @staticmethod
    def _missing_clean_value(instance, field_name: str) -> str:
        return DataCleaner.clean(getattr(instance, field_name, ""))

    @classmethod
    def _apply_missing_fields(cls, instance, cleaned_fields: dict[str, str]) -> None:
        update_fields = []
        for field_name, value in cleaned_fields.items():
            if value and not cls._missing_clean_value(instance, field_name):
                setattr(instance, field_name, value)
                update_fields.append(field_name)
        if update_fields:
            instance.save(update_fields=update_fields)

    @classmethod
    def get_or_create_company(cls, company_name: str, **fields) -> tuple[Company, bool]:
        cleaned_name = DataCleaner.clean_for_model_field("name", company_name)
        cleaned_fields = cls._clean_kwargs(cls._COMPANY_FIELD_MAP, fields)

        with transaction.atomic():
            company, created = Company.objects.get_or_create(
                name=cleaned_name,
                defaults=cleaned_fields,
            )
            if not created:
                cls._apply_missing_fields(company, cleaned_fields)
        return company, created

    @classmethod
    def get_or_create_contact(cls, first_name: str, last_name: str, **fields) -> tuple[Contact, bool]:
        full_name = cls._build_contact_name(first_name, last_name)
        cleaned_fields = cls._clean_kwargs(cls._CONTACT_FIELD_MAP, fields)

        with transaction.atomic():
            contact, created = Contact.objects.get_or_create(
                full_name=full_name,
                defaults=cleaned_fields,
            )
            if not created:
                cls._apply_missing_fields(contact, cleaned_fields)
        return contact, created


__all__ = [
    "EntityCreator",
]
