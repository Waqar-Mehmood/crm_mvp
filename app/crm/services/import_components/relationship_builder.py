"""Relationship and contact-detail builders for the import workflow."""

from crm.models import (
    Company,
    CompanySocialLink,
    Contact,
    ContactEmail,
    ContactPhone,
    ContactSocialLink,
)
from crm.services.import_components.data_cleaner import DataCleaner


class RelationshipBuilder:
    """Create CRM relationships and contact detail rows safely."""

    @staticmethod
    def _clean_email(email: str) -> str:
        return DataCleaner.clean_for_model_field("email", email)

    @staticmethod
    def _clean_phone(phone: str) -> str:
        return DataCleaner.clean_for_model_field("phone", phone)

    @staticmethod
    def _clean_url(url: str) -> str:
        return DataCleaner.clean_for_model_field("url", url)

    @classmethod
    def link_contact_to_company(cls, contact: Contact, company: Company) -> None:
        if not contact or not company:
            return
        if not company.contacts.filter(pk=contact.pk).exists():
            company.contacts.add(contact)

    @classmethod
    def create_contact_email(cls, contact: Contact, email: str) -> ContactEmail | None:
        cleaned_email = cls._clean_email(email)
        if not contact or not cleaned_email:
            return None
        email_row, _ = ContactEmail.objects.get_or_create(
            contact=contact,
            email=cleaned_email,
            defaults={"label": "work"},
        )
        return email_row

    @classmethod
    def create_contact_phone(cls, contact: Contact, phone: str) -> ContactPhone | None:
        cleaned_phone = cls._clean_phone(phone)
        if not contact or not cleaned_phone:
            return None
        phone_row, _ = ContactPhone.objects.get_or_create(
            contact=contact,
            phone=cleaned_phone,
            defaults={"label": "work"},
        )
        return phone_row

    @classmethod
    def create_contact_social_link(
        cls,
        contact: Contact,
        url: str,
        platform: str,
    ) -> ContactSocialLink | None:
        cleaned_url = cls._clean_url(url)
        cleaned_platform = DataCleaner.clean(platform)
        if not contact or not cleaned_url:
            return None
        social_link, _ = ContactSocialLink.objects.get_or_create(
            contact=contact,
            url=cleaned_url,
            defaults={"platform": cleaned_platform},
        )
        return social_link

    @classmethod
    def create_company_social_link(
        cls,
        company: Company,
        url: str,
        platform: str,
    ) -> CompanySocialLink | None:
        cleaned_url = cls._clean_url(url)
        cleaned_platform = DataCleaner.clean(platform)
        if not company or not cleaned_url:
            return None
        social_link, _ = CompanySocialLink.objects.get_or_create(
            company=company,
            url=cleaned_url,
            defaults={"platform": cleaned_platform},
        )
        return social_link


__all__ = [
    "RelationshipBuilder",
]
