"""Orchestrate the full import workflow using extracted import components."""

from __future__ import annotations

from urllib.parse import urlparse

from django.db import transaction

from crm.models import ImportFile, ImportRow
from crm.services.import_components.data_cleaner import DataCleaner
from crm.services.import_components.entity_creator import EntityCreator
from crm.services.import_components.field_mapper import FieldMapper
from crm.services.import_components.import_stats import ImportStats
from crm.services.import_components.relationship_builder import RelationshipBuilder


class ImportOrchestrator:
    """Coordinate row mapping, entity creation, and relationship building."""

    _ROW_SIGNATURE_FIELDS = (
        "company_name",
        "industry",
        "company_size",
        "revenue",
        "website",
        "contact_name",
        "import_row_contact_title",
        "email",
        "phone",
        "person_source",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
    )

    @classmethod
    def _normalize_mapping(cls, field_mapping: dict[str, str]) -> dict[str, str]:
        target_fields = set(FieldMapper.get_target_fields())
        if any(key in target_fields for key in field_mapping):
            return {
                key: DataCleaner.clean(value)
                for key, value in field_mapping.items()
                if key in target_fields and DataCleaner.clean(value)
            }
        return {
            value: DataCleaner.clean(key)
            for key, value in field_mapping.items()
            if value in target_fields and DataCleaner.clean(key)
        }

    @staticmethod
    def _normalize_row(row: dict) -> dict[str, object]:
        return {
            DataCleaner.clean(key): value
            for key, value in row.items()
            if DataCleaner.clean(key)
        }

    @classmethod
    def _mapped_value(
        cls,
        normalized_row: dict[str, object],
        mapping: dict[str, str],
        target_field: str,
    ) -> str:
        source_field = DataCleaner.clean(mapping.get(target_field, ""))
        if not source_field:
            return ""
        return DataCleaner.clean(normalized_row.get(source_field, ""))

    @classmethod
    def _extract_row_values(
        cls,
        row: dict,
        mapping: dict[str, str],
    ) -> dict[str, str]:
        normalized_row = cls._normalize_row(row)
        company_name = DataCleaner.clean_for_model_field(
            "name",
            cls._mapped_value(normalized_row, mapping, "company_name"),
        )
        first_name = DataCleaner.clean(
            cls._mapped_value(normalized_row, mapping, "contact_first_name")
        )
        last_name = DataCleaner.clean(
            cls._mapped_value(normalized_row, mapping, "contact_last_name")
        )
        contact_name = DataCleaner.clean_for_model_field(
            "full_name",
            cls._mapped_value(normalized_row, mapping, "contact_name"),
        )
        if not contact_name and (first_name or last_name):
            contact_name = DataCleaner.clean_for_model_field(
                "full_name",
                f"{first_name} {last_name}",
            )

        raw_contact_title = cls._mapped_value(normalized_row, mapping, "contact_title")
        return {
            "company_name": company_name,
            "industry": DataCleaner.clean_for_model_field(
                "industry",
                cls._mapped_value(normalized_row, mapping, "industry"),
            ),
            "company_size": DataCleaner.clean_for_model_field(
                "company_size",
                cls._mapped_value(normalized_row, mapping, "company_size"),
            ),
            "revenue": DataCleaner.clean_for_model_field(
                "revenue",
                cls._mapped_value(normalized_row, mapping, "revenue"),
            ),
            "website": DataCleaner.clean_for_model_field(
                "url",
                cls._mapped_value(normalized_row, mapping, "website"),
            ),
            "contact_name": contact_name,
            "contact_first_name": first_name,
            "contact_last_name": last_name,
            "contact_title": DataCleaner.clean_for_model_field(
                "title",
                raw_contact_title,
            ),
            "import_row_contact_title": DataCleaner.clean_for_model_field(
                "contact_title",
                raw_contact_title,
            ),
            "email": DataCleaner.clean_for_model_field(
                "email",
                cls._mapped_value(normalized_row, mapping, "email"),
            ),
            "phone": DataCleaner.clean_for_model_field(
                "phone",
                cls._mapped_value(normalized_row, mapping, "phone"),
            ),
            "person_source": DataCleaner.clean_for_model_field(
                "url",
                cls._mapped_value(normalized_row, mapping, "person_source"),
            ),
            "address": DataCleaner.clean_for_model_field(
                "address",
                cls._mapped_value(normalized_row, mapping, "address"),
            ),
            "city": DataCleaner.clean_for_model_field(
                "city",
                cls._mapped_value(normalized_row, mapping, "city"),
            ),
            "state": DataCleaner.clean_for_model_field(
                "state",
                cls._mapped_value(normalized_row, mapping, "state"),
            ),
            "zip_code": DataCleaner.clean_for_model_field(
                "zip_code",
                cls._mapped_value(normalized_row, mapping, "zip_code"),
            ),
            "country": DataCleaner.clean_for_model_field(
                "country",
                cls._mapped_value(normalized_row, mapping, "country"),
            ),
        }

    @classmethod
    def _row_signature(cls, row_values: dict[str, str]) -> tuple[str, ...]:
        return tuple(DataCleaner.clean(row_values.get(key, "")) for key in cls._ROW_SIGNATURE_FIELDS)

    @staticmethod
    def _infer_platform(url: str) -> str:
        if not url:
            return ""
        netloc = (urlparse(url).netloc or "").lower()
        if "linkedin.com" in netloc:
            return "linkedin"
        if "facebook.com" in netloc:
            return "facebook"
        if "instagram.com" in netloc:
            return "instagram"
        if "x.com" in netloc or "twitter.com" in netloc:
            return "x"
        if "youtube.com" in netloc:
            return "youtube"
        return "website"

    @staticmethod
    def _has_primary_entity(row_values: dict[str, str]) -> bool:
        return bool(row_values.get("company_name") or row_values.get("contact_name"))

    @staticmethod
    def _resolve_contact_name_parts(row_values: dict[str, str]) -> tuple[str, str]:
        if row_values.get("contact_first_name") or row_values.get("contact_last_name"):
            return row_values.get("contact_first_name", ""), row_values.get("contact_last_name", "")
        return row_values.get("contact_name", ""), ""

    @classmethod
    def execute(
        cls,
        csv_rows: list[dict],
        field_mapping: dict[str, str],
        import_file: ImportFile | None = None,
        progress_callback=None,
    ) -> ImportStats:
        stats = ImportStats()
        normalized_mapping = cls._normalize_mapping(field_mapping)
        total_rows = len(csv_rows)
        seen_signatures = set()

        for row_number, row in enumerate(csv_rows, start=2):
            stats.increment_rows_processed()
            try:
                row_values = cls._extract_row_values(row, normalized_mapping)

                if not any(
                    row_values.get(field)
                    for field in cls._ROW_SIGNATURE_FIELDS
                ):
                    stats.skipped_empty_rows += 1
                    stats.increment_failed()
                    stats.add_error(row_number, "Row was empty after mapping.")
                    continue

                signature = cls._row_signature(row_values)
                if signature in seen_signatures:
                    stats.skipped_duplicate_rows += 1
                    stats.increment_failed()
                    stats.add_error(row_number, "Duplicate mapped row in this import.")
                    continue
                seen_signatures.add(signature)

                company = None
                company_created = False
                contact = None
                contact_created = False

                with transaction.atomic():
                    if row_values["company_name"]:
                        company, company_created = EntityCreator.get_or_create_company(
                            row_values["company_name"],
                            industry=row_values["industry"],
                            company_size=row_values["company_size"],
                            revenue=row_values["revenue"],
                            address=row_values["address"],
                            city=row_values["city"],
                            state=row_values["state"],
                            zip_code=row_values["zip_code"],
                            country=row_values["country"],
                        )
                        if company_created:
                            stats.created_companies += 1

                    if row_values["contact_name"]:
                        first_name, last_name = cls._resolve_contact_name_parts(row_values)
                        contact, contact_created = EntityCreator.get_or_create_contact(
                            first_name,
                            last_name,
                            title=row_values["contact_title"],
                            email=row_values["email"],
                            phone=row_values["phone"],
                        )
                        if contact_created:
                            stats.created_contacts += 1

                    if company and contact:
                        link_created = not company.contacts.filter(pk=contact.pk).exists()
                        RelationshipBuilder.link_contact_to_company(contact, company)
                        if link_created:
                            stats.links_created += 1

                    if contact:
                        email_created = bool(
                            row_values["email"]
                            and not contact.emails.filter(email=row_values["email"]).exists()
                        )
                        RelationshipBuilder.create_contact_email(contact, row_values["email"])
                        if email_created:
                            stats.email_rows_created += 1

                        phone_created = bool(
                            row_values["phone"]
                            and not contact.phones.filter(phone=row_values["phone"]).exists()
                        )
                        RelationshipBuilder.create_contact_phone(contact, row_values["phone"])
                        if phone_created:
                            stats.phone_rows_created += 1

                        social_created = bool(
                            row_values["person_source"]
                            and not contact.social_links.filter(url=row_values["person_source"]).exists()
                        )
                        RelationshipBuilder.create_contact_social_link(
                            contact,
                            row_values["person_source"],
                            cls._infer_platform(row_values["person_source"]),
                        )
                        if social_created:
                            stats.social_rows_created += 1

                    if company:
                        company_social_created = bool(
                            row_values["website"]
                            and not company.social_links.filter(url=row_values["website"]).exists()
                        )
                        RelationshipBuilder.create_company_social_link(
                            company,
                            row_values["website"],
                            cls._infer_platform(row_values["website"]),
                        )
                        if company_social_created:
                            stats.company_social_rows_created += 1

                    if import_file is not None:
                        _, row_created = ImportRow.objects.update_or_create(
                            import_file=import_file,
                            row_number=row_number,
                            defaults={
                                "company": company,
                                "contact": contact,
                                "company_name": row_values["company_name"],
                                "website": row_values["website"],
                                "contact_name": row_values["contact_name"],
                                "contact_title": row_values["import_row_contact_title"],
                                "email_address": row_values["email"],
                                "phone_number": row_values["phone"],
                                "person_source": row_values["person_source"],
                                "address": row_values["address"],
                                "city": row_values["city"],
                                "state": row_values["state"],
                                "zip_code": row_values["zip_code"],
                                "country": row_values["country"],
                            },
                        )
                        if row_created:
                            stats.import_rows_created += 1
                        else:
                            stats.import_rows_updated += 1

                if company_created:
                    stats.increment_created()
                elif company:
                    stats.increment_updated()

                if contact_created:
                    stats.increment_created()
                elif contact:
                    stats.increment_updated()

                if not cls._has_primary_entity(row_values) or (not company and not contact):
                    stats.skipped_rows += 1
                    stats.increment_failed()
                    stats.add_error(
                        row_number,
                        "Row did not create or match a company or contact.",
                    )
            except Exception as exc:
                stats.skipped_rows += 1
                stats.increment_failed()
                stats.add_error(row_number, str(exc))
            finally:
                if progress_callback:
                    progress_callback(stats.rows_processed, total_rows)

        return stats


__all__ = [
    "ImportOrchestrator",
]
