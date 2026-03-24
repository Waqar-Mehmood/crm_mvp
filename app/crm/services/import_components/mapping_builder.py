"""Build reusable field-mapping metadata for the import UI."""

from __future__ import annotations

from crm.services.import_components.field_mapper import FieldMapper


class MappingBuilder:
    """Build and validate UI-facing field mapping definitions."""

    _FIELD_LABELS = {
        "company_name": "Company Name",
        "industry": "Industry / Business Type",
        "company_size": "Company Size",
        "revenue": "Revenue",
        "website": "Website / Company URL",
        "contact_name": "Contact Full Name",
        "contact_first_name": "Contact First Name",
        "contact_last_name": "Contact Last Name",
        "contact_title": "Contact Title",
        "email": "Email",
        "phone": "Phone",
        "person_source": "Person Source / Profile URL",
        "address": "Address / Location",
        "city": "City",
        "state": "State",
        "zip_code": "Zip Code",
        "country": "Country",
    }
    _REQUIRED_FIELDS = ["company_name", "contact_name"]
    _SPLIT_CONTACT_FIELDS = ("contact_first_name", "contact_last_name")

    @staticmethod
    def _clean(value) -> str:
        if value is None:
            return ""
        return " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()

    @classmethod
    def _clean_headers(cls, csv_headers: list[str]) -> list[str]:
        headers = []
        seen = set()
        for header in csv_headers:
            cleaned = cls._clean(header)
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            headers.append(cleaned)
        return headers

    @classmethod
    def _is_required_field(cls, field_name: str) -> bool:
        return field_name in cls._REQUIRED_FIELDS or field_name in cls._SPLIT_CONTACT_FIELDS

    @classmethod
    def build_mapping_fields(cls, csv_headers: list[str]) -> list[dict]:
        headers = cls._clean_headers(csv_headers)
        suggestions = FieldMapper.suggest_mapping(headers)
        mapping_fields = []

        for target_field in FieldMapper.get_target_fields():
            mapping_fields.append(
                {
                    "target_field": target_field,
                    "label": cls._FIELD_LABELS.get(target_field, target_field),
                    "suggested_column": suggestions.get(target_field, ""),
                    "required": cls._is_required_field(target_field),
                    "csv_options": list(headers),
                }
            )

        return sorted(
            mapping_fields,
            key=lambda field: (
                not field["required"],
                field["suggested_column"] == "",
                field["label"],
            ),
        )

    @classmethod
    def get_required_fields(cls) -> list[str]:
        return list(cls._REQUIRED_FIELDS)

    @classmethod
    def get_optional_fields(cls) -> list[str]:
        required = set(cls._REQUIRED_FIELDS) | set(cls._SPLIT_CONTACT_FIELDS)
        return [
            field_name
            for field_name in FieldMapper.get_target_fields()
            if field_name not in required
        ]

    @classmethod
    def validate_user_mapping(cls, user_mapping: dict) -> tuple[bool, str]:
        normalized_mapping = {
            key: cls._clean(value)
            for key, value in (user_mapping or {}).items()
            if key in FieldMapper.get_target_fields()
        }

        has_company = bool(normalized_mapping.get("company_name"))
        has_contact_name = bool(normalized_mapping.get("contact_name"))
        has_split_name = all(normalized_mapping.get(field) for field in cls._SPLIT_CONTACT_FIELDS)
        if not has_company and not has_contact_name and not has_split_name:
            return (
                False,
                "Map at least one identifying field: Company Name, Contact Full Name, or both Contact First Name and Contact Last Name.",
            )

        return True, ""


__all__ = [
    "MappingBuilder",
]
