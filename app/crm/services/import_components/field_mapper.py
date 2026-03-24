"""Field mapping helpers for the import workflow."""

TARGET_FIELDS = [
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
]

SOURCE_IMPORT_FIELD_MAP = {
    "company_name": "company_name",
    "industry": "industry",
    "company_size": "company_size",
    "revenue": "revenue",
    "website": "website",
    "contact_name": "contact_name",
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

SUGGEST_MAPPING_ALIASES = {
    "company_name": (
        "Company Name",
        "Company",
        "Organisation",
        "Organization",
    ),
    "industry": (
        "Industry",
        "Business Type",
        "Category",
    ),
    "company_size": (
        "Company size",
        "Company Size",
        "Estimated Number of Employees",
        "Employee Size",
    ),
    "revenue": ("Revenue",),
    "website": (
        "Website",
        "Company URL",
        "Company Website",
        "Website URL",
        "URL",
    ),
    "contact_name": (
        "Contact Name",
        "Full Name",
        "Name",
        "Owner/CEO Name",
        "Owner name (if possible)",
        "Owner Name",
        "CEO Name",
    ),
    "contact_first_name": (
        "First Name",
        "FirstName",
        "First_Name",
    ),
    "contact_last_name": (
        "Last Name",
        "LastName",
        "Last_Name",
    ),
    "contact_title": (
        "Contact Title",
        "Title",
        "Job Title",
        "JobRole",
        "Job Role",
        "RoleTitle",
    ),
    "email": (
        "Email Address",
        "Email",
        "Verified email address",
    ),
    "phone": (
        "Phone Number",
        "Phone",
    ),
    "person_source": (
        "Person source",
        "Linkedin",
        "LinkedIn",
        "LinkedIn Profile",
        "LinkedIn profile",
        "LinkedIn Profile (if available)",
        "LinkedIn profile (if available)",
    ),
    "address": (
        "Address",
        "Location",
        "City / location",
    ),
    "city": ("City",),
    "state": (
        "State",
        "State/Province",
    ),
    "zip_code": (
        "Zip Code",
        "Zip",
        "Postal Code",
        "Postal",
        "Postcode",
    ),
    "country": ("Country",),
}
class FieldMapper:
    """Encapsulate import field mapping configuration and suggestion logic."""

    @staticmethod
    def _clean(value):
        if value is None:
            return ""
        return " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()

    @classmethod
    def _normalize_mapping_header(cls, value):
        return "".join(
            character for character in cls._clean(value).lower() if character.isalnum()
        )

    @classmethod
    def suggest_mapping(cls, csv_headers: list[str]) -> dict[str, str]:
        raw_headers = {}
        normalized_headers = {}
        for header in csv_headers:
            cleaned_header = cls._clean(header)
            if not cleaned_header:
                continue
            raw_headers.setdefault(cleaned_header.lower(), cleaned_header)
            normalized_headers.setdefault(
                cls._normalize_mapping_header(cleaned_header),
                cleaned_header,
            )

        suggestions = {}
        for field in TARGET_FIELDS:
            suggestion = ""
            for alias in SUGGEST_MAPPING_ALIASES.get(field, ()):
                raw_alias = cls._clean(alias).lower()
                if raw_alias in raw_headers:
                    suggestion = raw_headers[raw_alias]
                    break
            if not suggestion:
                for alias in SUGGEST_MAPPING_ALIASES.get(field, ()):
                    normalized_alias = cls._normalize_mapping_header(alias)
                    if normalized_alias in normalized_headers:
                        suggestion = normalized_headers[normalized_alias]
                        break
            suggestions[field] = suggestion
        return suggestions

    @staticmethod
    def get_target_fields() -> list[str]:
        return list(TARGET_FIELDS)

    @staticmethod
    def get_source_field_map() -> dict[str, str]:
        return dict(SOURCE_IMPORT_FIELD_MAP)

__all__ = [
    "FieldMapper",
    "SOURCE_IMPORT_FIELD_MAP",
    "SUGGEST_MAPPING_ALIASES",
    "TARGET_FIELDS",
]
