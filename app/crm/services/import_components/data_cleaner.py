"""Data cleaning helpers for the import workflow."""


class DataCleaner:
    """Normalize raw import values before model writes and comparisons."""

    _FIELD_MAX_LENGTHS = {
        "name": 255,
        "industry": 255,
        "company_size": 100,
        "revenue": 100,
        "url": 200,
        "full_name": 255,
        "title": 100,
        "contact_title": 255,
        "email": 254,
        "phone": 50,
        "address": 255,
        "city": 100,
        "state": 100,
        "zip_code": 20,
        "country": 100,
    }

    @staticmethod
    def _normalize_text(value):
        if value is None:
            return ""
        return " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()

    @classmethod
    def _get_field_max_length(cls, field_name):
        return cls._FIELD_MAX_LENGTHS.get(field_name)

    @classmethod
    def clean(cls, value: str) -> str:
        return cls._normalize_text(value)

    @classmethod
    def clean_for_model_field(cls, field_name: str, value: str) -> str:
        normalized = cls.clean(value)
        if not normalized:
            return ""
        max_length = cls._get_field_max_length(field_name)
        if max_length:
            return normalized[:max_length]
        return normalized


__all__ = [
    "DataCleaner",
]
