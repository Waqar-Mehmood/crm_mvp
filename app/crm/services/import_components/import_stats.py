"""Reusable import statistics accumulator."""


class ImportStats:
    """Accumulate row-level import metrics and error details."""

    def __init__(self) -> None:
        self.rows_processed = 0
        self.rows_created = 0
        self.rows_updated = 0
        self.rows_failed = 0
        self.errors = []
        self.created_companies = 0
        self.created_contacts = 0
        self.links_created = 0
        self.email_rows_created = 0
        self.phone_rows_created = 0
        self.social_rows_created = 0
        self.company_social_rows_created = 0
        self.import_rows_created = 0
        self.import_rows_updated = 0
        self.skipped_rows = 0
        self.skipped_empty_rows = 0
        self.skipped_duplicate_rows = 0

    def increment_rows_processed(self) -> None:
        self.rows_processed += 1

    def increment_created(self) -> None:
        self.rows_created += 1

    def increment_updated(self) -> None:
        self.rows_updated += 1

    def increment_failed(self) -> None:
        self.rows_failed += 1

    def add_error(self, row_num: int, message: str) -> None:
        self.errors.append(
            {
                "row_num": row_num,
                "message": message,
            }
        )

    def get_summary(self) -> dict:
        return {
            "rows_processed": self.rows_processed,
            "rows_created": self.rows_created,
            "rows_updated": self.rows_updated,
            "rows_failed": self.rows_failed,
            "errors": list(self.errors),
            "created_companies": self.created_companies,
            "created_contacts": self.created_contacts,
            "links_created": self.links_created,
            "email_rows_created": self.email_rows_created,
            "phone_rows_created": self.phone_rows_created,
            "social_rows_created": self.social_rows_created,
            "company_social_rows_created": self.company_social_rows_created,
            "import_rows_created": self.import_rows_created,
            "import_rows_updated": self.import_rows_updated,
            "skipped_rows": self.skipped_rows,
            "skipped_empty_rows": self.skipped_empty_rows,
            "skipped_duplicate_rows": self.skipped_duplicate_rows,
        }


__all__ = [
    "ImportStats",
]
