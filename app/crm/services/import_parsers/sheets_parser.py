"""Google Sheets parser helpers for import sources."""

from __future__ import annotations

from crm.services import google_sheets


def parse_google_sheet(sheet_url: str) -> list[dict[str, str]]:
    """Parse a Google Sheet URL into row dictionaries."""
    return google_sheets.fetch_google_sheet_rows(sheet_url)
