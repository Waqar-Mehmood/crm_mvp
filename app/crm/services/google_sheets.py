"""Pure helper functions for working with Google Sheets URLs.

Usage example:
    # sheet_url = (
    #     "https://docs.google.com/spreadsheets/d/"
    #     "1ngu9sB-ZtIFoA3BqBnd2AwBroIZL9_c_8ZdREHTe8NM/edit?gid=0#gid=0"
    # )
    # sheet_id = extract_sheet_id(sheet_url)
    # gid = extract_gid(sheet_url)
    # csv_url = build_csv_export_url(sheet_url)
    # rows = fetch_google_sheet_rows(sheet_url)
"""

from __future__ import annotations

import csv
import re
from io import StringIO
from urllib.parse import ParseResult, parse_qs, urlparse

import requests


_SHEET_ID_PATTERN = re.compile(r"/spreadsheets/d/([a-zA-Z0-9-_]+)")
_REQUEST_TIMEOUT_SECONDS = 30


def _parse_google_sheets_url(sheet_url: str) -> ParseResult:
    """Parse and validate a Google Sheets URL."""
    if not isinstance(sheet_url, str) or not sheet_url.strip():
        raise ValueError("Google Sheets URL must be a non-empty string.")

    parsed = urlparse(sheet_url.strip())
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Google Sheets URL must start with http:// or https://.")
    if parsed.netloc != "docs.google.com":
        raise ValueError("Google Sheets URL must use the docs.google.com domain.")
    if "/spreadsheets/" not in parsed.path:
        raise ValueError("URL does not look like a Google Sheets document URL.")

    return parsed


def extract_sheet_id(sheet_url: str) -> str:
    """Return the Google Sheet ID from a Google Sheets URL.

    Raises:
        ValueError: If the URL is invalid or does not contain a sheet ID.
    """
    parsed = _parse_google_sheets_url(sheet_url)
    match = _SHEET_ID_PATTERN.search(parsed.path)
    if not match:
        raise ValueError("Could not extract a Google Sheet ID from the URL.")
    return match.group(1)


def extract_gid(sheet_url: str) -> str:
    """Return the sheet gid from a Google Sheets URL.

    The gid defaults to ``"0"`` when it is missing from the URL.

    Raises:
        ValueError: If the base URL is not a valid Google Sheets URL.
    """
    parsed = _parse_google_sheets_url(sheet_url)

    query_gid = parse_qs(parsed.query).get("gid", [""])[0].strip()
    if query_gid:
        return query_gid

    fragment_gid = parse_qs(parsed.fragment).get("gid", [""])[0].strip()
    if fragment_gid:
        return fragment_gid

    return "0"


def build_csv_export_url(sheet_url: str) -> str:
    """Build the direct CSV export URL for a Google Sheets document.

    Raises:
        ValueError: If the URL is invalid or the sheet ID cannot be extracted.
    """
    sheet_id = extract_sheet_id(sheet_url)
    gid = extract_gid(sheet_url)
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def fetch_google_sheet_rows(sheet_url: str) -> list[dict[str, str]]:
    """Fetch a Google Sheet as CSV and return its rows as dictionaries.

    This function validates the Google Sheets URL, converts it to the direct CSV
    export URL, downloads the CSV, and parses it with ``csv.DictReader``.

    Args:
        sheet_url: A standard Google Sheets URL.

    Returns:
        A list of dictionaries where each dictionary represents one CSV row.

    Raises:
        ValueError: If the provided URL is not a valid Google Sheets URL.
        RuntimeError: If the CSV request fails or the CSV cannot be parsed.
    """
    csv_export_url = build_csv_export_url(sheet_url)

    try:
        response = requests.get(csv_export_url, timeout=_REQUEST_TIMEOUT_SECONDS)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Failed to fetch CSV data from Google Sheets: {exc}"
        ) from exc

    try:
        csv_buffer = StringIO(response.text)
        reader = csv.DictReader(csv_buffer)
        return [
            {str(key): "" if value is None else value for key, value in row.items()}
            for row in reader
        ]
    except (csv.Error, TypeError) as exc:
        raise RuntimeError(f"Failed to parse CSV data from Google Sheets: {exc}") from exc


# Usage example:
# sheet_url = (
#     "https://docs.google.com/spreadsheets/d/"
#     "1ngu9sB-ZtIFoA3BqBnd2AwBroIZL9_c_8ZdREHTe8NM/edit?gid=0#gid=0"
# )
# rows = fetch_google_sheet_rows(sheet_url)
