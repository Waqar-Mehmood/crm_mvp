import csv
from pathlib import Path
from urllib.parse import urlparse

from django.db import transaction

from crm.models import (
    Company,
    Contact,
    ImportFile,
    ImportRow,
)
from crm.services.import_components import (
    DataCleaner,
    FieldMapper,
    ImportOrchestrator,
)


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

APPLY_UPDATE_FIELDS = {
    "industry": {"label": "Company Industry", "entity": "company", "source": "industry", "target": "industry"},
    "company_size": {"label": "Company Size", "entity": "company", "source": "company_size", "target": "company_size"},
    "revenue": {"label": "Company Revenue", "entity": "company", "source": "revenue", "target": "revenue"},
    "address": {"label": "Company Address", "entity": "company", "source": "address", "target": "address"},
    "city": {"label": "Company City", "entity": "company", "source": "city", "target": "city"},
    "state": {"label": "Company State", "entity": "company", "source": "state", "target": "state"},
    "zip_code": {"label": "Company Zip Code", "entity": "company", "source": "zip_code", "target": "zip_code"},
    "country": {"label": "Company Country", "entity": "company", "source": "country", "target": "country"},
    "contact_title": {"label": "Contact Title", "entity": "contact", "source": "contact_title", "target": "title"},
    "email_address": {"label": "Contact Email", "entity": "contact", "source": "email_address", "target": "email"},
    "phone_number": {"label": "Contact Phone", "entity": "contact", "source": "phone_number", "target": "phone"},
}

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

# Backwards compatibility - components now live in import_components
suggest_mapping = FieldMapper.suggest_mapping
clean = DataCleaner.clean
clean_for_model_field = DataCleaner.clean_for_model_field


def infer_platform(url):
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


def mapped_value(row, mapping, target):
    source = clean(mapping.get(target))
    if not source:
        return ""
    return clean(row.get(source))


def _csv_lookup_for_sources(import_file, source_fields, mapping_overrides=None):
    source_path = clean(import_file.source_path)
    if not source_path:
        return {}

    csv_path = Path(source_path)
    if not csv_path.exists():
        return {}

    headers = detect_headers(csv_path)
    suggested = suggest_mapping(headers)
    mapping_overrides = mapping_overrides or {}

    source_to_header = {}
    for source_field in source_fields:
        mapping_key = SOURCE_IMPORT_FIELD_MAP.get(source_field)
        if not mapping_key:
            continue

        override_header = clean(mapping_overrides.get(source_field))
        if override_header and override_header in headers:
            source_to_header[source_field] = override_header
            continue

        suggested_header = clean(suggested.get(mapping_key))
        if suggested_header and suggested_header in headers:
            source_to_header[source_field] = suggested_header

    lookup = {}
    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            values = {}
            for source_field, header_name in source_to_header.items():
                values[source_field] = clean(row.get(header_name))
            lookup[row_number] = values
    return lookup


def _row_value_for_source(import_row, source_field, csv_lookup_row):
    import_row_value = clean(getattr(import_row, source_field, ""))
    if import_row_value:
        return import_row_value
    return clean((csv_lookup_row or {}).get(source_field))


def detect_headers(csv_path):
    path = Path(csv_path)
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [clean(h) for h in (reader.fieldnames or [])]


def _legacy_failed_rows_from_errors(errors):
    return [
        {
            "row_number": error.get("row_num"),
            "reason": error.get("message", ""),
        }
        for error in errors
    ]


def _legacy_stats_from_import_stats(stats):
    failed_rows = _legacy_failed_rows_from_errors(getattr(stats, "errors", []))
    return {
        "rows_processed": getattr(stats, "rows_processed", 0),
        "created_companies": getattr(stats, "created_companies", 0),
        "created_contacts": getattr(stats, "created_contacts", 0),
        "links_created": getattr(stats, "links_created", 0),
        "email_rows_created": getattr(stats, "email_rows_created", 0),
        "phone_rows_created": getattr(stats, "phone_rows_created", 0),
        "social_rows_created": getattr(stats, "social_rows_created", 0),
        "company_social_rows_created": getattr(stats, "company_social_rows_created", 0),
        "import_rows_created": getattr(stats, "import_rows_created", 0),
        "import_rows_updated": getattr(stats, "import_rows_updated", 0),
        "skipped_rows": getattr(stats, "skipped_rows", 0),
        "skipped_empty_rows": getattr(stats, "skipped_empty_rows", 0),
        "skipped_duplicate_rows": getattr(stats, "skipped_duplicate_rows", 0),
        "failed_rows": failed_rows,
    }


def import_csv_with_mapping(
    csv_path,
    file_name,
    mapping,
    source_path="",
    *,
    import_file=None,
    progress_callback=None,
    progress_interval=25,
):
    csv_path = Path(csv_path)
    if import_file is None:
        import_file, _ = ImportFile.objects.get_or_create(
            file_name=file_name,
            defaults={"source_path": source_path or str(csv_path)},
        )
    if source_path and source_path != import_file.source_path:
        import_file.source_path = source_path
        import_file.save(update_fields=["source_path", "updated_at"])
    progress_interval = max(int(progress_interval or 1), 1)

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        csv_rows = list(csv.DictReader(f))

    partial_progress = {"rows_processed": 0}

    def _orchestrator_progress(current_row, total_rows):
        partial_progress["rows_processed"] = current_row
        if progress_callback and current_row % progress_interval == 0:
            progress_callback(import_file, partial_progress, final=False)

    stats_obj = ImportOrchestrator.execute(
        csv_rows,
        mapping,
        import_file=import_file,
        progress_callback=_orchestrator_progress if progress_callback else None,
    )
    stats = _legacy_stats_from_import_stats(stats_obj)

    if progress_callback:
        progress_callback(import_file, stats, final=True)

    return import_file, stats


def build_import_result_summary(stats):
    """Normalize raw import stats into a UI-friendly summary payload."""
    failed_rows = list(stats.get("failed_rows", []))
    rows_skipped = (
        stats.get("skipped_rows", 0)
        + stats.get("skipped_empty_rows", 0)
        + stats.get("skipped_duplicate_rows", 0)
    )
    return {
        "rows_processed": stats.get("rows_processed", 0),
        "companies_created": stats.get("created_companies", 0),
        "contacts_created": stats.get("created_contacts", 0),
        "rows_skipped": rows_skipped,
        "failed_rows_count": len(failed_rows),
        "failed_rows": failed_rows,
        "error_messages": [f'Row {row["row_number"]}: {row["reason"]}' for row in failed_rows],
    }


@transaction.atomic
def apply_updates_from_import_file(import_file, selected_fields, mapping_overrides=None):
    fields = [field for field in selected_fields if field in APPLY_UPDATE_FIELDS]
    stats = {
        "rows_processed": 0,
        "companies_matched": 0,
        "contacts_matched": 0,
        "companies_updated": 0,
        "contacts_updated": 0,
        "field_updates": 0,
    }
    if not fields:
        return stats

    source_fields = [APPLY_UPDATE_FIELDS[field]["source"] for field in fields]
    csv_lookup = _csv_lookup_for_sources(import_file, source_fields, mapping_overrides=mapping_overrides)

    rows = import_file.rows.select_related("company", "contact").order_by("row_number")
    for row in rows:
        stats["rows_processed"] += 1
        csv_lookup_row = csv_lookup.get(row.row_number, {})
        company = row.company
        contact = row.contact

        if not company and clean(row.company_name):
            company = Company.objects.filter(name=clean(row.company_name)).first()
        if not contact and clean(row.contact_name):
            contact = Contact.objects.filter(full_name=clean(row.contact_name)).first()

        company_changed = False
        contact_changed = False
        if company:
            stats["companies_matched"] += 1
        if contact:
            stats["contacts_matched"] += 1

        for field in fields:
            config = APPLY_UPDATE_FIELDS[field]
            value = _row_value_for_source(row, config["source"], csv_lookup_row)
            if not value:
                continue

            if config["entity"] == "company" and company:
                target = config["target"]
                if clean(getattr(company, target, "")) != value:
                    setattr(company, target, value)
                    company_changed = True
                    stats["field_updates"] += 1
            if config["entity"] == "contact" and contact:
                target = config["target"]
                if clean(getattr(contact, target, "")) != value:
                    setattr(contact, target, value)
                    contact_changed = True
                    stats["field_updates"] += 1

        if company_changed:
            company.save()
            stats["companies_updated"] += 1
        if contact_changed:
            contact.save()
            stats["contacts_updated"] += 1

    return stats


def analyze_updates_from_import_file(import_file, selected_fields, mapping_overrides=None):
    fields = [field for field in selected_fields if field in APPLY_UPDATE_FIELDS]
    stats = {
        "rows_processed": 0,
        "companies_matched": 0,
        "contacts_matched": 0,
        "field_updates": 0,
        "fields": [],
    }
    if not fields:
        return stats

    source_fields = [APPLY_UPDATE_FIELDS[field]["source"] for field in fields]
    csv_lookup = _csv_lookup_for_sources(import_file, source_fields, mapping_overrides=mapping_overrides)

    field_stats = {
        field: {
            "key": field,
            "label": APPLY_UPDATE_FIELDS[field]["label"],
            "entity": APPLY_UPDATE_FIELDS[field]["entity"],
            "non_empty": 0,
            "matched": 0,
            "will_change": 0,
        }
        for field in fields
    }

    rows = import_file.rows.select_related("company", "contact").order_by("row_number")
    for row in rows:
        stats["rows_processed"] += 1
        csv_lookup_row = csv_lookup.get(row.row_number, {})
        company = row.company
        contact = row.contact

        if not company and clean(row.company_name):
            company = Company.objects.filter(name=clean(row.company_name)).first()
        if not contact and clean(row.contact_name):
            contact = Contact.objects.filter(full_name=clean(row.contact_name)).first()

        if company:
            stats["companies_matched"] += 1
        if contact:
            stats["contacts_matched"] += 1

        for field in fields:
            config = APPLY_UPDATE_FIELDS[field]
            value = _row_value_for_source(row, config["source"], csv_lookup_row)
            if not value:
                continue

            row_field_stats = field_stats[field]
            row_field_stats["non_empty"] += 1
            target_obj = company if config["entity"] == "company" else contact
            if not target_obj:
                continue

            row_field_stats["matched"] += 1
            current_value = clean(getattr(target_obj, config["target"], ""))
            if current_value != value:
                row_field_stats["will_change"] += 1
                stats["field_updates"] += 1

    stats["fields"] = [field_stats[field] for field in fields]
    return stats


@transaction.atomic
def hydrate_import_rows_from_source(import_file, source_fields, mapping_overrides=None):
    stats = {
        "rows_scanned": 0,
        "rows_touched": 0,
        "field_values_written": 0,
    }

    source_path = clean(import_file.source_path)
    if not source_path:
        return stats

    csv_path = Path(source_path)
    if not csv_path.exists():
        return stats

    target_fields = [field for field in source_fields if field in SOURCE_IMPORT_FIELD_MAP]
    if not target_fields:
        return stats

    headers = detect_headers(csv_path)
    suggested = suggest_mapping(headers)
    mapping = dict(suggested)
    if mapping_overrides:
        for source_field, header_name in mapping_overrides.items():
            mapping_key = SOURCE_IMPORT_FIELD_MAP.get(source_field)
            if not mapping_key:
                continue
            if header_name in headers:
                mapping[mapping_key] = header_name

    with csv_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row_number, row in enumerate(reader, start=2):
            stats["rows_scanned"] += 1
            updates = {}
            for field in target_fields:
                mapping_key = SOURCE_IMPORT_FIELD_MAP[field]
                value = mapped_value(row, mapping, mapping_key)
                if field == "contact_name" and not value:
                    first_name = mapped_value(row, mapping, "contact_first_name")
                    last_name = mapped_value(row, mapping, "contact_last_name")
                    value = clean(f"{first_name} {last_name}")
                if value:
                    updates[field] = value

            if not updates:
                continue

            updated = ImportRow.objects.filter(
                import_file=import_file,
                row_number=row_number,
            ).update(**updates)
            if updated:
                stats["rows_touched"] += 1
                stats["field_values_written"] += len(updates)

    return stats
