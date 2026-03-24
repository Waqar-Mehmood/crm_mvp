import csv
from pathlib import Path
from urllib.parse import urlparse

from django.db import transaction

from crm.models import (
    Company,
    CompanySocialLink,
    Contact,
    ContactEmail,
    ContactPhone,
    ContactSocialLink,
    ImportFile,
    ImportRow,
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


def clean(value):
    if value is None:
        return ""
    return " ".join(str(value).replace("\r", " ").replace("\n", " ").split()).strip()


def _normalize_mapping_header(value):
    return "".join(character for character in clean(value).lower() if character.isalnum())


def clean_for_model_field(model, field_name, value):
    normalized = clean(value)
    if not normalized:
        return ""
    field = model._meta.get_field(field_name)
    max_length = getattr(field, "max_length", None)
    if max_length:
        return normalized[:max_length]
    return normalized


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


def _row_signature(values):
    ordered_keys = (
        "company_name",
        "industry",
        "company_size",
        "revenue",
        "website",
        "contact_name",
        "contact_title",
        "email",
        "phone",
        "person_source",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
    )
    return tuple(clean(values.get(key, "")) for key in ordered_keys)


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


def suggest_mapping(headers):
    raw_headers = {}
    normalized_headers = {}
    for header in headers:
        cleaned_header = clean(header)
        if not cleaned_header:
            continue
        raw_headers.setdefault(cleaned_header.lower(), cleaned_header)
        normalized_headers.setdefault(
            _normalize_mapping_header(cleaned_header),
            cleaned_header,
        )

    suggestions = {}
    for field in TARGET_FIELDS:
        suggestion = ""
        for alias in SUGGEST_MAPPING_ALIASES.get(field, ()):
            raw_alias = clean(alias).lower()
            if raw_alias in raw_headers:
                suggestion = raw_headers[raw_alias]
                break
        if not suggestion:
            for alias in SUGGEST_MAPPING_ALIASES.get(field, ()):
                normalized_alias = _normalize_mapping_header(alias)
                if normalized_alias in normalized_headers:
                    suggestion = normalized_headers[normalized_alias]
                    break
        suggestions[field] = suggestion
    return suggestions


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

    stats = {
        "rows_processed": 0,
        "created_companies": 0,
        "created_contacts": 0,
        "links_created": 0,
        "email_rows_created": 0,
        "phone_rows_created": 0,
        "social_rows_created": 0,
        "company_social_rows_created": 0,
        "import_rows_created": 0,
        "import_rows_updated": 0,
        "skipped_rows": 0,
        "skipped_empty_rows": 0,
        "skipped_duplicate_rows": 0,
        "failed_rows": [],
    }
    seen_signatures = set()
    progress_interval = max(int(progress_interval or 1), 1)

    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row_index, row in enumerate(reader, start=2):
            stats["rows_processed"] += 1
            company_name = clean_for_model_field(
                Company,
                "name",
                mapped_value(row, mapping, "company_name"),
            )
            industry = clean_for_model_field(
                Company,
                "industry",
                mapped_value(row, mapping, "industry"),
            )
            company_size = clean_for_model_field(
                Company,
                "company_size",
                mapped_value(row, mapping, "company_size"),
            )
            revenue = clean_for_model_field(
                Company,
                "revenue",
                mapped_value(row, mapping, "revenue"),
            )
            website = clean_for_model_field(
                CompanySocialLink,
                "url",
                mapped_value(row, mapping, "website"),
            )
            contact_name = clean_for_model_field(
                Contact,
                "full_name",
                mapped_value(row, mapping, "contact_name"),
            )
            first_name = mapped_value(row, mapping, "contact_first_name")
            last_name = mapped_value(row, mapping, "contact_last_name")
            raw_contact_title = mapped_value(row, mapping, "contact_title")
            contact_title = clean_for_model_field(Contact, "title", raw_contact_title)
            import_row_contact_title = clean_for_model_field(
                ImportRow,
                "contact_title",
                raw_contact_title,
            )
            email = clean_for_model_field(
                Contact,
                "email",
                mapped_value(row, mapping, "email"),
            )
            phone = clean_for_model_field(
                Contact,
                "phone",
                mapped_value(row, mapping, "phone"),
            )
            person_source = clean_for_model_field(
                ContactSocialLink,
                "url",
                mapped_value(row, mapping, "person_source"),
            )
            address = clean_for_model_field(
                Company,
                "address",
                mapped_value(row, mapping, "address"),
            )
            city = clean_for_model_field(
                Company,
                "city",
                mapped_value(row, mapping, "city"),
            )
            state = clean_for_model_field(
                Company,
                "state",
                mapped_value(row, mapping, "state"),
            )
            zip_code = clean_for_model_field(
                Company,
                "zip_code",
                mapped_value(row, mapping, "zip_code"),
            )
            country = clean_for_model_field(
                Company,
                "country",
                mapped_value(row, mapping, "country"),
            )

            if not contact_name and (first_name or last_name):
                contact_name = clean_for_model_field(
                    Contact,
                    "full_name",
                    f"{first_name} {last_name}",
                )

            row_values = {
                "company_name": company_name,
                "industry": industry,
                "company_size": company_size,
                "revenue": revenue,
                "website": website,
                "contact_name": contact_name,
                "contact_title": import_row_contact_title,
                "email": email,
                "phone": phone,
                "person_source": person_source,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "country": country,
            }

            if not any(row_values.values()):
                stats["skipped_empty_rows"] += 1
                stats["failed_rows"].append(
                    {"row_number": row_index, "reason": "Row was empty after mapping."}
                )
                continue

            signature = _row_signature(row_values)
            if signature in seen_signatures:
                stats["skipped_duplicate_rows"] += 1
                stats["failed_rows"].append(
                    {"row_number": row_index, "reason": "Duplicate mapped row in this import."}
                )
                continue
            seen_signatures.add(signature)

            company = None
            if company_name:
                company, company_created = Company.objects.get_or_create(
                    name=company_name,
                    defaults={
                        "industry": industry,
                        "company_size": company_size,
                        "revenue": revenue,
                        "address": address,
                        "city": city,
                        "state": state,
                        "zip_code": zip_code,
                        "country": country,
                    },
                )
                if company_created:
                    stats["created_companies"] += 1
                else:
                    fields = []
                    if industry and not clean(company.industry):
                        company.industry = industry
                        fields.append("industry")
                    if company_size and not clean(company.company_size):
                        company.company_size = company_size
                        fields.append("company_size")
                    if revenue and not clean(company.revenue):
                        company.revenue = revenue
                        fields.append("revenue")
                    if address and not clean(company.address):
                        company.address = address
                        fields.append("address")
                    if city and not clean(company.city):
                        company.city = city
                        fields.append("city")
                    if state and not clean(company.state):
                        company.state = state
                        fields.append("state")
                    if zip_code and not clean(company.zip_code):
                        company.zip_code = zip_code
                        fields.append("zip_code")
                    if country and not clean(company.country):
                        company.country = country
                        fields.append("country")
                    if fields:
                        company.save(update_fields=fields)

            contact = None
            if contact_name:
                contact, contact_created = Contact.objects.get_or_create(
                    full_name=contact_name,
                    defaults={
                        "title": contact_title,
                        "email": email,
                        "phone": phone,
                    },
                )
                if contact_created:
                    stats["created_contacts"] += 1
                else:
                    fields = []
                    if contact_title and not clean(contact.title):
                        contact.title = contact_title
                        fields.append("title")
                    if email and not clean(contact.email):
                        contact.email = email
                        fields.append("email")
                    if phone and not clean(contact.phone):
                        contact.phone = phone
                        fields.append("phone")
                    if fields:
                        contact.save(update_fields=fields)

            if company and contact and not company.contacts.filter(pk=contact.pk).exists():
                company.contacts.add(contact)
                stats["links_created"] += 1

            if contact and email and not ContactEmail.objects.filter(contact=contact, email=email).exists():
                ContactEmail.objects.create(contact=contact, email=email, label="work")
                stats["email_rows_created"] += 1

            if contact and phone and not ContactPhone.objects.filter(contact=contact, phone=phone).exists():
                ContactPhone.objects.create(contact=contact, phone=phone, label="work")
                stats["phone_rows_created"] += 1

            if contact and person_source and not ContactSocialLink.objects.filter(contact=contact, url=person_source).exists():
                ContactSocialLink.objects.create(
                    contact=contact,
                    url=person_source,
                    platform=infer_platform(person_source),
                )
                stats["social_rows_created"] += 1

            if company and website and not CompanySocialLink.objects.filter(company=company, url=website).exists():
                CompanySocialLink.objects.create(
                    company=company,
                    url=website,
                    platform=infer_platform(website),
                )
                stats["company_social_rows_created"] += 1

            import_row_defaults = {
                "company": company,
                "contact": contact,
                "company_name": company_name,
                "website": website,
                "contact_name": contact_name,
                "contact_title": import_row_contact_title,
                "email_address": email,
                "phone_number": phone,
                "person_source": person_source,
                "address": address,
                "city": city,
                "state": state,
                "zip_code": zip_code,
                "country": country,
            }
            _, row_created = ImportRow.objects.update_or_create(
                import_file=import_file,
                row_number=row_index,
                defaults=import_row_defaults,
            )
            if row_created:
                stats["import_rows_created"] += 1
            else:
                stats["import_rows_updated"] += 1

            if not company and not contact:
                stats["skipped_rows"] += 1
                stats["failed_rows"].append(
                    {
                        "row_number": row_index,
                        "reason": "Row did not create or match a company or contact.",
                    }
                )

            if progress_callback and stats["rows_processed"] % progress_interval == 0:
                progress_callback(import_file, stats, final=False)

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
