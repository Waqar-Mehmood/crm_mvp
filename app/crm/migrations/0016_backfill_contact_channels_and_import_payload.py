from django.db import migrations


IMPORT_ROW_PAYLOAD_KEY_MAP = {
    "company_name": "company_name",
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


def _clean(value):
    return " ".join(str(value or "").split()).strip()


def backfill_contact_channels_and_import_payload(apps, schema_editor):
    Contact = apps.get_model("crm", "Contact")
    ContactEmail = apps.get_model("crm", "ContactEmail")
    ContactPhone = apps.get_model("crm", "ContactPhone")
    ImportRow = apps.get_model("crm", "ImportRow")

    for contact in Contact.objects.all().iterator():
        email_rows = list(ContactEmail.objects.filter(contact_id=contact.pk).order_by("id"))
        phone_rows = list(ContactPhone.objects.filter(contact_id=contact.pk).order_by("id"))

        primary_email_row = None
        primary_phone_row = None

        email_value = _clean(getattr(contact, "email", ""))
        if email_value:
            primary_email_row = next((row for row in email_rows if _clean(row.email) == email_value), None)
            if primary_email_row is None:
                primary_email_row = ContactEmail.objects.create(
                    contact_id=contact.pk,
                    email=email_value,
                    label="work",
                    is_primary=False,
                )
                email_rows.append(primary_email_row)
        elif email_rows:
            primary_email_row = email_rows[0]

        phone_value = _clean(getattr(contact, "phone", ""))
        if phone_value:
            primary_phone_row = next((row for row in phone_rows if _clean(row.phone) == phone_value), None)
            if primary_phone_row is None:
                primary_phone_row = ContactPhone.objects.create(
                    contact_id=contact.pk,
                    phone=phone_value,
                    label="work",
                    is_primary=False,
                )
                phone_rows.append(primary_phone_row)
        elif phone_rows:
            primary_phone_row = phone_rows[0]

        for row in email_rows:
            should_be_primary = primary_email_row is not None and row.pk == primary_email_row.pk
            if row.is_primary != should_be_primary:
                row.is_primary = should_be_primary
                row.save(update_fields=["is_primary"])

        for row in phone_rows:
            should_be_primary = primary_phone_row is not None and row.pk == primary_phone_row.pk
            if row.is_primary != should_be_primary:
                row.is_primary = should_be_primary
                row.save(update_fields=["is_primary"])

    for import_row in ImportRow.objects.all().iterator():
        payload = dict(import_row.mapped_payload or {})
        changed = False
        for field_name, payload_key in IMPORT_ROW_PAYLOAD_KEY_MAP.items():
            value = _clean(getattr(import_row, field_name, ""))
            if not value:
                continue
            if _clean(payload.get(payload_key, "")) == value:
                continue
            payload[payload_key] = value
            changed = True
        if changed:
            import_row.mapped_payload = payload
            import_row.save(update_fields=["mapped_payload"])


class Migration(migrations.Migration):

    dependencies = [
        ("crm", "0015_alter_contactemail_options_and_more"),
    ]

    operations = [
        migrations.RunPython(
            backfill_contact_channels_and_import_payload,
            migrations.RunPython.noop,
        ),
    ]
