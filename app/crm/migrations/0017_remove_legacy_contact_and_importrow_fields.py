from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("crm", "0016_backfill_contact_channels_and_import_payload"),
    ]

    operations = [
        migrations.AddConstraint(
            model_name="contactemail",
            constraint=models.UniqueConstraint(
                condition=models.Q(is_primary=True),
                fields=("contact",),
                name="unique_primary_contact_email",
            ),
        ),
        migrations.AddConstraint(
            model_name="contactphone",
            constraint=models.UniqueConstraint(
                condition=models.Q(is_primary=True),
                fields=("contact",),
                name="unique_primary_contact_phone",
            ),
        ),
        migrations.RemoveField(
            model_name="contact",
            name="email",
        ),
        migrations.RemoveField(
            model_name="contact",
            name="phone",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="address",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="city",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="company_name",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="contact_name",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="contact_title",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="country",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="email_address",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="person_source",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="phone_number",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="state",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="website",
        ),
        migrations.RemoveField(
            model_name="importrow",
            name="zip_code",
        ),
    ]
