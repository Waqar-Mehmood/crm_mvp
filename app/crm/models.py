from django.db import models
from django.db.models import Q


IMPORT_ROW_PAYLOAD_PROPERTY_MAP = {
    "company_name": "company_name",
    "industry": "industry",
    "company_size": "company_size",
    "revenue": "revenue",
    "website": "website",
    "contact_name": "contact_name",
    "contact_first_name": "contact_first_name",
    "contact_last_name": "contact_last_name",
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


class SiteBranding(models.Model):
    site_name = models.CharField(max_length=255, blank=True)
    logo_image = models.ImageField(upload_to="branding/", blank=True)
    logo_alt_text = models.CharField(max_length=255, blank=True)

    class Meta:
        verbose_name = "Site Branding"
        verbose_name_plural = "Site Branding"

    def __str__(self):
        return self.site_name or "Site Branding"


class Company(models.Model):
    name = models.CharField(max_length=255, db_index=True)
    industry = models.CharField(max_length=255, blank=True, db_index=True)
    company_size = models.CharField(max_length=100, blank=True)
    revenue = models.CharField(max_length=100, blank=True, db_index=True)
    address = models.CharField(max_length=255, blank=True)
    city = models.CharField(max_length=100, blank=True, db_index=True)
    state = models.CharField(max_length=100, blank=True, db_index=True)
    zip_code = models.CharField(max_length=20, blank=True)
    country = models.CharField(max_length=100, blank=True, db_index=True)
    notes = models.TextField(blank=True)
    contacts = models.ManyToManyField("Contact", related_name="companies", blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    def __str__(self):
        return self.name
    
    class Meta:
        verbose_name_plural = "Companies"


class CompanyPhone(models.Model):
    company = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        related_name="phones",
    )
    phone = models.CharField(max_length=50)
    label = models.CharField(max_length=50, blank=True)  # e.g. office, mobile

    def __str__(self):
        return f"{self.company.name} - {self.phone}"


class CompanyEmail(models.Model):
    company = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        related_name="emails",
    )
    email = models.EmailField()
    label = models.CharField(max_length=50, blank=True)  # e.g. sales, support

    def __str__(self):
        return f"{self.company.name} - {self.email}"


class CompanySocialLink(models.Model):
    company = models.ForeignKey(
        Company,
        on_delete=models.CASCADE,
        related_name="social_links",
    )
    platform = models.CharField(max_length=50, blank=True)  # e.g. linkedin
    url = models.URLField()

    def __str__(self):
        return f"{self.company.name} - {self.platform or self.url}"


class Contact(models.Model):
    full_name = models.CharField(max_length=255, db_index=True)
    first_name = models.CharField(max_length=255, blank=True)
    middle_name = models.CharField(max_length=255, blank=True)
    last_name = models.CharField(max_length=255, blank=True)
    title = models.CharField(max_length=100, blank=True, db_index=True)
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    def __str__(self):
        return self.full_name

    def _primary_related_row(self, related_name):
        prefetched = getattr(self, "_prefetched_objects_cache", {})
        if related_name in prefetched:
            rows = list(prefetched[related_name])
        elif not self.pk:
            rows = []
        else:
            rows = list(getattr(self, related_name).all())
        if not rows:
            return None
        rows.sort(key=lambda row: (not bool(getattr(row, "is_primary", False)), row.pk or 0))
        return rows[0]

    @property
    def primary_email_row(self):
        return self._primary_related_row("emails")

    @property
    def primary_phone_row(self):
        return self._primary_related_row("phones")

    @property
    def primary_email(self):
        row = self.primary_email_row
        return row.email if row else ""

    @property
    def primary_phone(self):
        row = self.primary_phone_row
        return row.phone if row else ""

    @property
    def email(self):
        return self.primary_email

    @property
    def phone(self):
        return self.primary_phone


class ContactPhone(models.Model):
    contact = models.ForeignKey(
        Contact,
        on_delete=models.CASCADE,
        related_name="phones",
    )
    phone = models.CharField(max_length=50)
    label = models.CharField(max_length=50, blank=True)  # e.g. work, mobile
    is_primary = models.BooleanField(default=False, db_index=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["contact"],
                condition=Q(is_primary=True),
                name="unique_primary_contact_phone",
            )
        ]
        ordering = ("-is_primary", "id")

    def __str__(self):
        return f"{self.contact.full_name} - {self.phone}"


class ContactEmail(models.Model):
    contact = models.ForeignKey(
        Contact,
        on_delete=models.CASCADE,
        related_name="emails",
    )
    email = models.EmailField()
    label = models.CharField(max_length=50, blank=True)  # e.g. personal, work
    is_primary = models.BooleanField(default=False, db_index=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["contact"],
                condition=Q(is_primary=True),
                name="unique_primary_contact_email",
            )
        ]
        ordering = ("-is_primary", "id")

    def __str__(self):
        return f"{self.contact.full_name} - {self.email}"


class ContactSocialLink(models.Model):
    contact = models.ForeignKey(
        Contact,
        on_delete=models.CASCADE,
        related_name="social_links",
    )
    platform = models.CharField(max_length=50, blank=True)  # e.g. linkedin
    url = models.URLField()

    def __str__(self):
        return f"{self.contact.full_name} - {self.platform or self.url}"


class ImportFile(models.Model):
    class Status(models.TextChoices):
        QUEUED = "queued", "Queued"
        RUNNING = "running", "Running"
        COMPLETED = "completed", "Completed"
        FAILED = "failed", "Failed"

    file_name = models.CharField(max_length=255, unique=True)
    source_path = models.TextField(blank=True)
    original_source_path = models.TextField(blank=True)
    original_source_name = models.CharField(max_length=255, blank=True)
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.COMPLETED,
        db_index=True,
    )
    mapping = models.JSONField(default=dict, blank=True)
    total_rows = models.PositiveIntegerField(default=0)
    processed_rows = models.PositiveIntegerField(default=0)
    result_summary = models.JSONField(default=dict, blank=True)
    error_message = models.TextField(blank=True)
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.file_name


class ImportRow(models.Model):
    import_file = models.ForeignKey(
        ImportFile,
        on_delete=models.CASCADE,
        related_name="rows",
    )
    row_number = models.PositiveIntegerField()
    company = models.ForeignKey(
        Company,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="import_rows",
    )
    contact = models.ForeignKey(
        Contact,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="import_rows",
    )
    mapped_payload = models.JSONField(default=dict, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["import_file", "row_number"],
                name="unique_import_row_per_file",
            )
        ]

    def __str__(self):
        return f"{self.import_file.file_name} - row {self.row_number}"

    def _payload_value(self, field_name):
        payload = self.mapped_payload or {}
        payload_key = IMPORT_ROW_PAYLOAD_PROPERTY_MAP.get(field_name, field_name)
        value = payload.get(payload_key, "")
        return "" if value is None else value

    @property
    def company_name(self):
        return self._payload_value("company_name")

    @property
    def industry(self):
        return self._payload_value("industry")

    @property
    def company_size(self):
        return self._payload_value("company_size")

    @property
    def revenue(self):
        return self._payload_value("revenue")

    @property
    def website(self):
        return self._payload_value("website")

    @property
    def contact_name(self):
        return self._payload_value("contact_name")

    @property
    def contact_first_name(self):
        return self._payload_value("contact_first_name")

    @property
    def contact_last_name(self):
        return self._payload_value("contact_last_name")

    @property
    def contact_title(self):
        return self._payload_value("contact_title")

    @property
    def email_address(self):
        return self._payload_value("email_address")

    @property
    def phone_number(self):
        return self._payload_value("phone_number")

    @property
    def person_source(self):
        return self._payload_value("person_source")

    @property
    def address(self):
        return self._payload_value("address")

    @property
    def city(self):
        return self._payload_value("city")

    @property
    def state(self):
        return self._payload_value("state")

    @property
    def zip_code(self):
        return self._payload_value("zip_code")

    @property
    def country(self):
        return self._payload_value("country")
