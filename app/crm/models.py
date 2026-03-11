from django.db import models


class Company(models.Model):
    name = models.CharField(max_length=255)
    industry = models.CharField(max_length=255, blank=True)
    company_size = models.CharField(max_length=100, blank=True)
    revenue = models.CharField(max_length=100, blank=True)
    address = models.CharField(max_length=255, blank=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True)
    zip_code = models.CharField(max_length=20, blank=True)
    country = models.CharField(max_length=100, blank=True)
    notes = models.TextField(blank=True)
    contacts = models.ManyToManyField("Contact", related_name="companies", blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

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
    full_name = models.CharField(max_length=255)
    email = models.EmailField(blank=True)
    phone = models.CharField(max_length=50, blank=True)
    title = models.CharField(max_length=100, blank=True)
    notes = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return self.full_name


class ContactPhone(models.Model):
    contact = models.ForeignKey(
        Contact,
        on_delete=models.CASCADE,
        related_name="phones",
    )
    phone = models.CharField(max_length=50)
    label = models.CharField(max_length=50, blank=True)  # e.g. work, mobile

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
    file_name = models.CharField(max_length=255, unique=True)
    source_path = models.TextField(blank=True)
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
    company_name = models.CharField(max_length=255, blank=True)
    website = models.URLField(blank=True)
    contact_name = models.CharField(max_length=255, blank=True)
    contact_title = models.CharField(max_length=255, blank=True)
    email_address = models.EmailField(blank=True)
    phone_number = models.CharField(max_length=50, blank=True)
    person_source = models.URLField(blank=True)
    address = models.CharField(max_length=255, blank=True)
    city = models.CharField(max_length=100, blank=True)
    state = models.CharField(max_length=100, blank=True)
    zip_code = models.CharField(max_length=20, blank=True)
    country = models.CharField(max_length=100, blank=True)
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
