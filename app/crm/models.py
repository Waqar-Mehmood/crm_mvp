from django.db import models


class Company(models.Model):
    name = models.CharField(max_length=255)
    address = models.TextField(blank=True)
    notes = models.TextField(blank=True)
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