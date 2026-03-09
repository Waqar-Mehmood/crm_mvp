from django.contrib import admin
from .models import Company, CompanyPhone, CompanyEmail, CompanySocialLink


class CompanyPhoneInline(admin.TabularInline):
    model = CompanyPhone
    extra = 1


class CompanyEmailInline(admin.TabularInline):
    model = CompanyEmail
    extra = 1


class CompanySocialLinkInline(admin.TabularInline):
    model = CompanySocialLink
    extra = 1


@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    list_display = ("name",)

    fieldsets = (
        ("Company Info", {
            "fields": ("name",)
        }),
        ("Additional Info", {
            "fields": ("address", "notes")
        }),
    )   

    inlines = [
        CompanyPhoneInline,
        CompanyEmailInline,
        CompanySocialLinkInline,
    ]


admin.site.register(CompanyPhone)
admin.site.register(CompanyEmail)
admin.site.register(CompanySocialLink)