import csv
import shlex
import uuid
from pathlib import Path

from django import forms
from django.conf import settings
from django.contrib import admin
from django.contrib import messages
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.urls import path, reverse
from django.db.models import Q
from django.utils.html import format_html_join
from django.utils.safestring import mark_safe
from .import_utils import (
    APPLY_UPDATE_FIELDS,
    TARGET_FIELDS,
    analyze_updates_from_import_file,
    apply_updates_from_import_file,
    detect_headers,
    import_csv_with_mapping,
    suggest_mapping,
)
from .models import (
    Company,
    CompanyPhone,
    CompanyEmail,
    CompanySocialLink,
    Contact,
    ContactPhone,
    ContactEmail,
    ContactSocialLink,
    ImportFile,
    ImportRow,
)


TARGET_LABELS = {
    "company_name": "Company Name",
    "industry": "Industry / Business Type",
    "company_size": "Company Size",
    "revenue": "Revenue",
    "website": "Website / Company URL",
    "contact_name": "Contact Full Name",
    "contact_first_name": "Contact First Name",
    "contact_last_name": "Contact Last Name",
    "contact_title": "Contact Title",
    "email": "Email",
    "phone": "Phone",
    "person_source": "Person Source / Profile URL",
    "address": "Address / Location",
    "city": "City",
    "state": "State",
    "zip_code": "Zip Code",
    "country": "Country",
}
MAPPING_FIELD_KEYS = [f"map_{key}" for key in TARGET_FIELDS]


def mapping_choice_field(label):
    return forms.ChoiceField(
        required=False,
        label=label,
        choices=[("", "-- Not mapped --")],
    )


class ImportFileAdminForm(forms.ModelForm):
    csv_file = forms.FileField(
        required=False,
        help_text="Upload a CSV file to import rows directly from admin.",
    )
    detected_headers = forms.CharField(
        required=False,
        widget=forms.Textarea(attrs={"rows": 2, "readonly": "readonly"}),
        help_text="Headers are filled automatically after selecting a CSV file.",
        label="Detected Headers",
    )
    map_company_name = mapping_choice_field(TARGET_LABELS["company_name"])
    map_industry = mapping_choice_field(TARGET_LABELS["industry"])
    map_company_size = mapping_choice_field(TARGET_LABELS["company_size"])
    map_revenue = mapping_choice_field(TARGET_LABELS["revenue"])
    map_website = mapping_choice_field(TARGET_LABELS["website"])
    map_contact_name = mapping_choice_field(TARGET_LABELS["contact_name"])
    map_contact_first_name = mapping_choice_field(TARGET_LABELS["contact_first_name"])
    map_contact_last_name = mapping_choice_field(TARGET_LABELS["contact_last_name"])
    map_contact_title = mapping_choice_field(TARGET_LABELS["contact_title"])
    map_email = mapping_choice_field(TARGET_LABELS["email"])
    map_phone = mapping_choice_field(TARGET_LABELS["phone"])
    map_person_source = mapping_choice_field(TARGET_LABELS["person_source"])
    map_address = mapping_choice_field(TARGET_LABELS["address"])
    map_city = mapping_choice_field(TARGET_LABELS["city"])
    map_state = mapping_choice_field(TARGET_LABELS["state"])
    map_zip_code = mapping_choice_field(TARGET_LABELS["zip_code"])
    map_country = mapping_choice_field(TARGET_LABELS["country"])

    class Meta:
        model = ImportFile
        fields = "__all__"

    @staticmethod
    def _extract_headers(uploaded_file):
        if not uploaded_file:
            return []

        position = uploaded_file.tell() if hasattr(uploaded_file, "tell") else None
        raw = uploaded_file.read()
        if hasattr(uploaded_file, "seek"):
            uploaded_file.seek(position or 0)

        if isinstance(raw, bytes):
            text = raw.decode("utf-8-sig", errors="replace")
        else:
            text = raw or ""

        first_line = ""
        for line in text.splitlines():
            if line.strip():
                first_line = line
                break
        if not first_line:
            return []

        headers = next(csv.reader([first_line]), [])
        return [header.strip() for header in headers if header and header.strip()]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        headers = self._extract_headers((self.files or {}).get("csv_file"))
        suggestions = suggest_mapping(headers) if headers else {}

        choices = [("", "-- Not mapped --")] + [(header, header) for header in headers]
        for key in TARGET_FIELDS:
            field_name = f"map_{key}"
            self.fields[field_name].choices = choices
            self.fields[field_name].initial = suggestions.get(key, "")

        if headers:
            self.fields["detected_headers"].initial = ", ".join(headers)

    def clean(self):
        cleaned_data = super().clean()
        csv_file = cleaned_data.get("csv_file")
        if not self.instance.pk and not csv_file:
            raise forms.ValidationError("CSV file is required when creating an import file from admin.")
        if csv_file:
            headers = self._extract_headers(csv_file)
            if not headers:
                raise forms.ValidationError("Uploaded CSV has no headers.")

            valid_headers = set(headers)
            for key in TARGET_FIELDS:
                selected = (cleaned_data.get(f"map_{key}") or "").strip()
                if selected and selected not in valid_headers:
                    self.add_error(f"map_{key}", "Choose a header from the uploaded CSV.")
            cleaned_data["_csv_headers"] = headers
        return cleaned_data


class ImportFileApplyUpdatesForm(forms.Form):
    UPDATE_FIELD_ORDER = (
        "industry",
        "company_size",
        "revenue",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
        "contact_title",
        "email_address",
        "phone_number",
    )

    def __init__(self, *args, **kwargs):
        import_file = kwargs.pop("import_file", None)
        super().__init__(*args, **kwargs)
        header_choices = [("", "-- Not mapped --")]
        if import_file and import_file.source_path:
            try:
                headers = detect_headers(import_file.source_path)
            except Exception:
                headers = []
            header_choices += [(header, header) for header in headers]

        for key in self.UPDATE_FIELD_ORDER:
            if key not in APPLY_UPDATE_FIELDS:
                continue
            self.fields[f"map_{key}"] = forms.ChoiceField(
                required=False,
                label=APPLY_UPDATE_FIELDS[key]["label"],
                choices=header_choices,
            )

    def selected_update_fields(self):
        selected = []
        for key in self.UPDATE_FIELD_ORDER:
            if self.cleaned_data.get(f"map_{key}"):
                selected.append(key)
        return selected

    def selected_mapping_overrides(self):
        overrides = {}
        for key in self.UPDATE_FIELD_ORDER:
            selected_header = (self.cleaned_data.get(f"map_{key}") or "").strip()
            if not selected_header or key not in APPLY_UPDATE_FIELDS:
                continue
            source_field = APPLY_UPDATE_FIELDS[key]["source"]
            overrides[source_field] = selected_header
        return overrides

    def clean(self):
        cleaned_data = super().clean()
        if not any(cleaned_data.get(f"map_{key}") for key in self.UPDATE_FIELD_ORDER):
            raise forms.ValidationError("Select at least one CSV header mapping to preview/apply updates.")
        return cleaned_data


class CompanyPhoneInline(admin.TabularInline):
    model = CompanyPhone
    extra = 1


class CompanyEmailInline(admin.TabularInline):
    model = CompanyEmail
    extra = 1


class CompanySocialLinkInline(admin.TabularInline):
    model = CompanySocialLink
    extra = 1


class ContactPhoneInline(admin.TabularInline):
    model = ContactPhone
    extra = 1


class ContactEmailInline(admin.TabularInline):
    model = ContactEmail
    extra = 1


class ContactSocialLinkInline(admin.TabularInline):
    model = ContactSocialLink
    extra = 1


class ImportRowInline(admin.TabularInline):
    model = ImportRow
    extra = 0
    readonly_fields = (
        "row_number",
        "company",
        "contact",
        "company_name",
        "website",
        "contact_name",
        "contact_title",
        "email_address",
        "phone_number",
        "person_source",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
        "created_at",
        "updated_at",
    )
    can_delete = False
    show_change_link = True


@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "industry",
        "company_size",
        "revenue",
        "contacts_display",
        "phones_display",
        "emails_display",
        "social_links_display",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
    )
    search_fields = (
        "name",
        "industry",
        "company_size",
        "revenue",
        "address",
        "city",
        "state",
        "zip_code",
        "country",
        "contacts__full_name",
        "contacts__email",
        "contacts__phone",
        "phones__phone",
        "emails__email",
        "social_links__url",
    )
    list_filter = ("industry", "company_size", "city", "state", "country", "created_at")
    search_help_text = (
        "Use plain text or advanced key:value search. "
        "Keys: name, industry, size, revenue, address, city, state, zip, country, "
        "contact, email, phone, social."
    )
    filter_horizontal = ("contacts",)

    fieldsets = (
        ("Company Info", {
            "fields": ("name", "industry", "company_size", "revenue")
        }),
        ("Additional Info", {
            "fields": ("address", "city", "state", "zip_code", "country", "notes", "contacts")
        }),
    )   

    inlines = [
        CompanyPhoneInline,
        CompanyEmailInline,
        CompanySocialLinkInline,
    ]

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.prefetch_related("contacts", "phones", "emails", "social_links")

    def _build_term_query(self, term):
        term_query = Q()
        for raw_field in self.search_fields:
            if raw_field.startswith("^"):
                lookup = f"{raw_field[1:]}__istartswith"
            elif raw_field.startswith("="):
                lookup = f"{raw_field[1:]}__iexact"
            elif raw_field.startswith("@"):
                lookup = raw_field[1:]
            else:
                lookup = f"{raw_field}__icontains"
            term_query |= Q(**{lookup: term})
        return term_query

    def get_search_results(self, request, queryset, search_term):
        if not search_term:
            return queryset, False

        advanced_map = {
            "name": "name__icontains",
            "industry": "industry__icontains",
            "size": "company_size__icontains",
            "revenue": "revenue__icontains",
            "address": "address__icontains",
            "city": "city__icontains",
            "state": "state__icontains",
            "zip": "zip_code__icontains",
            "country": "country__icontains",
            "contact": "contacts__full_name__icontains",
            "email": "emails__email__icontains",
            "phone": "phones__phone__icontains",
            "social": "social_links__url__icontains",
        }

        try:
            terms = shlex.split(search_term)
        except ValueError:
            terms = search_term.split()

        filtered = queryset
        plain_terms = []
        for term in terms:
            if ":" in term:
                key, value = term.split(":", 1)
                key = key.strip().lower()
                value = value.strip()
                lookup = advanced_map.get(key)
                if lookup and value:
                    filtered = filtered.filter(**{lookup: value})
                    continue
            plain_terms.append(term)

        for term in plain_terms:
            filtered = filtered.filter(self._build_term_query(term))

        return filtered.distinct(), True

    @admin.display(description="Contacts")
    def contacts_display(self, obj):
        contacts = obj.contacts.all()
        if not contacts:
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            "{}",
            ((contact.full_name,) for contact in contacts),
        )

    @admin.display(description="Phones")
    def phones_display(self, obj):
        phones = obj.phones.all()
        if not phones:
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            "{}{}",
            (
                (
                    f"{phone.label}: " if phone.label else "",
                    phone.phone,
                )
                for phone in phones
            ),
        )

    @admin.display(description="Emails")
    def emails_display(self, obj):
        emails = obj.emails.all()
        if not emails:
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            "{}{}",
            (
                (
                    f"{email.label}: " if email.label else "",
                    email.email,
                )
                for email in emails
            ),
        )

    @admin.display(description="Social Links")
    def social_links_display(self, obj):
        social_links = obj.social_links.all()
        if not social_links:
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            "{}{}",
            (
                (
                    f"{social_link.platform}: " if social_link.platform else "",
                    social_link.url,
                )
                for social_link in social_links
            ),
        )


@admin.register(Contact)
class ContactAdmin(admin.ModelAdmin):
    list_display = ("full_name", "title", "emails_display", "phones_display", "social_links_display")
    search_fields = (
        "full_name",
        "email",
        "phone",
        "title",
        "emails__email",
        "phones__phone",
        "social_links__url",
        "social_links__platform",
    )
    list_filter = ("title", "created_at")
    search_help_text = (
        "Use plain text or advanced key:value search. "
        "Keys: name, title, email, phone, company, social."
    )
    inlines = [
        ContactPhoneInline,
        ContactEmailInline,
        ContactSocialLinkInline,
    ]

    def get_queryset(self, request):
        queryset = super().get_queryset(request)
        return queryset.prefetch_related("emails", "phones", "social_links")

    def _build_term_query(self, term):
        term_query = Q()
        for raw_field in self.search_fields:
            if raw_field.startswith("^"):
                lookup = f"{raw_field[1:]}__istartswith"
            elif raw_field.startswith("="):
                lookup = f"{raw_field[1:]}__iexact"
            elif raw_field.startswith("@"):
                lookup = raw_field[1:]
            else:
                lookup = f"{raw_field}__icontains"
            term_query |= Q(**{lookup: term})
        return term_query

    def get_search_results(self, request, queryset, search_term):
        if not search_term:
            return queryset, False

        advanced_map = {
            "name": "full_name__icontains",
            "title": "title__icontains",
            "email": "emails__email__icontains",
            "phone": "phones__phone__icontains",
            "company": "companies__name__icontains",
            "social": "social_links__url__icontains",
        }

        try:
            terms = shlex.split(search_term)
        except ValueError:
            terms = search_term.split()

        filtered = queryset
        plain_terms = []
        for term in terms:
            if ":" in term:
                key, value = term.split(":", 1)
                key = key.strip().lower()
                value = value.strip()
                lookup = advanced_map.get(key)
                if lookup and value:
                    filtered = filtered.filter(**{lookup: value})
                    continue
            plain_terms.append(term)

        for term in plain_terms:
            filtered = filtered.filter(self._build_term_query(term))

        return filtered.distinct(), True

    @admin.display(description="Emails")
    def emails_display(self, obj):
        emails = obj.emails.all()
        if not emails and not obj.email:
            return "-"
        if emails:
            return format_html_join(
                mark_safe("<br>"),
                "{}{}",
                (
                    (
                        f"{email.label}: " if email.label else "",
                        email.email,
                    )
                    for email in emails
                ),
            )
        return obj.email

    @admin.display(description="Phones")
    def phones_display(self, obj):
        phones = obj.phones.all()
        if not phones and not obj.phone:
            return "-"
        if phones:
            return format_html_join(
                mark_safe("<br>"),
                "{}{}",
                (
                    (
                        f"{phone.label}: " if phone.label else "",
                        phone.phone,
                    )
                    for phone in phones
                ),
            )
        return obj.phone

    @admin.display(description="Social Links")
    def social_links_display(self, obj):
        social_links = obj.social_links.all()
        if not social_links:
            return "-"
        return format_html_join(
            mark_safe("<br>"),
            "{}{}",
            (
                (
                    f"{social_link.platform}: " if social_link.platform else "",
                    social_link.url,
                )
                for social_link in social_links
            ),
        )


@admin.register(ImportFile)
class ImportFileAdmin(admin.ModelAdmin):
    list_display = ("file_name", "source_path", "created_at", "updated_at")
    search_fields = ("file_name", "source_path")
    inlines = [ImportRowInline]
    form = ImportFileAdminForm
    readonly_fields = ("source_path", "created_at", "updated_at")

    class Media:
        js = ("crm/import_file_admin.js",)

    def get_fields(self, request, obj=None):
        fields = ("file_name", "csv_file", "detected_headers", *MAPPING_FIELD_KEYS, "source_path")
        if obj:
            return fields + ("apply_updates_link",)
        return fields

    def get_readonly_fields(self, request, obj=None):
        fields = list(super().get_readonly_fields(request, obj))
        if obj:
            fields.append("apply_updates_link")
        return tuple(fields)

    @admin.display(description="Bulk Update Existing Records")
    def apply_updates_link(self, obj):
        if not obj or not obj.pk:
            return "-"
        url = reverse("admin:crm_importfile_apply_updates", args=[obj.pk])
        return mark_safe(f'<a class="button" href="{url}">Apply updates from this import file</a>')

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/apply-updates/",
                self.admin_site.admin_view(self.apply_updates_view),
                name="crm_importfile_apply_updates",
            ),
        ]
        return custom_urls + urls

    def apply_updates_view(self, request, object_id):
        import_file = get_object_or_404(ImportFile, pk=object_id)
        preview = None

        if request.method == "POST":
            form = ImportFileApplyUpdatesForm(request.POST, import_file=import_file)
            if form.is_valid():
                selected = form.selected_update_fields()
                mapping_overrides = form.selected_mapping_overrides()
                preview = analyze_updates_from_import_file(
                    import_file,
                    selected,
                    mapping_overrides=mapping_overrides,
                )
                if request.POST.get("action") == "apply":
                    stats = apply_updates_from_import_file(
                        import_file,
                        selected,
                        mapping_overrides=mapping_overrides,
                    )
                    message = (
                        "Bulk update completed. "
                        f"Rows processed: {stats['rows_processed']}, "
                        f"companies updated: {stats['companies_updated']}, "
                        f"contacts updated: {stats['contacts_updated']}, "
                        f"field updates: {stats['field_updates']}."
                    )
                    if stats["field_updates"] == 0:
                        zero_reason = ", ".join(
                            f"{item['label']}: non-empty={item['non_empty']}, will-change={item['will_change']}"
                            for item in preview["fields"]
                        ) or "No selectable fields found."
                        message = f"{message} No changes detected. {zero_reason}"
                    self.message_user(request, message, level=messages.SUCCESS)
                    return HttpResponseRedirect(reverse("admin:crm_importfile_change", args=[import_file.pk]))
        else:
            form = ImportFileApplyUpdatesForm(import_file=import_file)

        context = {
            **self.admin_site.each_context(request),
            "opts": self.model._meta,
            "import_file": import_file,
            "form": form,
            "preview": preview,
            "title": "Apply updates from import file",
            "original": import_file,
        }
        return render(request, "admin/crm/importfile/apply_updates.html", context)

    def save_model(self, request, obj, form, change):
        csv_file = form.cleaned_data.get("csv_file")
        if csv_file:
            uploads_dir = Path(settings.BASE_DIR) / "data" / "uploads"
            uploads_dir.mkdir(parents=True, exist_ok=True)
            temp_name = f"{uuid.uuid4().hex}_{csv_file.name}"
            temp_path = uploads_dir / temp_name
            with temp_path.open("wb") as out:
                for chunk in csv_file.chunks():
                    out.write(chunk)

            headers = form.cleaned_data.get("_csv_headers") or detect_headers(temp_path)
            suggested_mapping = suggest_mapping(headers)
            mapping = {}
            valid_headers = set(headers)
            for key in TARGET_FIELDS:
                selected = (form.cleaned_data.get(f"map_{key}") or "").strip()
                if selected and selected in valid_headers:
                    mapping[key] = selected
                else:
                    mapping[key] = suggested_mapping.get(key, "")
            file_name = (obj.file_name or csv_file.name).strip() or csv_file.name

            import_file, stats = import_csv_with_mapping(
                csv_path=temp_path,
                file_name=file_name,
                mapping=mapping,
                source_path=str(temp_path),
            )
            obj.pk = import_file.pk
            obj.file_name = import_file.file_name
            obj.source_path = import_file.source_path

            messages.success(
                request,
                (
                    "CSV import completed. "
                    f"Rows created: {stats['import_rows_created']}, "
                    f"rows updated: {stats['import_rows_updated']}."
                ),
            )
            return

        super().save_model(request, obj, form, change)


@admin.register(ImportRow)
class ImportRowAdmin(admin.ModelAdmin):
    list_display = (
        "import_file",
        "row_number",
        "company_name",
        "contact_name",
        "email_address",
        "phone_number",
        "city",
        "state",
        "country",
    )
    search_fields = (
        "import_file__file_name",
        "company_name",
        "contact_name",
        "email_address",
        "phone_number",
        "website",
        "person_source",
        "city",
        "state",
        "zip_code",
        "country",
    )
    list_filter = ("import_file",)

    def get_model_perms(self, request):
        return {}
