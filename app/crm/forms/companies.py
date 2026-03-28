"""Frontend company forms."""

import re

from django import forms
from django.forms import inlineformset_factory

from crm.channel_choices import (
    COMPANY_EMAIL_LABEL_CHOICES,
    COMPANY_PHONE_LABEL_CHOICES,
    COMPANY_PROFILE_PLATFORM_CHOICES,
    configure_optional_choice_field,
)
from crm.forms._styling import apply_crm_widget_classes
from crm.models import Company, CompanyEmail, CompanyPhone, CompanySocialLink, Contact

COMPANY_SIZE_PATTERN = re.compile(r"^\d+(?:-\d+)?$")


class CompanyForm(forms.ModelForm):
    contacts = forms.ModelMultipleChoiceField(
        queryset=Contact.objects.none(),
        required=False,
        widget=forms.MultipleHiddenInput(),
        help_text="Link existing contacts to this company.",
    )

    class Meta:
        model = Company
        fields = [
            "name",
            "industry",
            "company_size",
            "revenue",
            "address",
            "city",
            "state",
            "zip_code",
            "country",
            "notes",
        ]
        widgets = {
            "industry": forms.TextInput(
                attrs={
                    "autocomplete": "off",
                    "placeholder": "Search or paste an industry",
                }
            ),
            "company_size": forms.TextInput(
                attrs={
                    "inputmode": "numeric",
                    "placeholder": "55 or 50-100",
                }
            ),
            "notes": forms.Textarea(attrs={"rows": 5}),
        }
        help_texts = {
            "industry": "Type or paste to search existing industries. Keep typing to save a new value.",
            "company_size": "Use a whole number or numeric range, for example 55 or 50-100.",
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        apply_crm_widget_classes(self)
        if self.is_bound:
            selected_contact_ids = [
                value
                for value in self.data.getlist("contacts")
                if str(value).strip()
            ]
            self.fields["contacts"].queryset = Contact.objects.filter(
                pk__in=selected_contact_ids
            ).order_by("full_name")
        elif self.instance.pk:
            self.fields["contacts"].queryset = self.instance.contacts.order_by("full_name")
            self.fields["contacts"].initial = self.fields["contacts"].queryset
        self.order_fields(
            [
                "name",
                "industry",
                "company_size",
                "revenue",
                "address",
                "city",
                "state",
                "zip_code",
                "country",
                "notes",
                "contacts",
            ]
        )

    def clean_industry(self):
        return self.cleaned_data["industry"].strip()

    def clean_company_size(self):
        value = (self.cleaned_data.get("company_size") or "").strip()
        if not value:
            return ""

        normalized_value = re.sub(r"\s*-\s*", "-", value)
        if not COMPANY_SIZE_PATTERN.match(normalized_value):
            raise forms.ValidationError(
                "Enter a whole number or range like 55 or 50-100."
            )
        return normalized_value


class CompanyPhoneForm(forms.ModelForm):
    use_required_attribute = False
    label = forms.ChoiceField(required=False)

    class Meta:
        model = CompanyPhone
        fields = ["label", "phone"]
        widgets = {
            "phone": forms.TextInput(attrs={"type": "tel"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        configure_optional_choice_field(self, "label", COMPANY_PHONE_LABEL_CHOICES)
        apply_crm_widget_classes(self)


class CompanyEmailForm(forms.ModelForm):
    use_required_attribute = False
    label = forms.ChoiceField(required=False)

    class Meta:
        model = CompanyEmail
        fields = ["label", "email"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        configure_optional_choice_field(self, "label", COMPANY_EMAIL_LABEL_CHOICES)
        apply_crm_widget_classes(self)


class CompanySocialLinkForm(forms.ModelForm):
    use_required_attribute = False
    platform = forms.ChoiceField(required=False)

    class Meta:
        model = CompanySocialLink
        fields = ["platform", "url"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        configure_optional_choice_field(
            self,
            "platform",
            COMPANY_PROFILE_PLATFORM_CHOICES,
        )
        apply_crm_widget_classes(self)


CompanyPhoneFormSet = inlineformset_factory(
    Company,
    CompanyPhone,
    form=CompanyPhoneForm,
    fields=["label", "phone"],
    extra=0,
    can_delete=True,
)

CompanyEmailFormSet = inlineformset_factory(
    Company,
    CompanyEmail,
    form=CompanyEmailForm,
    fields=["label", "email"],
    extra=0,
    can_delete=True,
)

CompanySocialLinkFormSet = inlineformset_factory(
    Company,
    CompanySocialLink,
    form=CompanySocialLinkForm,
    fields=["platform", "url"],
    extra=0,
    can_delete=True,
)

__all__ = [
    "CompanyEmailFormSet",
    "CompanyForm",
    "CompanyPhoneFormSet",
    "CompanySocialLinkFormSet",
]
