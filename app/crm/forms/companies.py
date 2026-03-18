"""Frontend company forms."""

from django import forms
from django.forms import inlineformset_factory

from crm.models import Company, CompanyEmail, CompanyPhone, CompanySocialLink, Contact


class CompanyForm(forms.ModelForm):
    contacts = forms.ModelMultipleChoiceField(
        queryset=Contact.objects.none(),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 8}),
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
            "notes": forms.Textarea(attrs={"rows": 5}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["contacts"].queryset = Contact.objects.order_by("full_name")
        if self.instance.pk:
            self.fields["contacts"].initial = self.instance.contacts.order_by("full_name")
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


class CompanyPhoneForm(forms.ModelForm):
    class Meta:
        model = CompanyPhone
        fields = ["label", "phone"]


class CompanyEmailForm(forms.ModelForm):
    class Meta:
        model = CompanyEmail
        fields = ["label", "email"]


class CompanySocialLinkForm(forms.ModelForm):
    class Meta:
        model = CompanySocialLink
        fields = ["platform", "url"]


CompanyPhoneFormSet = inlineformset_factory(
    Company,
    CompanyPhone,
    form=CompanyPhoneForm,
    fields=["label", "phone"],
    extra=1,
    can_delete=True,
)

CompanyEmailFormSet = inlineformset_factory(
    Company,
    CompanyEmail,
    form=CompanyEmailForm,
    fields=["label", "email"],
    extra=1,
    can_delete=True,
)

CompanySocialLinkFormSet = inlineformset_factory(
    Company,
    CompanySocialLink,
    form=CompanySocialLinkForm,
    fields=["platform", "url"],
    extra=1,
    can_delete=True,
)

__all__ = [
    "CompanyEmailFormSet",
    "CompanyForm",
    "CompanyPhoneFormSet",
    "CompanySocialLinkFormSet",
]
