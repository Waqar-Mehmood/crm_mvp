"""Frontend contact forms."""

from django import forms
from django.forms import inlineformset_factory

from crm.models import Company, Contact, ContactEmail, ContactPhone, ContactSocialLink


class ContactForm(forms.ModelForm):
    companies = forms.ModelMultipleChoiceField(
        queryset=Company.objects.none(),
        required=False,
        widget=forms.SelectMultiple(attrs={"size": 8}),
        help_text="Link existing companies to this contact.",
    )

    class Meta:
        model = Contact
        fields = [
            "full_name",
            "email",
            "phone",
            "title",
            "notes",
        ]
        widgets = {
            "notes": forms.Textarea(attrs={"rows": 5}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["companies"].queryset = Company.objects.order_by("name")
        if self.instance.pk:
            self.fields["companies"].initial = self.instance.companies.order_by("name")
        self.order_fields(
            [
                "full_name",
                "email",
                "phone",
                "title",
                "notes",
                "companies",
            ]
        )


class ContactPhoneForm(forms.ModelForm):
    class Meta:
        model = ContactPhone
        fields = ["label", "phone"]


class ContactEmailForm(forms.ModelForm):
    class Meta:
        model = ContactEmail
        fields = ["label", "email"]


class ContactSocialLinkForm(forms.ModelForm):
    class Meta:
        model = ContactSocialLink
        fields = ["platform", "url"]


ContactPhoneFormSet = inlineformset_factory(
    Contact,
    ContactPhone,
    form=ContactPhoneForm,
    fields=["label", "phone"],
    extra=1,
    can_delete=True,
)

ContactEmailFormSet = inlineformset_factory(
    Contact,
    ContactEmail,
    form=ContactEmailForm,
    fields=["label", "email"],
    extra=1,
    can_delete=True,
)

ContactSocialLinkFormSet = inlineformset_factory(
    Contact,
    ContactSocialLink,
    form=ContactSocialLinkForm,
    fields=["platform", "url"],
    extra=1,
    can_delete=True,
)

__all__ = [
    "ContactEmailFormSet",
    "ContactForm",
    "ContactPhoneFormSet",
    "ContactSocialLinkFormSet",
]
