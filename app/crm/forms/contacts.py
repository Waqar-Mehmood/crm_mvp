"""Frontend contact forms."""

from django import forms
from django.forms import inlineformset_factory

from crm.channel_choices import (
    CONTACT_EMAIL_LABEL_CHOICES,
    CONTACT_PHONE_LABEL_CHOICES,
    CONTACT_PROFILE_PLATFORM_CHOICES,
    configure_optional_choice_field,
)
from crm.models import Company, Contact, ContactEmail, ContactPhone, ContactSocialLink


class ContactForm(forms.ModelForm):
    companies = forms.ModelMultipleChoiceField(
        queryset=Company.objects.none(),
        required=False,
        widget=forms.MultipleHiddenInput(),
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
            "phone": forms.TextInput(attrs={"type": "tel"}),
            "notes": forms.Textarea(attrs={"rows": 5}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.is_bound:
            selected_company_ids = [
                value
                for value in self.data.getlist("companies")
                if str(value).strip()
            ]
            self.fields["companies"].queryset = Company.objects.filter(
                pk__in=selected_company_ids
            ).order_by("name")
        elif self.instance.pk:
            self.fields["companies"].queryset = self.instance.companies.order_by("name")
            self.fields["companies"].initial = self.fields["companies"].queryset
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
    use_required_attribute = False
    label = forms.ChoiceField(required=False)

    class Meta:
        model = ContactPhone
        fields = ["label", "phone"]
        widgets = {
            "phone": forms.TextInput(attrs={"type": "tel"}),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        configure_optional_choice_field(self, "label", CONTACT_PHONE_LABEL_CHOICES)


class ContactEmailForm(forms.ModelForm):
    use_required_attribute = False
    label = forms.ChoiceField(required=False)

    class Meta:
        model = ContactEmail
        fields = ["label", "email"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        configure_optional_choice_field(self, "label", CONTACT_EMAIL_LABEL_CHOICES)


class ContactSocialLinkForm(forms.ModelForm):
    use_required_attribute = False
    platform = forms.ChoiceField(required=False)

    class Meta:
        model = ContactSocialLink
        fields = ["platform", "url"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        configure_optional_choice_field(
            self,
            "platform",
            CONTACT_PROFILE_PLATFORM_CHOICES,
        )


ContactPhoneFormSet = inlineformset_factory(
    Contact,
    ContactPhone,
    form=ContactPhoneForm,
    fields=["label", "phone"],
    extra=0,
    can_delete=True,
)

ContactEmailFormSet = inlineformset_factory(
    Contact,
    ContactEmail,
    form=ContactEmailForm,
    fields=["label", "email"],
    extra=0,
    can_delete=True,
)

ContactSocialLinkFormSet = inlineformset_factory(
    Contact,
    ContactSocialLink,
    form=ContactSocialLinkForm,
    fields=["platform", "url"],
    extra=0,
    can_delete=True,
)

__all__ = [
    "ContactEmailFormSet",
    "ContactForm",
    "ContactPhoneFormSet",
    "ContactSocialLinkFormSet",
]
