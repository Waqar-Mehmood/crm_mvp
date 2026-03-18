"""Company-focused service helpers."""

from django.db import transaction


def save_company_bundle(
    form,
    phone_formset,
    email_formset,
    social_link_formset,
):
    is_valid = (
        form.is_valid()
        and phone_formset.is_valid()
        and email_formset.is_valid()
        and social_link_formset.is_valid()
    )
    if not is_valid:
        return None

    with transaction.atomic():
        company = form.save()
        company.contacts.set(form.cleaned_data["contacts"])

        phone_formset.instance = company
        email_formset.instance = company
        social_link_formset.instance = company

        phone_formset.save()
        email_formset.save()
        social_link_formset.save()

    return company


__all__ = ["save_company_bundle"]
