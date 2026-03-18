"""Contact-focused service helpers."""

from django.db import transaction


def save_contact_bundle(
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
        contact = form.save()
        contact.companies.set(form.cleaned_data["companies"])

        phone_formset.instance = contact
        email_formset.instance = contact
        social_link_formset.instance = contact

        phone_formset.save()
        email_formset.save()
        social_link_formset.save()

    return contact


__all__ = ["save_contact_bundle"]
