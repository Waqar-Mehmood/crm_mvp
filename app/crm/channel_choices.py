"""Canonical choices for related-channel labels and platforms."""

BLANK_CHOICE = ("", "---------")

COMPANY_PHONE_LABEL_CHOICES = (
    ("main", "Main"),
    ("office", "Office"),
    ("mobile", "Mobile"),
    ("sales", "Sales"),
    ("support", "Support"),
)

COMPANY_EMAIL_LABEL_CHOICES = (
    ("main", "Main"),
    ("sales", "Sales"),
    ("support", "Support"),
    ("billing", "Billing"),
)

COMPANY_PROFILE_PLATFORM_CHOICES = (
    ("website", "Website"),
    ("linkedin", "LinkedIn"),
    ("facebook", "Facebook"),
    ("instagram", "Instagram"),
    ("x", "X"),
)

CONTACT_PHONE_LABEL_CHOICES = (
    ("work", "Work"),
    ("mobile", "Mobile"),
    ("personal", "Personal"),
)

CONTACT_EMAIL_LABEL_CHOICES = (
    ("work", "Work"),
    ("personal", "Personal"),
)

CONTACT_PROFILE_PLATFORM_CHOICES = (
    ("linkedin", "LinkedIn"),
    ("website", "Website"),
    ("facebook", "Facebook"),
    ("instagram", "Instagram"),
    ("x", "X"),
)

CHANNEL_DISPLAY_LABELS = dict(
    COMPANY_PHONE_LABEL_CHOICES
    + COMPANY_EMAIL_LABEL_CHOICES
    + COMPANY_PROFILE_PLATFORM_CHOICES
    + CONTACT_PHONE_LABEL_CHOICES
    + CONTACT_EMAIL_LABEL_CHOICES
    + CONTACT_PROFILE_PLATFORM_CHOICES
)


def build_optional_choices(base_choices, current_value=""):
    """Return optional choices, preserving a legacy current value when needed."""

    choices = [BLANK_CHOICE, *base_choices]
    current_value = (current_value or "").strip()
    if current_value and current_value not in {value for value, _label in base_choices}:
        return [BLANK_CHOICE, (current_value, humanize_channel_value(current_value)), *base_choices]
    return choices


def configure_optional_choice_field(form, field_name, base_choices):
    """Attach canonical choices to a form field while keeping legacy instance values editable."""

    current_value = ""
    if getattr(form.instance, "pk", None):
        current_value = getattr(form.instance, field_name, "")
    form.fields[field_name].choices = build_optional_choices(base_choices, current_value)


def humanize_channel_value(value):
    """Render canonical lowercase values with polished labels and preserve legacy custom text."""

    value = (value or "").strip()
    if not value:
        return ""
    return CHANNEL_DISPLAY_LABELS.get(value, value)


__all__ = [
    "BLANK_CHOICE",
    "CHANNEL_DISPLAY_LABELS",
    "COMPANY_EMAIL_LABEL_CHOICES",
    "COMPANY_PHONE_LABEL_CHOICES",
    "COMPANY_PROFILE_PLATFORM_CHOICES",
    "CONTACT_EMAIL_LABEL_CHOICES",
    "CONTACT_PHONE_LABEL_CHOICES",
    "CONTACT_PROFILE_PLATFORM_CHOICES",
    "build_optional_choices",
    "configure_optional_choice_field",
    "humanize_channel_value",
]
