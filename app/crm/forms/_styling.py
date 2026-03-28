"""Shared Tailwind styling helpers for Django forms."""

from __future__ import annotations

from django import forms


def _append_widget_class(widget, classes: str) -> None:
    existing = widget.attrs.get("class", "")
    widget.attrs["class"] = " ".join(
        part for part in (existing, classes) if part
    )


def apply_crm_widget_classes(form: forms.Form) -> None:
    """Attach shared Tailwind classes to visible widgets."""
    for field in form.fields.values():
        widget = field.widget

        if isinstance(widget, (forms.HiddenInput, forms.MultipleHiddenInput)):
            continue

        if isinstance(widget, forms.CheckboxInput):
            _append_widget_class(
                widget,
                "h-4 w-4 rounded border border-brand-surface-border text-brand-accent "
                "focus:ring-2 focus:ring-brand-accent/20",
            )
            continue

        classes = "tw-input"
        if isinstance(widget, forms.Textarea):
            classes += " min-h-[9rem] resize-y"
        elif isinstance(widget, forms.Select):
            classes += " pr-10"

        _append_widget_class(widget, classes)

