"""Shared Tailwind styling helpers for Django forms."""

from __future__ import annotations

from django import forms


def _append_widget_class(widget, classes: str) -> None:
    existing = widget.attrs.get("class", "")
    widget.attrs["class"] = " ".join(
        part for part in (existing, classes) if part
    )


def configure_enhanced_select(
    field: forms.Field,
    *,
    search_enabled: bool = True,
    remove_item_button: bool = False,
    placeholder: str | None = None,
    should_sort: bool = False,
) -> None:
    """Mark a select widget for global Choices.js enhancement."""
    widget = field.widget
    widget.attrs["data-choice-select"] = "true"
    widget.attrs["data-choice-search-enabled"] = "true" if search_enabled else "false"
    widget.attrs["data-choice-remove-button"] = "true" if remove_item_button else "false"
    widget.attrs["data-choice-should-sort"] = "true" if should_sort else "false"
    if placeholder:
        widget.attrs["data-choice-placeholder"] = placeholder


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

        if isinstance(widget, forms.SelectMultiple):
            _append_widget_class(widget, "tw-select tw-select-multiple")
            configure_enhanced_select(
                field,
                search_enabled=True,
                remove_item_button=True,
            )
            continue

        if isinstance(widget, forms.Select):
            _append_widget_class(widget, "tw-select")
            continue

        classes = "tw-input"
        if isinstance(widget, forms.Textarea):
            classes += " min-h-[9rem] resize-y"

        _append_widget_class(widget, classes)
