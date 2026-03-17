"""Shared view-layer helpers for the CRM app."""

from __future__ import annotations

from django.core.paginator import Paginator
from django.utils.dateparse import parse_date

from crm.export_utils import export_rows_to_csv_response, export_rows_to_xlsx_response

PAGE_SIZE = 10
BOOLEAN_FILTER_LABELS = {
    "yes": "Yes",
    "no": "No",
}


def _paginate(request, queryset, per_page=PAGE_SIZE):
    paginator = Paginator(queryset, per_page)
    return paginator.get_page(request.GET.get("page"))


def _query_string(request, remove_keys=None, extra=None):
    query = request.GET.copy()
    for key in remove_keys or ():
        query.pop(key, None)
    for key, value in (extra or {}).items():
        if value in (None, ""):
            query.pop(key, None)
        else:
            query[key] = value
    return query.urlencode()


def _page_query(request):
    return _query_string(request, remove_keys={"page", "export"})


def _export_query(request, export_format):
    return _query_string(
        request,
        remove_keys={"page", "export"},
        extra={"export": export_format},
    )


def _clean_text(value):
    return (value or "").strip()


def _clean_export_format(value):
    value = _clean_text(value).lower()
    return value if value in {"csv", "xlsx"} else ""


def _clean_toggle(value):
    value = _clean_text(value).lower()
    return value if value in BOOLEAN_FILTER_LABELS else ""


def _parse_int(value):
    value = _clean_text(value)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _parse_date_value(value):
    return parse_date(_clean_text(value))


def _distinct_nonempty_values(queryset, field_name):
    return list(
        queryset.exclude(**{field_name: ""})
        .order_by(field_name)
        .values_list(field_name, flat=True)
        .distinct()
    )


def _add_active_filter(active_filters, label, value):
    if value:
        active_filters.append({"label": label, "value": value})


def _apply_toggle_filter(queryset, field_name, toggle_value):
    if toggle_value == "yes":
        return queryset.filter(**{field_name: True})
    if toggle_value == "no":
        return queryset.filter(**{field_name: False})
    return queryset


def _export_response(export_format, base_name, sheet_name, columns, rows):
    if export_format == "csv":
        return export_rows_to_csv_response(base_name, columns, rows)
    return export_rows_to_xlsx_response(base_name, sheet_name, columns, rows)
