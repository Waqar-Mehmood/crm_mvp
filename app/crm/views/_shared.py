"""Internal shared helpers for CRM view modules.

This module is intentionally not part of the public ``crm.views`` import
surface. Route-facing view callables continue to live in the feature modules
and are re-exported from ``crm.views``.
"""

from __future__ import annotations

from django.core.paginator import Paginator
from django.utils.dateparse import parse_date

from crm.services.export_service import (
    export_rows_to_csv_response,
    export_rows_to_xlsx_response,
)

PAGE_SIZE = 10
PAGE_SIZE_OPTIONS = (10, 50, 100)
BOOLEAN_FILTER_LABELS = {
    "yes": "Yes",
    "no": "No",
}


def _paginate(request, queryset, per_page=PAGE_SIZE, page_key="page"):
    paginator = Paginator(queryset, per_page)
    return paginator.get_page(request.GET.get(page_key))


def _query_string(request, remove_keys=None, extra=None, query=None):
    query = (query or request.GET).copy()
    for key in remove_keys or ():
        query.pop(key, None)
    for key, value in (extra or {}).items():
        if value in (None, ""):
            query.pop(key, None)
        else:
            query[key] = value
    return query.urlencode()


def _page_query(request, *, page_key="page", remove_keys=None, query=None):
    keys_to_remove = {"export", page_key}
    if remove_keys:
        keys_to_remove.update(remove_keys)
    return _query_string(request, remove_keys=keys_to_remove, query=query)


def _export_query(request, export_format, *, page_key="page", remove_keys=None, query=None):
    keys_to_remove = {"export", page_key}
    if remove_keys:
        keys_to_remove.update(remove_keys)
    return _query_string(
        request,
        remove_keys=keys_to_remove,
        extra={"export": export_format},
        query=query,
    )


def _clean_text(value):
    return (value or "").strip()


def _clean_export_format(value):
    value = _clean_text(value).lower()
    return value if value in {"csv", "xlsx"} else ""


def _clean_toggle(value):
    value = _clean_text(value).lower()
    return value if value in BOOLEAN_FILTER_LABELS else ""


def _clean_sort_direction(value, default="asc"):
    value = _clean_text(value).lower()
    return value if value in {"asc", "desc"} else default


def _parse_int(value):
    value = _clean_text(value)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _clean_per_page(value):
    parsed_value = _parse_int(value)
    return parsed_value if parsed_value in PAGE_SIZE_OPTIONS else PAGE_SIZE


def _clean_column_list(value, allowed_keys, default_keys):
    allowed = set(allowed_keys)
    selected = []
    seen = set()
    for token in _clean_text(value).split(","):
        column_key = token.strip()
        if column_key and column_key in allowed and column_key not in seen:
            selected.append(column_key)
            seen.add(column_key)
    return selected or list(default_keys)


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


def _query_items(request, remove_keys=None, extra=None, query=None):
    query = (query or request.GET).copy()
    for key in remove_keys or ():
        query.pop(key, None)
    for key, value in (extra or {}).items():
        query.pop(key, None)
        if value in (None, ""):
            continue
        if isinstance(value, (list, tuple)):
            for item in value:
                query.appendlist(key, item)
        else:
            query[key] = value

    items = []
    for key, values in query.lists():
        for value in values:
            items.append((key, value))
    return items


def _export_response(export_format, base_name, sheet_name, columns, rows):
    if export_format == "csv":
        return export_rows_to_csv_response(base_name, columns, rows)
    return export_rows_to_xlsx_response(base_name, sheet_name, columns, rows)
