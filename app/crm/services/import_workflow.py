"""Transitional import workflow service entrypoint for the CRM app.

New code should prefer importing import workflow helpers from this module
rather than the legacy root-level ``crm.import_utils`` module.
"""

from crm.import_utils import (
    APPLY_UPDATE_FIELDS,
    SOURCE_IMPORT_FIELD_MAP,
    TARGET_FIELDS,
    analyze_updates_from_import_file,
    apply_updates_from_import_file,
    build_import_result_summary,
    clean,
    detect_headers,
    hydrate_import_rows_from_source,
    import_csv_with_mapping,
    infer_platform,
    mapped_value,
    suggest_mapping,
)

__all__ = [
    "APPLY_UPDATE_FIELDS",
    "SOURCE_IMPORT_FIELD_MAP",
    "TARGET_FIELDS",
    "analyze_updates_from_import_file",
    "apply_updates_from_import_file",
    "build_import_result_summary",
    "clean",
    "detect_headers",
    "hydrate_import_rows_from_source",
    "import_csv_with_mapping",
    "infer_platform",
    "mapped_value",
    "suggest_mapping",
]
