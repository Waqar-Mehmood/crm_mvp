"""Public CRM view exports.

This package preserves the old ``crm.views`` import surface while splitting
the implementation into smaller feature-based modules.
"""

from .company_views import company_list
from .contact_views import contact_list
from .import_views import (
    import_file_detail,
    import_file_list,
    import_google_sheets_preview,
    import_map_headers,
    import_upload,
)

__all__ = [
    "company_list",
    "contact_list",
    "import_file_detail",
    "import_file_list",
    "import_google_sheets_preview",
    "import_map_headers",
    "import_upload",
]
