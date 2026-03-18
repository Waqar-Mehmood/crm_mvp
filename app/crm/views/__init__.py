"""Public CRM view exports.

Only route-facing view callables are exported here. Internal helper modules in
``crm.views`` are implementation details and should be imported directly only
within the package.
"""

from .dashboard_views import dashboard_home
from .company_views import company_create, company_detail, company_edit, company_list
from .contact_views import contact_create, contact_detail, contact_edit, contact_list
from .import_views import (
    import_file_detail,
    import_file_list,
    import_google_sheets_preview,
    import_map_headers,
    import_upload,
)

__all__ = [
    "dashboard_home",
    "company_create",
    "company_detail",
    "company_edit",
    "company_list",
    "contact_create",
    "contact_detail",
    "contact_edit",
    "contact_list",
    "import_file_detail",
    "import_file_list",
    "import_google_sheets_preview",
    "import_map_headers",
    "import_upload",
]
