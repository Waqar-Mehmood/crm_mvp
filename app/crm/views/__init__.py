"""Public CRM view exports.

Only route-facing view callables are exported here. Internal helper modules in
``crm.views`` are implementation details and should be imported directly only
within the package.
"""

from .dashboard_views import dashboard_home
from .dev_views import dev_reload_token
from .company_views import (
    company_contact_search,
    company_create,
    company_detail,
    company_edit,
    company_industry_search,
    company_list,
)
from .contact_views import (
    contact_company_search,
    contact_create,
    contact_detail,
    contact_edit,
    contact_list,
)
from .import_views import (
    import_file_detail,
    import_file_download,
    import_file_list,
    import_file_raw_source,
    import_google_sheets_preview,
    import_map_headers,
    import_upload,
)

__all__ = [
    "dashboard_home",
    "dev_reload_token",
    "company_contact_search",
    "company_create",
    "company_detail",
    "company_edit",
    "company_industry_search",
    "company_list",
    "contact_company_search",
    "contact_create",
    "contact_detail",
    "contact_edit",
    "contact_list",
    "import_file_detail",
    "import_file_download",
    "import_file_list",
    "import_file_raw_source",
    "import_google_sheets_preview",
    "import_map_headers",
    "import_upload",
]
