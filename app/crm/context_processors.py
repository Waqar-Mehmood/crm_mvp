from django.conf import settings
from django.db.utils import OperationalError, ProgrammingError

from crm.auth import (
    ROLE_MANAGER,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    get_role_label,
    get_user_crm_role,
    get_user_role_status,
    user_has_minimum_crm_role,
)
from crm.models import SiteBranding


def branding(request):
    site_brand = settings.SITE_BRAND
    site_logo_url = ""
    site_logo_alt = ""

    try:
        branding_settings = SiteBranding.objects.order_by("id").first()
    except (OperationalError, ProgrammingError):
        branding_settings = None

    if branding_settings:
        if branding_settings.site_name:
            site_brand = branding_settings.site_name
        site_logo_url = branding_settings.logo_url
        site_logo_alt = branding_settings.logo_alt_text or site_brand

    crm_role = get_user_crm_role(getattr(request, "user", None))

    return {
        "site_brand": site_brand,
        "site_logo_url": site_logo_url,
        "site_logo_alt": site_logo_alt,
        "crm_role": crm_role,
        "crm_role_label": get_role_label(crm_role),
        "crm_role_status": get_user_role_status(getattr(request, "user", None)),
        "crm_can_browse": user_has_minimum_crm_role(getattr(request, "user", None), ROLE_STAFF),
        "crm_can_import": user_has_minimum_crm_role(getattr(request, "user", None), ROLE_TEAM_LEAD),
        "crm_can_admin": user_has_minimum_crm_role(getattr(request, "user", None), ROLE_MANAGER),
    }
