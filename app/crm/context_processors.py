from django.conf import settings
from django.db.utils import OperationalError, ProgrammingError
from django.urls import reverse

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
        if branding_settings.logo_image:
            site_logo_url = branding_settings.logo_image.url
        site_logo_alt = branding_settings.logo_alt_text or site_brand

    crm_role = get_user_crm_role(getattr(request, "user", None))
    dev_live_reload_enabled = settings.DEBUG
    dev_live_reload_url = reverse("dev_reload_token") if dev_live_reload_enabled else ""

    return {
        "site_brand": site_brand,
        "site_logo_url": site_logo_url,
        "site_logo_alt": site_logo_alt,
        "crm_role": crm_role,
        "crm_role_label": get_role_label(crm_role),
        "crm_role_status": get_user_role_status(getattr(request, "user", None)),
        "crm_can_browse": user_has_minimum_crm_role(getattr(request, "user", None), ROLE_STAFF),
        "crm_can_import": user_has_minimum_crm_role(getattr(request, "user", None), ROLE_TEAM_LEAD),
        "crm_can_manage_records": user_has_minimum_crm_role(
            getattr(request, "user", None),
            ROLE_TEAM_LEAD,
        ),
        "crm_can_admin": user_has_minimum_crm_role(getattr(request, "user", None), ROLE_MANAGER),
        "crm_dev_live_reload_enabled": dev_live_reload_enabled,
        "crm_dev_live_reload_url": dev_live_reload_url,
        "crm_dev_live_reload_interval_ms": 1500,
    }
