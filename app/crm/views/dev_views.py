from __future__ import annotations

from django.conf import settings
from django.http import Http404, HttpRequest, JsonResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET

from crm.dev_reload import get_dev_reload_token


@never_cache
@require_GET
def dev_reload_token(request: HttpRequest) -> JsonResponse:
    if not settings.DEBUG:
        raise Http404("Live reload is available only in local development.")

    return JsonResponse({"token": get_dev_reload_token()})
