from django.conf import settings
from django.contrib import admin
from django.urls import include, path, re_path

from crm.views import dev_reload_token
from .views import serve_media

urlpatterns = [
    path("", include("crm.urls")),
    path("admin/", admin.site.urls),
    re_path(r"^media/(?P<path>.*)$", serve_media),
]

if settings.DEBUG:
    urlpatterns.append(path("__dev__/reload-token/", dev_reload_token, name="dev_reload_token"))
