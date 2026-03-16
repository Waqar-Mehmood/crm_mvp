from django.contrib import admin
from django.urls import include, path, re_path

from .views import serve_media

urlpatterns = [
    path("", include("crm.urls")),
    path("admin/", admin.site.urls),
    re_path(r"^media/(?P<path>.*)$", serve_media),
]
