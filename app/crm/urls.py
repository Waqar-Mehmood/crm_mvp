from django.contrib.auth.views import LoginView, LogoutView
from django.urls import path
from .views import (
    company_list,
    contact_list,
    import_file_detail,
    import_file_list,
    import_map_headers,
    import_upload,
)

urlpatterns = [
    path(
        "login/",
        LoginView.as_view(
            template_name="registration/login.html",
            redirect_authenticated_user=True,
        ),
        name="login",
    ),
    path("logout/", LogoutView.as_view(), name="logout"),
    path("", company_list, name="home"),
    path("companies/", company_list, name="company_list"),
    path("contacts/", contact_list, name="contact_list"),
    path("imports/", import_file_list, name="import_file_list"),
    path("imports/upload/", import_upload, name="import_upload"),
    path("imports/map/", import_map_headers, name="import_map_headers"),
    path("imports/<int:file_id>/", import_file_detail, name="import_file_detail"),
]
