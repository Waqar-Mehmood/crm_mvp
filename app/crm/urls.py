from django.contrib.auth.views import LoginView, LogoutView
from django.urls import path
from .views import (
    company_create,
    company_detail,
    company_edit,
    company_list,
    contact_create,
    contact_detail,
    contact_edit,
    contact_list,
    dashboard_home,
    import_file_detail,
    import_file_list,
    import_google_sheets_preview,
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
    path("", dashboard_home, name="home"),
    path("companies/new/", company_create, name="company_create"),
    path("companies/<int:company_id>/edit/", company_edit, name="company_edit"),
    path("companies/<int:company_id>/", company_detail, name="company_detail"),
    path("companies/", company_list, name="company_list"),
    path("contacts/new/", contact_create, name="contact_create"),
    path("contacts/<int:contact_id>/edit/", contact_edit, name="contact_edit"),
    path("contacts/<int:contact_id>/", contact_detail, name="contact_detail"),
    path("contacts/", contact_list, name="contact_list"),
    path("import/google-sheets/", import_google_sheets_preview, name="import_google_sheets"),
    path("imports/", import_file_list, name="import_file_list"),
    path("imports/upload/", import_upload, name="import_upload"),
    path("imports/map/", import_map_headers, name="import_map_headers"),
    path("imports/<int:file_id>/", import_file_detail, name="import_file_detail"),
]
