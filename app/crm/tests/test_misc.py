from . import (
    CRMRoleTestMixin,
    Client,
    ROLE_OWNER,
    ROLE_STAFF,
    SiteBranding,
    TestCase,
    make_logo_file,
    override_settings,
    reverse,
    shutil,
    tempfile,
)


class BrandingMediaTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.owner_user = self.create_user("owner", role=ROLE_OWNER)
        self.temp_media_root = tempfile.mkdtemp()
        self.settings_override = override_settings(
            DEBUG=False,
            ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"],
            MEDIA_ROOT=self.temp_media_root,
            MEDIA_URL="/media/",
        )
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.addCleanup(shutil.rmtree, self.temp_media_root, ignore_errors=True)

    def test_branding_falls_back_to_site_name_when_no_logo_exists(self):
        SiteBranding.objects.create(site_name="The Zulfis CRM")

        response = self.client.get(reverse("login"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "The Zulfis CRM")
        self.assertNotContains(response, 'class="brand-logo"')

    def test_branding_image_renders_on_login_page(self):
        SiteBranding.objects.create(
            site_name="The Zulfis CRM",
            logo_image=make_logo_file(),
            logo_alt_text="The Zulfis logo",
        )

        response = self.client.get(reverse("login"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'class="brand-logo"')
        self.assertContains(response, "/media/branding/logo")
        self.assertContains(response, 'alt="The Zulfis logo"')

    def test_site_branding_admin_accepts_uploaded_logo_and_shows_preview(self):
        branding = SiteBranding.objects.create(site_name="The Zulfis CRM")
        self.client.force_login(self.owner_user)

        response = self.client.post(
            reverse("admin:crm_sitebranding_change", args=[branding.pk]),
            {
                "site_name": "The Zulfis CRM",
                "logo_alt_text": "The Zulfis logo",
                "logo_image": make_logo_file("uploaded-logo.png"),
                "_save": "Save",
            },
            follow=True,
        )
        branding.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(branding.logo_image.name.startswith("branding/"))
        self.assertContains(response, "/media/branding/")
        self.assertContains(response, "Logo Preview")

    def test_uploaded_logo_media_url_is_served_with_debug_false(self):
        branding = SiteBranding.objects.create(
            site_name="The Zulfis CRM",
            logo_image=make_logo_file(),
        )

        response = self.client.get(branding.logo_image.url)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["Content-Type"], "image/png")

    def test_missing_branding_media_returns_not_found(self):
        response = self.client.get("/media/branding/missing-logo.png")

        self.assertEqual(response.status_code, 404)


@override_settings(
    DEBUG=False,
    ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"],
    SITE_BRAND="The Zulfis CRM",
)
class NotFoundPageTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)

    def test_anonymous_missing_route_uses_branded_404_page(self):
        response = self.client.get("/missing-route/")

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "404.html")
        self.assertContains(response, "Page Not Found | The Zulfis CRM", status_code=404, html=False)
        self.assertContains(response, "The route", status_code=404)
        self.assertContains(response, "/missing-route/", status_code=404)
        self.assertContains(response, 'href="/login/"', status_code=404)

    def test_authenticated_missing_route_shows_companies_recovery_link(self):
        self.client.force_login(self.staff_user)

        response = self.client.get("/still-missing/")

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "404.html")
        self.assertContains(response, "The Zulfis CRM", status_code=404)
        self.assertContains(response, "/still-missing/", status_code=404)
        self.assertContains(response, 'href="/companies/"', status_code=404)
