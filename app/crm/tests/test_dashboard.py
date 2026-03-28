from . import (
    CRMRoleTestMixin,
    Client,
    Company,
    Contact,
    ImportFile,
    ImportRow,
    ROLE_MANAGER,
    ROLE_OWNER,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    TestCase,
    reverse,
    timezone,
)


class DashboardViewTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)
        self.manager_user = self.create_user("manager", role=ROLE_MANAGER)
        self.owner_user = self.create_user("owner", role=ROLE_OWNER)

        self.company = Company.objects.create(
            name="Acme Labs",
            city="San Francisco",
            country="US",
        )
        self.contact = Contact.objects.create(
            full_name="Jane Example",
            title="Operations Lead",
        )
        self.company.contacts.add(self.contact)

        self.import_file = ImportFile.objects.create(file_name="leads.csv")
        ImportRow.objects.create(
            import_file=self.import_file,
            row_number=1,
            company=self.company,
            contact=self.contact,
            mapped_payload={
                "company_name": "Acme Labs",
                "contact_name": "Jane Example",
            },
        )
        ImportRow.objects.create(
            import_file=self.import_file,
            row_number=2,
            mapped_payload={
                "company_name": "Unlinked Prospect",
                "contact_name": "Missing Match",
            },
        )
        ImportFile.objects.filter(pk=self.import_file.pk).update(updated_at=timezone.now())

    def test_home_route_uses_dashboard_template(self):
        self.client.force_login(self.staff_user)

        response = self.client.get(reverse("home"))

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "crm/dashboard/home.html")
        self.assertContains(response, "Workspace dashboard")
        self.assertContains(response, "leads.csv")
        self.assertEqual(
            response.context["dashboard_rows"][0]["panels"][0]["body_template"],
            "crm/components/content_panels/body_detail_cards.html",
        )
        self.assertEqual(
            response.context["dashboard_rows"][0]["panels"][1]["body_template"],
            "crm/components/content_panels/body_actions.html",
        )
        self.assertNotContains(response, "metric-card")
        self.assertNotContains(response, "record-card")
        self.assertNotContains(response, "detail-card")

    def test_owner_dashboard_shows_team_activity_section(self):
        self.client.force_login(self.owner_user)

        response = self.client.get(reverse("home"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Business and team coverage")
        self.assertContains(response, "Team member")
        self.assertContains(response, "owner")
        self.assertEqual(
            response.context["role_panel"]["body_template"],
            "crm/components/content_panels/body_stats_and_cards.html",
        )

    def test_team_lead_dashboard_exposes_import_actions(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.get(reverse("home"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'href="/imports/upload/"')
        self.assertContains(response, 'href="/import/google-sheets/"')
        self.assertContains(response, "Delivery queue")
        self.assertEqual(
            response.context["role_panel"]["body_template"],
            "crm/components/content_panels/body_detail_cards.html",
        )

    def test_staff_dashboard_uses_shared_review_queue_and_hides_upload_actions(self):
        self.client.force_login(self.staff_user)

        response = self.client.get(reverse("home"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Shared review queue")
        self.assertContains(response, "Open review rows")
        self.assertNotContains(response, "Upload import file")

    def test_manager_dashboard_shows_operations_metrics(self):
        self.client.force_login(self.manager_user)

        response = self.client.get(reverse("home"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Operations dashboard")
        self.assertContains(response, "Managed seats")
        self.assertContains(response, "Team operations")
