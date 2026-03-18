from . import (
    AdvancedFilterTestMixin,
    CRMRoleTestMixin,
    Client,
    Company,
    CompanyEmail,
    CompanyPhone,
    CompanySocialLink,
    Contact,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    TestCase,
    reverse,
    timedelta,
    timezone,
)


class AdvancedFilterTests(AdvancedFilterTestMixin, TestCase):
    def test_company_filters_narrow_results_and_keep_form_values(self):
        response = self.client.get(
            reverse("company_list"),
            {
                "industry": "SaaS",
                "state": "CA",
                "size_min": "10",
                "has_email": "yes",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({company.name for company in response.context["companies"]}, {"Acme Labs"})
        self.assertEqual(response.context["filters"]["industry"], "SaaS")
        self.assertEqual(response.context["filters"]["state"], "CA")
        self.assertEqual(response.context["filters"]["size_min"], "10")
        self.assertEqual(response.context["filters"]["has_email"], "yes")
        self.assertContains(response, "Matching companies")
        self.assertContains(response, "Industry:")

    def test_company_revenue_and_created_date_filters_work(self):
        response = self.client.get(
            reverse("company_list"),
            {
                "revenue": "$5M",
                "created_from": str((timezone.now() - timedelta(days=2)).date()),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({company.name for company in response.context["companies"]}, {"Cedar Staffing"})

    def test_company_pagination_preserves_filters(self):
        response = self.client.get(reverse("company_list"), {"state": "CA", "page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertEqual(response.context["page_query"], "state=CA")
        self.assertContains(response, "?page=1&state=CA")

    def test_company_filtered_empty_state_appears_when_no_results_match(self):
        response = self.client.get(reverse("company_list"), {"industry": "Nonexistent"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No companies matched the current filters.")
        self.assertNotContains(response, "No companies have landed in the ledger.")

    def test_company_csv_export_uses_filtered_rows_and_full_columns(self):
        response = self.client.get(
            reverse("company_list"),
            {
                "industry": "SaaS",
                "has_email": "yes",
                "export": "csv",
            },
        )
        headers, rows = self.parse_csv_response(response)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv; charset=utf-8")
        self.assertIn('attachment; filename="companies-export-', response["Content-Disposition"])
        self.assertEqual(
            headers,
            [
                "ID",
                "Company Name",
                "Industry",
                "Company Size",
                "Revenue",
                "Address",
                "City",
                "State",
                "Zip Code",
                "Country",
                "Notes",
                "Phones",
                "Emails",
                "Profiles",
                "Created At",
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "Acme Labs")
        self.assertEqual(rows[0][10], "Outbound team")
        self.assertIn("Office: 111-111", rows[0][11])
        self.assertIn("Sales: hello@acme.com", rows[0][12])
        self.assertIn("Linkedin: https://example.com/acme", rows[0][13])

    def test_company_export_ignores_pagination_and_invalid_export_falls_back_to_html(self):
        export_response = self.client.get(
            reverse("company_list"),
            {"state": "CA", "page": 2, "export": "csv"},
        )
        _headers, rows = self.parse_csv_response(export_response)

        self.assertEqual(export_response.status_code, 200)
        self.assertEqual(len(rows), 14)
        self.assertEqual({row[1] for row in rows}, {"Acme Labs", "Cedar Staffing", *{f"California Extra {index + 1}" for index in range(12)}})

        html_response = self.client.get(reverse("company_list"), {"export": "pdf"})
        self.assertEqual(html_response.status_code, 200)
        self.assertContains(html_response, "Company records")
        self.assertNotIn("Content-Disposition", html_response)

    def test_company_xlsx_export_and_empty_export_behave_correctly(self):
        response = self.client.get(
            reverse("company_list"),
            {
                "revenue": "$5M",
                "created_from": str((timezone.now() - timedelta(days=2)).date()),
                "export": "xlsx",
            },
        )
        headers, rows = self.parse_xlsx_response(response)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response["Content-Type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        self.assertIn('attachment; filename="companies-export-', response["Content-Disposition"])
        self.assertEqual(headers[1], "Company Name")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "Cedar Staffing")
        self.assertEqual(rows[0][4], "$5M")

        empty_response = self.client.get(
            reverse("company_list"),
            {"industry": "Nonexistent", "export": "csv"},
        )
        empty_headers, empty_rows = self.parse_csv_response(empty_response)
        self.assertEqual(empty_headers[1], "Company Name")
        self.assertEqual(empty_rows, [])


class CompanyCrudTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)

        self.company = Company.objects.create(
            name="Acme Labs",
            industry="Software",
            company_size="20",
            revenue="$2M",
            address="1 Market Street",
            city="San Francisco",
            state="CA",
            zip_code="94105",
            country="US",
            notes="Outbound focus",
        )
        self.primary_contact = Contact.objects.create(
            full_name="Jane Example",
            title="Operations Lead",
            email="jane@example.com",
            phone="555-0100",
        )
        self.secondary_contact = Contact.objects.create(
            full_name="Sam Seller",
            title="Account Executive",
            email="sam@example.com",
        )
        self.company.contacts.add(self.primary_contact)
        self.company_phone = CompanyPhone.objects.create(
            company=self.company,
            label="Office",
            phone="111-111",
        )
        self.company_email = CompanyEmail.objects.create(
            company=self.company,
            label="Sales",
            email="hello@acme.com",
        )
        self.company_profile = CompanySocialLink.objects.create(
            company=self.company,
            platform="LinkedIn",
            url="https://example.com/acme",
        )

    def _management_form(self, prefix, total_forms, initial_forms):
        return {
            f"{prefix}-TOTAL_FORMS": str(total_forms),
            f"{prefix}-INITIAL_FORMS": str(initial_forms),
            f"{prefix}-MIN_NUM_FORMS": "0",
            f"{prefix}-MAX_NUM_FORMS": "1000",
        }

    def _company_create_payload(self, **overrides):
        payload = {
            "name": "Blue Orbit Labs",
            "industry": "Aerospace",
            "company_size": "55",
            "revenue": "$9M",
            "address": "20 Launch Way",
            "city": "Houston",
            "state": "TX",
            "zip_code": "77058",
            "country": "US",
            "notes": "Priority launch account",
            "contacts": [str(self.primary_contact.pk), str(self.secondary_contact.pk)],
            **self._management_form("phones", 1, 0),
            "phones-0-label": "Office",
            "phones-0-phone": "555-1000",
            **self._management_form("emails", 1, 0),
            "emails-0-label": "Support",
            "emails-0-email": "ops@blueorbit.com",
            **self._management_form("social_links", 1, 0),
            "social_links-0-platform": "Website",
            "social_links-0-url": "https://blueorbit.example.com",
        }
        payload.update(overrides)
        return payload

    def _company_edit_payload(self, **overrides):
        payload = {
            "name": "Acme Labs Prime",
            "industry": "Software",
            "company_size": "24",
            "revenue": "$3M",
            "address": "99 Mission Street",
            "city": "San Francisco",
            "state": "CA",
            "zip_code": "94107",
            "country": "US",
            "notes": "Updated account plan",
            "contacts": [str(self.secondary_contact.pk)],
            **self._management_form("phones", 2, 1),
            "phones-0-id": str(self.company_phone.pk),
            "phones-0-label": "HQ",
            "phones-0-phone": "222-222",
            "phones-0-DELETE": "",
            "phones-1-id": "",
            "phones-1-label": "Support",
            "phones-1-phone": "333-333",
            "phones-1-DELETE": "",
            **self._management_form("emails", 2, 1),
            "emails-0-id": str(self.company_email.pk),
            "emails-0-label": "Sales",
            "emails-0-email": "revenue@acme.com",
            "emails-0-DELETE": "",
            "emails-1-id": "",
            "emails-1-label": "Support",
            "emails-1-email": "support@acme.com",
            "emails-1-DELETE": "",
            **self._management_form("social_links", 2, 1),
            "social_links-0-id": str(self.company_profile.pk),
            "social_links-0-platform": "LinkedIn",
            "social_links-0-url": "https://example.com/acme-prime",
            "social_links-0-DELETE": "",
            "social_links-1-id": "",
            "social_links-1-platform": "Website",
            "social_links-1-url": "https://acme-prime.example.com",
            "social_links-1-DELETE": "",
        }
        payload.update(overrides)
        return payload

    def test_company_detail_renders_profile_channels_and_linked_contacts_for_staff(self):
        self.client.force_login(self.staff_user)

        response = self.client.get(reverse("company_detail", args=[self.company.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "crm/companies/company_detail.html")
        self.assertContains(response, "Company snapshot")
        self.assertContains(response, "Jane Example")
        self.assertContains(response, "111-111")
        self.assertContains(response, "hello@acme.com")
        self.assertContains(response, "https://example.com/acme")

    def test_company_create_succeeds_for_team_lead_and_redirects_with_message(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(),
            follow=True,
        )

        created_company = Company.objects.get(name="Blue Orbit Labs")
        self.assertRedirects(response, reverse("company_detail", args=[created_company.pk]))
        self.assertContains(response, "Company created.")
        self.assertEqual(created_company.contacts.count(), 2)
        self.assertEqual(created_company.phones.get().phone, "555-1000")
        self.assertEqual(created_company.emails.get().email, "ops@blueorbit.com")
        self.assertEqual(created_company.social_links.get().url, "https://blueorbit.example.com")

    def test_company_edit_updates_core_fields_related_data_and_links(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_edit", args=[self.company.pk]),
            self._company_edit_payload(),
            follow=True,
        )

        self.company.refresh_from_db()
        self.assertRedirects(response, reverse("company_detail", args=[self.company.pk]))
        self.assertContains(response, "Company updated.")
        self.assertEqual(self.company.name, "Acme Labs Prime")
        self.assertEqual(self.company.address, "99 Mission Street")
        self.assertEqual(list(self.company.contacts.values_list("full_name", flat=True)), ["Sam Seller"])
        self.assertEqual(set(self.company.phones.values_list("phone", flat=True)), {"222-222", "333-333"})
        self.assertEqual(
            set(self.company.emails.values_list("email", flat=True)),
            {"revenue@acme.com", "support@acme.com"},
        )
        self.assertEqual(
            set(self.company.social_links.values_list("url", flat=True)),
            {"https://example.com/acme-prime", "https://acme-prime.example.com"},
        )

        linked_contact_response = self.client.get(reverse("contact_detail", args=[self.secondary_contact.pk]))
        self.assertContains(linked_contact_response, "Acme Labs Prime")

    def test_company_edit_can_delete_existing_inline_child_rows(self):
        self.client.force_login(self.team_lead_user)

        payload = self._company_edit_payload(
            **{
                "phones-0-DELETE": "on",
                "phones-1-label": "",
                "phones-1-phone": "",
                "emails-1-label": "",
                "emails-1-email": "",
                "social_links-1-platform": "",
                "social_links-1-url": "",
            }
        )
        response = self.client.post(
            reverse("company_edit", args=[self.company.pk]),
            payload,
            follow=True,
        )

        self.assertRedirects(response, reverse("company_detail", args=[self.company.pk]))
        self.assertFalse(CompanyPhone.objects.filter(pk=self.company_phone.pk).exists())

    def test_company_invalid_create_preserves_submitted_values_and_writes_nothing(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                name="",
                industry="Healthcare",
                **{"phones-0-phone": "555-2222"},
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(Company.objects.filter(name="Blue Orbit Labs").count(), 0)
        self.assertContains(response, "This field is required.")
        self.assertContains(response, 'value="Healthcare"', html=False)
        self.assertContains(response, 'value="555-2222"', html=False)

    def test_staff_cannot_access_company_create_or_edit(self):
        self.client.force_login(self.staff_user)

        create_response = self.client.get(reverse("company_create"))
        edit_response = self.client.get(reverse("company_edit", args=[self.company.pk]))

        self.assertEqual(create_response.status_code, 403)
        self.assertEqual(edit_response.status_code, 403)
