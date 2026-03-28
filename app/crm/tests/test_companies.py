from urllib.parse import parse_qs, urlsplit

from crm.channel_choices import (
    BLANK_CHOICE,
    COMPANY_EMAIL_LABEL_CHOICES,
    COMPANY_PHONE_LABEL_CHOICES,
    COMPANY_PROFILE_PLATFORM_CHOICES,
)

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
    def test_company_list_defaults_to_new_columns_and_blank_cells(self):
        response = self.client.get(reverse("company_list"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.context["visible_columns"],
            ["row", "company", "industry", "address", "size", "revenue", "location", "phones", "emails", "profiles"],
        )
        self.assertEqual(
            [header["key"] for header in response.context["table_headers"]],
            ["row", "company", "industry", "address", "size", "revenue", "location", "phones", "emails", "profiles"],
        )
        self.assertEqual(response.context["table_headers"][0]["label"], "#")
        self.assertEqual(
            response.context["filter_ui"]["fields_template"],
            "crm/components/list_workspace/filter_fields.html",
        )
        self.assertEqual(
            response.context["table_ui"]["row_template"],
            "crm/components/list_workspace/table_row.html",
        )
        self.assertNotContains(response, "No street address")
        self.assertNotContains(response, "Unknown")
        self.assertNotContains(response, "Undisclosed")
        self.assertNotContains(response, "No phones")
        self.assertNotContains(response, "No emails")

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

    def test_company_filter_panel_is_collapsed_by_default_and_opens_with_filters(self):
        default_response = self.client.get(reverse("company_list"))
        filtered_response = self.client.get(reverse("company_list"), {"industry": "SaaS"})

        self.assertContains(default_response, "data-animated-disclosure")
        self.assertContains(filtered_response, "data-animated-disclosure open")
        self.assertContains(default_response, "Show filters")
        self.assertContains(filtered_response, "Hide filters")
        self.assertNotContains(default_response, ">Clear filters<", html=False)
        self.assertContains(default_response, ">Reset<", html=False)
        self.assertContains(default_response, 'name="q"', html=False)
        self.assertContains(default_response, 'name="industry"', html=False)
        self.assertContains(default_response, 'name="created_to"', html=False)
        self.assertContains(default_response, 'value="Apply filters"', html=False)

    def test_company_column_picker_uses_requested_columns_only(self):
        response = self.client.get(
            reverse("company_list"),
            {"columns": "row,company,address"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["visible_columns"], ["row", "company", "address"])
        self.assertEqual(
            [header["key"] for header in response.context["table_headers"]],
            ["row", "company", "address"],
        )

    def test_company_row_numbers_continue_across_pages_and_per_page_changes(self):
        paged_response = self.client.get(
            reverse("company_list"),
            {"page": 2, "state": "CA", "columns": "row,company"},
        )
        expanded_response = self.client.get(reverse("company_list"), {"per_page": 50})
        per_page_menu = {
            item["value"]: item for item in paged_response.context["per_page_menu_options"]
        }

        self.assertEqual(paged_response.context["page_obj"].start_index(), 11)
        self.assertEqual(paged_response.context["table_rows"][0]["cells"][0]["text"], 11)
        self.assertContains(paged_response, "Rows: 10")
        self.assertContains(paged_response, ">Columns<", html=False)
        self.assertContains(paged_response, ">Export<", html=False)
        self.assertTrue(per_page_menu[10]["is_active"])
        self.assertIn("state=CA", per_page_menu[50]["url"])
        self.assertIn("columns=row%2Ccompany", per_page_menu[50]["url"])
        self.assertEqual(
            parse_qs(urlsplit(per_page_menu[50]["url"]).query).get("per_page"),
            ["50"],
        )
        self.assertNotIn("page", parse_qs(urlsplit(per_page_menu[50]["url"]).query))
        self.assertEqual(expanded_response.context["page_obj"].paginator.per_page, 50)
        self.assertContains(expanded_response, "Rows: 50")
        self.assertEqual(len(expanded_response.context["companies"]), 15)

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
            "phones-0-label": "office",
            "phones-0-phone": "555-1000",
            **self._management_form("emails", 1, 0),
            "emails-0-label": "support",
            "emails-0-email": "ops@blueorbit.com",
            **self._management_form("social_links", 1, 0),
            "social_links-0-platform": "website",
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
            "phones-0-label": "office",
            "phones-0-phone": "222-222",
            "phones-0-DELETE": "",
            "phones-1-id": "",
            "phones-1-label": "support",
            "phones-1-phone": "333-333",
            "phones-1-DELETE": "",
            **self._management_form("emails", 2, 1),
            "emails-0-id": str(self.company_email.pk),
            "emails-0-label": "sales",
            "emails-0-email": "revenue@acme.com",
            "emails-0-DELETE": "",
            "emails-1-id": "",
            "emails-1-label": "support",
            "emails-1-email": "support@acme.com",
            "emails-1-DELETE": "",
            **self._management_form("social_links", 2, 1),
            "social_links-0-id": str(self.company_profile.pk),
            "social_links-0-platform": "linkedin",
            "social_links-0-url": "https://example.com/acme-prime",
            "social_links-0-DELETE": "",
            "social_links-1-id": "",
            "social_links-1-platform": "website",
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

    def test_company_create_form_uses_selects_with_fixed_channel_choices(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.get(reverse("company_create"))

        self.assertEqual(len(response.context["phone_formset"].forms), 0)
        self.assertEqual(len(response.context["email_formset"].forms), 0)
        self.assertEqual(len(response.context["social_link_formset"].forms), 0)

        self.assertContains(response, 'data-single-autocomplete', html=False)
        self.assertContains(response, reverse("company_industry_search"))
        self.assertContains(response, 'placeholder="55 or 50-100"', html=False)

        form = response.context["form"]
        self.assertEqual(form.fields["company_size"].widget.attrs["inputmode"], "numeric")

        phone_field = response.context["phone_formset"].empty_form.fields["label"]
        email_field = response.context["email_formset"].empty_form.fields["label"]
        profile_field = response.context["social_link_formset"].empty_form.fields["platform"]
        phone_number_field = response.context["phone_formset"].empty_form.fields["phone"]

        self.assertEqual(phone_field.widget.__class__.__name__, "Select")
        self.assertEqual(email_field.widget.__class__.__name__, "Select")
        self.assertEqual(profile_field.widget.__class__.__name__, "Select")
        self.assertEqual(phone_number_field.widget.input_type, "tel")
        self.assertEqual(list(phone_field.choices), [BLANK_CHOICE, *COMPANY_PHONE_LABEL_CHOICES])
        self.assertEqual(list(email_field.choices), [BLANK_CHOICE, *COMPANY_EMAIL_LABEL_CHOICES])
        self.assertEqual(list(profile_field.choices), [BLANK_CHOICE, *COMPANY_PROFILE_PLATFORM_CHOICES])

    def test_company_industry_search_returns_distinct_matching_values(self):
        self.client.force_login(self.team_lead_user)
        Company.objects.create(name="Northwind", industry="Software")
        Company.objects.create(name="Southwind", industry="Software")
        Company.objects.create(name="Orbit", industry="Aerospace")
        Company.objects.create(name="Blank Industry", industry="")

        response = self.client.get(reverse("company_industry_search"), {"q": "soft"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["results"], [{"value": "Software", "label": "Software"}])

    def test_company_edit_form_preserves_legacy_channel_values(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.get(reverse("company_edit", args=[self.company.pk]))

        existing_phone_choices = list(response.context["phone_formset"].forms[0].fields["label"].choices)
        new_phone_choices = list(response.context["phone_formset"].empty_form.fields["label"].choices)
        existing_email_choices = list(response.context["email_formset"].forms[0].fields["label"].choices)
        new_email_choices = list(response.context["email_formset"].empty_form.fields["label"].choices)
        existing_profile_choices = list(response.context["social_link_formset"].forms[0].fields["platform"].choices)
        new_profile_choices = list(response.context["social_link_formset"].empty_form.fields["platform"].choices)

        self.assertIn(("Office", "Office"), existing_phone_choices)
        self.assertNotIn(("Office", "Office"), new_phone_choices)
        self.assertIn(("Sales", "Sales"), existing_email_choices)
        self.assertNotIn(("Sales", "Sales"), new_email_choices)
        self.assertIn(("LinkedIn", "LinkedIn"), existing_profile_choices)
        self.assertNotIn(("LinkedIn", "LinkedIn"), new_profile_choices)

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
        self.assertEqual(created_company.phones.get().label, "office")
        self.assertEqual(created_company.emails.get().email, "ops@blueorbit.com")
        self.assertEqual(created_company.emails.get().label, "support")
        self.assertEqual(created_company.social_links.get().url, "https://blueorbit.example.com")
        self.assertEqual(created_company.social_links.get().platform, "website")

    def test_company_create_accepts_new_industry_text_without_match(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                industry="Space Mining",
                **self._management_form("phones", 0, 0),
                **self._management_form("emails", 0, 0),
                **self._management_form("social_links", 0, 0),
            ),
            follow=True,
        )

        created_company = Company.objects.get(name="Blue Orbit Labs")
        self.assertRedirects(response, reverse("company_detail", args=[created_company.pk]))
        self.assertEqual(created_company.industry, "Space Mining")

    def test_company_create_normalizes_company_size_ranges(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                company_size="50 - 100",
                **self._management_form("phones", 0, 0),
                **self._management_form("emails", 0, 0),
                **self._management_form("social_links", 0, 0),
            ),
            follow=True,
        )

        created_company = Company.objects.get(name="Blue Orbit Labs")
        self.assertRedirects(response, reverse("company_detail", args=[created_company.pk]))
        self.assertEqual(created_company.company_size, "50-100")

    def test_company_create_rejects_invalid_company_size_format(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                company_size="10 to 20",
                **self._management_form("phones", 0, 0),
                **self._management_form("emails", 0, 0),
                **self._management_form("social_links", 0, 0),
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(Company.objects.filter(name="Blue Orbit Labs").count(), 0)
        self.assertContains(response, "Enter a whole number or range like 55 or 50-100.")
        self.assertContains(response, 'value="10 to 20"', html=False)

    def test_company_create_can_save_without_related_email_rows(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                **self._management_form("emails", 0, 0),
                **self._management_form("phones", 0, 0),
                **self._management_form("social_links", 0, 0),
            ),
            follow=True,
        )

        created_company = Company.objects.get(name="Blue Orbit Labs")
        self.assertRedirects(response, reverse("company_detail", args=[created_company.pk]))
        self.assertEqual(created_company.emails.count(), 0)
        self.assertEqual(created_company.phones.count(), 0)
        self.assertEqual(created_company.social_links.count(), 0)

    def test_company_create_ignores_completely_blank_related_rows(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                **{
                    "phones-0-label": "",
                    "phones-0-phone": "",
                    "emails-0-label": "",
                    "emails-0-email": "",
                    "social_links-0-platform": "",
                    "social_links-0-url": "",
                }
            ),
            follow=True,
        )

        created_company = Company.objects.get(name="Blue Orbit Labs")
        self.assertRedirects(response, reverse("company_detail", args=[created_company.pk]))
        self.assertEqual(created_company.phones.count(), 0)
        self.assertEqual(created_company.emails.count(), 0)
        self.assertEqual(created_company.social_links.count(), 0)

    def test_company_create_rejects_partially_filled_email_row(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("company_create"),
            self._company_create_payload(
                **self._management_form("phones", 0, 0),
                **self._management_form("social_links", 0, 0),
                **{
                    "emails-0-label": "support",
                    "emails-0-email": "",
                }
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(Company.objects.filter(name="Blue Orbit Labs").count(), 0)
        self.assertContains(response, "This field is required.")
        self.assertContains(response, 'name="emails-0-label"', html=False)

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
        self.assertEqual(set(self.company.phones.values_list("label", flat=True)), {"office", "support"})
        self.assertEqual(
            set(self.company.emails.values_list("email", flat=True)),
            {"revenue@acme.com", "support@acme.com"},
        )
        self.assertEqual(set(self.company.emails.values_list("label", flat=True)), {"sales", "support"})
        self.assertEqual(
            set(self.company.social_links.values_list("url", flat=True)),
            {"https://example.com/acme-prime", "https://acme-prime.example.com"},
        )
        self.assertEqual(
            set(self.company.social_links.values_list("platform", flat=True)),
            {"linkedin", "website"},
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
