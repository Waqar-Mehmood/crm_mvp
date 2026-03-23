from urllib.parse import parse_qs, urlsplit

from crm.channel_choices import (
    BLANK_CHOICE,
    CONTACT_EMAIL_LABEL_CHOICES,
    CONTACT_PHONE_LABEL_CHOICES,
    CONTACT_PROFILE_PLATFORM_CHOICES,
)

from . import (
    AdvancedFilterTestMixin,
    CRMRoleTestMixin,
    Client,
    Company,
    Contact,
    ContactEmail,
    ContactPhone,
    ContactSocialLink,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    TestCase,
    reverse,
    timedelta,
    timezone,
)


class AdvancedFilterTests(AdvancedFilterTestMixin, TestCase):
    def test_contact_list_defaults_to_new_columns_and_blank_cells(self):
        response = self.client.get(reverse("contact_list"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.context["visible_columns"],
            ["row", "contact", "title", "email", "phone", "companies", "profiles"],
        )
        self.assertContains(response, "<th>#</th>", html=True)
        self.assertNotContains(response, "<th>ID</th>", html=True)
        self.assertNotContains(response, "<th>Created</th>", html=True)
        self.assertNotContains(response, "No email")
        self.assertNotContains(response, "No phone")
        self.assertNotContains(response, "No company links")
        self.assertNotContains(response, "No profiles")

    def test_contact_filters_match_related_data_and_presence_rules(self):
        response = self.client.get(
            reverse("contact_list"),
            {
                "q": "alice@acme.com",
                "company": "Acme",
                "has_profile": "yes",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({contact.full_name for contact in response.context["contacts"]}, {"Alice Johnson"})
        self.assertEqual(response.context["filters"]["company"], "Acme")
        self.assertEqual(response.context["filters"]["has_profile"], "yes")

    def test_contact_created_and_presence_filters_work(self):
        response = self.client.get(
            reverse("contact_list"),
            {
                "has_company": "no",
                "has_email": "no",
                "has_phone": "no",
                "created_to": str((timezone.now() - timedelta(days=3)).date()),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({contact.full_name for contact in response.context["contacts"]}, {"Bob Stone"})

    def test_contact_pagination_preserves_filters(self):
        response = self.client.get(reverse("contact_list"), {"title": "Analyst", "page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertEqual(response.context["page_query"], "title=Analyst")
        self.assertContains(response, "?page=1&title=Analyst")

    def test_contact_filtered_empty_state_appears_when_no_results_match(self):
        response = self.client.get(reverse("contact_list"), {"title": "Astronaut"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No contacts matched the current filters.")
        self.assertNotContains(response, "No contacts are available yet.")

    def test_contact_filter_panel_is_collapsed_by_default_and_opens_with_filters(self):
        default_response = self.client.get(reverse("contact_list"))
        filtered_response = self.client.get(reverse("contact_list"), {"title": "Analyst"})

        self.assertContains(
            default_response,
            '<details class="form-card filter-card contact-filter-card filter-disclosure" data-animated-disclosure>',
            html=False,
        )
        self.assertContains(
            filtered_response,
            '<details class="form-card filter-card contact-filter-card filter-disclosure" data-animated-disclosure open>',
            html=False,
        )
        self.assertContains(default_response, "Show filters")
        self.assertContains(filtered_response, "Hide filters")
        self.assertNotContains(default_response, ">Clear filters<", html=False)
        self.assertContains(default_response, ">Reset<", html=False)
        self.assertContains(default_response, 'class="form-layout list-filter-form"', html=False)
        self.assertContains(default_response, 'class="list-filter-control"', html=False)
        self.assertContains(default_response, 'class="filter-actions list-filter-actions"', html=False)
        self.assertContains(default_response, 'class="list-filter-submit"', html=False)
        self.assertContains(default_response, 'class="button-link is-secondary list-filter-reset"', html=False)

    def test_contact_column_picker_uses_requested_columns_only(self):
        response = self.client.get(
            reverse("contact_list"),
            {"columns": "row,contact,companies"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["visible_columns"], ["row", "contact", "companies"])
        self.assertContains(response, "<th>Companies</th>", html=True)
        self.assertNotContains(response, "<th>Title</th>", html=True)
        self.assertNotContains(response, "<th>Email</th>", html=True)

    def test_contact_row_numbers_continue_across_pages_and_per_page_changes(self):
        paged_response = self.client.get(
            reverse("contact_list"),
            {"page": 2, "title": "Analyst", "columns": "row,contact"},
        )
        expanded_response = self.client.get(reverse("contact_list"), {"per_page": 50})
        per_page_menu = {
            item["value"]: item for item in paged_response.context["per_page_menu_options"]
        }

        self.assertEqual(paged_response.context["page_obj"].start_index(), 11)
        self.assertContains(paged_response, "<td>11</td>", html=True)
        self.assertContains(paged_response, "Rows: 10")
        self.assertContains(paged_response, 'class="button-link is-secondary rows-menu-toggle toolbar-menu-toggle"', html=False)
        self.assertContains(paged_response, 'class="button-link is-secondary table-menu-toggle toolbar-menu-toggle"', html=False)
        self.assertContains(
            paged_response,
            'class="button-link is-secondary export-menu-toggle toolbar-menu-toggle toolbar-menu-toggle-accent"',
            html=False,
        )
        self.assertTrue(per_page_menu[10]["is_active"])
        self.assertIn("title=Analyst", per_page_menu[50]["url"])
        self.assertIn("columns=row%2Ccontact", per_page_menu[50]["url"])
        self.assertEqual(
            parse_qs(urlsplit(per_page_menu[50]["url"]).query).get("per_page"),
            ["50"],
        )
        self.assertNotIn("page", parse_qs(urlsplit(per_page_menu[50]["url"]).query))
        self.assertEqual(expanded_response.context["page_obj"].paginator.per_page, 50)
        self.assertContains(expanded_response, "Rows: 50")
        self.assertEqual(len(expanded_response.context["contacts"]), 15)

    def test_contact_csv_export_uses_filtered_rows_and_joined_columns(self):
        response = self.client.get(
            reverse("contact_list"),
            {
                "q": "alice@acme.com",
                "company": "Acme",
                "has_profile": "yes",
                "export": "csv",
            },
        )
        headers, rows = self.parse_csv_response(response)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv; charset=utf-8")
        self.assertIn('attachment; filename="contacts-export-', response["Content-Disposition"])
        self.assertEqual(
            headers,
            [
                "ID",
                "Full Name",
                "Title",
                "Notes",
                "Primary Email",
                "Primary Phone",
                "Emails",
                "Phones",
                "Companies",
                "Profiles",
                "Created At",
            ],
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1], "Alice Johnson")
        self.assertIn("Work: alice@acme.com", rows[0][6])
        self.assertIn("Work: 555-0101", rows[0][7])
        self.assertEqual(rows[0][8], "Acme Labs")
        self.assertIn("Linkedin: https://example.com/alice", rows[0][9])

    def test_contact_export_ignores_pagination_and_xlsx_returns_filtered_rows(self):
        csv_response = self.client.get(
            reverse("contact_list"),
            {"title": "Analyst", "page": 2, "export": "csv"},
        )
        _headers, csv_rows = self.parse_csv_response(csv_response)
        self.assertEqual(csv_response.status_code, 200)
        self.assertEqual(len(csv_rows), 12)

        xlsx_response = self.client.get(
            reverse("contact_list"),
            {
                "has_company": "no",
                "has_email": "no",
                "created_to": str((timezone.now() - timedelta(days=3)).date()),
                "export": "xlsx",
            },
        )
        headers, xlsx_rows = self.parse_xlsx_response(xlsx_response)
        self.assertEqual(headers[1], "Full Name")
        self.assertEqual(len(xlsx_rows), 1)
        self.assertEqual(xlsx_rows[0][1], "Bob Stone")


class ContactCrudTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)

        self.primary_company = Company.objects.create(
            name="Acme Labs",
            industry="Software",
            city="San Francisco",
            state="CA",
            country="US",
        )
        self.secondary_company = Company.objects.create(
            name="Blue Orbit",
            industry="Aerospace",
            city="Houston",
            state="TX",
            country="US",
        )
        self.contact = Contact.objects.create(
            full_name="Jane Example",
            title="Operations Lead",
            email="jane@example.com",
            phone="555-0100",
            notes="Main buying contact",
        )
        self.contact.companies.add(self.primary_company)
        self.contact_phone = ContactPhone.objects.create(
            contact=self.contact,
            label="Work",
            phone="111-111",
        )
        self.contact_email = ContactEmail.objects.create(
            contact=self.contact,
            label="Work",
            email="work@example.com",
        )
        self.contact_profile = ContactSocialLink.objects.create(
            contact=self.contact,
            platform="LinkedIn",
            url="https://example.com/jane",
        )

    def _management_form(self, prefix, total_forms, initial_forms):
        return {
            f"{prefix}-TOTAL_FORMS": str(total_forms),
            f"{prefix}-INITIAL_FORMS": str(initial_forms),
            f"{prefix}-MIN_NUM_FORMS": "0",
            f"{prefix}-MAX_NUM_FORMS": "1000",
        }

    def _contact_create_payload(self, **overrides):
        payload = {
            "full_name": "Chris Vector",
            "email": "chris@example.com",
            "phone": "555-2222",
            "title": "Revenue Operations",
            "notes": "New stakeholder",
            "companies": [str(self.primary_company.pk), str(self.secondary_company.pk)],
            **self._management_form("phones", 1, 0),
            "phones-0-label": "mobile",
            "phones-0-phone": "555-3000",
            **self._management_form("emails", 1, 0),
            "emails-0-label": "work",
            "emails-0-email": "chris.vector@example.com",
            **self._management_form("social_links", 1, 0),
            "social_links-0-platform": "linkedin",
            "social_links-0-url": "https://example.com/chris",
        }
        payload.update(overrides)
        return payload

    def _contact_edit_payload(self, **overrides):
        payload = {
            "full_name": "Jane Example-Smith",
            "email": "jane.smith@example.com",
            "phone": "555-0200",
            "title": "VP Operations",
            "notes": "Expanded executive contact",
            "companies": [str(self.secondary_company.pk)],
            **self._management_form("phones", 2, 1),
            "phones-0-id": str(self.contact_phone.pk),
            "phones-0-label": "work",
            "phones-0-phone": "222-222",
            "phones-0-DELETE": "",
            "phones-1-id": "",
            "phones-1-label": "mobile",
            "phones-1-phone": "333-333",
            "phones-1-DELETE": "",
            **self._management_form("emails", 2, 1),
            "emails-0-id": str(self.contact_email.pk),
            "emails-0-label": "work",
            "emails-0-email": "ops@example.com",
            "emails-0-DELETE": "",
            "emails-1-id": "",
            "emails-1-label": "personal",
            "emails-1-email": "jane.personal@example.com",
            "emails-1-DELETE": "",
            **self._management_form("social_links", 2, 1),
            "social_links-0-id": str(self.contact_profile.pk),
            "social_links-0-platform": "linkedin",
            "social_links-0-url": "https://example.com/jane-smith",
            "social_links-0-DELETE": "",
            "social_links-1-id": "",
            "social_links-1-platform": "website",
            "social_links-1-url": "https://jane.example.com",
            "social_links-1-DELETE": "",
        }
        payload.update(overrides)
        return payload

    def test_contact_detail_renders_profile_channels_and_linked_companies_for_staff(self):
        self.client.force_login(self.staff_user)

        response = self.client.get(reverse("contact_detail", args=[self.contact.pk]))

        self.assertEqual(response.status_code, 200)
        self.assertTemplateUsed(response, "crm/contacts/contact_detail.html")
        self.assertContains(response, "Contact snapshot")
        self.assertContains(response, "Acme Labs")
        self.assertContains(response, "111-111")
        self.assertContains(response, "work@example.com")
        self.assertContains(response, "https://example.com/jane")

    def test_contact_create_form_uses_selects_with_fixed_channel_choices(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.get(reverse("contact_create"))

        self.assertEqual(len(response.context["phone_formset"].forms), 0)
        self.assertEqual(len(response.context["email_formset"].forms), 0)
        self.assertEqual(len(response.context["social_link_formset"].forms), 0)

        form = response.context["form"]
        phone_field = response.context["phone_formset"].empty_form.fields["label"]
        email_field = response.context["email_formset"].empty_form.fields["label"]
        profile_field = response.context["social_link_formset"].empty_form.fields["platform"]
        phone_number_field = response.context["phone_formset"].empty_form.fields["phone"]

        self.assertEqual(form.fields["phone"].widget.input_type, "tel")
        self.assertEqual(phone_field.widget.__class__.__name__, "Select")
        self.assertEqual(email_field.widget.__class__.__name__, "Select")
        self.assertEqual(profile_field.widget.__class__.__name__, "Select")
        self.assertEqual(phone_number_field.widget.input_type, "tel")
        self.assertEqual(list(phone_field.choices), [BLANK_CHOICE, *CONTACT_PHONE_LABEL_CHOICES])
        self.assertEqual(list(email_field.choices), [BLANK_CHOICE, *CONTACT_EMAIL_LABEL_CHOICES])
        self.assertEqual(list(profile_field.choices), [BLANK_CHOICE, *CONTACT_PROFILE_PLATFORM_CHOICES])

    def test_contact_edit_form_preserves_legacy_channel_values(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.get(reverse("contact_edit", args=[self.contact.pk]))

        existing_phone_choices = list(response.context["phone_formset"].forms[0].fields["label"].choices)
        new_phone_choices = list(response.context["phone_formset"].empty_form.fields["label"].choices)
        existing_email_choices = list(response.context["email_formset"].forms[0].fields["label"].choices)
        new_email_choices = list(response.context["email_formset"].empty_form.fields["label"].choices)
        existing_profile_choices = list(response.context["social_link_formset"].forms[0].fields["platform"].choices)
        new_profile_choices = list(response.context["social_link_formset"].empty_form.fields["platform"].choices)

        self.assertIn(("Work", "Work"), existing_phone_choices)
        self.assertNotIn(("Work", "Work"), new_phone_choices)
        self.assertIn(("Work", "Work"), existing_email_choices)
        self.assertNotIn(("Work", "Work"), new_email_choices)
        self.assertIn(("LinkedIn", "LinkedIn"), existing_profile_choices)
        self.assertNotIn(("LinkedIn", "LinkedIn"), new_profile_choices)

    def test_contact_create_succeeds_for_team_lead_and_redirects_with_message(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("contact_create"),
            self._contact_create_payload(),
            follow=True,
        )

        created_contact = Contact.objects.get(full_name="Chris Vector")
        self.assertRedirects(response, reverse("contact_detail", args=[created_contact.pk]))
        self.assertContains(response, "Contact created.")
        self.assertEqual(created_contact.companies.count(), 2)
        self.assertEqual(created_contact.phones.get().phone, "555-3000")
        self.assertEqual(created_contact.phones.get().label, "mobile")
        self.assertEqual(created_contact.emails.get().email, "chris.vector@example.com")
        self.assertEqual(created_contact.emails.get().label, "work")
        self.assertEqual(created_contact.social_links.get().url, "https://example.com/chris")
        self.assertEqual(created_contact.social_links.get().platform, "linkedin")

    def test_contact_create_can_save_without_related_channel_rows(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("contact_create"),
            self._contact_create_payload(
                **self._management_form("phones", 0, 0),
                **self._management_form("emails", 0, 0),
                **self._management_form("social_links", 0, 0),
            ),
            follow=True,
        )

        created_contact = Contact.objects.get(full_name="Chris Vector")
        self.assertRedirects(response, reverse("contact_detail", args=[created_contact.pk]))
        self.assertEqual(created_contact.phones.count(), 0)
        self.assertEqual(created_contact.emails.count(), 0)
        self.assertEqual(created_contact.social_links.count(), 0)

    def test_contact_create_ignores_completely_blank_related_rows(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("contact_create"),
            self._contact_create_payload(
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

        created_contact = Contact.objects.get(full_name="Chris Vector")
        self.assertRedirects(response, reverse("contact_detail", args=[created_contact.pk]))
        self.assertEqual(created_contact.phones.count(), 0)
        self.assertEqual(created_contact.emails.count(), 0)
        self.assertEqual(created_contact.social_links.count(), 0)

    def test_contact_create_rejects_partially_filled_email_row(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("contact_create"),
            self._contact_create_payload(
                **self._management_form("phones", 0, 0),
                **self._management_form("social_links", 0, 0),
                **{
                    "emails-0-label": "work",
                    "emails-0-email": "",
                }
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(Contact.objects.filter(full_name="Chris Vector").count(), 0)
        self.assertContains(response, "This field is required.")
        self.assertContains(response, 'name="emails-0-label"', html=False)

    def test_contact_edit_updates_core_fields_related_data_and_links(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("contact_edit", args=[self.contact.pk]),
            self._contact_edit_payload(),
            follow=True,
        )

        self.contact.refresh_from_db()
        self.assertRedirects(response, reverse("contact_detail", args=[self.contact.pk]))
        self.assertContains(response, "Contact updated.")
        self.assertEqual(self.contact.full_name, "Jane Example-Smith")
        self.assertEqual(self.contact.email, "jane.smith@example.com")
        self.assertEqual(
            list(self.contact.companies.values_list("name", flat=True)),
            ["Blue Orbit"],
        )
        self.assertEqual(set(self.contact.phones.values_list("phone", flat=True)), {"222-222", "333-333"})
        self.assertEqual(set(self.contact.phones.values_list("label", flat=True)), {"work", "mobile"})
        self.assertEqual(
            set(self.contact.emails.values_list("email", flat=True)),
            {"ops@example.com", "jane.personal@example.com"},
        )
        self.assertEqual(set(self.contact.emails.values_list("label", flat=True)), {"work", "personal"})
        self.assertEqual(
            set(self.contact.social_links.values_list("url", flat=True)),
            {"https://example.com/jane-smith", "https://jane.example.com"},
        )
        self.assertEqual(
            set(self.contact.social_links.values_list("platform", flat=True)),
            {"linkedin", "website"},
        )

        linked_company_response = self.client.get(reverse("company_detail", args=[self.secondary_company.pk]))
        self.assertContains(linked_company_response, "Jane Example-Smith")

    def test_contact_edit_can_delete_existing_inline_child_rows(self):
        self.client.force_login(self.team_lead_user)

        payload = self._contact_edit_payload(
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
            reverse("contact_edit", args=[self.contact.pk]),
            payload,
            follow=True,
        )

        self.assertRedirects(response, reverse("contact_detail", args=[self.contact.pk]))
        self.assertFalse(ContactPhone.objects.filter(pk=self.contact_phone.pk).exists())

    def test_contact_invalid_create_preserves_submitted_values_and_writes_nothing(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("contact_create"),
            self._contact_create_payload(
                full_name="",
                title="Finance Lead",
                **{"phones-0-phone": "555-4444"},
            ),
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(Contact.objects.filter(full_name="Chris Vector").count(), 0)
        self.assertContains(response, "This field is required.")
        self.assertContains(response, 'value="Finance Lead"', html=False)
        self.assertContains(response, 'value="555-4444"', html=False)

    def test_staff_cannot_access_contact_create_or_edit(self):
        self.client.force_login(self.staff_user)

        create_response = self.client.get(reverse("contact_create"))
        edit_response = self.client.get(reverse("contact_edit", args=[self.contact.pk]))

        self.assertEqual(create_response.status_code, 403)
        self.assertEqual(edit_response.status_code, 403)
