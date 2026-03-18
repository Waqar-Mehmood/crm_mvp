from . import AdvancedFilterTestMixin, TestCase, reverse, timedelta, timezone


class AdvancedFilterTests(AdvancedFilterTestMixin, TestCase):
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
