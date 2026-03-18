from . import AdvancedFilterTestMixin, TestCase, reverse, timedelta, timezone


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
