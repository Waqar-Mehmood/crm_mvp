from datetime import timedelta
from urllib.parse import parse_qs, urlsplit

from django.core.management import call_command
from django.utils import timezone

from crm.services.import_workflow import suggest_mapping
from crm.views.import_views import _default_import_display_name

from types import SimpleNamespace

from . import (
    BytesIO,
    CRMRoleTestMixin,
    Client,
    Contact,
    ImportFile,
    ImportRow,
    Mock,
    Path,
    ROLE_OWNER,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    TestCase,
    Workbook,
    build_csv_export_url,
    build_import_result_summary,
    django_apps,
    extract_gid,
    extract_sheet_id,
    fetch_google_sheet_rows,
    get_row_headers,
    importlib,
    json,
    make_csv_file,
    make_json_file,
    make_xlsx_file,
    override_settings,
    parse_csv_file,
    parse_google_sheet,
    parse_json_file,
    parse_rows_from_source,
    parse_xlsx_file,
    patch,
    load_workbook,
    requests,
    reverse,
    rows_to_temporary_csv,
    rows_to_uploaded_csv,
    select_import_parser,
    shutil,
    tempfile,
)


class GoogleSheetsServiceTests(TestCase):
    def test_extract_helpers_build_expected_export_url(self):
        sheet_url = (
            "https://docs.google.com/spreadsheets/d/"
            "1ngu9sB-ZtIFoA3BqBnd2AwBroIZL9_c_8ZdREHTe8NM/edit?gid=0#gid=0"
        )

        self.assertEqual(
            extract_sheet_id(sheet_url),
            "1ngu9sB-ZtIFoA3BqBnd2AwBroIZL9_c_8ZdREHTe8NM",
        )
        self.assertEqual(extract_gid(sheet_url), "0")
        self.assertEqual(
            build_csv_export_url(sheet_url),
            "https://docs.google.com/spreadsheets/d/"
            "1ngu9sB-ZtIFoA3BqBnd2AwBroIZL9_c_8ZdREHTe8NM/export?format=csv&gid=0",
        )

    def test_extract_gid_defaults_to_zero_when_missing(self):
        sheet_url = "https://docs.google.com/spreadsheets/d/test-sheet-id/edit"

        self.assertEqual(extract_gid(sheet_url), "0")

    @patch("crm.services.google_sheets.requests.get")
    def test_fetch_google_sheet_rows_parses_csv_into_dicts(self, mock_get):
        mock_response = Mock()
        mock_response.text = "Name,Email\nAlice,alice@example.com\nBob,bob@example.com\n"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        rows = fetch_google_sheet_rows(
            "https://docs.google.com/spreadsheets/d/test-sheet-id/edit?gid=0#gid=0"
        )

        self.assertEqual(
            rows,
            [
                {"Name": "Alice", "Email": "alice@example.com"},
                {"Name": "Bob", "Email": "bob@example.com"},
            ],
        )
        mock_get.assert_called_once()

    @patch("crm.services.google_sheets.requests.get")
    def test_fetch_google_sheet_rows_raises_runtime_error_on_request_failure(self, mock_get):
        mock_get.side_effect = requests.RequestException("network down")

        with self.assertRaises(RuntimeError):
            fetch_google_sheet_rows(
                "https://docs.google.com/spreadsheets/d/test-sheet-id/edit?gid=0#gid=0"
            )

    def test_fetch_google_sheet_rows_raises_value_error_for_invalid_url(self):
        with self.assertRaises(ValueError):
            fetch_google_sheet_rows("https://example.com/not-a-sheet")


class ImportServiceTests(TestCase):
    def test_default_import_display_name_removes_noise_and_extension(self):
        self.assertEqual(
            _default_import_display_name(
                "1.Template-Pharmaceutical Society of Japan-20260309_143924.xlsx"
            ),
            "Pharmaceutical Society of Japan",
        )
        self.assertEqual(
            _default_import_display_name("client_roster_export.csv"),
            "client roster export",
        )
        self.assertEqual(
            _default_import_display_name("2-Worksheet_Market Map.json"),
            "Market Map",
        )

    def test_get_row_headers_preserves_first_seen_order(self):
        rows = [
            {"Company Name": "Acme", "Email": "hello@acme.com"},
            {"Company Name": "Beta", "Phone": "555-0101"},
        ]

        self.assertEqual(get_row_headers(rows), ["Company Name", "Email", "Phone"])

    def test_rows_to_temporary_csv_writes_csv_file(self):
        rows = [
            {"Company Name": "Acme", "Email": "hello@acme.com"},
            {"Company Name": "Beta", "Phone": "555-0101"},
        ]

        temp_path = Path(rows_to_temporary_csv(rows))
        self.addCleanup(temp_path.unlink, missing_ok=True)

        self.assertTrue(temp_path.exists())
        self.assertEqual(
            temp_path.read_text(encoding="utf-8"),
            "Company Name,Email,Phone\nAcme,hello@acme.com,\nBeta,,555-0101\n",
        )

    def test_rows_to_uploaded_csv_returns_simple_uploaded_file(self):
        rows = [{"Company Name": "Acme", "Email": "hello@acme.com"}]

        uploaded = rows_to_uploaded_csv(rows)

        self.assertEqual(uploaded.name, "google_sheet_import.csv")
        self.assertEqual(uploaded.content_type, "text/csv")
        self.assertEqual(
            uploaded.read().decode("utf-8"),
            "Company Name,Email\r\nAcme,hello@acme.com\r\n",
        )

    def test_rows_to_uploaded_csv_rejects_empty_rows(self):
        with self.assertRaises(ValueError):
            rows_to_uploaded_csv([])

    def test_select_import_parser_supports_explicit_source_types(self):
        self.assertIs(select_import_parser(source_type="csv"), parse_csv_file)
        self.assertIs(select_import_parser(source_type="xlsx"), parse_xlsx_file)
        self.assertIs(select_import_parser(source_type="json"), parse_json_file)
        self.assertIs(select_import_parser(source_type="google_sheets"), parse_google_sheet)

    def test_select_import_parser_can_infer_from_filename(self):
        self.assertIs(select_import_parser(filename="contacts.csv"), parse_csv_file)
        self.assertIs(select_import_parser(filename="contacts.xlsx"), parse_xlsx_file)
        self.assertIs(select_import_parser(filename="contacts.json"), parse_json_file)

    def test_select_import_parser_rejects_unsupported_source_type(self):
        with self.assertRaises(ValueError):
            select_import_parser(source_type="xml")

    def test_parse_rows_from_source_uses_selected_parser(self):
        temp_path = Path(tempfile.mkstemp(suffix=".csv")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        temp_path.write_text("Name,Email\nAlice,alice@example.com\n", encoding="utf-8")

        rows = parse_rows_from_source(temp_path, filename="contacts.csv")

        self.assertEqual(rows, [{"Name": "Alice", "Email": "alice@example.com"}])

    def test_build_import_result_summary_normalizes_counts_and_errors(self):
        summary = build_import_result_summary(
            {
                "rows_processed": 5,
                "created_companies": 2,
                "created_contacts": 1,
                "skipped_rows": 1,
                "skipped_empty_rows": 1,
                "skipped_duplicate_rows": 0,
                "failed_rows": [
                    {"row_number": 3, "reason": "Row was empty after mapping."},
                    {"row_number": 5, "reason": "Duplicate mapped row in this import."},
                ],
            }
        )

        self.assertEqual(summary["rows_processed"], 5)
        self.assertEqual(summary["companies_created"], 2)
        self.assertEqual(summary["contacts_created"], 1)
        self.assertEqual(summary["rows_skipped"], 2)
        self.assertEqual(summary["failed_rows_count"], 2)
        self.assertEqual(
            summary["error_messages"],
            [
                "Row 3: Row was empty after mapping.",
                "Row 5: Duplicate mapped row in this import.",
            ],
        )


class ImportMappingSuggestionTests(TestCase):
    def test_suggest_mapping_matches_reported_header_variants(self):
        mapping = suggest_mapping(["Organisation", "FirstName", "LastName", "Email"])

        self.assertEqual(mapping["company_name"], "Organisation")
        self.assertEqual(mapping["contact_first_name"], "FirstName")
        self.assertEqual(mapping["contact_last_name"], "LastName")
        self.assertEqual(mapping["email"], "Email")

    def test_suggest_mapping_matches_normalized_profile_and_region_variants(self):
        mapping = suggest_mapping(["LinkedIn Profile", "State/Province", "Postcode"])

        self.assertEqual(mapping["person_source"], "LinkedIn Profile")
        self.assertEqual(mapping["state"], "State/Province")
        self.assertEqual(mapping["zip_code"], "Postcode")
        self.assertEqual(suggest_mapping(["Linkedin"])["person_source"], "Linkedin")

    def test_suggest_mapping_matches_business_aliases_from_real_import_patterns(self):
        mapping = suggest_mapping(
            [
                "JobRole",
                "Employee Size",
                "Verified email address",
                "Owner/CEO Name",
            ]
        )

        self.assertEqual(mapping["contact_title"], "JobRole")
        self.assertEqual(mapping["company_size"], "Employee Size")
        self.assertEqual(mapping["email"], "Verified email address")
        self.assertEqual(mapping["contact_name"], "Owner/CEO Name")
        self.assertEqual(suggest_mapping(["Job Title"])["contact_title"], "Job Title")


class ImportParserTests(TestCase):
    def test_parse_csv_file_returns_row_dicts(self):
        temp_path = Path(tempfile.mkstemp(suffix=".csv")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        temp_path.write_text(
            "Company Name,Email\nAcme,hello@acme.com\nBeta,team@beta.com\n",
            encoding="utf-8",
        )

        rows = parse_csv_file(temp_path)

        self.assertEqual(
            rows,
            [
                {"Company Name": "Acme", "Email": "hello@acme.com"},
                {"Company Name": "Beta", "Email": "team@beta.com"},
            ],
        )

    def test_parse_json_file_accepts_top_level_rows_list(self):
        temp_path = Path(tempfile.mkstemp(suffix=".json")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        temp_path.write_text(
            json.dumps(
                [
                    {"Company Name": "Acme", "Email": "hello@acme.com"},
                    {"Company Name": "Beta", "Email": "team@beta.com"},
                ]
            ),
            encoding="utf-8",
        )

        rows = parse_json_file(temp_path)

        self.assertEqual(
            rows,
            [
                {"Company Name": "Acme", "Email": "hello@acme.com"},
                {"Company Name": "Beta", "Email": "team@beta.com"},
            ],
        )

    def test_parse_json_file_accepts_object_with_rows_key(self):
        temp_path = Path(tempfile.mkstemp(suffix=".json")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        temp_path.write_text(
            json.dumps({"rows": [{"Company Name": "Acme", "Email": "hello@acme.com"}]}),
            encoding="utf-8",
        )

        rows = parse_json_file(temp_path)

        self.assertEqual(rows, [{"Company Name": "Acme", "Email": "hello@acme.com"}])

    def test_parse_json_file_rejects_unsupported_payload_shape(self):
        temp_path = Path(tempfile.mkstemp(suffix=".json")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        temp_path.write_text(json.dumps({"invalid": "payload"}), encoding="utf-8")

        with self.assertRaises(ValueError):
            parse_json_file(temp_path)

    def test_parse_xlsx_file_returns_row_dicts(self):
        temp_path = Path(tempfile.mkstemp(suffix=".xlsx")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "Contacts"
        worksheet.append(["Company Name", "Email"])
        worksheet.append(["Acme", "hello@acme.com"])
        worksheet.append(["Beta", "team@beta.com"])
        workbook.save(temp_path)
        workbook.close()

        rows = parse_xlsx_file(temp_path)

        self.assertEqual(
            rows,
            [
                {"Company Name": "Acme", "Email": "hello@acme.com"},
                {"Company Name": "Beta", "Email": "team@beta.com"},
            ],
        )

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_parse_google_sheet_reuses_google_sheet_service(self, mock_fetch):
        mock_fetch.return_value = [{"Company Name": "Acme", "Email": "hello@acme.com"}]

        rows = parse_google_sheet(
            "https://docs.google.com/spreadsheets/d/test-sheet-id/edit?gid=0#gid=0"
        )

        self.assertEqual(rows, [{"Company Name": "Acme", "Email": "hello@acme.com"}])
        mock_fetch.assert_called_once()


class GoogleSheetsImportFlowTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)
        self.client.force_login(self.team_lead_user)
        self.temp_root = Path(tempfile.mkdtemp())
        self.temp_base_dir = self.temp_root / "base"
        self.temp_media_root = self.temp_root / "media"
        self.temp_base_dir.mkdir(parents=True, exist_ok=True)
        self.temp_media_root.mkdir(parents=True, exist_ok=True)
        self.settings_override = override_settings(
            BASE_DIR=self.temp_base_dir,
            MEDIA_ROOT=self.temp_media_root,
            MEDIA_URL="/media/",
        )
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.addCleanup(shutil.rmtree, self.temp_root, ignore_errors=True)

    def test_google_sheets_preview_page_renders(self):
        response = self.client.get(reverse("import_google_sheets"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Google Sheets URL")
        self.assertContains(response, "Preview sheet")

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_google_sheets_preview_post_shows_headers_and_first_rows(self, mock_fetch):
        mock_fetch.return_value = [
            {"Company Name": "Acme", "Email": "hello@acme.com"},
            {"Company Name": "Beta", "Email": "team@beta.com"},
            {"Company Name": "Cedar", "Email": "ops@cedar.com"},
        ]

        response = self.client.post(
            reverse("import_google_sheets"),
            {
                "sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0",
                "action": "preview",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["headers"], ["Company Name", "Email"])
        self.assertEqual(len(response.context["preview_rows"]), 3)
        self.assertEqual(response.context["total_rows"], 3)
        self.assertContains(response, "Continue to mapping")

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_google_sheets_import_action_reuses_mapping_session_flow(self, mock_fetch):
        mock_fetch.return_value = [
            {"Company Name": "Acme", "Email": "hello@acme.com"},
            {"Company Name": "Beta", "Email": "team@beta.com"},
        ]

        response = self.client.post(
            reverse("import_google_sheets"),
            {
                "sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0",
                "action": "import",
            },
        )

        self.assertRedirects(response, reverse("import_map_headers"))
        session = self.client.session
        temp_path = Path(session["import_csv_temp_path"])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        self.assertTrue(temp_path.exists())
        self.assertEqual(session["import_csv_original_name"], "Google Sheet - test-sheet.csv")
        self.assertEqual(session["import_csv_headers"], ["Company Name", "Email"])

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_google_sheets_preview_shows_user_friendly_fetch_error(self, mock_fetch):
        mock_fetch.side_effect = RuntimeError("Failed to fetch CSV data from Google Sheets: boom")

        response = self.client.post(
            reverse("import_google_sheets"),
            {
                "sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0",
                "action": "preview",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Failed to fetch CSV data from Google Sheets: boom")


class ImportFileVisibilityTests(CRMRoleTestMixin, TestCase):
    class FakeImportQuerySet:
        def __init__(self, items):
            self.items = list(items)

        def order_by(self, *fields):
            items = list(self.items)
            for field in reversed(fields):
                reverse = field.startswith("-")
                field_name = field[1:] if reverse else field
                items.sort(key=lambda item: getattr(item, field_name), reverse=reverse)
            return self.__class__(items)

        def filter(self, **kwargs):
            items = list(self.items)
            for key, value in kwargs.items():
                if key == "file_name__icontains":
                    items = [
                        item for item in items
                        if value.lower() in item.file_name.lower()
                    ]
                elif key == "status":
                    items = [item for item in items if item.status == value]
                elif key == "updated_at__date__gte":
                    items = [item for item in items if item.updated_at.date() >= value]
                elif key == "updated_at__date__lte":
                    items = [item for item in items if item.updated_at.date() <= value]
                else:
                    raise AssertionError(f"Unsupported filter in test fake queryset: {key}")
            return self.__class__(items)

        def count(self):
            return len(self.items)

        def __len__(self):
            return len(self.items)

        def __iter__(self):
            return iter(self.items)

        def __getitem__(self, key):
            return self.items[key]

    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.client.force_login(self.staff_user)

    def _build_page(self, object_list):
        return SimpleNamespace(
            object_list=object_list,
            paginator=SimpleNamespace(
                count=len(object_list),
                num_pages=1,
                page_range=[1],
            ),
            start_index=lambda: 1 if object_list else 0,
            end_index=lambda: len(object_list),
            has_previous=False,
            has_next=False,
            number=1,
        )

    def _make_import_file(
        self,
        *,
        import_id,
        file_name,
        status="completed",
        updated_at=None,
        stored_rows=0,
        total_rows=0,
        processed_rows=0,
        source_path="",
    ):
        return SimpleNamespace(
            id=import_id,
            file_name=file_name,
            source_path=source_path,
            status=status,
            get_status_display=dict(ImportFile.Status.choices).get(status, status.title()),
            updated_at=updated_at or timezone.now(),
            stored_rows=stored_rows,
            total_rows=total_rows,
            processed_rows=processed_rows,
        )

    def _render_import_list(self, items, params=None):
        with (
            patch("crm.views.import_views.ImportFile.objects.count", return_value=len(items)),
            patch(
                "crm.views.import_views.ImportFile.objects.annotate",
                return_value=self.FakeImportQuerySet(items),
            ),
        ):
            return self.client.get(reverse("import_file_list"), params or {})

    def test_import_list_hides_stored_source_path(self):
        temp_path = Path(tempfile.mkstemp(suffix=".csv")[1])
        self.addCleanup(temp_path.unlink, missing_ok=True)
        temp_path.write_text("Company Name\nAcme Labs\n", encoding="utf-8")
        import_file = self._make_import_file(
            import_id=99,
            file_name="hidden-path-import.csv",
            source_path=str(temp_path),
            stored_rows=7,
        )
        response = self._render_import_list([import_file])

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Actions")
        self.assertNotContains(response, "<th>ID</th>", html=False)
        self.assertEqual(response.context["table_headers"][0]["label"], "#")
        self.assertEqual(response.context["row_number_offset"], 0)
        self.assertContains(response, "<td>1</td>", html=False)
        self.assertContains(response, "hidden-path-import.csv")
        self.assertContains(response, reverse("import_file_download", args=[import_file.id]))
        self.assertContains(response, reverse("import_file_raw_source", args=[import_file.id]))
        self.assertContains(response, reverse("import_file_detail", args=[import_file.id]))
        self.assertNotContains(response, "Source path")
        self.assertNotContains(response, str(temp_path))

    def test_import_list_row_numbering_continues_across_pages(self):
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=index + 100,
                    file_name=f"completed-{index:02d}.csv",
                    status="completed",
                    updated_at=timezone.now() - timedelta(minutes=index),
                    stored_rows=index + 20,
                )
                for index in range(1, 13)
            ],
            {"page": 2},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertEqual(response.context["row_number_offset"], 10)
        self.assertContains(response, "<td>11</td>", html=False)
        self.assertContains(response, "completed-11.csv")

    def test_import_list_rows_menu_changes_page_size_and_preserves_filters_and_sort(self):
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=index,
                    file_name=f"completed-{index:02d}.csv",
                    status="completed",
                    updated_at=timezone.now() - timedelta(minutes=index),
                    stored_rows=index,
                )
                for index in range(1, 13)
            ],
            {"per_page": 50, "q": "completed", "sort": "file_name", "direction": "asc"},
        )

        per_page_menu = {
            item["value"]: item for item in response.context["per_page_menu_options"]
        }

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["per_page"], 50)
        self.assertEqual(response.context["page_obj"].paginator.per_page, 50)
        self.assertContains(response, "Rows: 50")
        self.assertTrue(per_page_menu[50]["is_active"])
        query = parse_qs(urlsplit(per_page_menu[10]["url"]).query)
        self.assertEqual(query["per_page"], ["10"])
        self.assertEqual(query["q"], ["completed"])
        self.assertEqual(query["sort"], ["file_name"])
        self.assertEqual(query["direction"], ["asc"])
        self.assertNotIn("page", query)

    def test_import_list_defaults_to_updated_desc_sort(self):
        newest = timezone.now()
        items = [
            self._make_import_file(
                import_id=1,
                file_name="older.csv",
                updated_at=newest - timedelta(days=2),
                stored_rows=2,
            ),
            self._make_import_file(
                import_id=2,
                file_name="newest.csv",
                updated_at=newest,
                stored_rows=4,
            ),
            self._make_import_file(
                import_id=3,
                file_name="middle.csv",
                updated_at=newest - timedelta(days=1),
                stored_rows=3,
            ),
        ]

        response = self._render_import_list(items)

        headers = {
            item["label"]: item
            for item in response.context["table_headers"]
            if item["is_sortable"]
        }

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item.file_name for item in response.context["import_files"]],
            ["newest.csv", "middle.csv", "older.csv"],
        )
        self.assertEqual(response.context["sort"], "updated_at")
        self.assertEqual(response.context["direction"], "desc")
        self.assertTrue(headers["Updated"]["is_active"])
        self.assertEqual(headers["Updated"]["direction"], "desc")
        self.assertEqual(parse_qs(urlsplit(headers["Updated"]["url"]).query)["direction"], ["asc"])

    def test_import_list_supports_sorting_each_data_column(self):
        base_time = timezone.now()
        items = [
            self._make_import_file(
                import_id=1,
                file_name="beta.csv",
                status="running",
                updated_at=base_time - timedelta(days=1),
                stored_rows=5,
            ),
            self._make_import_file(
                import_id=2,
                file_name="alpha.csv",
                status="queued",
                updated_at=base_time - timedelta(days=2),
                stored_rows=8,
            ),
            self._make_import_file(
                import_id=3,
                file_name="gamma.csv",
                status="completed",
                updated_at=base_time - timedelta(days=3),
                stored_rows=3,
            ),
        ]

        cases = (
            ({"sort": "file_name", "direction": "asc"}, ["alpha.csv", "beta.csv", "gamma.csv"]),
            ({"sort": "file_name", "direction": "desc"}, ["gamma.csv", "beta.csv", "alpha.csv"]),
            ({"sort": "status", "direction": "asc"}, ["gamma.csv", "alpha.csv", "beta.csv"]),
            ({"sort": "status", "direction": "desc"}, ["beta.csv", "alpha.csv", "gamma.csv"]),
            ({"sort": "stored_rows", "direction": "asc"}, ["gamma.csv", "beta.csv", "alpha.csv"]),
            ({"sort": "stored_rows", "direction": "desc"}, ["alpha.csv", "beta.csv", "gamma.csv"]),
            ({"sort": "updated_at", "direction": "asc"}, ["gamma.csv", "alpha.csv", "beta.csv"]),
            ({"sort": "updated_at", "direction": "desc"}, ["beta.csv", "alpha.csv", "gamma.csv"]),
        )

        for params, expected_order in cases:
            with self.subTest(params=params):
                response = self._render_import_list(items, params)
                self.assertEqual(
                    [item.file_name for item in response.context["import_files"]],
                    expected_order,
                )

    def test_import_list_sort_links_preserve_filters_and_rows_and_drop_page(self):
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=index,
                    file_name=f"queued-{index:02d}.csv",
                    status="queued",
                    updated_at=timezone.now() - timedelta(minutes=index),
                    stored_rows=index,
                )
                for index in range(1, 13)
            ],
            {
                "q": "queued",
                "status": "queued",
                "per_page": 50,
                "page": 2,
                "sort": "updated_at",
                "direction": "desc",
            },
        )

        headers = {
            item["label"]: item
            for item in response.context["table_headers"]
            if item["is_sortable"]
        }

        query = parse_qs(urlsplit(headers["File"]["url"]).query)
        self.assertEqual(query["q"], ["queued"])
        self.assertEqual(query["status"], ["queued"])
        self.assertEqual(query["per_page"], ["50"])
        self.assertEqual(query["sort"], ["file_name"])
        self.assertEqual(query["direction"], ["asc"])
        self.assertNotIn("page", query)

    def test_import_list_invalid_sort_params_fall_back_to_updated_desc(self):
        newest = timezone.now()
        items = [
            self._make_import_file(
                import_id=1,
                file_name="older.csv",
                updated_at=newest - timedelta(days=1),
            ),
            self._make_import_file(
                import_id=2,
                file_name="newer.csv",
                updated_at=newest,
            ),
        ]

        response = self._render_import_list(
            items,
            {"sort": "unknown", "direction": "sideways"},
        )

        self.assertEqual(response.context["sort"], "updated_at")
        self.assertEqual(response.context["direction"], "desc")
        self.assertEqual(
            [item.file_name for item in response.context["import_files"]],
            ["newer.csv", "older.csv"],
        )

    def test_import_list_disables_download_and_raw_actions_when_source_is_missing(self):
        import_file = self._make_import_file(
            import_id=1,
            file_name="missing-actions.csv",
            source_path="/tmp/imports/missing-actions.csv",
            stored_rows=0,
        )

        response = self._render_import_list([import_file])

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Stored source unavailable")
        self.assertNotContains(response, reverse("import_file_download", args=[import_file.id]))
        self.assertNotContains(response, reverse("import_file_raw_source", args=[import_file.id]))
        self.assertContains(response, reverse("import_file_detail", args=[import_file.id]))

    def test_import_filters_narrow_results_and_keep_form_values(self):
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=1,
                    file_name="queued-alpha.csv",
                    status="queued",
                ),
                self._make_import_file(
                    import_id=2,
                    file_name="completed-beta.csv",
                    status="completed",
                ),
            ],
            {"q": "queued", "status": "queued"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item.file_name for item in response.context["import_files"]],
            ["queued-alpha.csv"],
        )
        self.assertEqual(response.context["filters"]["q"], "queued")
        self.assertEqual(response.context["filters"]["status"], "queued")
        self.assertContains(response, "Matching imports")
        self.assertContains(response, "Search:")
        self.assertContains(response, "Status:")
        self.assertContains(response, "Queued")

    def test_import_status_and_updated_date_filters_work(self):
        now = timezone.now()
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=1,
                    file_name="completed-recent.csv",
                    status="completed",
                    updated_at=now,
                ),
                self._make_import_file(
                    import_id=2,
                    file_name="completed-old.csv",
                    status="completed",
                    updated_at=now - timedelta(days=5),
                ),
                self._make_import_file(
                    import_id=3,
                    file_name="failed-recent.csv",
                    status="failed",
                    updated_at=now - timedelta(days=1),
                ),
            ],
            {
                "status": "completed",
                "updated_from": str((now - timedelta(days=1)).date()),
                "updated_to": str(now.date()),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item.file_name for item in response.context["import_files"]],
            ["completed-recent.csv"],
        )
        self.assertEqual(response.context["filters"]["updated_from"], str((now - timedelta(days=1)).date()))
        self.assertEqual(response.context["filters"]["updated_to"], str(now.date()))

    def test_import_pagination_preserves_filters(self):
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=index,
                    file_name=f"completed-{index:02d}.csv",
                    status="completed",
                    updated_at=timezone.now() - timedelta(minutes=index),
                )
                for index in range(1, 13)
            ],
            {"status": "completed", "page": 2},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertEqual(response.context["page_query"], "status=completed")
        self.assertContains(response, "?page=1&status=completed")

    def test_import_filtered_empty_state_appears_when_no_results_match(self):
        response = self._render_import_list(
            [
                self._make_import_file(
                    import_id=1,
                    file_name="completed-only.csv",
                    status="completed",
                ),
            ],
            {"status": "failed"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No imports matched the current filters.")
        self.assertNotContains(response, "Your import ledger is still empty.")
        self.assertContains(response, "Clear filters")

    def test_import_filter_panel_is_collapsed_by_default_and_opens_with_filters(self):
        items = [
            self._make_import_file(
                import_id=1,
                file_name="alpha.csv",
                status="completed",
            ),
        ]

        default_response = self._render_import_list(items)
        filtered_response = self._render_import_list(items, {"status": "completed"})

        self.assertContains(
            default_response,
            '<details class="group space-y-2" data-animated-disclosure>',
            html=False,
        )
        self.assertContains(
            filtered_response,
            '<details class="group space-y-2" data-animated-disclosure open>',
            html=False,
        )
        self.assertContains(default_response, "Show filters")
        self.assertContains(filtered_response, "Hide filters")
        self.assertContains(default_response, ">Reset<", html=False)
        self.assertContains(default_response, 'crm/vendor/choices/choices.min.css', html=False)
        self.assertContains(default_response, 'crm/vendor/flatpickr/flatpickr.min.css', html=False)
        self.assertContains(default_response, 'crm/vendor/choices/choices.min.js', html=False)
        self.assertContains(default_response, 'crm/vendor/flatpickr/flatpickr.min.js', html=False)
        self.assertContains(default_response, 'class="tw-import-filter-form"', html=False)
        self.assertContains(default_response, 'class="tw-import-filter-grid"', html=False)
        self.assertContains(default_response, 'data-import-status-select', html=False)
        self.assertContains(default_response, 'data-import-date-field', html=False)
        self.assertContains(default_response, 'class="tw-import-filter-actions"', html=False)
        self.assertContains(default_response, 'class="tw-button-primary w-full sm:w-auto"', html=False)

    def test_import_detail_hides_stored_source_path(self):
        source_path = "/tmp/imports/hidden-path-import.csv"
        rows_manager = Mock()
        rows_manager.select_related.return_value.order_by.return_value = object()
        import_file = SimpleNamespace(
            id=1,
            file_name="hidden-path-import.csv",
            source_path=source_path,
            status="completed",
            result_summary={},
            rows=rows_manager,
        )
        with (
            patch("crm.views.import_views.get_object_or_404", return_value=import_file),
            patch("crm.views.import_views._paginate", return_value=self._build_page([])),
        ):
            response = self.client.get(reverse("import_file_detail", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "hidden-path-import.csv")
        self.assertNotContains(response, "Source path")
        self.assertNotContains(response, source_path)


class ImportUploadStorageTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)
        self.owner_user = self.create_user("owner", role=ROLE_OWNER)
        self.temp_root = Path(tempfile.mkdtemp())
        self.temp_base_dir = self.temp_root / "base"
        self.temp_media_root = self.temp_root / "media"
        self.temp_base_dir.mkdir(parents=True, exist_ok=True)
        self.temp_media_root.mkdir(parents=True, exist_ok=True)
        self.settings_override = override_settings(
            BASE_DIR=self.temp_base_dir,
            MEDIA_ROOT=self.temp_media_root,
            MEDIA_URL="/media/",
        )
        self.settings_override.enable()
        self.addCleanup(self.settings_override.disable)
        self.addCleanup(shutil.rmtree, self.temp_root, ignore_errors=True)

    def _assert_mapping_page_state(self, expected_source_type, expected_name):
        response = self.client.get(reverse("import_map_headers"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, expected_source_type)
        self.assertContains(response, expected_name)
        self.assertContains(response, 'value="Company Name" selected')
        self.assertContains(response, 'value="Email" selected')
        return response

    def _assert_no_staged_queue(self):
        session = self.client.session
        self.assertFalse(session.get("import_staged_sources"))
        self.assertFalse(session.get("import_csv_temp_path"))
        self.assertFalse(session.get("import_csv_original_name"))
        self.assertFalse(session.get("import_csv_headers"))
        self.assertFalse(session.get("import_source_type"))
        self.assertFalse(session.get("import_active_job_id"))

    def _run_import_worker_once(self):
        call_command("run_import_worker", once=True)

    def test_frontend_import_upload_and_mapping_store_files_under_media_imports(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_csv_file("frontend-import.csv")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        session = self.client.session
        temp_path = Path(session["import_csv_temp_path"])
        self.assertEqual(temp_path.parent, self.temp_media_root / "imports")
        self.assertTrue(temp_path.exists())

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "frontend-import.csv",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="frontend-import.csv")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))
        self.assertEqual(import_file.status, ImportFile.Status.QUEUED)
        self.assertEqual(import_file.source_path, str(temp_path))
        self.assertTrue(Path(import_file.source_path).exists())
        self.assertTrue(import_file.original_source_path)
        self.assertTrue(Path(import_file.original_source_path).exists())
        self.assertEqual(import_file.original_source_name, "frontend-import.csv")
        self.assertEqual(import_file.total_rows, 1)
        self.assertFalse(ImportRow.objects.filter(import_file=import_file).exists())

        self._run_import_worker_once()
        import_file.refresh_from_db()
        detail_response = self.client.get(reverse("import_file_detail", args=[import_file.id]))

        self.assertEqual(detail_response.status_code, 200)
        self.assertEqual(import_file.status, ImportFile.Status.COMPLETED)
        self.assertContains(detail_response, "Import result")
        self.assertEqual(detail_response.context["import_result"]["rows_processed"], 1)
        self.assertEqual(detail_response.context["import_result"]["companies_created"], 1)
        self.assertEqual(detail_response.context["import_result"]["contacts_created"], 0)
        self.assertEqual(detail_response.context["import_result"]["rows_skipped"], 0)
        self.assertEqual(detail_response.context["import_result"]["failed_rows_count"], 0)
        self._assert_no_staged_queue()

    def test_import_detail_hero_uses_generic_title_with_file_name_metadata(self):
        self.client.force_login(self.team_lead_user)
        import_file = ImportFile.objects.create(
            file_name="rizwanmehmood2ATgmail.com-Portal-Requested-05-03-26",
            source_path="/tmp/test-import.csv",
            status=ImportFile.Status.COMPLETED,
            total_rows=83,
            processed_rows=83,
            result_summary=build_import_result_summary(
                {
                    "rows_processed": 83,
                    "created_companies": 0,
                    "created_contacts": 0,
                    "skipped_rows": 0,
                    "skipped_empty_rows": 0,
                    "skipped_duplicate_rows": 0,
                    "failed_rows": [],
                }
            ),
        )

        response = self.client.get(reverse("import_file_detail", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "<h1 class=\"page-title\">Import detail</h1>", html=True)
        self.assertContains(response, "File name")
        self.assertContains(response, import_file.file_name)
        self.assertNotContains(
            response,
            f"<h1 class=\"page-title\">{import_file.file_name}</h1>",
            html=False,
        )

    def test_raw_source_page_is_available_to_staff(self):
        staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.client.force_login(staff_user)
        source_path = self.temp_media_root / "imports" / "staff-preview.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text("Company Name,Email\nAcme Labs,person@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="staff-preview.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_raw_source", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Raw import source")
        self.assertContains(response, "Normalized CSV snapshot")
        self.assertContains(response, "Rows: 10")
        self.assertContains(response, "Export")
        self.assertContains(response, "Search this preview")

    def test_download_source_route_is_available_to_staff(self):
        staff_user = self.create_user("staff-download", role=ROLE_STAFF)
        self.client.force_login(staff_user)
        source_path = self.temp_media_root / "imports" / "staff-download.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text("Company Name,Email\nAcme Labs,person@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="staff-download.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_download", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertIn("staff-download.csv", response["Content-Disposition"])
        self.assertEqual(
            b"".join(response.streaming_content),
            source_path.read_bytes(),
        )

    def test_raw_source_page_falls_back_to_normalized_csv_without_exposing_paths(self):
        self.client.force_login(self.team_lead_user)
        source_path = self.temp_media_root / "imports" / "fallback.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text("Company Name,Email\nAcme Labs,person@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="fallback-import.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_raw_source", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Viewing the normalized CSV snapshot")
        self.assertContains(response, "Company Name")
        self.assertContains(response, "Acme Labs")
        self.assertTrue(response.context["preview_context"]["is_tabular"])
        self.assertEqual(response.context["preview_context"]["filtered_row_count"], 1)
        self.assertNotContains(response, str(source_path))

    def test_raw_source_page_applies_search_filter_and_opens_disclosure(self):
        self.client.force_login(self.team_lead_user)
        source_path = self.temp_media_root / "imports" / "filtered-preview.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(
            "Company Name,Email\nAcme Labs,acme@example.com\nBeta Labs,beta@example.com\n",
            encoding="utf-8",
        )
        import_file = ImportFile.objects.create(
            file_name="filtered-preview.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(
            reverse("import_file_raw_source", args=[import_file.id]),
            {"q": "beta"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            '<details class="group flex flex-col gap-2" data-animated-disclosure open>',
            html=False,
        )
        self.assertContains(response, "Beta Labs")
        self.assertNotContains(response, "Acme Labs")
        self.assertEqual(response.context["preview_context"]["filters"]["q"], "beta")
        self.assertEqual(response.context["preview_context"]["filtered_row_count"], 1)

    def test_raw_source_page_rows_menu_changes_page_size_and_preserves_filters(self):
        self.client.force_login(self.team_lead_user)
        source_path = self.temp_media_root / "imports" / "per-page-preview.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(
            "Company Name,Email\n"
            + "\n".join(
                f"Acme Labs {index},person{index}@example.com"
                for index in range(1, 13)
            )
            + "\n",
            encoding="utf-8",
        )
        import_file = ImportFile.objects.create(
            file_name="per-page-preview.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(
            reverse("import_file_raw_source", args=[import_file.id]),
            {"per_page": 50, "q": "Acme"},
        )

        options = {
            item["value"]: item for item in response.context["preview_context"]["per_page_menu_options"]
        }

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["preview_context"]["page_obj"].paginator.per_page, 50)
        self.assertContains(response, "Rows: 50")
        self.assertIn("q=Acme", options[10]["url"])
        self.assertEqual(parse_qs(urlsplit(options[10]["url"]).query)["per_page"], ["10"])

    def test_raw_source_page_shows_unavailable_state_when_source_file_is_missing(self):
        self.client.force_login(self.team_lead_user)
        missing_path = self.temp_media_root / "imports" / "missing.csv"
        import_file = ImportFile.objects.create(
            file_name="missing-preview.csv",
            source_path=str(missing_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_raw_source", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No stored source file is available for this import.")

    def test_raw_source_page_pretty_prints_original_json_upload(self):
        self.client.force_login(self.team_lead_user)
        original_path = self.temp_media_root / "imports" / "payload.json"
        fallback_path = self.temp_media_root / "imports" / "payload.csv"
        original_path.parent.mkdir(parents=True, exist_ok=True)
        original_path.write_text(
            json.dumps([{"Company Name": "Acme Labs", "Email": "person@example.com"}]),
            encoding="utf-8",
        )
        fallback_path.write_text("Company Name,Email\nAcme Labs,person@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="payload-import.csv",
            source_path=str(fallback_path),
            original_source_path=str(original_path),
            original_source_name="payload.json",
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_raw_source", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Viewing the original uploaded source")
        self.assertEqual(
            response.context["preview_context"]["formatted_json"],
            json.dumps(
                [{"Company Name": "Acme Labs", "Email": "person@example.com"}],
                indent=2,
                ensure_ascii=False,
            ),
        )
        self.assertContains(response, "&quot;Company Name&quot;: &quot;Acme Labs&quot;", html=False)
        self.assertContains(response, "JSON")
        self.assertFalse(response.context["preview_context"]["is_tabular"])
        self.assertNotContains(response, "Rows:")
        self.assertNotContains(response, "Export filtered preview as CSV")
        self.assertNotContains(response, str(original_path))

    def test_raw_source_page_renders_all_xlsx_sheet_tabs(self):
        self.client.force_login(self.team_lead_user)
        workbook_path = self.temp_media_root / "imports" / "workbook.xlsx"
        fallback_path = self.temp_media_root / "imports" / "workbook.csv"
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook = Workbook()
        primary_sheet = workbook.active
        primary_sheet.title = "Leads"
        primary_sheet.append(["Company Name", "Email"])
        primary_sheet.append(["Acme Labs", "lead@example.com"])
        secondary_sheet = workbook.create_sheet("Customers")
        secondary_sheet.append(["Company Name", "Email"])
        secondary_sheet.append(["Beta Labs", "customer@example.com"])
        workbook.save(workbook_path)
        workbook.close()
        fallback_path.write_text("Company Name,Email\nAcme Labs,lead@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="workbook-import.csv",
            source_path=str(fallback_path),
            original_source_path=str(workbook_path),
            original_source_name="workbook.xlsx",
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(
            reverse("import_file_raw_source", args=[import_file.id]),
            {"sheet": "Customers"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Leads")
        self.assertContains(response, "Customers")
        self.assertContains(response, "Beta Labs")
        self.assertEqual(
            response.context["preview_context"]["selected_sheet"],
            "Customers",
        )
        self.assertNotContains(response, str(workbook_path))

    def test_raw_source_xlsx_sheet_tabs_preserve_search_and_per_page(self):
        self.client.force_login(self.team_lead_user)
        workbook_path = self.temp_media_root / "imports" / "workbook-query.xlsx"
        fallback_path = self.temp_media_root / "imports" / "workbook-query.csv"
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook = Workbook()
        primary_sheet = workbook.active
        primary_sheet.title = "Leads"
        primary_sheet.append(["Company Name", "Email"])
        primary_sheet.append(["Acme Labs", "lead@example.com"])
        secondary_sheet = workbook.create_sheet("Customers")
        secondary_sheet.append(["Company Name", "Email"])
        secondary_sheet.append(["Beta Labs", "customer@example.com"])
        workbook.save(workbook_path)
        workbook.close()
        fallback_path.write_text("Company Name,Email\nAcme Labs,lead@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="workbook-query-import.csv",
            source_path=str(fallback_path),
            original_source_path=str(workbook_path),
            original_source_name="workbook-query.xlsx",
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(
            reverse("import_file_raw_source", args=[import_file.id]),
            {"sheet": "Customers", "q": "Beta", "per_page": 50},
        )

        tabs = {
            item["name"]: item for item in response.context["preview_context"]["sheet_tabs"]
        }

        self.assertEqual(response.status_code, 200)
        self.assertIn("q=Beta", tabs["Leads"]["url"])
        self.assertIn("per_page=50", tabs["Leads"]["url"])
        self.assertIn("sheet=Leads", tabs["Leads"]["url"])

    def test_raw_source_preview_export_csv_returns_filtered_rows(self):
        self.client.force_login(self.team_lead_user)
        source_path = self.temp_media_root / "imports" / "export-preview.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text(
            "Company Name,Email\nAcme Labs,acme@example.com\nBeta Labs,beta@example.com\n",
            encoding="utf-8",
        )
        import_file = ImportFile.objects.create(
            file_name="export-preview.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(
            reverse("import_file_raw_source", args=[import_file.id]),
            {"q": "Beta", "export": "csv"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response["Content-Type"], "text/csv; charset=utf-8")
        self.assertIn('attachment; filename="export-preview-export-', response["Content-Disposition"])
        rows = response.content.decode("utf-8").splitlines()
        self.assertEqual(rows[0], "Company Name,Email")
        self.assertIn("Beta Labs,beta@example.com", rows[1])
        self.assertEqual(len(rows), 2)

    def test_raw_source_preview_export_xlsx_uses_selected_sheet_and_filters(self):
        self.client.force_login(self.team_lead_user)
        workbook_path = self.temp_media_root / "imports" / "export-workbook.xlsx"
        fallback_path = self.temp_media_root / "imports" / "export-workbook.csv"
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook = Workbook()
        primary_sheet = workbook.active
        primary_sheet.title = "Leads"
        primary_sheet.append(["Company Name", "Email"])
        primary_sheet.append(["Acme Labs", "lead@example.com"])
        secondary_sheet = workbook.create_sheet("Customers")
        secondary_sheet.append(["Company Name", "Email"])
        secondary_sheet.append(["Beta Labs", "customer@example.com"])
        workbook.save(workbook_path)
        workbook.close()
        fallback_path.write_text("Company Name,Email\nAcme Labs,lead@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="export-workbook.csv",
            source_path=str(fallback_path),
            original_source_path=str(workbook_path),
            original_source_name="export-workbook.xlsx",
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(
            reverse("import_file_raw_source", args=[import_file.id]),
            {"sheet": "Customers", "q": "Beta", "export": "xlsx"},
        )

        workbook = load_workbook(BytesIO(response.content))
        worksheet = workbook.active
        rows = list(worksheet.iter_rows(values_only=True))

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", response["Content-Type"])
        self.assertEqual(worksheet.title, "Customers")
        self.assertEqual(rows[0], ("Company Name", "Email"))
        self.assertEqual(rows[1], ("Beta Labs", "customer@example.com"))
        self.assertEqual(len(rows), 2)

    def test_download_source_route_falls_back_to_normalized_csv_snapshot(self):
        self.client.force_login(self.team_lead_user)
        source_path = self.temp_media_root / "imports" / "fallback-download.csv"
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.write_text("Company Name,Email\nAcme Labs,person@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="fallback-download.csv",
            source_path=str(source_path),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_download", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertIn("fallback-download.csv", response["Content-Disposition"])
        self.assertEqual(
            b"".join(response.streaming_content),
            source_path.read_bytes(),
        )
        self.assertEqual(response["Content-Type"], "text/csv")

    def test_download_source_route_prefers_original_uploaded_xlsx(self):
        self.client.force_login(self.team_lead_user)
        workbook_path = self.temp_media_root / "imports" / "download-workbook.xlsx"
        fallback_path = self.temp_media_root / "imports" / "download-workbook.csv"
        workbook_path.parent.mkdir(parents=True, exist_ok=True)
        workbook = Workbook()
        worksheet = workbook.active
        worksheet.title = "Leads"
        worksheet.append(["Company Name", "Email"])
        worksheet.append(["Acme Labs", "lead@example.com"])
        workbook.save(workbook_path)
        workbook.close()
        fallback_path.write_text("Company Name,Email\nAcme Labs,lead@example.com\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="download-workbook.csv",
            source_path=str(fallback_path),
            original_source_path=str(workbook_path),
            original_source_name="download-workbook.xlsx",
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_download", args=[import_file.id]))

        self.assertEqual(response.status_code, 200)
        self.assertIn("download-workbook.xlsx", response["Content-Disposition"])
        self.assertEqual(
            b"".join(response.streaming_content),
            workbook_path.read_bytes(),
        )
        self.assertEqual(
            response["Content-Type"],
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def test_download_source_route_returns_404_when_source_is_missing(self):
        self.client.force_login(self.team_lead_user)
        import_file = ImportFile.objects.create(
            file_name="missing-download.csv",
            source_path=str(self.temp_media_root / "imports" / "missing-download.csv"),
            status=ImportFile.Status.COMPLETED,
        )

        response = self.client.get(reverse("import_file_download", args=[import_file.id]))

        self.assertEqual(response.status_code, 404)

    def test_mapping_page_renders_for_csv_source(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_csv_file("frontend-import.csv")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("CSV file", "frontend-import")

    def test_mapping_page_suggests_extended_header_aliases(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": make_csv_file(
                    "alias-import.csv",
                    (
                        "Organisation,FirstName,LastName,JobRole,LinkedIn,Postcode,"
                        "State/Province,Verified email address\n"
                        "Acme Labs,Ada,Lovelace,Founder,"
                        "https://www.linkedin.com/in/ada,10001,NY,ada@example.com\n"
                    ),
                )
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        response = self.client.get(reverse("import_map_headers"))
        selected = {
            field["key"]: field["selected"]
            for field in response.context["mapping_fields"]
        }

        self.assertEqual(response.status_code, 200)
        self.assertEqual(selected["company_name"], "Organisation")
        self.assertEqual(selected["contact_first_name"], "FirstName")
        self.assertEqual(selected["contact_last_name"], "LastName")
        self.assertEqual(selected["contact_title"], "JobRole")
        self.assertEqual(selected["person_source"], "LinkedIn")
        self.assertEqual(selected["zip_code"], "Postcode")
        self.assertEqual(selected["state"], "State/Province")
        self.assertEqual(selected["email"], "Verified email address")
        self.assertContains(response, 'value="Organisation" selected')
        self.assertContains(response, 'value="FirstName" selected')
        self.assertContains(response, 'value="LastName" selected')

    def test_mapping_page_renders_for_xlsx_source(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_xlsx_file("frontend-import.xlsx")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("Excel workbook", "frontend-import")

    def test_mapping_page_prefills_cleaned_display_name_for_long_filename(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": make_xlsx_file(
                    "1.Template-Pharmaceutical Society of Japan-20260309_143924.xlsx"
                )
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        response = self.client.get(reverse("import_map_headers"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            'value="Pharmaceutical Society of Japan"',
            html=False,
        )
        self.assertContains(response, "Pharmaceutical Society of Japan")
        self.assertNotContains(
            response,
            "1.Template-Pharmaceutical Society of Japan-20260309_143924.xlsx",
        )

    def test_mapping_page_renders_for_json_source(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_json_file("frontend-import.json")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("JSON file", "frontend-import")

    def test_frontend_multi_file_upload_stages_queue_and_lands_on_first_mapping(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": [
                    make_csv_file("frontend-first.csv", "Company Name,Email\nAcme Labs,first@example.com\n"),
                    make_csv_file("frontend-second.csv", "Organisation,Email\nBeta Labs,second@example.com\n"),
                ],
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        session = self.client.session
        staged_queue = session["import_staged_sources"]
        self.assertEqual(len(staged_queue), 2)
        self.assertEqual(staged_queue[0]["original_name"], "frontend-first.csv")
        self.assertEqual(staged_queue[0]["queue_position"], 1)
        self.assertEqual(staged_queue[0]["queue_total"], 2)
        self.assertEqual(staged_queue[1]["original_name"], "frontend-second.csv")
        self.assertEqual(staged_queue[1]["queue_position"], 2)
        self.assertEqual(staged_queue[1]["queue_total"], 2)
        self.assertEqual(session["import_csv_original_name"], "frontend-first.csv")

        mapping_response = self.client.get(reverse("import_map_headers"))

        self.assertEqual(mapping_response.status_code, 200)
        self.assertContains(mapping_response, "frontend-first")
        self.assertContains(mapping_response, "File 1 of 2")
        self.assertContains(mapping_response, "CSV file")

    def test_multi_file_upload_rejects_duplicate_file_names_without_staging_queue(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": [
                    make_csv_file("duplicate.csv", "Company Name,Email\nAcme Labs,first@example.com\n"),
                    make_csv_file("duplicate.csv", "Company Name,Email\nBeta Labs,second@example.com\n"),
                ],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Each selected file must have a unique file name.")
        self._assert_no_staged_queue()

    def test_multi_file_upload_rejects_whole_selection_when_one_file_is_invalid(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": [
                    make_csv_file("valid.csv", "Company Name,Email\nAcme Labs,first@example.com\n"),
                    make_csv_file("unsupported.txt", "Company Name,Email\nBeta Labs,second@example.com\n"),
                ],
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Unsupported import source type.")
        self._assert_no_staged_queue()

    def test_multi_file_mapping_advances_to_next_file_and_finishes_on_detail(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": [
                    make_csv_file("frontend-first.csv", "Company Name,Email\nAcme Labs,first@example.com\n"),
                    make_csv_file("frontend-second.csv", "Organisation,Email\nBeta Labs,second@example.com\n"),
                ],
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        first_mapping_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "frontend-first.csv",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        first_import_file = ImportFile.objects.get(file_name="frontend-first.csv")

        self.assertRedirects(first_mapping_response, reverse("import_file_detail", args=[first_import_file.id]))
        self.assertEqual(first_import_file.status, ImportFile.Status.QUEUED)

        self._run_import_worker_once()
        first_import_file.refresh_from_db()
        first_detail_response = self.client.get(
            reverse("import_file_detail", args=[first_import_file.id]),
            follow=True,
        )

        self.assertEqual(first_detail_response.status_code, 200)
        self.assertEqual(first_import_file.status, ImportFile.Status.COMPLETED)
        self.assertContains(first_detail_response, "Imported frontend-first.csv. Continue with file 2 of 2.")
        self.assertContains(first_detail_response, "frontend-second")
        self.assertContains(first_detail_response, "File 2 of 2")
        self.assertContains(first_detail_response, "Organisation")
        self.assertTrue(ImportFile.objects.filter(file_name="frontend-first.csv").exists())
        self.assertEqual(self.client.session["import_csv_original_name"], "frontend-second.csv")

        second_mapping_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "frontend-second.csv",
                "map_company_name": "Organisation",
                "map_email": "Email",
            },
        )
        second_import_file = ImportFile.objects.get(file_name="frontend-second.csv")

        self.assertRedirects(second_mapping_response, reverse("import_file_detail", args=[second_import_file.id]))
        self.assertEqual(second_import_file.status, ImportFile.Status.QUEUED)

        self._run_import_worker_once()
        second_import_file.refresh_from_db()
        second_detail_response = self.client.get(
            reverse("import_file_detail", args=[second_import_file.id]),
            follow=True,
        )

        self.assertEqual(second_detail_response.status_code, 200)
        self.assertContains(second_detail_response, "Import result")
        self.assertEqual(second_detail_response.context["import_file"].file_name, "frontend-second.csv")
        self.assertEqual(
            ImportFile.objects.filter(file_name__in=["frontend-first.csv", "frontend-second.csv"]).count(),
            2,
        )
        self._assert_no_staged_queue()

    def test_reset_queue_clears_all_staged_files(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": [
                    make_csv_file("frontend-first.csv", "Company Name,Email\nAcme Labs,first@example.com\n"),
                    make_csv_file("frontend-second.csv", "Organisation,Email\nBeta Labs,second@example.com\n"),
                ],
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        reset_response = self.client.get(f"{reverse('import_upload')}?reset_queue=1")

        self.assertEqual(reset_response.status_code, 200)
        self._assert_no_staged_queue()

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_frontend_google_sheets_url_stages_mapping_flow(self, mock_fetch):
        self.client.force_login(self.team_lead_user)
        mock_fetch.return_value = [
            {"Company Name": "Acme Labs", "Email": "person@example.com"},
        ]

        response = self.client.post(
            reverse("import_upload"),
            {"sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0"},
        )

        self.assertRedirects(response, reverse("import_map_headers"))
        session = self.client.session
        self.assertEqual(session["import_csv_original_name"], "Google Sheet - test-sheet.csv")
        self.assertEqual(session["import_csv_headers"], ["Company Name", "Email"])
        self.assertEqual(session["import_source_type"], "google_sheets")

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_mapping_page_renders_for_google_sheets_source(self, mock_fetch):
        self.client.force_login(self.team_lead_user)
        mock_fetch.return_value = [
            {"Company Name": "Acme Labs", "Email": "person@example.com"},
        ]

        upload_response = self.client.post(
            reverse("import_upload"),
            {"sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0"},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("Google Sheets", "Google Sheet - test-sheet")

    def test_import_upload_requires_a_file_or_google_sheet_url(self):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(reverse("import_upload"), {})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Choose an import file or enter a Google Sheets URL.")

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_import_upload_rejects_file_and_google_sheet_url_together(self, mock_fetch):
        self.client.force_login(self.team_lead_user)

        response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": make_csv_file("frontend-import.csv"),
                "sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertContains(
            response,
            "Choose either an import file or a Google Sheets URL, not both.",
        )
        mock_fetch.assert_not_called()

    def test_frontend_xlsx_upload_is_converted_and_redirects_to_mapping(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_xlsx_file("frontend-import.xlsx")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        session = self.client.session
        temp_path = Path(session["import_csv_temp_path"])
        self.assertEqual(temp_path.parent, self.temp_media_root / "imports")
        self.assertEqual(temp_path.suffix, ".csv")
        self.assertTrue(temp_path.exists())
        self.assertEqual(session["import_csv_original_name"], "frontend-import.xlsx")
        self.assertEqual(session["import_csv_headers"], ["Company Name", "Email"])

    def test_frontend_xlsx_mapping_submission_works_with_standardized_rows(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_xlsx_file("frontend-import.xlsx")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        session = self.client.session
        temp_path = Path(session["import_csv_temp_path"])

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "frontend-import.xlsx",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="frontend-import.xlsx")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))
        self.assertEqual(import_file.status, ImportFile.Status.QUEUED)
        self.assertEqual(import_file.source_path, str(temp_path))
        self.assertTrue(Path(import_file.source_path).exists())
        self.assertTrue(import_file.original_source_path)
        self.assertEqual(import_file.original_source_name, "frontend-import.xlsx")
        self.assertTrue(Path(import_file.original_source_path).exists())

        self._run_import_worker_once()
        import_file.refresh_from_db()
        detail_response = self.client.get(reverse("import_file_detail", args=[import_file.id]))

        self.assertEqual(detail_response.status_code, 200)
        self.assertContains(detail_response, "Import result")
        self.assertEqual(detail_response.context["import_result"]["rows_processed"], 1)
        self.assertEqual(detail_response.context["import_result"]["companies_created"], 1)
        self.assertEqual(detail_response.context["import_result"]["failed_rows_count"], 0)

    def test_frontend_json_mapping_submission_preserves_original_source_upload(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_json_file("frontend-import.json")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        session = self.client.session
        temp_path = Path(session["import_csv_temp_path"])

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "frontend-import.json",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="frontend-import.json")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))
        self.assertEqual(import_file.status, ImportFile.Status.QUEUED)
        self.assertEqual(import_file.source_path, str(temp_path))
        self.assertTrue(Path(import_file.source_path).exists())
        self.assertTrue(import_file.original_source_path)
        self.assertEqual(import_file.original_source_name, "frontend-import.json")
        self.assertTrue(Path(import_file.original_source_path).exists())

        raw_response = self.client.get(reverse("import_file_raw_source", args=[import_file.id]))

        self.assertEqual(raw_response.status_code, 200)
        self.assertContains(raw_response, "Viewing the original uploaded source")
        self.assertContains(raw_response, "JSON")
        self.assertNotContains(raw_response, import_file.original_source_path)

    @patch("crm.services.google_sheets.fetch_google_sheet_rows")
    def test_google_sheets_import_file_keeps_original_source_fields_blank(self, mock_fetch):
        self.client.force_login(self.team_lead_user)
        mock_fetch.return_value = [
            {"Company Name": "Acme Labs", "Email": "person@example.com"},
        ]

        upload_response = self.client.post(
            reverse("import_upload"),
            {"sheet_url": "https://docs.google.com/spreadsheets/d/test-sheet/edit?gid=0#gid=0"},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "Google Sheet - test-sheet.csv",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="Google Sheet - test-sheet.csv")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))
        self.assertEqual(import_file.original_source_path, "")
        self.assertEqual(import_file.original_source_name, "")

        raw_response = self.client.get(reverse("import_file_raw_source", args=[import_file.id]))

        self.assertEqual(raw_response.status_code, 200)
        self.assertContains(raw_response, "Viewing the normalized CSV snapshot")
        self.assertContains(raw_response, "Acme Labs")
        self.assertNotContains(raw_response, import_file.source_path)

    def test_mapping_submission_uses_cleaned_display_name_when_left_unchanged(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": make_csv_file(
                    "1.Template-Pharmaceutical Society of Japan-20260309_143924.csv"
                )
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "Pharmaceutical Society of Japan",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="Pharmaceutical Society of Japan")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))
        self.assertEqual(import_file.file_name, "Pharmaceutical Society of Japan")

    def test_import_result_reports_failed_rows_for_empty_mapped_input(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": make_csv_file(
                    "frontend-import.csv",
                    "Company Name,Email\nAcme Labs,person@example.com\n,\n",
                ),
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "frontend-import.csv",
                "map_company_name": "Company Name",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="frontend-import.csv")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))

        self._run_import_worker_once()
        import_file.refresh_from_db()
        detail_response = self.client.get(reverse("import_file_detail", args=[import_file.id]))

        self.assertEqual(detail_response.status_code, 200)
        self.assertContains(detail_response, "Failed rows")
        self.assertContains(detail_response, "Row was empty after mapping.")
        self.assertEqual(detail_response.context["import_result"]["rows_processed"], 2)
        self.assertEqual(detail_response.context["import_result"]["rows_skipped"], 1)
        self.assertEqual(detail_response.context["import_result"]["failed_rows_count"], 1)
        self.assertEqual(
            detail_response.context["import_result"]["failed_rows"],
            [{"row_number": 3, "reason": "Row was empty after mapping."}],
        )

    def test_import_mapping_truncates_overlong_contact_title_for_model_limits(self):
        self.client.force_login(self.team_lead_user)
        long_title = "Senior Strategic Partnerships " * 12

        upload_response = self.client.post(
            reverse("import_upload"),
            {
                "csv_file": make_csv_file(
                    "long-title-import.csv",
                    (
                        "First Name,Last Name,Title,Email\n"
                        f"Jane,Doe,{long_title},jane@example.com\n"
                    ),
                ),
            },
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))

        map_response = self.client.post(
            reverse("import_map_headers"),
            {
                "file_name": "long-title-import.csv",
                "map_contact_first_name": "First Name",
                "map_contact_last_name": "Last Name",
                "map_contact_title": "Title",
                "map_email": "Email",
            },
        )
        import_file = ImportFile.objects.get(file_name="long-title-import.csv")

        self.assertRedirects(map_response, reverse("import_file_detail", args=[import_file.id]))

        self._run_import_worker_once()
        import_file.refresh_from_db()
        detail_response = self.client.get(reverse("import_file_detail", args=[import_file.id]))

        contact = Contact.objects.get(full_name="Jane Doe")
        import_row = ImportRow.objects.get(contact=contact)

        self.assertEqual(detail_response.status_code, 200)
        self.assertContains(detail_response, "Import result")
        self.assertEqual(contact.title, long_title[:100])
        self.assertEqual(import_row.contact_title, long_title[:255])
        self.assertEqual(detail_response.context["import_result"]["contacts_created"], 1)
        self.assertEqual(detail_response.context["import_result"]["failed_rows_count"], 0)

    def test_import_worker_once_processes_only_one_queued_job(self):
        first_path = self.temp_media_root / "imports" / "queued-one.csv"
        second_path = self.temp_media_root / "imports" / "queued-two.csv"
        first_path.parent.mkdir(parents=True, exist_ok=True)
        first_path.write_text("Company Name,Email\nAcme Labs,first@example.com\n", encoding="utf-8")
        second_path.write_text("Company Name,Email\nBeta Labs,second@example.com\n", encoding="utf-8")

        first_job = ImportFile.objects.create(
            file_name="queued-one.csv",
            source_path=str(first_path),
            status=ImportFile.Status.QUEUED,
            mapping={"company_name": "Company Name", "email": "Email"},
            total_rows=1,
        )
        second_job = ImportFile.objects.create(
            file_name="queued-two.csv",
            source_path=str(second_path),
            status=ImportFile.Status.QUEUED,
            mapping={"company_name": "Company Name", "email": "Email"},
            total_rows=1,
        )

        self._run_import_worker_once()
        first_job.refresh_from_db()
        second_job.refresh_from_db()

        self.assertEqual(first_job.status, ImportFile.Status.COMPLETED)
        self.assertEqual(first_job.processed_rows, 1)
        self.assertEqual(first_job.result_summary["rows_processed"], 1)
        self.assertEqual(second_job.status, ImportFile.Status.QUEUED)

    def test_import_worker_marks_failed_job_with_error_message(self):
        import_file = ImportFile.objects.create(
            file_name="missing-source.csv",
            source_path=str(self.temp_media_root / "imports" / "missing-source.csv"),
            status=ImportFile.Status.QUEUED,
            mapping={"company_name": "Company Name"},
            total_rows=1,
        )

        self._run_import_worker_once()
        import_file.refresh_from_db()

        self.assertEqual(import_file.status, ImportFile.Status.FAILED)
        self.assertIn("Queued import source path does not exist", import_file.error_message)
        self.assertEqual(import_file.processed_rows, 0)

    def test_admin_import_upload_stores_file_under_media_imports(self):
        self.client.force_login(self.owner_user)

        response = self.client.post(
            reverse("admin:crm_importfile_add"),
            {
                "file_name": "admin-import.csv",
                "csv_file": make_csv_file("admin-import.csv"),
                "rows-TOTAL_FORMS": "0",
                "rows-INITIAL_FORMS": "0",
                "rows-MIN_NUM_FORMS": "0",
                "rows-MAX_NUM_FORMS": "1000",
                "_save": "Save",
            },
            follow=True,
        )
        import_file = ImportFile.objects.get(file_name="admin-import.csv")
        source_path = Path(import_file.source_path)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(source_path.parent, self.temp_media_root / "imports")
        self.assertTrue(source_path.exists())

    def test_legacy_import_uploads_are_copied_to_media_and_paths_are_updated(self):
        legacy_uploads_dir = self.temp_base_dir / "data" / "uploads"
        legacy_uploads_dir.mkdir(parents=True, exist_ok=True)
        legacy_file = legacy_uploads_dir / "legacy-import.csv"
        legacy_file.write_text("Company Name\nAcme Labs\n", encoding="utf-8")
        orphan_file = legacy_uploads_dir / "orphan-import.csv"
        orphan_file.write_text("Company Name\nOrphan Co\n", encoding="utf-8")
        import_file = ImportFile.objects.create(
            file_name="legacy-import.csv",
            source_path=str(legacy_file),
        )

        migration_module = importlib.import_module("crm.migrations.0011_move_import_uploads_to_media")
        migration_module.move_legacy_import_uploads(django_apps, None)
        import_file.refresh_from_db()

        migrated_file = self.temp_media_root / "imports" / legacy_file.name
        migrated_orphan = self.temp_media_root / "imports" / orphan_file.name

        self.assertEqual(import_file.source_path, str(migrated_file))
        self.assertTrue(migrated_file.exists())
        self.assertEqual(
            migrated_file.read_text(encoding="utf-8"),
            legacy_file.read_text(encoding="utf-8"),
        )
        self.assertTrue(migrated_orphan.exists())
