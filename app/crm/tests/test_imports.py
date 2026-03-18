from . import (
    CRMRoleTestMixin,
    Client,
    ImportFile,
    Mock,
    Path,
    ROLE_OWNER,
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
            follow=True,
        )
        import_file = ImportFile.objects.get(file_name="frontend-import.csv")

        self.assertEqual(map_response.status_code, 200)
        self.assertEqual(import_file.source_path, str(temp_path))
        self.assertTrue(Path(import_file.source_path).exists())
        self.assertContains(map_response, "Import result")
        self.assertEqual(map_response.context["import_result"]["rows_processed"], 1)
        self.assertEqual(map_response.context["import_result"]["companies_created"], 1)
        self.assertEqual(map_response.context["import_result"]["contacts_created"], 0)
        self.assertEqual(map_response.context["import_result"]["rows_skipped"], 0)
        self.assertEqual(map_response.context["import_result"]["failed_rows_count"], 0)
        self.assertEqual(self.client.session.get("import_source_type"), None)

    def test_mapping_page_renders_for_csv_source(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_csv_file("frontend-import.csv")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("CSV file", "frontend-import.csv")

    def test_mapping_page_renders_for_xlsx_source(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_xlsx_file("frontend-import.xlsx")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("Excel workbook", "frontend-import.xlsx")

    def test_mapping_page_renders_for_json_source(self):
        self.client.force_login(self.team_lead_user)

        upload_response = self.client.post(
            reverse("import_upload"),
            {"csv_file": make_json_file("frontend-import.json")},
        )

        self.assertRedirects(upload_response, reverse("import_map_headers"))
        self._assert_mapping_page_state("JSON file", "frontend-import.json")

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
        self._assert_mapping_page_state("Google Sheets", "Google Sheet - test-sheet.csv")

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
            follow=True,
        )
        import_file = ImportFile.objects.get(file_name="frontend-import.xlsx")

        self.assertEqual(map_response.status_code, 200)
        self.assertEqual(import_file.source_path, str(temp_path))
        self.assertTrue(Path(import_file.source_path).exists())
        self.assertContains(map_response, "Import result")
        self.assertEqual(map_response.context["import_result"]["rows_processed"], 1)
        self.assertEqual(map_response.context["import_result"]["companies_created"], 1)
        self.assertEqual(map_response.context["import_result"]["failed_rows_count"], 0)

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
            follow=True,
        )

        self.assertEqual(map_response.status_code, 200)
        self.assertContains(map_response, "Failed rows")
        self.assertContains(map_response, "Row was empty after mapping.")
        self.assertEqual(map_response.context["import_result"]["rows_processed"], 2)
        self.assertEqual(map_response.context["import_result"]["rows_skipped"], 1)
        self.assertEqual(map_response.context["import_result"]["failed_rows_count"], 1)
        self.assertEqual(
            map_response.context["import_result"]["failed_rows"],
            [{"row_number": 3, "reason": "Row was empty after mapping."}],
        )

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
