from types import SimpleNamespace
from pathlib import Path

from django.contrib.sessions.backends.db import SessionStore

from crm.models import Company, Contact, ImportFile, ImportRow
from crm.services.import_components import (
    DataCleaner,
    EntityCreator,
    FieldMapper,
    FileManager,
    ImportOrchestrator,
    ImportSessionManager,
    ImportStats,
    MappingBuilder,
    RelationshipBuilder,
    UploadHandler,
)

from . import TestCase, make_csv_file


class ImportComponentTests(TestCase):
    def test_field_mapper_suggest_mapping(self):
        mapping = FieldMapper.suggest_mapping(["Organisation", "FirstName", "LastName", "Email"])

        self.assertEqual(mapping["company_name"], "Organisation")
        self.assertEqual(mapping["contact_first_name"], "FirstName")
        self.assertEqual(mapping["contact_last_name"], "LastName")
        self.assertEqual(mapping["email"], "Email")

    def test_data_cleaner_clean(self):
        self.assertEqual(DataCleaner.clean("  Hello \n world \r\n"), "Hello world")
        self.assertEqual(DataCleaner.clean_for_model_field("title", "x" * 120), "x" * 100)

    def test_entity_creator_create_company_contact(self):
        company, company_created = EntityCreator.get_or_create_company(
            "Acme Labs",
            industry="SaaS",
        )
        contact, contact_created = EntityCreator.get_or_create_contact(
            "Ada",
            "Lovelace",
            title="Founder",
            email="ada@example.com",
        )

        self.assertTrue(company_created)
        self.assertTrue(contact_created)
        self.assertEqual(company.industry, "SaaS")
        self.assertEqual(contact.full_name, "Ada Lovelace")
        self.assertEqual(contact.title, "Founder")

    def test_relationship_builder_create_links(self):
        company = Company.objects.create(name="Acme Labs")
        contact = Contact.objects.create(full_name="Ada Lovelace")

        RelationshipBuilder.link_contact_to_company(contact, company)
        email = RelationshipBuilder.create_contact_email(contact, "ada@example.com")
        phone = RelationshipBuilder.create_contact_phone(contact, "555-0101")
        social = RelationshipBuilder.create_contact_social_link(
            contact,
            "https://www.linkedin.com/in/ada",
            "linkedin",
        )
        company_social = RelationshipBuilder.create_company_social_link(
            company,
            "https://acme.example.com",
            "website",
        )

        self.assertTrue(company.contacts.filter(pk=contact.pk).exists())
        self.assertEqual(email.email, "ada@example.com")
        self.assertEqual(phone.phone, "555-0101")
        self.assertEqual(social.platform, "linkedin")
        self.assertEqual(company_social.platform, "website")

    def test_import_stats_basic_counts(self):
        stats = ImportStats()
        stats.increment_rows_processed()
        stats.increment_created()
        stats.increment_updated()
        stats.increment_failed()
        stats.add_error(2, "Invalid email")

        summary = stats.get_summary()

        self.assertEqual(summary["rows_processed"], 1)
        self.assertEqual(summary["rows_created"], 1)
        self.assertEqual(summary["rows_updated"], 1)
        self.assertEqual(summary["rows_failed"], 1)
        self.assertEqual(summary["errors"], [{"row_num": 2, "message": "Invalid email"}])

    def test_import_orchestrator_execute_sample_rows(self):
        import_file = ImportFile.objects.create(file_name="orchestrator-test.csv")
        rows = [
            {
                "Company Name": "Acme Labs",
                "First Name": "Ada",
                "Last Name": "Lovelace",
                "Email": "ada@example.com",
                "LinkedIn": "https://www.linkedin.com/in/ada",
            }
        ]
        mapping = {
            "company_name": "Company Name",
            "contact_first_name": "First Name",
            "contact_last_name": "Last Name",
            "email": "Email",
            "person_source": "LinkedIn",
        }

        stats = ImportOrchestrator.execute(rows, mapping, import_file=import_file)

        self.assertEqual(stats.rows_processed, 1)
        self.assertEqual(stats.created_companies, 1)
        self.assertEqual(stats.created_contacts, 1)
        self.assertEqual(stats.email_rows_created, 1)
        self.assertEqual(stats.social_rows_created, 1)
        self.assertTrue(Company.objects.filter(name="Acme Labs").exists())
        self.assertTrue(Contact.objects.filter(full_name="Ada Lovelace").exists())
        self.assertTrue(ImportRow.objects.filter(import_file=import_file, row_number=2).exists())

    def test_upload_handler_validate_and_parse(self):
        uploaded = make_csv_file("contacts.csv", "Company Name,Email\nAcme Labs,hello@example.com\n")

        is_valid, error_message = UploadHandler.validate_file(uploaded)
        result = UploadHandler.process_uploaded_file(uploaded)

        self.assertTrue(is_valid)
        self.assertEqual(error_message, "")
        self.assertEqual(result["source_type"], "csv")
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(result["rows"][0]["Company Name"], "Acme Labs")

    def test_mapping_builder_validation(self):
        fields = MappingBuilder.build_mapping_fields(["Company", "FirstName", "LastName", "Email"])

        self.assertTrue(fields)
        self.assertEqual(fields[0]["target_field"], "company_name")
        self.assertEqual(MappingBuilder.get_required_fields(), ["company_name", "contact_name"])
        self.assertIn("industry", MappingBuilder.get_optional_fields())
        self.assertEqual(
            MappingBuilder.validate_user_mapping({"company_name": "Company"}),
            (True, ""),
        )
        self.assertEqual(
            MappingBuilder.validate_user_mapping({}),
            (
                False,
                "Map at least one identifying field: Company Name, Contact Full Name, or both Contact First Name and Contact Last Name.",
            ),
        )

    def test_file_manager_temp_csv_lifecycle(self):
        path = FileManager.create_temp_csv(
            [{"Company Name": "Acme Labs", "Email": "hello@example.com"}]
        )

        self.assertEqual(
            FileManager.validate_filename("My import #001.xlsx"),
            "My_import_001.csv",
        )
        self.assertTrue(path.endswith(".csv"))

        FileManager.cleanup_temp_file(path)

        self.assertFalse(Path(path).exists())

    def test_import_session_queue(self):
        request = SimpleNamespace(session=SessionStore())

        self.assertEqual(ImportSessionManager.get_staged_queue(request), [])
        self.assertIsNone(ImportSessionManager.get_active_job(request))

        ImportSessionManager.add_to_queue(
            request,
            {
                "rows": [{"Company Name": "Acme Labs"}],
                "file_name": "contacts.csv",
                "source_type": "csv",
                "row_count": 1,
            },
        )
        ImportSessionManager.set_active_job(request, "42")

        entry = ImportSessionManager.pop_from_queue(request)

        self.assertEqual(entry["file_name"], "contacts.csv")
        self.assertEqual(ImportSessionManager.get_staged_queue(request), [])
        self.assertEqual(ImportSessionManager.get_active_job(request), "42")

        ImportSessionManager.mark_job_complete(request)
        ImportSessionManager.clear_queue(request)

        self.assertIsNone(ImportSessionManager.get_active_job(request))
        self.assertEqual(ImportSessionManager.get_staged_queue(request), [])
