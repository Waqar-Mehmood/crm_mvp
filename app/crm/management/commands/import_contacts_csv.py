from pathlib import Path

from django.core.management.base import BaseCommand, CommandError

from crm.services.import_workflow import (
    detect_headers,
    import_csv_with_mapping,
    suggest_mapping,
)


class Command(BaseCommand):
    help = "Import companies/contacts from a CSV file and connect them."

    def add_arguments(self, parser):
        parser.add_argument("csv_path", type=str, help="Path to the CSV file")
        parser.add_argument(
            "--file-name",
            type=str,
            default="",
            help="Logical name used to group imported rows in DB (defaults to CSV filename).",
        )
    def handle(self, *args, **options):
        csv_path = Path(options["csv_path"]).expanduser()
        if not csv_path.exists():
            raise CommandError(f"File not found: {csv_path}")
        headers = detect_headers(csv_path)
        mapping = suggest_mapping(headers)
        file_name = options.get("file_name") or csv_path.name
        import_file, stats = import_csv_with_mapping(
            csv_path=csv_path,
            file_name=file_name,
            mapping=mapping,
            source_path=str(csv_path),
        )

        self.stdout.write(self.style.SUCCESS("Import completed."))
        self.stdout.write(f"Import file: {import_file.file_name}")
        self.stdout.write(f"Companies created: {stats['created_companies']}")
        self.stdout.write(f"Contacts created: {stats['created_contacts']}")
        self.stdout.write(f"Company-contact links created: {stats['links_created']}")
        self.stdout.write(f"Contact emails created: {stats['email_rows_created']}")
        self.stdout.write(f"Contact phones created: {stats['phone_rows_created']}")
        self.stdout.write(f"Contact social links created: {stats['social_rows_created']}")
        self.stdout.write(f"Company social links created: {stats['company_social_rows_created']}")
        self.stdout.write(f"Import rows created: {stats['import_rows_created']}")
        self.stdout.write(f"Import rows updated: {stats['import_rows_updated']}")
        self.stdout.write(f"Empty rows skipped: {stats['skipped_empty_rows']}")
        self.stdout.write(f"Duplicate rows skipped: {stats['skipped_duplicate_rows']}")
        self.stdout.write(f"Rows without company/contact match: {stats['skipped_rows']}")
