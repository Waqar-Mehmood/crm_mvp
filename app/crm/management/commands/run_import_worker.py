import time

from django.core.management.base import BaseCommand

from crm.services.import_jobs import (
    claim_next_import_job,
    fail_import_job,
    process_import_job,
)


class Command(BaseCommand):
    help = "Process queued frontend imports in the background."

    def add_arguments(self, parser):
        parser.add_argument(
            "--once",
            action="store_true",
            help="Process at most one queued import, then exit.",
        )
        parser.add_argument(
            "--poll-interval",
            type=float,
            default=2.0,
            help="Seconds to wait before polling again when no jobs are queued.",
        )

    def handle(self, *args, **options):
        process_once = options["once"]
        poll_interval = max(options["poll_interval"], 0.1)

        while True:
            import_file = claim_next_import_job()
            if import_file is None:
                if process_once:
                    self.stdout.write("No queued import jobs.")
                    return
                time.sleep(poll_interval)
                continue

            self.stdout.write(f"Processing import job {import_file.id}: {import_file.file_name}")
            try:
                process_import_job(import_file)
            except Exception as exc:
                fail_import_job(import_file, exc)
                self.stderr.write(
                    self.style.ERROR(
                        f"Import job {import_file.id} failed: {exc}"
                    )
                )

            if process_once:
                return
