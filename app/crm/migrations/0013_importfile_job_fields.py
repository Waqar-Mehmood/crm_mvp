from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("crm", "0012_add_search_indexes"),
    ]

    operations = [
        migrations.AddField(
            model_name="importfile",
            name="completed_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="importfile",
            name="error_message",
            field=models.TextField(blank=True),
        ),
        migrations.AddField(
            model_name="importfile",
            name="mapping",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="importfile",
            name="processed_rows",
            field=models.PositiveIntegerField(default=0),
        ),
        migrations.AddField(
            model_name="importfile",
            name="result_summary",
            field=models.JSONField(blank=True, default=dict),
        ),
        migrations.AddField(
            model_name="importfile",
            name="started_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="importfile",
            name="status",
            field=models.CharField(
                choices=[
                    ("queued", "Queued"),
                    ("running", "Running"),
                    ("completed", "Completed"),
                    ("failed", "Failed"),
                ],
                db_index=True,
                default="completed",
                max_length=20,
            ),
        ),
        migrations.AddField(
            model_name="importfile",
            name="total_rows",
            field=models.PositiveIntegerField(default=0),
        ),
    ]
