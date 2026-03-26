from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("crm", "0013_importfile_job_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="importfile",
            name="original_source_name",
            field=models.CharField(blank=True, max_length=255),
        ),
        migrations.AddField(
            model_name="importfile",
            name="original_source_path",
            field=models.TextField(blank=True),
        ),
    ]
