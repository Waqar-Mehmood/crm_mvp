from django.conf import settings
from django.db import migrations


CRM_ROLE_NAMES = (
    "staff",
    "team_lead",
    "manager",
    "owner",
)


def bootstrap_crm_roles(apps, schema_editor):
    Group = apps.get_model("auth", "Group")
    user_app_label, user_model_name = settings.AUTH_USER_MODEL.split(".")
    User = apps.get_model(user_app_label, user_model_name)

    for role_name in CRM_ROLE_NAMES:
        Group.objects.get_or_create(name=role_name)

    User.objects.filter(is_superuser=False).update(is_staff=False)


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ("crm", "0008_sitebranding"),
    ]

    operations = [
        migrations.RunPython(bootstrap_crm_roles, migrations.RunPython.noop),
    ]
