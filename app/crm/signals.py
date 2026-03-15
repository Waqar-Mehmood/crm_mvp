from django.apps import apps
from django.contrib.auth import get_user_model
from django.db.models import Q
from django.db.models.signals import m2m_changed, post_migrate
from django.dispatch import receiver

from crm.auth import (
    CRM_ROLE_ORDER,
    ROLE_MANAGER,
    ROLE_OWNER,
    sync_user_staff_status,
)


@receiver(post_migrate)
def ensure_crm_groups_and_permissions(sender, **kwargs):
    if sender.label != "crm":
        return

    Group = apps.get_model("auth", "Group")
    Permission = apps.get_model("auth", "Permission")

    for role_name in CRM_ROLE_ORDER:
        Group.objects.get_or_create(name=role_name)

    admin_permissions = Permission.objects.filter(
        Q(content_type__app_label="auth", content_type__model="user")
        | Q(content_type__app_label="crm")
    ).distinct()

    for role_name in (ROLE_MANAGER, ROLE_OWNER):
        group = Group.objects.get(name=role_name)
        group.permissions.set(admin_permissions)


@receiver(m2m_changed, sender=get_user_model().groups.through)
def sync_user_staff_from_groups(sender, instance, action, reverse, **kwargs):
    if reverse:
        return
    if action in {"post_add", "post_remove", "post_clear"}:
        sync_user_staff_status(instance)
