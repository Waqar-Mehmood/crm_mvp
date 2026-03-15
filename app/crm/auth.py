from functools import wraps

from django.contrib.auth.decorators import login_required
from django.contrib.auth.models import Group
from django.core.exceptions import PermissionDenied


ROLE_STAFF = "staff"
ROLE_TEAM_LEAD = "team_lead"
ROLE_MANAGER = "manager"
ROLE_OWNER = "owner"
ROLE_SUPERUSER = "superuser"

CRM_ROLE_ORDER = (
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    ROLE_MANAGER,
    ROLE_OWNER,
)

CRM_ROLE_CHOICES = (
    (ROLE_STAFF, "Staff"),
    (ROLE_TEAM_LEAD, "Team Lead"),
    (ROLE_MANAGER, "Manager"),
    (ROLE_OWNER, "Owner"),
)

CRM_ROLE_LABELS = dict(CRM_ROLE_CHOICES)
CRM_ROLE_LABELS[ROLE_SUPERUSER] = "Superuser"
CRM_ROLE_RANK = {role: index for index, role in enumerate(CRM_ROLE_ORDER)}
MANAGER_ASSIGNABLE_ROLES = frozenset((ROLE_STAFF, ROLE_TEAM_LEAD))
MANAGER_ASSIGNABLE_ROLE_CHOICES = tuple(
    choice for choice in CRM_ROLE_CHOICES if choice[0] in MANAGER_ASSIGNABLE_ROLES
)


def get_role_label(role_name):
    return CRM_ROLE_LABELS.get(role_name, "Unassigned")


def get_user_crm_roles(user):
    if not user or not getattr(user, "is_authenticated", False):
        return []
    if getattr(user, "is_superuser", False):
        return [ROLE_SUPERUSER]
    roles = [
        group.name
        for group in user.groups.all()
        if group.name in CRM_ROLE_RANK
    ]
    return sorted(roles, key=lambda role_name: CRM_ROLE_RANK[role_name])


def get_user_crm_role(user):
    if not user or not getattr(user, "is_authenticated", False):
        return None
    if getattr(user, "is_superuser", False):
        return ROLE_SUPERUSER
    roles = get_user_crm_roles(user)
    if len(roles) == 1:
        return roles[0]
    return None


def get_admin_actor_role(user):
    return get_user_crm_role(user)


def get_admin_assignable_role_choices(user):
    if get_admin_actor_role(user) == ROLE_MANAGER:
        return MANAGER_ASSIGNABLE_ROLE_CHOICES
    return CRM_ROLE_CHOICES


def is_same_admin_user(actor, target):
    return bool(actor and target and getattr(actor, "pk", None) and actor.pk == target.pk)


def role_meets_minimum(role_name, minimum_role):
    if role_name == ROLE_SUPERUSER:
        return True
    if role_name not in CRM_ROLE_RANK or minimum_role not in CRM_ROLE_RANK:
        return False
    return CRM_ROLE_RANK[role_name] >= CRM_ROLE_RANK[minimum_role]


def user_has_minimum_crm_role(user, minimum_role):
    return role_meets_minimum(get_user_crm_role(user), minimum_role)


def user_has_valid_crm_role(user):
    return get_user_crm_role(user) is not None


def get_user_role_status(user):
    if not user or not getattr(user, "is_authenticated", False):
        return "anonymous"
    if getattr(user, "is_superuser", False):
        return "superuser"
    roles = get_user_crm_roles(user)
    if not roles:
        return "missing"
    if len(roles) > 1:
        return "multiple"
    return "valid"


def user_can_view_admin_target(actor, target):
    if not actor or not getattr(actor, "is_authenticated", False) or target is None:
        return False
    if getattr(actor, "is_superuser", False):
        return True
    if getattr(target, "is_superuser", False):
        return False
    return get_admin_actor_role(actor) in {ROLE_MANAGER, ROLE_OWNER}


def user_can_change_admin_target(actor, target):
    return user_can_edit_admin_target_profile(actor, target)


def user_can_edit_admin_target_profile(actor, target):
    if not user_can_view_admin_target(actor, target):
        return False
    if getattr(actor, "is_superuser", False):
        return True

    actor_role = get_admin_actor_role(actor)
    target_role = get_user_crm_role(target)
    if actor_role == ROLE_OWNER:
        return True
    if actor_role == ROLE_MANAGER:
        return is_same_admin_user(actor, target) or target_role in MANAGER_ASSIGNABLE_ROLES
    return False


def user_can_edit_admin_target_access(actor, target):
    if not user_can_view_admin_target(actor, target):
        return False
    if getattr(actor, "is_superuser", False):
        return True

    actor_role = get_admin_actor_role(actor)
    target_role = get_user_crm_role(target)
    if actor_role == ROLE_OWNER:
        return not is_same_admin_user(actor, target)
    if actor_role == ROLE_MANAGER:
        return (not is_same_admin_user(actor, target)) and target_role in MANAGER_ASSIGNABLE_ROLES
    return False


def user_can_delete_admin_target(actor, target):
    if target is None:
        return False
    if getattr(actor, "is_superuser", False):
        return True

    actor_role = get_admin_actor_role(actor)
    if is_same_admin_user(actor, target):
        return False
    if actor_role == ROLE_OWNER:
        return user_can_view_admin_target(actor, target)
    if actor_role == ROLE_MANAGER:
        return get_user_crm_role(target) in MANAGER_ASSIGNABLE_ROLES
    return False


def user_can_reset_admin_target_password(actor, target):
    if not user_can_view_admin_target(actor, target):
        return False
    if getattr(actor, "is_superuser", False):
        return True

    actor_role = get_admin_actor_role(actor)
    target_role = get_user_crm_role(target)
    if actor_role == ROLE_OWNER:
        return True
    if actor_role == ROLE_MANAGER:
        return is_same_admin_user(actor, target) or target_role in MANAGER_ASSIGNABLE_ROLES
    return False


def user_can_access_site_branding_admin(user):
    return get_admin_actor_role(user) in {ROLE_OWNER, ROLE_SUPERUSER}


def sync_user_staff_status(user, save=True):
    should_be_staff = (
        getattr(user, "is_superuser", False)
        or user_has_minimum_crm_role(user, ROLE_MANAGER)
    )
    if user.is_staff != should_be_staff:
        user.is_staff = should_be_staff
        if save and user.pk:
            user.save(update_fields=["is_staff"])
    elif save and user.pk and user.is_superuser and not user.is_staff:
        user.save(update_fields=["is_staff"])


def clear_crm_roles(user, save=True):
    if not user.pk:
        raise ValueError("User must be saved before roles can be cleared.")
    role_groups = list(Group.objects.filter(name__in=CRM_ROLE_ORDER))
    if role_groups:
        user.groups.remove(*role_groups)
    sync_user_staff_status(user, save=save)


def assign_crm_role(user, role_name, save=True):
    if role_name not in CRM_ROLE_RANK:
        raise ValueError(f"Unknown CRM role: {role_name}")
    if not user.pk:
        raise ValueError("User must be saved before a CRM role can be assigned.")
    role_groups = list(Group.objects.filter(name__in=CRM_ROLE_ORDER))
    if role_groups:
        user.groups.remove(*role_groups)
    role_group = Group.objects.get(name=role_name)
    user.groups.add(role_group)
    sync_user_staff_status(user, save=save)


def crm_role_required(minimum_role=ROLE_STAFF):
    def decorator(view_func):
        @login_required
        @wraps(view_func)
        def wrapped_view(request, *args, **kwargs):
            if not user_has_minimum_crm_role(request.user, minimum_role):
                raise PermissionDenied("You do not have permission to access this CRM page.")
            return view_func(request, *args, **kwargs)

        return wrapped_view

    return decorator
