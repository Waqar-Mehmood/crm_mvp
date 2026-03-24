"""Dashboard views."""

from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db.models import Count, Q
from django.shortcuts import render
from django.utils import timezone

from crm.auth import (
    CRM_ROLE_ORDER,
    ROLE_MANAGER,
    ROLE_OWNER,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    crm_role_required,
    get_role_label,
    get_user_crm_role,
    user_has_minimum_crm_role,
)
from crm.models import Company, Contact, ImportFile, ImportRow

RECENT_ITEMS_LIMIT = 5


def _get_dashboard_flags(user):
    is_owner_dashboard = user_has_minimum_crm_role(user, ROLE_OWNER)
    is_manager_dashboard = (
        not is_owner_dashboard
        and user_has_minimum_crm_role(user, ROLE_MANAGER)
    )
    is_team_lead_dashboard = (
        not is_owner_dashboard
        and not is_manager_dashboard
        and user_has_minimum_crm_role(user, ROLE_TEAM_LEAD)
    )
    return {
        "is_owner_dashboard": is_owner_dashboard,
        "is_manager_dashboard": is_manager_dashboard,
        "is_team_lead_dashboard": is_team_lead_dashboard,
        "is_staff_dashboard": not (
            is_owner_dashboard or is_manager_dashboard or is_team_lead_dashboard
        ),
    }


def _dashboard_copy(flags):
    if flags["is_owner_dashboard"]:
        return {
            "title": "Owner dashboard",
            "description": (
                "Start with the broad business picture, then move into team coverage "
                "and import risks that need attention."
            ),
            "scope_label": "Executive view",
        }
    if flags["is_manager_dashboard"]:
        return {
            "title": "Operations dashboard",
            "description": (
                "Track team throughput, recent record growth, and import cleanup work "
                "from one operating view."
            ),
            "scope_label": "Manager view",
        }
    if flags["is_team_lead_dashboard"]:
        return {
            "title": "Delivery dashboard",
            "description": (
                "Focus on day-to-day import flow, recent CRM additions, and batches "
                "that still need follow-up."
            ),
            "scope_label": "Team lead view",
        }
    return {
        "title": "Workspace dashboard",
        "description": (
            "Review the latest CRM additions and shared import activity available from "
            "your staff workspace."
        ),
        "scope_label": "Staff view",
    }


def _annotated_import_files():
    return ImportFile.objects.annotate(
        stored_rows=Count("rows", distinct=True),
        review_rows=Count(
            "rows",
            filter=Q(rows__company__isnull=True, rows__contact__isnull=True),
            distinct=True,
        ),
    )


def _build_recent_contacts():
    contacts = []
    recent_contacts = Contact.objects.prefetch_related("companies").order_by(
        "-created_at",
        "-id",
    )[:RECENT_ITEMS_LIMIT]
    for contact in recent_contacts:
        company_names = [company.name for company in contact.companies.all()]
        contacts.append(
            {
                "name": contact.full_name,
                "title": contact.title or "No title recorded",
                "companies": ", ".join(company_names[:2]) or "No linked company",
                "created_at": contact.created_at,
            }
        )
    return contacts


def _build_team_section(flags):
    if not (flags["is_owner_dashboard"] or flags["is_manager_dashboard"]):
        return {"role_counts": [], "recent_members": []}

    visible_roles = CRM_ROLE_ORDER
    if flags["is_manager_dashboard"]:
        visible_roles = (ROLE_STAFF, ROLE_TEAM_LEAD)

    role_counts = {role_name: 0 for role_name in visible_roles}
    members = []
    fallback_timestamp = timezone.now() - timedelta(days=36500)
    user_model = get_user_model()
    for user in user_model.objects.filter(is_active=True).prefetch_related("groups"):
        role_name = get_user_crm_role(user)
        if role_name not in role_counts:
            continue
        role_counts[role_name] += 1
        members.append(
            {
                "username": user.username,
                "role_label": get_role_label(role_name),
                "last_login": user.last_login,
                "date_joined": getattr(user, "date_joined", None),
            }
        )

    members.sort(
        key=lambda item: (
            item["last_login"] is not None,
            item["last_login"] or item["date_joined"] or fallback_timestamp,
        ),
        reverse=True,
    )

    return {
        "role_counts": [
            {"label": get_role_label(role_name), "value": role_counts[role_name]}
            for role_name in visible_roles
        ],
        "recent_members": members[:RECENT_ITEMS_LIMIT],
        "managed_member_total": sum(role_counts.values()),
    }


def _build_summary_cards(flags, totals, team_section):
    if flags["is_owner_dashboard"]:
        return [
            {
                "label": "Companies",
                "value": totals["companies_total"],
                "subtext": "Total organizations tracked across the CRM.",
            },
            {
                "label": "Contacts",
                "value": totals["contacts_total"],
                "subtext": "People captured across imported and linked records.",
            },
            {
                "label": "Import batches",
                "value": totals["imports_total"],
                "subtext": f'{totals["imports_last_30_days"]} batches updated in the last 30 days.',
            },
            {
                "label": "Rows needing review",
                "value": totals["review_rows_total"],
                "subtext": "Import rows still missing both linked company and contact records.",
            },
        ]

    if flags["is_manager_dashboard"]:
        return [
            {
                "label": "Managed seats",
                "value": team_section["managed_member_total"],
                "subtext": "Staff and team leads currently active in the CRM team layer.",
            },
            {
                "label": "Imports this month",
                "value": totals["imports_last_30_days"],
                "subtext": "Recent batch updates moving through the operating pipeline.",
            },
            {
                "label": "Companies added",
                "value": totals["companies_last_30_days"],
                "subtext": "New organizations created in the last 30 days.",
            },
            {
                "label": "Rows needing review",
                "value": totals["review_rows_total"],
                "subtext": "Rows still waiting on final company/contact linkage.",
            },
        ]

    if flags["is_team_lead_dashboard"]:
        return [
            {
                "label": "Imports this week",
                "value": totals["imports_last_7_days"],
                "subtext": "Batch activity updated across the last 7 days.",
            },
            {
                "label": "Import rows",
                "value": totals["import_rows_total"],
                "subtext": "Stored source rows currently available for review.",
            },
            {
                "label": "Contacts added",
                "value": totals["contacts_last_30_days"],
                "subtext": "New people records created in the last 30 days.",
            },
            {
                "label": "Rows needing review",
                "value": totals["review_rows_total"],
                "subtext": "Rows still missing both linked company and contact records.",
            },
        ]

    return [
        {
            "label": "Companies",
            "value": totals["companies_total"],
            "subtext": "Current company records available across the shared CRM.",
        },
        {
            "label": "Contacts",
            "value": totals["contacts_total"],
            "subtext": "Current people records available across the shared CRM.",
        },
        {
            "label": "Recent imports",
            "value": totals["imports_last_30_days"],
            "subtext": "Import batches updated during the last 30 days.",
        },
        {
            "label": "Rows needing review",
            "value": totals["review_rows_total"],
            "subtext": "Shared rows still missing both linked company and contact records.",
        },
    ]


def _build_hero_metrics(flags, totals, team_section):
    role_label = "Staff"
    scope_value = totals["review_rows_total"]
    scope_label = "Rows needing review"
    scope_subtext = "Shared import follow-up still open across stored rows."

    if flags["is_owner_dashboard"]:
        role_label = "Owner"
        scope_value = team_section["managed_member_total"]
        scope_label = "Team members"
        scope_subtext = "CRM roles currently active across the team."
    elif flags["is_manager_dashboard"]:
        role_label = "Manager"
        scope_value = team_section["managed_member_total"]
        scope_label = "Managed seats"
        scope_subtext = "Team leads and staff currently in your operating layer."
    elif flags["is_team_lead_dashboard"]:
        role_label = "Team Lead"
        scope_value = totals["imports_last_7_days"]
        scope_label = "Imports this week"
        scope_subtext = "Recent batch updates across the last 7 days."

    return [
        {
            "label": "Your role",
            "value": role_label,
            "subtext": "One dashboard template with role-aware sections.",
        },
        {
            "label": scope_label,
            "value": scope_value,
            "subtext": scope_subtext,
        },
    ]


@crm_role_required(ROLE_STAFF)
def dashboard_home(request):
    flags = _get_dashboard_flags(request.user)
    copy = _dashboard_copy(flags)
    now = timezone.now()
    last_7_days = now - timedelta(days=7)
    last_30_days = now - timedelta(days=30)

    totals = {
        "companies_total": Company.objects.count(),
        "contacts_total": Contact.objects.count(),
        "imports_total": ImportFile.objects.count(),
        "import_rows_total": ImportRow.objects.count(),
        "review_rows_total": ImportRow.objects.filter(
            company__isnull=True,
            contact__isnull=True,
        ).count(),
        "linked_company_rows_total": ImportRow.objects.filter(company__isnull=False).count(),
        "linked_contact_rows_total": ImportRow.objects.filter(contact__isnull=False).count(),
        "companies_last_30_days": Company.objects.filter(created_at__gte=last_30_days).count(),
        "contacts_last_30_days": Contact.objects.filter(created_at__gte=last_30_days).count(),
        "imports_last_7_days": ImportFile.objects.filter(updated_at__gte=last_7_days).count(),
        "imports_last_30_days": ImportFile.objects.filter(updated_at__gte=last_30_days).count(),
    }

    recent_imports = list(
        _annotated_import_files().order_by("-updated_at", "-id")[:RECENT_ITEMS_LIMIT]
    )
    follow_up_imports = list(
        _annotated_import_files()
        .filter(review_rows__gt=0)
        .order_by("-updated_at", "-id")[:RECENT_ITEMS_LIMIT]
    )
    recent_companies = Company.objects.order_by("-created_at", "-id")[:RECENT_ITEMS_LIMIT]
    recent_contacts = _build_recent_contacts()
    team_section = _build_team_section(flags)

    context = {
        **flags,
        "dashboard_title": copy["title"],
        "dashboard_description": copy["description"],
        "dashboard_scope_label": copy["scope_label"],
        "hero_metrics": _build_hero_metrics(flags, totals, team_section),
        "summary_cards": _build_summary_cards(flags, totals, team_section),
        "recent_imports": recent_imports,
        "follow_up_imports": follow_up_imports,
        "recent_companies": recent_companies,
        "recent_contacts": recent_contacts,
        "team_role_counts": team_section["role_counts"],
        "recent_team_members": team_section["recent_members"],
        "review_rows_total": totals["review_rows_total"],
        "linked_company_rows_total": totals["linked_company_rows_total"],
        "linked_contact_rows_total": totals["linked_contact_rows_total"],
        "imports_last_7_days": totals["imports_last_7_days"],
    }
    return render(request, "crm/dashboard/home.html", context)
