"""Dashboard views."""

from __future__ import annotations

from datetime import timedelta

from django.contrib.auth import get_user_model
from django.db.models import Count, Q
from django.shortcuts import render
from django.urls import reverse
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
DETAIL_CARDS_PARTIAL = "crm/components/dashboard/panel_detail_cards.html"
STATS_AND_CARDS_PARTIAL = "crm/components/dashboard/panel_stats_and_cards.html"
ACTION_BUTTONS_PARTIAL = "crm/components/dashboard/panel_actions.html"
RECORD_COLUMNS_PARTIAL = "crm/components/dashboard/panel_record_columns.html"


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


def _dashboard_action(label, href, variant="secondary"):
    return {
        "label": label,
        "href": href,
        "variant": variant,
    }


def _dashboard_panel(
    kicker,
    title,
    *,
    action=None,
    intro=None,
    stats=None,
    items=None,
    columns=None,
    actions=None,
    empty_text="",
    content_partial=DETAIL_CARDS_PARTIAL,
):
    return {
        "kicker": kicker,
        "title": title,
        "action": action,
        "intro": intro,
        "stats": stats or [],
        "items": items or [],
        "columns": columns or [],
        "actions": actions or [],
        "empty_text": empty_text,
        "content_partial": content_partial,
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


def _format_datetime(value):
    if not value:
        return ""
    return timezone.localtime(value).strftime("%Y-%m-%d %H:%M")


def _format_date(value):
    if not value:
        return ""
    return timezone.localtime(value).strftime("%Y-%m-%d")


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
                "primary": contact.full_name,
                "secondary": (
                    f'{contact.title or "No title recorded"}'
                    f' · {", ".join(company_names[:2]) or "No linked company"}'
                    f' · {_format_date(contact.created_at)}'
                ),
            }
        )
    return contacts


def _build_recent_companies():
    companies = []
    recent_companies = Company.objects.order_by("-created_at", "-id")[:RECENT_ITEMS_LIMIT]
    for company in recent_companies:
        companies.append(
            {
                "primary": company.name,
                "secondary": (
                    f'{company.city or "No city"}, {company.country or "No country"}'
                    f' · {_format_date(company.created_at)}'
                ),
            }
        )
    return companies


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


def _build_import_cards(import_files, *, kicker, badge_builder, body_builder=None, link_label):
    cards = []
    for import_file in import_files:
        cards.append(
            {
                "kicker": kicker,
                "title": import_file.file_name,
                "badge": badge_builder(import_file),
                "meta": f"Updated {_format_datetime(import_file.updated_at)}",
                "body": body_builder(import_file) if body_builder else "",
                "link": {
                    "href": reverse("import_file_detail", args=[import_file.id]),
                    "label": link_label,
                },
            }
        )
    return cards


def _build_team_member_cards(members):
    cards = []
    for member in members:
        cards.append(
            {
                "kicker": "Team member",
                "title": member["username"],
                "badge": member["role_label"],
                "meta": (
                    f'Last login {_format_datetime(member["last_login"])}'
                    if member["last_login"]
                    else "No login recorded yet."
                ),
                "body": "",
                "link": None,
            }
        )
    return cards


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


def _build_hero_actions(can_import):
    actions = [
        _dashboard_action("Browse companies", reverse("company_list"), variant="primary"),
        _dashboard_action("Browse contacts", reverse("contact_list")),
    ]

    if can_import:
        actions.append(_dashboard_action("Start a new import", reverse("import_upload")))
    else:
        actions.append(_dashboard_action("Review import ledger", reverse("import_file_list")))

    return actions


def _build_quick_actions(scope_label, can_import):
    actions = [
        _dashboard_action("Open companies", reverse("company_list"), variant="primary"),
        _dashboard_action("Open contacts", reverse("contact_list")),
        _dashboard_action("Open import ledger", reverse("import_file_list")),
    ]

    if can_import:
        actions.append(_dashboard_action("Upload import file", reverse("import_upload")))
        actions.append(_dashboard_action("Import Google Sheet", reverse("import_google_sheets")))

    return _dashboard_panel(
        "Quick actions",
        scope_label,
        actions=actions,
        content_partial=ACTION_BUTTONS_PARTIAL,
    )


def _build_recent_imports_panel(recent_imports):
    return _dashboard_panel(
        "Recent activity",
        "Recent imports",
        action=_dashboard_action("Open ledger", reverse("import_file_list")),
        items=_build_import_cards(
            recent_imports,
            kicker="Import batch",
            badge_builder=lambda import_file: f"{import_file.stored_rows} rows",
            body_builder=lambda import_file: (
                f"{import_file.review_rows} stored rows still need review."
                if import_file.review_rows
                else "All stored rows in this batch are linked to a company or contact."
            ),
            link_label="Open import detail",
        ),
        empty_text="No import batches have been stored yet.",
        content_partial=DETAIL_CARDS_PARTIAL,
    )


def _build_import_health_panel(totals, follow_up_imports):
    return _dashboard_panel(
        "Import health",
        "Review queue",
        stats=[
            {"label": "Rows Needing Review", "value": totals["review_rows_total"]},
            {"label": "Rows Linked To Companies", "value": totals["linked_company_rows_total"]},
            {"label": "Rows Linked To Contacts", "value": totals["linked_contact_rows_total"]},
            {"label": "Imports This Week", "value": totals["imports_last_7_days"]},
        ],
        items=_build_import_cards(
            follow_up_imports,
            kicker="Follow-up batch",
            badge_builder=lambda import_file: f"{import_file.review_rows} open",
            link_label="Review rows",
        ),
        empty_text="No batches currently have stored rows waiting for follow-up.",
        content_partial=STATS_AND_CARDS_PARTIAL,
    )


def _build_recent_records_panel(recent_companies, recent_contacts):
    return _dashboard_panel(
        "Recent records",
        "Latest additions",
        columns=[
            {
                "kicker": "Companies",
                "rows": recent_companies,
                "empty_text": "No companies have been added yet.",
            },
            {
                "kicker": "Contacts",
                "rows": recent_contacts,
                "empty_text": "No contacts have been added yet.",
            },
        ],
        content_partial=RECORD_COLUMNS_PARTIAL,
    )


def _build_team_activity_panel(flags, team_section):
    title = (
        "Business and team coverage"
        if flags["is_owner_dashboard"]
        else "Team operations"
    )
    return _dashboard_panel(
        "Team activity",
        title,
        stats=team_section["role_counts"],
        items=_build_team_member_cards(team_section["recent_members"]),
        empty_text="No team members with CRM roles are currently available.",
        content_partial=STATS_AND_CARDS_PARTIAL,
    )


def _build_shared_queue_panel(flags, follow_up_imports):
    is_team_lead_dashboard = flags["is_team_lead_dashboard"]
    return _dashboard_panel(
        "Follow-up actions" if is_team_lead_dashboard else "Shared workspace",
        "Delivery queue" if is_team_lead_dashboard else "Shared review queue",
        intro=(
            "Use the latest batch signals below to keep import quality tight and push "
            "unresolved rows through the mapping flow quickly."
            if is_team_lead_dashboard
            else "Ownership tracking is not stored on records yet, so this staff view "
            "shows the shared queue and latest batch signals available to browse."
        ),
        items=_build_import_cards(
            follow_up_imports,
            kicker="Open batch",
            badge_builder=lambda import_file: f"{import_file.review_rows} open",
            link_label="Open review rows",
        ),
        empty_text="The shared queue is clear right now.",
        content_partial=DETAIL_CARDS_PARTIAL,
    )


def _build_dashboard_rows(copy, totals, recent_imports, follow_up_imports, recent_companies, recent_contacts, can_import):
    return [
        {
            "panels": [
                _build_recent_imports_panel(recent_imports),
                _build_quick_actions(copy["scope_label"], can_import),
            ],
        },
        {
            "panels": [
                _build_import_health_panel(totals, follow_up_imports),
                _build_recent_records_panel(recent_companies, recent_contacts),
            ],
        },
    ]


def _build_role_panel(flags, team_section, follow_up_imports):
    if flags["is_owner_dashboard"] or flags["is_manager_dashboard"]:
        return _build_team_activity_panel(flags, team_section)
    return _build_shared_queue_panel(flags, follow_up_imports)


@crm_role_required(ROLE_STAFF)
def dashboard_home(request):
    flags = _get_dashboard_flags(request.user)
    copy = _dashboard_copy(flags)
    can_import = user_has_minimum_crm_role(request.user, ROLE_TEAM_LEAD)
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
    recent_companies = _build_recent_companies()
    recent_contacts = _build_recent_contacts()
    team_section = _build_team_section(flags)

    context = {
        **flags,
        "dashboard_title": copy["title"],
        "dashboard_description": copy["description"],
        "hero_metrics": _build_hero_metrics(flags, totals, team_section),
        "hero_actions": _build_hero_actions(can_import),
        "summary_cards": _build_summary_cards(flags, totals, team_section),
        "dashboard_rows": _build_dashboard_rows(
            copy,
            totals,
            recent_imports,
            follow_up_imports,
            recent_companies,
            recent_contacts,
            can_import,
        ),
        "role_panel": _build_role_panel(flags, team_section, follow_up_imports),
    }
    return render(request, "crm/dashboard/home.html", context)
