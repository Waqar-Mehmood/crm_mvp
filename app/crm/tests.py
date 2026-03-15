from datetime import timedelta

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.test import Client, TestCase, override_settings
from django.urls import reverse
from django.utils import timezone

from crm.auth import (
    CRM_ROLE_ORDER,
    ROLE_MANAGER,
    ROLE_OWNER,
    ROLE_STAFF,
    ROLE_TEAM_LEAD,
    assign_crm_role,
    get_admin_assignable_role_choices,
    get_user_crm_role,
    get_user_role_status,
    user_has_minimum_crm_role,
    user_has_valid_crm_role,
)
from crm.models import (
    Company,
    CompanyEmail,
    CompanyPhone,
    CompanySocialLink,
    Contact,
    ContactEmail,
    ContactPhone,
    ContactSocialLink,
    ImportFile,
    ImportRow,
    SiteBranding,
)


class CRMRoleTestMixin:
    default_password = "test-pass-123"

    def create_user(self, username, role=None, password=None, **extra_fields):
        user = get_user_model().objects.create_user(
            username=username,
            password=password or self.default_password,
            **extra_fields,
        )
        if role:
            assign_crm_role(user, role)
            user.refresh_from_db()
        return user

    def create_superuser(self, username="superadmin", password=None, **extra_fields):
        return get_user_model().objects.create_superuser(
            username=username,
            password=password or self.default_password,
            **extra_fields,
        )


class CRMRoleHelperTests(CRMRoleTestMixin, TestCase):
    def test_crm_groups_exist_after_migration(self):
        self.assertEqual(Group.objects.filter(name__in=CRM_ROLE_ORDER).count(), len(CRM_ROLE_ORDER))

    def test_single_role_resolves_correctly(self):
        user = self.create_user("staff-user", role=ROLE_STAFF)

        self.assertEqual(get_user_crm_role(user), ROLE_STAFF)
        self.assertEqual(get_user_role_status(user), "valid")
        self.assertTrue(user_has_valid_crm_role(user))
        self.assertFalse(user.is_staff)

    def test_user_with_no_role_is_invalid(self):
        user = self.create_user("no-role")

        self.assertIsNone(get_user_crm_role(user))
        self.assertEqual(get_user_role_status(user), "missing")
        self.assertFalse(user_has_valid_crm_role(user))
        self.assertFalse(user_has_minimum_crm_role(user, ROLE_STAFF))

    def test_user_with_multiple_roles_is_invalid(self):
        user = self.create_user("multi-role")
        user.groups.add(
            Group.objects.get(name=ROLE_STAFF),
            Group.objects.get(name=ROLE_TEAM_LEAD),
        )
        user.refresh_from_db()

        self.assertIsNone(get_user_crm_role(user))
        self.assertEqual(get_user_role_status(user), "multiple")
        self.assertFalse(user_has_valid_crm_role(user))
        self.assertFalse(user.is_staff)

    def test_superuser_bypasses_crm_role_validation(self):
        user = self.create_superuser()

        self.assertTrue(user_has_valid_crm_role(user))
        self.assertEqual(get_user_role_status(user), "superuser")
        self.assertTrue(user_has_minimum_crm_role(user, ROLE_OWNER))
        self.assertTrue(user.is_staff)

    def test_assigning_and_switching_roles_syncs_staff_flag(self):
        user = self.create_user("role-switcher")

        assign_crm_role(user, ROLE_MANAGER)
        user.refresh_from_db()
        self.assertEqual(get_user_crm_role(user), ROLE_MANAGER)
        self.assertTrue(user.is_staff)

        assign_crm_role(user, ROLE_STAFF)
        user.refresh_from_db()
        self.assertEqual(get_user_crm_role(user), ROLE_STAFF)
        self.assertFalse(user.is_staff)

    def test_manager_assignable_role_choices_are_limited(self):
        user = self.create_user("manager-user", role=ROLE_MANAGER)

        assignable_roles = [role_name for role_name, _label in get_admin_assignable_role_choices(user)]

        self.assertEqual(assignable_roles, [ROLE_STAFF, ROLE_TEAM_LEAD])


class FrontendRoleAccessTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)
        self.manager_user = self.create_user("manager", role=ROLE_MANAGER)
        self.owner_user = self.create_user("owner", role=ROLE_OWNER)
        self.invalid_user = self.create_user("invalid-user")
        self.superuser = self.create_superuser(username="waqar")

        Company.objects.create(name="Acme Labs")
        Contact.objects.create(full_name="Jane Example")
        self.import_file = ImportFile.objects.create(file_name="seed.csv")
        ImportRow.objects.create(
            import_file=self.import_file,
            row_number=1,
            company_name="Acme Labs",
            contact_name="Jane Example",
        )

    def prime_import_session(self):
        session = self.client.session
        session["import_csv_temp_path"] = "/tmp/test-import.csv"
        session["import_csv_original_name"] = "test-import.csv"
        session["import_csv_headers"] = ["Company Name", "Email"]
        session.save()

    def test_anonymous_users_are_redirected_to_login(self):
        protected_urls = [
            reverse("home"),
            reverse("company_list"),
            reverse("contact_list"),
            reverse("import_file_list"),
            reverse("import_file_detail", args=[self.import_file.pk]),
            reverse("import_upload"),
            reverse("import_map_headers"),
        ]

        for url in protected_urls:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 302)
                self.assertIn(f"{reverse('login')}?next=", response["Location"])

    def test_staff_can_browse_but_not_access_import_mutations(self):
        self.client.login(username="staffer", password=self.default_password)

        for url in [
            reverse("company_list"),
            reverse("contact_list"),
            reverse("import_file_list"),
            reverse("import_file_detail", args=[self.import_file.pk]),
        ]:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 200)

        for url in [reverse("import_upload"), reverse("import_map_headers")]:
            with self.subTest(url=url):
                response = self.client.get(url)
                self.assertEqual(response.status_code, 403)

    def test_team_lead_and_above_can_access_import_pages(self):
        for username in ("teamlead", "manager", "owner", "waqar"):
            with self.subTest(username=username):
                self.client.force_login(get_user_model().objects.get(username=username))
                upload_response = self.client.get(reverse("import_upload"))
                self.assertEqual(upload_response.status_code, 200)

                self.prime_import_session()
                mapping_response = self.client.get(reverse("import_map_headers"))
                self.assertEqual(mapping_response.status_code, 200)
                self.client.logout()

    def test_invalid_role_user_gets_forbidden_on_crm_pages(self):
        self.client.login(username="invalid-user", password=self.default_password)

        browse_response = self.client.get(reverse("company_list"))
        upload_response = self.client.get(reverse("import_upload"))

        self.assertEqual(browse_response.status_code, 403)
        self.assertEqual(upload_response.status_code, 403)

    def test_multiple_role_user_gets_forbidden_on_crm_pages(self):
        user = self.create_user("broken-user", role=ROLE_STAFF)
        user.groups.add(Group.objects.get(name=ROLE_MANAGER))
        self.client.login(username="broken-user", password=self.default_password)

        response = self.client.get(reverse("contact_list"))

        self.assertEqual(response.status_code, 403)

    def test_upload_navigation_is_role_aware(self):
        self.client.login(username="staffer", password=self.default_password)
        staff_response = self.client.get(reverse("import_file_list"))
        self.assertEqual(staff_response.status_code, 200)
        self.assertNotContains(staff_response, 'href="/imports/upload/"')
        self.client.logout()

        self.client.login(username="teamlead", password=self.default_password)
        lead_response = self.client.get(reverse("import_file_list"))
        self.assertEqual(lead_response.status_code, 200)
        self.assertContains(lead_response, 'href="/imports/upload/"')

    def test_login_redirects_authenticated_users_to_companies(self):
        self.client.login(username="staffer", password=self.default_password)

        response = self.client.get(reverse("login"))

        self.assertRedirects(response, reverse("company_list"))

    def test_login_uses_next_parameter(self):
        response = self.client.post(
            reverse("login"),
            {
                "username": "staffer",
                "password": self.default_password,
                "next": reverse("contact_list"),
            },
        )

        self.assertRedirects(response, reverse("contact_list"))

    def test_logout_redirects_to_login(self):
        self.client.login(username="staffer", password=self.default_password)

        response = self.client.post(reverse("logout"))

        self.assertRedirects(response, reverse("login"))

    def test_login_template_renders_branding(self):
        response = self.client.get(reverse("login"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "The Zulfis CRM")


class AdminAccessTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)
        self.manager_user = self.create_user("manager", role=ROLE_MANAGER)
        self.owner_user = self.create_user("owner", role=ROLE_OWNER)
        self.superuser = self.create_superuser(username="superadmin")

    def test_staff_and_team_lead_cannot_access_admin(self):
        for username in ("staffer", "teamlead"):
            with self.subTest(username=username):
                self.client.login(username=username, password=self.default_password)
                response = self.client.get(reverse("admin:auth_user_changelist"))
                self.assertEqual(response.status_code, 302)
                self.assertIn(reverse("admin:login"), response["Location"])
                self.client.logout()

    def test_manager_owner_and_superuser_can_access_admin(self):
        for username in ("manager", "owner", "superadmin"):
            with self.subTest(username=username):
                self.client.login(username=username, password=self.default_password)
                index_response = self.client.get(reverse("admin:index"))
                changelist_response = self.client.get(reverse("admin:auth_user_changelist"))
                self.assertEqual(index_response.status_code, 200)
                self.assertEqual(changelist_response.status_code, 200)
                self.client.logout()


class UserAdminRoleManagementTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.admin_user = self.create_superuser(username="superadmin")
        self.target_user = self.create_user("target-user")
        self.client.force_login(self.admin_user)

    def update_user_role_via_admin(self, role_name):
        return self.client.post(
            reverse("admin:auth_user_change", args=[self.target_user.pk]),
            {
                "username": self.target_user.username,
                "first_name": "",
                "last_name": "",
                "email": "",
                "crm_role": role_name,
                "is_active": "on",
                "_save": "Save",
            },
            follow=True,
        )

    def test_assigning_manager_role_via_admin_updates_staff_status(self):
        response = self.update_user_role_via_admin(ROLE_MANAGER)
        self.target_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(get_user_crm_role(self.target_user), ROLE_MANAGER)
        self.assertTrue(self.target_user.is_staff)

    def test_switching_manager_to_staff_via_admin_removes_admin_access(self):
        self.update_user_role_via_admin(ROLE_MANAGER)
        self.update_user_role_via_admin(ROLE_STAFF)
        self.target_user.refresh_from_db()

        self.assertEqual(get_user_crm_role(self.target_user), ROLE_STAFF)
        self.assertFalse(self.target_user.is_staff)


class ManagerScopedAdminRestrictionsTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.manager_user = self.create_user("manager", role=ROLE_MANAGER)
        self.owner_user = self.create_user("owner", role=ROLE_OWNER)
        self.peer_manager = self.create_user("peer-manager", role=ROLE_MANAGER)
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.team_lead_user = self.create_user("teamlead", role=ROLE_TEAM_LEAD)
        self.superuser = self.create_superuser(username="superadmin")
        self.site_branding = SiteBranding.objects.create(site_name="The Zulfis CRM")

    def force_login(self, user):
        self.client.force_login(user)

    def admin_change_url(self, user):
        return reverse("admin:auth_user_change", args=[user.pk])

    def admin_delete_url(self, user):
        return reverse("admin:auth_user_delete", args=[user.pk])

    def admin_password_url(self, user):
        return reverse("admin:auth_user_password_change", args=[user.pk])

    def extract_role_choices(self, response):
        return [
            choice_value
            for choice_value, _choice_label in response.context["adminform"].form.fields["crm_role"].choices
            if choice_value
        ]

    def editable_field_names(self, response):
        return set(response.context["adminform"].form.fields.keys())

    def app_model_names(self, response, app_label):
        for app in response.context["app_list"]:
            if app["app_label"] == app_label:
                return {model["object_name"] for model in app["models"]}
        return set()

    def test_manager_add_form_shows_only_lower_roles(self):
        self.force_login(self.manager_user)

        response = self.client.get(reverse("admin:auth_user_add"))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.extract_role_choices(response), [ROLE_STAFF, ROLE_TEAM_LEAD])

    def test_owner_and_superuser_add_forms_show_all_roles(self):
        for actor in (self.owner_user, self.superuser):
            with self.subTest(actor=actor.username):
                self.force_login(actor)
                response = self.client.get(reverse("admin:auth_user_add"))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    self.extract_role_choices(response),
                    [ROLE_STAFF, ROLE_TEAM_LEAD, ROLE_MANAGER, ROLE_OWNER],
                )
                self.client.logout()

    def test_manager_change_form_for_lower_role_shows_only_lower_roles(self):
        self.force_login(self.manager_user)

        response = self.client.get(self.admin_change_url(self.staff_user))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["has_change_permission"], True)
        self.assertEqual(self.extract_role_choices(response), [ROLE_STAFF, ROLE_TEAM_LEAD])

    def test_manager_can_reassign_lower_role_user_to_team_lead(self):
        self.force_login(self.manager_user)

        response = self.client.post(
            self.admin_change_url(self.staff_user),
            {
                "username": self.staff_user.username,
                "first_name": "",
                "last_name": "",
                "email": "",
                "crm_role": ROLE_TEAM_LEAD,
                "is_active": "on",
                "_save": "Save",
            },
            follow=True,
        )
        self.staff_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(get_user_crm_role(self.staff_user), ROLE_TEAM_LEAD)

    def test_manager_cannot_escalate_lower_role_user_to_manager(self):
        self.force_login(self.manager_user)

        response = self.client.post(
            self.admin_change_url(self.staff_user),
            {
                "username": self.staff_user.username,
                "first_name": "",
                "last_name": "",
                "email": "",
                "crm_role": ROLE_MANAGER,
                "is_active": "on",
                "_save": "Save",
            },
        )
        self.staff_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Select a valid choice")
        self.assertEqual(get_user_crm_role(self.staff_user), ROLE_STAFF)

    def test_manager_changelist_shows_owner_and_managers_but_hides_superusers(self):
        self.force_login(self.manager_user)

        response = self.client.get(reverse("admin:auth_user_changelist"))

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, self.manager_user.username)
        self.assertContains(response, self.peer_manager.username)
        self.assertContains(response, self.owner_user.username)
        self.assertContains(response, self.staff_user.username)
        self.assertNotContains(response, self.superuser.username)

    def test_manager_can_open_owner_and_manager_records_in_read_only_mode(self):
        self.force_login(self.manager_user)

        for target in (self.peer_manager, self.owner_user):
            with self.subTest(target=target.username):
                response = self.client.get(self.admin_change_url(target))
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.context["has_change_permission"], False)
                self.assertNotContains(response, 'name="_save"')

    def test_manager_can_open_self_with_profile_only_edit_access(self):
        self.force_login(self.manager_user)

        response = self.client.get(self.admin_change_url(self.manager_user))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["has_change_permission"], True)
        self.assertContains(response, 'name="_save"')
        self.assertEqual(
            self.editable_field_names(response),
            {"first_name", "last_name", "email", "password"},
        )

    def test_owner_can_open_self_with_profile_only_edit_access(self):
        self.force_login(self.owner_user)

        response = self.client.get(self.admin_change_url(self.owner_user))

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["has_change_permission"], True)
        self.assertContains(response, 'name="_save"')
        self.assertEqual(
            self.editable_field_names(response),
            {"first_name", "last_name", "email", "password"},
        )

    def test_manager_cannot_edit_disallowed_targets_via_post(self):
        self.force_login(self.manager_user)

        for target in (self.peer_manager, self.owner_user):
            with self.subTest(target=target.username):
                response = self.client.post(
                    self.admin_change_url(target),
                    {
                        "username": target.username,
                        "first_name": "",
                        "last_name": "",
                        "email": "",
                        "crm_role": ROLE_STAFF,
                        "is_active": "on",
                        "_save": "Save",
                    },
                )
                self.assertEqual(response.status_code, 403)

    def test_manager_can_update_own_profile_without_changing_access_fields(self):
        self.force_login(self.manager_user)

        response = self.client.post(
            self.admin_change_url(self.manager_user),
            {
                "username": "hacked-manager",
                "first_name": "Rizwan",
                "last_name": "Manager",
                "email": "manager@example.com",
                "crm_role": ROLE_OWNER,
                "_save": "Save",
            },
            follow=True,
        )
        self.manager_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.manager_user.username, "manager")
        self.assertEqual(self.manager_user.first_name, "Rizwan")
        self.assertEqual(self.manager_user.last_name, "Manager")
        self.assertEqual(self.manager_user.email, "manager@example.com")
        self.assertEqual(get_user_crm_role(self.manager_user), ROLE_MANAGER)
        self.assertTrue(self.manager_user.is_active)

    def test_owner_can_update_own_profile_without_changing_access_fields(self):
        self.force_login(self.owner_user)

        response = self.client.post(
            self.admin_change_url(self.owner_user),
            {
                "username": "hacked-owner",
                "first_name": "Owner",
                "last_name": "Profile",
                "email": "owner@example.com",
                "crm_role": ROLE_STAFF,
                "_save": "Save",
            },
            follow=True,
        )
        self.owner_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.owner_user.username, "owner")
        self.assertEqual(self.owner_user.first_name, "Owner")
        self.assertEqual(self.owner_user.last_name, "Profile")
        self.assertEqual(self.owner_user.email, "owner@example.com")
        self.assertEqual(get_user_crm_role(self.owner_user), ROLE_OWNER)
        self.assertTrue(self.owner_user.is_active)

    def test_manager_cannot_delete_self_or_higher_role_users(self):
        self.force_login(self.manager_user)

        for target in (self.manager_user, self.peer_manager, self.owner_user):
            with self.subTest(target=target.username):
                response = self.client.post(self.admin_delete_url(target), {"post": "yes"})
                self.assertEqual(response.status_code, 403)

    def test_owner_cannot_delete_self(self):
        self.force_login(self.owner_user)

        response = self.client.post(self.admin_delete_url(self.owner_user), {"post": "yes"})

        self.assertEqual(response.status_code, 403)

    def test_manager_can_access_password_change_for_self_and_lower_roles(self):
        self.force_login(self.manager_user)

        for target in (self.manager_user, self.staff_user, self.team_lead_user):
            with self.subTest(target=target.username):
                response = self.client.get(self.admin_password_url(target))
                self.assertEqual(response.status_code, 200)

    def test_manager_cannot_access_password_change_for_manager_or_above(self):
        self.force_login(self.manager_user)

        for target in (self.peer_manager, self.owner_user, self.superuser):
            with self.subTest(target=target.username):
                response = self.client.get(self.admin_password_url(target))
                self.assertEqual(response.status_code, 403)

    def test_manager_can_change_own_password(self):
        self.force_login(self.manager_user)

        response = self.client.post(
            self.admin_password_url(self.manager_user),
            {
                "password1": "manager-new-pass-123",
                "password2": "manager-new-pass-123",
            },
            follow=True,
        )
        self.manager_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.manager_user.check_password("manager-new-pass-123"))

    def test_owner_can_change_own_password(self):
        self.force_login(self.owner_user)

        response = self.client.post(
            self.admin_password_url(self.owner_user),
            {
                "password1": "owner-new-pass-123",
                "password2": "owner-new-pass-123",
            },
            follow=True,
        )
        self.owner_user.refresh_from_db()

        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.owner_user.check_password("owner-new-pass-123"))

    def test_manager_can_delete_lower_role_user(self):
        self.force_login(self.manager_user)

        confirm_response = self.client.get(self.admin_delete_url(self.team_lead_user))
        delete_response = self.client.post(self.admin_delete_url(self.team_lead_user), {"post": "yes"}, follow=True)

        self.assertEqual(confirm_response.status_code, 200)
        self.assertEqual(delete_response.status_code, 200)
        self.assertFalse(get_user_model().objects.filter(pk=self.team_lead_user.pk).exists())

    def test_manager_cannot_view_site_branding_in_index_or_direct_url(self):
        self.force_login(self.manager_user)

        index_response = self.client.get(reverse("admin:index"))
        changelist_response = self.client.get(reverse("admin:crm_sitebranding_changelist"))

        self.assertEqual(index_response.status_code, 200)
        self.assertNotIn("SiteBranding", self.app_model_names(index_response, "crm"))
        self.assertEqual(changelist_response.status_code, 403)

    def test_owner_and_superuser_can_access_site_branding(self):
        for actor in (self.owner_user, self.superuser):
            with self.subTest(actor=actor.username):
                self.force_login(actor)
                index_response = self.client.get(reverse("admin:index"))
                changelist_response = self.client.get(reverse("admin:crm_sitebranding_changelist"))
                self.assertEqual(index_response.status_code, 200)
                self.assertIn("SiteBranding", self.app_model_names(index_response, "crm"))
                self.assertEqual(changelist_response.status_code, 200)
                self.client.logout()


class AdvancedFilterTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)
        self.client.force_login(self.staff_user)

        self.acme = Company.objects.create(
            name="Acme Labs",
            industry="SaaS",
            company_size="12",
            revenue="$1M",
            address="1 Market Street",
            city="San Francisco",
            state="CA",
            country="US",
            notes="Outbound team",
        )
        self.brick = Company.objects.create(
            name="Brick Health",
            industry="Medical Facilities",
            company_size="4",
            city="Austin",
            state="TX",
            country="US",
        )
        self.cedar = Company.objects.create(
            name="Cedar Staffing",
            industry="Staffing and Recruiting",
            company_size="30",
            revenue="$5M",
            city="Los Angeles",
            state="CA",
            country="US",
        )

        CompanyPhone.objects.create(company=self.acme, phone="111-111", label="office")
        CompanyEmail.objects.create(company=self.acme, email="hello@acme.com", label="sales")
        CompanySocialLink.objects.create(company=self.acme, platform="linkedin", url="https://example.com/acme")
        CompanyEmail.objects.create(company=self.cedar, email="team@cedar.com", label="support")

        self.alice = Contact.objects.create(
            full_name="Alice Johnson",
            title="Growth Marketing Manager",
            notes="Works the SaaS pipeline",
        )
        self.bob = Contact.objects.create(
            full_name="Bob Stone",
            title="Director",
            notes="No linked company",
        )
        self.carla = Contact.objects.create(
            full_name="Carla Recruiter",
            title="Owner / Recruiter",
            email="carla@example.com",
            phone="555-0199",
        )

        ContactEmail.objects.create(contact=self.alice, email="alice@acme.com", label="work")
        ContactPhone.objects.create(contact=self.alice, phone="555-0101", label="work")
        ContactSocialLink.objects.create(contact=self.alice, platform="linkedin", url="https://example.com/alice")
        self.acme.contacts.add(self.alice)
        self.cedar.contacts.add(self.carla)

        base_time = timezone.now()
        self._set_created_at(self.acme, base_time - timedelta(days=12))
        self._set_created_at(self.brick, base_time - timedelta(days=5))
        self._set_created_at(self.cedar, base_time - timedelta(days=1))
        self._set_created_at(self.alice, base_time - timedelta(days=11))
        self._set_created_at(self.bob, base_time - timedelta(days=4))
        self._set_created_at(self.carla, base_time - timedelta(days=1))

        for index in range(12):
            company = Company.objects.create(
                name=f"California Extra {index + 1}",
                industry="SaaS",
                company_size="8",
                city="San Diego",
                state="CA",
                country="US",
            )
            self._set_created_at(company, base_time - timedelta(days=20))

        for index in range(12):
            contact = Contact.objects.create(
                full_name=f"Analyst {index + 1}",
                title="Analyst",
            )
            self.acme.contacts.add(contact)
            self._set_created_at(contact, base_time - timedelta(days=15))

    def _set_created_at(self, obj, created_at):
        obj.__class__.objects.filter(pk=obj.pk).update(created_at=created_at)
        obj.refresh_from_db()

    def test_company_filters_narrow_results_and_keep_form_values(self):
        response = self.client.get(
            reverse("company_list"),
            {
                "industry": "SaaS",
                "state": "CA",
                "size_min": "10",
                "has_email": "yes",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({company.name for company in response.context["companies"]}, {"Acme Labs"})
        self.assertEqual(response.context["filters"]["industry"], "SaaS")
        self.assertEqual(response.context["filters"]["state"], "CA")
        self.assertEqual(response.context["filters"]["size_min"], "10")
        self.assertEqual(response.context["filters"]["has_email"], "yes")
        self.assertContains(response, "Matching companies")
        self.assertContains(response, "Industry:")

    def test_company_revenue_and_created_date_filters_work(self):
        response = self.client.get(
            reverse("company_list"),
            {
                "revenue": "$5M",
                "created_from": str((timezone.now() - timedelta(days=2)).date()),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({company.name for company in response.context["companies"]}, {"Cedar Staffing"})

    def test_company_pagination_preserves_filters(self):
        response = self.client.get(reverse("company_list"), {"state": "CA", "page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertEqual(response.context["page_query"], "state=CA")
        self.assertContains(response, "?page=1&state=CA")

    def test_company_filtered_empty_state_appears_when_no_results_match(self):
        response = self.client.get(reverse("company_list"), {"industry": "Nonexistent"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No companies matched the current filters.")
        self.assertNotContains(response, "No companies have landed in the ledger.")

    def test_contact_filters_match_related_data_and_presence_rules(self):
        response = self.client.get(
            reverse("contact_list"),
            {
                "q": "alice@acme.com",
                "company": "Acme",
                "has_profile": "yes",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({contact.full_name for contact in response.context["contacts"]}, {"Alice Johnson"})
        self.assertEqual(response.context["filters"]["company"], "Acme")
        self.assertEqual(response.context["filters"]["has_profile"], "yes")

    def test_contact_created_and_presence_filters_work(self):
        response = self.client.get(
            reverse("contact_list"),
            {
                "has_company": "no",
                "has_email": "no",
                "has_phone": "no",
                "created_to": str((timezone.now() - timedelta(days=3)).date()),
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual({contact.full_name for contact in response.context["contacts"]}, {"Bob Stone"})

    def test_contact_pagination_preserves_filters(self):
        response = self.client.get(reverse("contact_list"), {"title": "Analyst", "page": 2})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.context["page_obj"].number, 2)
        self.assertEqual(response.context["page_query"], "title=Analyst")
        self.assertContains(response, "?page=1&title=Analyst")

    def test_contact_filtered_empty_state_appears_when_no_results_match(self):
        response = self.client.get(reverse("contact_list"), {"title": "Astronaut"})

        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "No contacts matched the current filters.")
        self.assertNotContains(response, "No contacts are available yet.")


@override_settings(
    DEBUG=False,
    ALLOWED_HOSTS=["testserver", "localhost", "127.0.0.1"],
    SITE_BRAND="The Zulfis CRM",
)
class NotFoundPageTests(CRMRoleTestMixin, TestCase):
    def setUp(self):
        self.client = Client()
        self.staff_user = self.create_user("staffer", role=ROLE_STAFF)

    def test_anonymous_missing_route_uses_branded_404_page(self):
        response = self.client.get("/missing-route/")

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "404.html")
        self.assertContains(response, "Page Not Found | The Zulfis CRM", status_code=404, html=False)
        self.assertContains(response, "The route", status_code=404)
        self.assertContains(response, "/missing-route/", status_code=404)
        self.assertContains(response, 'href="/login/"', status_code=404)

    def test_authenticated_missing_route_shows_companies_recovery_link(self):
        self.client.force_login(self.staff_user)

        response = self.client.get("/still-missing/")

        self.assertEqual(response.status_code, 404)
        self.assertTemplateUsed(response, "404.html")
        self.assertContains(response, "The Zulfis CRM", status_code=404)
        self.assertContains(response, "/still-missing/", status_code=404)
        self.assertContains(response, 'href="/companies/"', status_code=404)
