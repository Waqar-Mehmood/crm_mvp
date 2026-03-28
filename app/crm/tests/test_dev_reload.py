import importlib
from pathlib import Path

from django.urls import Resolver404, clear_url_caches, resolve, reverse

from . import Client, TestCase, json, override_settings, shutil, tempfile
from crm.dev_reload import get_dev_reload_token


class DevReloadTokenTests(TestCase):
    def setUp(self):
        self.temp_base_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_base_dir, ignore_errors=True)

    def test_reload_token_changes_when_watched_files_change(self):
        template_dir = Path(self.temp_base_dir) / "crm/templates/crm"
        static_dir = Path(self.temp_base_dir) / "crm/static/crm"
        template_dir.mkdir(parents=True, exist_ok=True)
        static_dir.mkdir(parents=True, exist_ok=True)

        template_path = template_dir / "sample.html"
        static_path = static_dir / "base.js"
        template_path.write_text("before", encoding="utf-8")
        static_path.write_text("console.log('one');", encoding="utf-8")

        initial_token = get_dev_reload_token(base_dir=Path(self.temp_base_dir))

        template_path.write_text("after", encoding="utf-8")
        changed_token = get_dev_reload_token(base_dir=Path(self.temp_base_dir))

        self.assertNotEqual(initial_token, changed_token)

    def test_reload_token_ignores_hidden_and_node_modules_entries(self):
        template_dir = Path(self.temp_base_dir) / "crm/templates/crm"
        ignored_dir = Path(self.temp_base_dir) / "crm/static/node_modules"
        hidden_dir = Path(self.temp_base_dir) / "crm/static/.cache"
        template_dir.mkdir(parents=True, exist_ok=True)
        ignored_dir.mkdir(parents=True, exist_ok=True)
        hidden_dir.mkdir(parents=True, exist_ok=True)

        watched_path = template_dir / "sample.html"
        watched_path.write_text("stable", encoding="utf-8")
        ignored_path = ignored_dir / "bundle.js"
        ignored_path.write_text("one", encoding="utf-8")
        hidden_path = hidden_dir / "shadow.js"
        hidden_path.write_text("one", encoding="utf-8")

        initial_token = get_dev_reload_token(base_dir=Path(self.temp_base_dir))

        ignored_path.write_text("two", encoding="utf-8")
        hidden_path.write_text("two", encoding="utf-8")
        unchanged_token = get_dev_reload_token(base_dir=Path(self.temp_base_dir))

        self.assertEqual(initial_token, unchanged_token)


class DevReloadEndpointTests(TestCase):
    def setUp(self):
        self.client = Client()
        self.addCleanup(self._restore_root_urlconf)

    def _reload_root_urlconf(self):
        import config.urls as root_urls

        clear_url_caches()
        importlib.reload(root_urls)

    def _restore_root_urlconf(self):
        self._reload_root_urlconf()

    def test_debug_false_omits_route_and_body_attributes(self):
        with override_settings(DEBUG=False):
            self._reload_root_urlconf()

            with self.assertRaises(Resolver404):
                resolve("/__dev__/reload-token/")

            response = self.client.get(reverse("login"))

        self.assertEqual(response.status_code, 200)
        self.assertNotContains(response, 'data-dev-reload-enabled="true"')
        self.assertNotContains(response, 'data-dev-reload-url="/__dev__/reload-token/"')

    def test_debug_true_exposes_route_and_layout_attributes(self):
        with override_settings(DEBUG=True):
            self._reload_root_urlconf()

            match = resolve("/__dev__/reload-token/")
            response = self.client.get(reverse("login"))
            token_response = self.client.get("/__dev__/reload-token/")

        self.assertEqual(match.view_name, "dev_reload_token")
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, 'data-dev-reload-enabled="true"')
        self.assertContains(response, 'data-dev-reload-url="/__dev__/reload-token/"')
        self.assertContains(response, 'data-dev-reload-interval="1500"')
        self.assertEqual(token_response.status_code, 200)
        self.assertEqual(token_response["Content-Type"], "application/json")
        self.assertIn("no-cache", token_response["Cache-Control"])
        self.assertIn("no-store", token_response["Cache-Control"])
        self.assertIsInstance(json.loads(token_response.content)["token"], str)
