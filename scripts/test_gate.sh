#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
    cat <<'EOF'
Usage: scripts/test_gate.sh <scope>

Scopes:
  imports
  contacts
  companies
  dashboard
  auth
  shared
  full

Guidance:
  - page-specific template/view/model change: run that page scope
  - shared component/layout/JS/theme/form-workspace/list-workspace change: run shared
  - major backend or uncertain cross-cutting change: run full
EOF
}

SCOPE="${1:-}"
if [[ -z "$SCOPE" ]]; then
    usage
    exit 1
fi

collect_changed_files() {
    if git rev-parse --verify HEAD >/dev/null 2>&1; then
        git diff --name-only HEAD --
    fi
    git ls-files --others --exclude-standard
}

CHANGED_FILES="$(collect_changed_files | sed '/^$/d' | sort -u)"

changed_file_matches() {
    local pattern="$1"
    printf '%s\n' "$CHANGED_FILES" | grep -Eq "$pattern"
}

run_js_syntax_check() {
    echo "==> JS syntax check"
    docker run --rm -v "$ROOT_DIR:/work" -w /work node:20 sh -lc \
        'find app/crm/static/crm -path "app/crm/static/crm/vendor" -prune -o -type f -name "*.js" -print | sort | while read -r file; do node --check "$file"; done'
}

run_css_build() {
    echo "==> Tailwind build"
    docker run --rm -v "$ROOT_DIR:/work" -w /work node:20 sh -lc 'npm run build:css'
}

run_django_tests() {
    echo "==> Django tests: $*"
    docker compose exec -T web python manage.py test "$@" --noinput
}

maybe_run_targeted_asset_checks() {
    if changed_file_matches '^(app/crm/static/crm/.*\.js)$'; then
        run_js_syntax_check
    else
        echo "==> Skipping JS syntax check (no app JS changes detected)"
    fi

    if changed_file_matches '^(tailwind\.config\.js|package\.json|package-lock\.json|postcss\.config\.js|app/crm/static_src/.*|app/crm/templates/.*\.html)$'; then
        run_css_build
    else
        echo "==> Skipping Tailwind build (no frontend style changes detected)"
    fi
}

case "$SCOPE" in
    imports)
        maybe_run_targeted_asset_checks
        run_django_tests crm.tests.test_imports crm.tests.test_import_components crm.tests.test_auth
        ;;
    contacts)
        maybe_run_targeted_asset_checks
        run_django_tests crm.tests.test_contacts crm.tests.test_auth
        ;;
    companies)
        maybe_run_targeted_asset_checks
        run_django_tests crm.tests.test_companies crm.tests.test_auth
        ;;
    dashboard)
        maybe_run_targeted_asset_checks
        run_django_tests crm.tests.test_dashboard crm.tests.test_auth
        ;;
    auth)
        run_django_tests crm.tests.test_auth crm.tests.test_misc
        ;;
    shared)
        run_js_syntax_check
        run_css_build
        run_django_tests \
            crm.tests.test_imports \
            crm.tests.test_import_components \
            crm.tests.test_contacts \
            crm.tests.test_companies \
            crm.tests.test_dashboard \
            crm.tests.test_auth \
            crm.tests.test_misc \
            crm.tests.test_dev_reload
        ;;
    full)
        run_js_syntax_check
        run_css_build
        echo "==> Full Django suite"
        docker compose exec -T web python manage.py test crm --noinput
        ;;
    *)
        usage
        exit 1
        ;;
esac

