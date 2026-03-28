from __future__ import annotations

import hashlib
import os
from pathlib import Path

from django.conf import settings


WATCHED_PATHS = (
    Path("crm/templates"),
    Path("crm/static"),
)
IGNORED_NAMES = {
    "__pycache__",
    "media",
    "node_modules",
    "uploads",
}


def _is_ignored(name: str) -> bool:
    return name.startswith(".") or name in IGNORED_NAMES


def _iter_file_signatures(base_dir: Path):
    for relative_root in WATCHED_PATHS:
        root = base_dir / relative_root
        if not root.exists():
            continue

        for current_root, dirs, files in os.walk(root):
            dirs[:] = [directory for directory in sorted(dirs) if not _is_ignored(directory)]

            for filename in sorted(files):
                if _is_ignored(filename):
                    continue

                file_path = Path(current_root) / filename
                try:
                    stat_result = file_path.stat()
                except OSError:
                    continue

                yield (
                    file_path.relative_to(base_dir).as_posix(),
                    stat_result.st_mtime_ns,
                    stat_result.st_size,
                )


def get_dev_reload_token(base_dir: Path | None = None) -> str:
    source_root = Path(base_dir or settings.BASE_DIR)
    digest = hashlib.sha1()

    for relative_path, modified_ns, size in _iter_file_signatures(source_root):
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(modified_ns).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\n")

    return digest.hexdigest()
