"""Session-backed state helpers for the import workflow."""

from __future__ import annotations


STAGED_IMPORTS_SESSION_KEY = "import_staged_sources"
ACTIVE_IMPORT_JOB_SESSION_KEY = "import_active_job_id"


class ImportSessionManager:
    """Manage staged import queue and active import job state in the session."""

    @staticmethod
    def get_staged_queue(request) -> list[dict]:
        queue = request.session.get(STAGED_IMPORTS_SESSION_KEY, [])
        if isinstance(queue, list):
            return queue
        return []

    @classmethod
    def add_to_queue(cls, request, staged_entry: dict) -> None:
        queue = cls.get_staged_queue(request)
        queue.append(dict(staged_entry or {}))
        request.session[STAGED_IMPORTS_SESSION_KEY] = queue
        request.session.modified = True

    @classmethod
    def pop_from_queue(cls, request) -> dict | None:
        queue = cls.get_staged_queue(request)
        if not queue:
            return None

        entry = queue.pop(0)
        if queue:
            request.session[STAGED_IMPORTS_SESSION_KEY] = queue
        else:
            request.session.pop(STAGED_IMPORTS_SESSION_KEY, None)
        request.session.modified = True
        return entry

    @staticmethod
    def get_active_job(request) -> str | None:
        job_id = request.session.get(ACTIVE_IMPORT_JOB_SESSION_KEY)
        if job_id in (None, ""):
            return None
        return str(job_id)

    @staticmethod
    def set_active_job(request, job_id: str) -> None:
        if job_id in (None, ""):
            request.session.pop(ACTIVE_IMPORT_JOB_SESSION_KEY, None)
        else:
            request.session[ACTIVE_IMPORT_JOB_SESSION_KEY] = str(job_id)
        request.session.modified = True

    @staticmethod
    def mark_job_complete(request) -> None:
        request.session.pop(ACTIVE_IMPORT_JOB_SESSION_KEY, None)
        request.session.modified = True

    @staticmethod
    def clear_queue(request) -> None:
        request.session.pop(STAGED_IMPORTS_SESSION_KEY, None)
        request.session.modified = True


__all__ = [
    "ACTIVE_IMPORT_JOB_SESSION_KEY",
    "ImportSessionManager",
    "STAGED_IMPORTS_SESSION_KEY",
]
