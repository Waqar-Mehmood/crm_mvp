import uuid
from pathlib import Path

from django.conf import settings


def get_import_uploads_dir():
    uploads_dir = Path(settings.MEDIA_ROOT) / "imports"
    uploads_dir.mkdir(parents=True, exist_ok=True)
    return uploads_dir


def save_import_upload(uploaded_file):
    file_name = Path(getattr(uploaded_file, "name", "upload.csv")).name
    temp_path = get_import_uploads_dir() / f"{uuid.uuid4().hex}_{file_name}"
    with temp_path.open("wb") as out:
        for chunk in uploaded_file.chunks():
            out.write(chunk)
    return temp_path
