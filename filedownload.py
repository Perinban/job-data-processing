from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import gdown
from googleapiclient.discovery import build


def _required_environment(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ValueError(f"{name} environment variable is required")
    return value


def find_latest_job_file() -> dict[str, Any]:
    api_key = _required_environment("GOOGLE_API_KEY")
    folder_id = _required_environment("GDRIVE_FOLDER_ID")
    file_name = os.getenv("GDRIVE_FILE_NAME", "job_summary.json").strip() or "job_summary.json"

    service = build("drive", "v3", developerKey=api_key, cache_discovery=False)
    escaped_name = file_name.replace("'", "\\'")
    query = f"name='{escaped_name}' and '{folder_id}' in parents and trashed=false"
    result = (
        service.files()
        .list(
            q=query,
            orderBy="modifiedTime desc",
            pageSize=1,
            fields="files(id,name,modifiedTime,size,md5Checksum)",
        )
        .execute()
    )
    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"No {file_name} file found in the configured Google Drive folder")
    return files[0]


def validate_feed_freshness(metadata: dict[str, Any], max_age_hours: float) -> None:
    if max_age_hours <= 0:
        raise ValueError("max_age_hours must be greater than zero")
    modified_time = str(metadata.get("modifiedTime", "")).strip()
    if not modified_time:
        raise ValueError("Google Drive metadata did not include modifiedTime")
    modified = datetime.fromisoformat(modified_time.replace("Z", "+00:00"))
    if modified.tzinfo is None:
        modified = modified.replace(tzinfo=timezone.utc)
    age_hours = (datetime.now(timezone.utc) - modified.astimezone(timezone.utc)).total_seconds() / 3600
    if age_hours < -1:
        raise ValueError(f"Google Drive feed modifiedTime is unexpectedly in the future: {modified_time}")
    if age_hours > max_age_hours:
        raise ValueError(
            f"Refusing stale Google Drive feed: modified {age_hours:.1f} hours ago; maximum is {max_age_hours:.1f}"
        )


def _md5(path: Path) -> str:
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download_latest_job_file(
    output_file: str | Path = "job_data.json",
    *,
    max_age_hours: float | None = None,
) -> tuple[Path, dict[str, Any]]:
    metadata = find_latest_job_file()
    if max_age_hours is not None:
        validate_feed_freshness(metadata, max_age_hours)

    destination = Path(output_file).expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".part")
    temporary.unlink(missing_ok=True)

    try:
        downloaded = gdown.download(id=metadata["id"], output=str(temporary), quiet=False)
        if not downloaded or not temporary.is_file() or temporary.stat().st_size == 0:
            raise RuntimeError("Google Drive download did not produce a valid file")

        expected_size = metadata.get("size")
        if expected_size and temporary.stat().st_size != int(expected_size):
            raise RuntimeError(
                f"Downloaded file size mismatch: got {temporary.stat().st_size}, expected {expected_size}"
            )

        expected_md5 = str(metadata.get("md5Checksum", "")).strip().lower()
        if expected_md5:
            actual_md5 = _md5(temporary)
            if actual_md5 != expected_md5:
                raise RuntimeError(f"Downloaded file checksum mismatch: got {actual_md5}, expected {expected_md5}")

        temporary.replace(destination)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise

    return destination, metadata
