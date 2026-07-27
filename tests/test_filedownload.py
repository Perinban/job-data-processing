from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from filedownload import download_latest_job_file, validate_feed_freshness


class FileDownloadTests(unittest.TestCase):
    def test_validate_feed_freshness_rejects_stale_metadata(self) -> None:
        modified = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat().replace("+00:00", "Z")
        with self.assertRaisesRegex(ValueError, "Refusing stale Google Drive feed"):
            validate_feed_freshness({"modifiedTime": modified}, max_age_hours=36)

    def test_download_validates_size_and_checksum_before_replacing(self) -> None:
        content = b"[{\"Job_URL\":\"https://join.com/example\"}]"
        metadata = {
            "id": "drive-file-id",
            "name": "job_summary.json",
            "modifiedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "size": str(len(content)),
            "md5Checksum": hashlib.md5(content, usedforsecurity=False).hexdigest(),
        }

        def fake_download(*, id: str, output: str, quiet: bool):
            self.assertEqual(id, "drive-file-id")
            Path(output).write_bytes(content)
            return output

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "job_data.json"
            destination.write_text("old", encoding="utf-8")
            with patch("filedownload.find_latest_job_file", return_value=metadata), patch(
                "filedownload.gdown.download", side_effect=fake_download
            ):
                path, returned_metadata = download_latest_job_file(destination, max_age_hours=36)

            self.assertEqual(path.read_bytes(), content)
            self.assertEqual(returned_metadata["id"], "drive-file-id")
            self.assertFalse(Path(str(destination) + ".part").exists())

    def test_failed_download_keeps_existing_destination(self) -> None:
        metadata = {
            "id": "drive-file-id",
            "modifiedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "job_data.json"
            destination.write_text("known-good", encoding="utf-8")
            with patch("filedownload.find_latest_job_file", return_value=metadata), patch(
                "filedownload.gdown.download", return_value=None
            ):
                with self.assertRaisesRegex(RuntimeError, "did not produce a valid file"):
                    download_latest_job_file(destination, max_age_hours=36)
            self.assertEqual(destination.read_text(encoding="utf-8"), "known-good")


if __name__ == "__main__":
    unittest.main()
