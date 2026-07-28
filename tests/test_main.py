from __future__ import annotations

import unittest

from main import _run_id


class MainTests(unittest.TestCase):
    def test_drive_run_id_is_stable_for_reruns(self) -> None:
        metadata = {
            "id": "drive-file-id",
            "modifiedTime": "2026-07-28T06:50:40.348Z",
        }
        first = _run_id(metadata, "")
        second = _run_id(metadata, "")
        self.assertEqual(first, second)
        self.assertEqual(first, "drive-drive-file-id-2026-07-28T06:50:40.348Z")

    def test_configured_run_id_is_preserved_for_local_imports(self) -> None:
        self.assertEqual(_run_id({}, "manual-run"), "manual-run")


if __name__ == "__main__":
    unittest.main()
