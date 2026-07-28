from __future__ import annotations

import gzip
import json
import unittest
from unittest.mock import Mock, patch

import requests

from oracle_import import build_batches, prepare_jobs, publish_jobs


def valid_job(url: str = "https://join.com/companies/acme/1-engineer") -> dict:
    return {
        "Company_Name": "Acme",
        "Company_Logo_Url": "https://cdn.example/logo.png",
        "Job_URL": url,
        "Job_Title": "Engineer",
        "Job_Location": "Berlin, DE",
        "Job_Status": "FULL_TIME",
        "Job_Domain": "Engineering",
        "Job_Salary": None,
        "Job_Details": [{"header": "Role", "content": "Build reliable systems."}],
        "Last_Updated": "2026-07-26",
        "reject_reason": None,
    }


def response(payload: dict) -> Mock:
    result = Mock()
    result.json.return_value = payload
    result.raise_for_status.return_value = None
    return result


class RecordingSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict]] = []
        self.closed = False

    def get(self, url: str, **kwargs):
        self.calls.append(("GET", url, kwargs))
        return response({"status": "ok"})

    def post(self, url: str, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if url.endswith("/finalize"):
            return response(
                {
                    "idempotent": False,
                    "deleted_count": 3,
                    "deleted_company_count": 1,
                    "run": {"discovered_count": 2, "inserted_count": 2, "updated_count": 0},
                }
            )
        payload = json.loads(gzip.decompress(kwargs["data"]).decode("utf-8"))
        return response(
            {
                "idempotent": False,
                "batch": {
                    "batch_index": payload["batchIndex"],
                    "discovered_count": len(payload["jobs"]),
                    "inserted_count": len(payload["jobs"]),
                    "updated_count": 0,
                },
            }
        )

    def close(self) -> None:
        self.closed = True


class FailingBatchSession(RecordingSession):
    def post(self, url: str, **kwargs):
        if url.endswith("/batches"):
            raise requests.ConnectionError("simulated batch failure")
        return super().post(url, **kwargs)


class FlakyBatchSession(RecordingSession):
    def __init__(self) -> None:
        super().__init__()
        self.failures_remaining = 2

    def post(self, url: str, **kwargs):
        if url.endswith("/batches") and self.failures_remaining:
            self.failures_remaining -= 1
            raise requests.ConnectionError("transient batch failure")
        return super().post(url, **kwargs)


class OracleImportTests(unittest.TestCase):
    def test_prepare_jobs_filters_rejections_and_deduplicates(self) -> None:
        first = valid_job()
        replacement = valid_job()
        replacement["Job_Title"] = "Senior Engineer"
        rejected = {"reject_reason": "HTTP 410"}

        jobs, report = prepare_jobs([first, rejected, replacement])

        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["Job_Title"], "Senior Engineer")
        self.assertEqual(report.total_records, 3)
        self.assertEqual(report.accepted_jobs, 1)
        self.assertEqual(report.rejected_records, 1)
        self.assertEqual(report.duplicate_urls, 1)
        self.assertEqual(report.rejection_reasons, {"scraper_rejected": 1})
        self.assertGreater(report.normalized_bytes, 0)

    def test_prepare_jobs_has_no_feed_count_limits(self) -> None:
        empty_jobs, empty_report = prepare_jobs([])
        self.assertEqual(empty_jobs, [])
        self.assertEqual(empty_report.accepted_jobs, 0)

        records = [valid_job(f"https://join.com/companies/acme/{index}") for index in range(3)]
        jobs, report = prepare_jobs(records)
        self.assertEqual(len(jobs), 3)
        self.assertEqual(report.accepted_jobs, 3)

    def test_build_batches_caps_each_request_without_limiting_total_feed(self) -> None:
        jobs = [valid_job(str(index)) for index in range(7)]
        batches = build_batches(jobs, target_bytes=100_000, max_jobs=3)
        self.assertEqual([len(batch) for batch in batches], [3, 3, 1])
        self.assertEqual(sum(len(batch) for batch in batches), len(jobs))

    def test_publish_jobs_checks_health_then_finalizes(self) -> None:
        jobs = [
            valid_job("https://join.com/companies/acme/1"),
            valid_job("https://join.com/companies/acme/2"),
        ]
        session = RecordingSession()

        with patch("oracle_import._session", return_value=session):
            result = publish_jobs(
                jobs,
                api_url="https://talent.example",
                token="secret-token",
                run_id="42",
                run_attempt="1",
                batch_target_bytes=100_000,
                batch_max_jobs=1,
            )

        self.assertEqual(result["run"]["inserted_count"], 2)
        self.assertEqual(result["batch_count"], 2)
        self.assertTrue(session.closed)
        self.assertEqual(len(session.calls), 4)
        self.assertEqual(session.calls[0][0:2], ("GET", "https://talent.example/api/health"))

        first_method, first_url, first_request = session.calls[1]
        self.assertEqual(first_method, "POST")
        self.assertEqual(first_url, "https://talent.example/api/internal/imports/jobs/batches")
        self.assertEqual(first_request["headers"]["Authorization"], "Bearer secret-token")
        self.assertEqual(first_request["headers"]["Content-Encoding"], "gzip")
        first_payload = json.loads(gzip.decompress(first_request["data"]).decode("utf-8"))
        self.assertEqual(first_payload["batchIndex"], 0)
        self.assertEqual(first_payload["batchCount"], 2)
        self.assertEqual(first_payload["jobs"][0]["Job_URL"], jobs[0]["Job_URL"])

        final_method, final_url, final_request = session.calls[-1]
        self.assertEqual(final_method, "POST")
        self.assertEqual(final_url, "https://talent.example/api/internal/imports/jobs/finalize")
        final_payload = json.loads(gzip.decompress(final_request["data"]).decode("utf-8"))
        self.assertEqual(final_payload["batchCount"], 2)
        self.assertEqual(final_payload["runId"], "42")

    def test_publish_jobs_retries_transient_batch_failure(self) -> None:
        session = FlakyBatchSession()
        with patch("oracle_import._session", return_value=session), patch("oracle_import.time.sleep") as sleep:
            result = publish_jobs(
                [valid_job("first"), valid_job("second")],
                api_url="https://talent.example",
                token="secret-token",
                run_id="retryable-run",
                batch_retry_attempts=3,
            )
        self.assertEqual(result["run"]["discovered_count"], 2)
        self.assertEqual(sleep.call_count, 2)

    def test_publish_jobs_does_not_finalize_after_batch_failure(self) -> None:
        session = FailingBatchSession()
        with patch("oracle_import._session", return_value=session), patch("oracle_import.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "Import failed at batch 1/1"):
                publish_jobs(
                    [valid_job()],
                    api_url="https://talent.example",
                    token="secret-token",
                    run_id="43",
                    batch_retry_attempts=2,
                )
        self.assertTrue(session.closed)
        self.assertFalse(any(url.endswith("/finalize") for _, url, _ in session.calls))

    def test_publish_jobs_requires_token_before_network_access(self) -> None:
        with patch("oracle_import._session") as session_factory:
            with self.assertRaisesRegex(ValueError, "PIPELINE_IMPORT_TOKEN"):
                publish_jobs(
                    [valid_job()],
                    api_url="https://talent.example",
                    token="",
                    run_id="44",
                )
        session_factory.assert_not_called()


if __name__ == "__main__":
    unittest.main()
