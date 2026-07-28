from __future__ import annotations

import gzip
import json
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


logger = logging.getLogger("job-data-processing.import")


@dataclass(frozen=True, slots=True)
class FeedReport:
    total_records: int
    accepted_jobs: int
    rejected_records: int
    duplicate_urls: int
    rejection_reasons: dict[str, int]
    normalized_bytes: int


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _is_http_url(value: str | None) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _normalise_sections(value: Any) -> list[dict[str, str]] | None:
    if not isinstance(value, list) or not value:
        return None

    sections: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            return None
        header = _text(item.get("header"))
        content = _text(item.get("content"))
        if not header or not content:
            return None
        sections.append({"header": header, "content": content})
    return sections


def _normalise_job(record: Any) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(record, dict):
        return None, "not_an_object"

    if record.get("reject_reason") not in (None, ""):
        return None, "scraper_rejected"

    company_name = _text(record.get("Company_Name"))
    job_url = _text(record.get("Job_URL"))
    title = _text(record.get("Job_Title"))
    domain = _text(record.get("Job_Domain"))
    sections = _normalise_sections(record.get("Job_Details"))

    if not company_name:
        return None, "missing_company"
    if not _is_http_url(job_url):
        return None, "invalid_job_url"
    if not title:
        return None, "missing_title"
    if not domain:
        return None, "missing_domain"
    if not sections:
        return None, "invalid_job_details"

    logo_url = _text(record.get("Company_Logo_Url"))
    if logo_url and not _is_http_url(logo_url):
        logo_url = None

    return {
        "Company_Name": company_name,
        "Company_Logo_Url": logo_url,
        "Job_URL": job_url,
        "Job_Title": title,
        "Job_Location": _text(record.get("Job_Location")),
        "Job_Status": _text(record.get("Job_Status")),
        "Job_Domain": domain,
        "Job_Salary": _text(record.get("Job_Salary")),
        "Job_Details": sections,
        "Last_Updated": _text(record.get("Last_Updated")),
        "reject_reason": None,
    }, None


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def prepare_jobs(records: Iterable[Any]) -> tuple[list[dict[str, Any]], FeedReport]:
    rows = list(records)
    reasons: Counter[str] = Counter()
    by_url: dict[str, dict[str, Any]] = {}
    duplicates = 0

    for record in rows:
        job, reason = _normalise_job(record)
        if not job:
            reasons[reason or "invalid_record"] += 1
            continue

        job_url = job["Job_URL"]
        if job_url in by_url:
            duplicates += 1
        by_url[job_url] = job

    jobs = [by_url[url] for url in sorted(by_url)]
    normalized_bytes = len(_json_bytes(jobs))
    report = FeedReport(
        total_records=len(rows),
        accepted_jobs=len(jobs),
        rejected_records=sum(reasons.values()),
        duplicate_urls=duplicates,
        rejection_reasons=dict(sorted(reasons.items())),
        normalized_bytes=normalized_bytes,
    )
    return jobs, report


def build_batches(
    jobs: list[dict[str, Any]],
    *,
    max_jobs: int = 250,
    max_bytes: int = 8 * 1024 * 1024,
) -> list[list[dict[str, Any]]]:
    if max_jobs < 1:
        raise ValueError("max_jobs must be at least 1")
    if max_bytes < 1_024:
        raise ValueError("max_bytes must be at least 1024")

    batches: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_bytes = 2

    for job in jobs:
        job_bytes = len(_json_bytes(job)) + (1 if current else 0)
        if job_bytes + 2 > max_bytes:
            raise ValueError(f"One job exceeds the maximum batch size: {job.get('Job_URL', 'unknown')}")
        if current and (len(current) >= max_jobs or current_bytes + job_bytes > max_bytes):
            batches.append(current)
            current = []
            current_bytes = 2
        current.append(job)
        current_bytes += job_bytes

    if current:
        batches.append(current)
    return batches


def _session() -> requests.Session:
    retries = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def _response_json(response: requests.Response, description: str) -> dict[str, Any]:
    response.raise_for_status()
    result = response.json()
    if not isinstance(result, dict):
        raise ValueError(f"TalentBliss returned an invalid {description} response")
    return result


def check_api_health(session: requests.Session, base_url: str, timeout: float) -> dict[str, Any]:
    response = session.get(f"{base_url}/api/health", timeout=(10, min(timeout, 60)))
    result = _response_json(response, "health")
    if result.get("status") != "ok":
        raise RuntimeError(f"TalentBliss API health is not ok: {result.get('status', 'missing')}")
    return result


def _post_gzip_json(
    session: requests.Session,
    url: str,
    payload: dict[str, Any],
    token: str,
    timeout: float,
) -> dict[str, Any]:
    body = gzip.compress(_json_bytes(payload), compresslevel=6)
    response = session.post(
        url,
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
            "Accept": "application/json",
        },
        timeout=(20, timeout),
    )
    return _response_json(response, "import")


def _validate_batch_response(result: dict[str, Any], expected_jobs: int, batch_index: int) -> None:
    batch = result.get("batch")
    if not isinstance(batch, dict):
        raise ValueError(f"TalentBliss batch {batch_index + 1} response did not include batch statistics")
    discovered = batch.get("discovered_count")
    if discovered != expected_jobs:
        raise ValueError(
            f"TalentBliss batch {batch_index + 1} acknowledged {discovered} jobs; expected {expected_jobs}"
        )


def publish_jobs(
    jobs: list[dict[str, Any]],
    *,
    api_url: str,
    token: str,
    source: str = "join",
    run_id: str,
    run_attempt: str = "1",
    timeout: float = 300,
    batch_size: int = 250,
    batch_max_bytes: int = 8 * 1024 * 1024,
) -> dict[str, Any]:
    base_url = api_url.strip().rstrip("/")
    import_token = token.strip()
    external_run_id = run_id.strip()
    external_run_attempt = run_attempt.strip() or "1"
    if not base_url:
        raise ValueError("TALENTBLISS_API_URL is required")
    if not import_token:
        raise ValueError("PIPELINE_IMPORT_TOKEN is required")
    if not external_run_id:
        raise ValueError("A stable run_id is required for batched imports")
    if not jobs:
        raise ValueError("Cannot publish an empty job feed")

    batches = build_batches(jobs, max_jobs=batch_size, max_bytes=batch_max_bytes)
    session = _session()
    batch_results: list[dict[str, Any]] = []
    try:
        check_api_health(session, base_url, timeout)
        logger.info("TalentBliss API health check passed")

        for index, batch in enumerate(batches):
            if index == 0 or (index + 1) % 10 == 0 or index + 1 == len(batches):
                logger.info("Publishing batch %d/%d (%d jobs)", index + 1, len(batches), len(batch))
            try:
                result = _post_gzip_json(
                    session,
                    f"{base_url}/api/internal/imports/jobs/batches",
                    {
                        "source": source,
                        "runId": external_run_id,
                        "runAttempt": external_run_attempt,
                        "batchIndex": index,
                        "batchCount": len(batches),
                        "jobs": batch,
                    },
                    import_token,
                    timeout,
                )
                _validate_batch_response(result, len(batch), index)
                batch_results.append(result)
            except Exception as error:
                raise RuntimeError(f"Import failed at batch {index + 1}/{len(batches)}") from error

        logger.info("Finalizing %d completed batches", len(batches))
        try:
            finalized = _post_gzip_json(
                session,
                f"{base_url}/api/internal/imports/jobs/finalize",
                {
                    "source": source,
                    "runId": external_run_id,
                    "runAttempt": external_run_attempt,
                    "batchCount": len(batches),
                },
                import_token,
                timeout,
            )
        except Exception as error:
            raise RuntimeError("All batches were uploaded, but import finalization failed") from error

        run = finalized.get("run")
        if not isinstance(run, dict):
            raise ValueError("TalentBliss finalization response did not include run statistics")
        if run.get("discovered_count") != len(jobs):
            raise ValueError(
                f"TalentBliss finalized {run.get('discovered_count')} jobs; expected {len(jobs)}"
            )
    finally:
        session.close()

    finalized["batch_count"] = len(batches)
    finalized["batch_results"] = batch_results
    return finalized
