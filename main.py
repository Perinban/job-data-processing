from __future__ import annotations

import argparse
import json
import logging
import os
from pathlib import Path
from typing import Any, Sequence

from filedownload import download_latest_job_file
from oracle_import import prepare_jobs, publish_jobs


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("job-data-processing")


def _boolean_environment(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _integer_environment(name: str, default: int) -> int:
    value = os.getenv(name, "").strip()
    return int(value) if value else default


def _float_environment(name: str, default: float) -> float:
    value = os.getenv(name, "").strip()
    return float(value) if value else default


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate the latest JOIN feed and publish it to the TalentBliss Oracle API."
    )
    parser.add_argument(
        "--job-data-file",
        default=os.getenv("JOB_DATA_FILE", "").strip(),
        help="Use a local JSON feed instead of downloading from Google Drive.",
    )
    parser.add_argument(
        "--download-path",
        default=os.getenv("JOB_DOWNLOAD_PATH", "job_data.json"),
        help="Destination for the downloaded Google Drive feed.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=_boolean_environment("DRY_RUN"),
        help="Validate and report the feed without contacting TalentBliss.",
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv("TALENTBLISS_API_URL", ""),
        help="TalentBliss API base URL.",
    )
    parser.add_argument(
        "--source",
        default=os.getenv("TALENTBLISS_SOURCE", "join"),
        help="TalentBliss source identifier.",
    )
    parser.add_argument(
        "--run-id",
        default=os.getenv("IMPORT_RUN_ID", "").strip() or os.getenv("GITHUB_RUN_ID", "").strip(),
        help="Stable external import run ID.",
    )
    parser.add_argument(
        "--run-attempt",
        default=os.getenv("GITHUB_RUN_ATTEMPT", "1"),
        help="External import run attempt.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=_float_environment("IMPORT_TIMEOUT_SECONDS", 300),
        help="Per-request read timeout for the TalentBliss API.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=_integer_environment("IMPORT_BATCH_SIZE", 250),
        help="Maximum jobs per API batch.",
    )
    parser.add_argument(
        "--batch-max-bytes",
        type=int,
        default=_integer_environment("IMPORT_BATCH_MAX_BYTES", 8 * 1024 * 1024),
        help="Maximum uncompressed JSON bytes per API batch.",
    )
    return parser


def _load_feed(args: argparse.Namespace) -> tuple[list[object], Path, dict[str, Any]]:
    metadata: dict[str, Any] = {}
    if args.job_data_file:
        path = Path(args.job_data_file).expanduser().resolve()
        if not path.is_file():
            raise FileNotFoundError(f"JOB_DATA_FILE does not exist: {path}")
        logger.info("Using configured job feed: %s", path)
    else:
        path, metadata = download_latest_job_file(args.download_path)
        logger.info(
            "Downloaded %s from Google Drive (file id %s, modified %s)",
            metadata.get("name", path.name),
            metadata.get("id", "unknown"),
            metadata.get("modifiedTime", "unknown"),
        )

    with path.open("r", encoding="utf-8") as handle:
        records = json.load(handle)
    if not isinstance(records, list):
        raise ValueError("The Google Drive job feed must be a JSON array")
    return records, path, metadata


def _run_id(metadata: dict[str, Any], configured_run_id: str) -> str:
    configured = configured_run_id.strip()
    if configured:
        return configured
    file_id = str(metadata.get("id", "")).strip()
    modified = str(metadata.get("modifiedTime", "")).strip()
    if file_id:
        return "drive-" + file_id + ("-" + modified if modified else "")
    raise ValueError("--run-id, IMPORT_RUN_ID, or GITHUB_RUN_ID is required when using a local feed")


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    records, path, metadata = _load_feed(args)
    jobs, report = prepare_jobs(records)

    logger.info(
        "Validated feed %s: %d records, %d unique jobs, %d rejected, %d duplicates, %.2f MiB normalized",
        path,
        report.total_records,
        report.accepted_jobs,
        report.rejected_records,
        report.duplicate_urls,
        report.normalized_bytes / (1024 * 1024),
    )
    if report.rejection_reasons:
        logger.info("Rejected record reasons: %s", report.rejection_reasons)

    if args.dry_run:
        logger.info("DRY_RUN enabled; Oracle import skipped")
        return 0

    result = publish_jobs(
        jobs,
        api_url=args.api_url,
        token=os.getenv("PIPELINE_IMPORT_TOKEN", ""),
        source=args.source,
        run_id=_run_id(metadata, args.run_id),
        run_attempt=args.run_attempt,
        timeout=args.timeout_seconds,
        batch_size=args.batch_size,
        batch_max_bytes=args.batch_max_bytes,
    )
    run = result.get("run", {})
    logger.info(
        "Oracle import finalized: batches=%s discovered=%s inserted=%s updated=%s deleted=%s deleted_companies=%s%s",
        result.get("batch_count", "?"),
        run.get("discovered_count", "?"),
        run.get("inserted_count", "?"),
        run.get("updated_count", "?"),
        result.get("deleted_count", "?"),
        result.get("deleted_company_count", "?"),
        " idempotent=true" if result.get("idempotent") else "",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
