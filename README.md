# TalentBliss job-data processing

This repository is the final stage of the existing TalentBliss job pipeline:

1. `Perinban/join_companies` refreshes the JOIN company catalog.
2. `Perinban/WebScrapJobs` scrapes JOIN jobs, combines `job_summary.json`, and uploads it to Google Drive.
3. This repository downloads the latest Drive feed, validates it, removes scraper failures and duplicate URLs, then imports the complete validated feed through the TalentBliss Oracle API.

The TalentBliss API performs the PostgreSQL transaction, company upserts, job upserts, idempotency checks, deletion of jobs missing from a successfully finalized complete feed, and cleanup of imported companies that no longer have jobs. This repository does not truncate tables, write directly to PostgreSQL, or use Supabase at runtime.

## Safety behavior

The loader fails before finalization when:

- the Drive feed cannot be found or downloaded;
- the downloaded size or checksum does not match Drive metadata;
- the TalentBliss health endpoint is unavailable or not healthy;
- the import token or API URL is missing;
- any API batch fails or returns inconsistent counts; or
- finalization does not acknowledge the complete job count.

Jobs missing from the new feed are only deleted by the TalentBliss finalize endpoint after every expected batch has completed. A partial upload cannot finalize and therefore cannot delete the existing job set.

Imports flow through a producer-consumer queue. Each API request respects the server contract of at most 5,000 jobs and a byte target, but there is no limit on the total feed. Transient batch failures are retried independently, and downloaded Drive feeds use a stable file-derived run identity so reruns can reuse completed server-side batches instead of restarting the import.

## Required GitHub configuration

Repository secrets:

```text
GOOGLE_API_KEY
GDRIVE_FOLDER_ID
PIPELINE_IMPORT_TOKEN
```

When using the private Oracle staging endpoint through SSH, also configure:

```text
ORACLE_SSH_PRIVATE_KEY
DEPLOY_KNOWN_HOSTS
```

Repository variables:

```text
ENABLE_ORACLE_IMPORT=false
USE_ORACLE_SSH_TUNNEL=true
ORACLE_HOST=80.225.207.43
ORACLE_USER=ubuntu
ORACLE_STAGING_PORT=3100
```

Keep `ENABLE_ORACLE_IMPORT=false` until one confirmed `workflow_dispatch` import has succeeded. For a future public API route, set `USE_ORACLE_SSH_TUNNEL=false` and configure `TALENTBLISS_API_URL` instead.

## Schedule

The scraper is configured for `1 0 * * *` UTC, although GitHub has recently delayed its actual starts by several hours. The loader is scheduled for:

```text
17 9 * * *
```

That is 09:17 UTC daily, 11:17 in Europe/Berlin during CEST, and 10:17 during CET. The delay is based on observed scraper completion times rather than the nominal scraper cron.

Scheduled imports run only when `ENABLE_ORACLE_IMPORT=true`. Pull requests and pushes to `main` run validation only. A manual workflow run performs an import only when the `confirm_import` input is explicitly selected.

## Local validation

```bash
python -m pip install -r requirements.txt
python -m pip check
python -m unittest discover -s tests -v
python -m compileall -q .
python main.py --help
JOB_DATA_FILE=/path/to/job_summary.json DRY_RUN=true python main.py
```

A local non-dry import also requires a stable `IMPORT_RUN_ID`, `TALENTBLISS_API_URL`, and `PIPELINE_IMPORT_TOKEN`. Google Drive imports derive the run ID from the Drive file ID and modification timestamp automatically.
