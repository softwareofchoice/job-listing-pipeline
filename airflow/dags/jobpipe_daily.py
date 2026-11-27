"""Daily pipeline: ingest -> dbt build -> report, plus S3 and Athena (ADR-028, ADR-030, ADR-034).

    ingest          fetch every active board's postings into data/raw/
    upload_raw      copy new/changed raw files to s3://<bucket>/raw/
    dbt_build       rebuild staging, SCD2 history, features and scores; run tests
    report          write data/reports/dt=<as_of>/top_matches.{csv,html}
    export_curated  write the finished tables as Parquet to data/curated/<table>/dt=<as_of>/
    upload_curated  copy them to s3://<bucket>/curated/, where Athena reads them

                 ┌─> upload_raw
        ingest ──┤                ┌─> report
                 └─> dbt_build ───┤
                                  └─> export_curated ──> upload_curated

Design notes:
  * No catch-up. Ingestion can only see *today's* postings, so running it
    "for" a missed day would store today's data under an old date. Missed
    days stay gaps, which SCD2 already treats as "unknown" (ADR-014).
  * ingest exits 1 if ANY board failed. That marks the task failed (red in
    the UI, so you notice), but dbt_build still runs (trigger_rule all_done):
    the other boards' fresh data is worth building, and dbt rebuilds from all
    raw files anyway (ADR-013).
  * report only runs if dbt_build succeeded (default all_success). If a data
    test fails, no report is published from data we know is wrong: a
    "circuit breaker".
  * max_active_runs=1 and the one-slot pool: DuckDB allows a single writer.
  * upload_raw is a side branch, not a step before dbt_build: dbt reads the
    LOCAL raw files (ADR-031), so an S3 or network problem never blocks the
    report. Like dbt_build it runs after a partial ingest failure
    (all_done): whatever was fetched is worth keeping. It exits 99 ->
    "skipped" while JOBPIPE_S3_BUCKET isn't set.
  * export_curated has the same circuit breaker as report: no Athena
    snapshot from a build whose data tests failed. It and report don't
    depend on each other, so a broken export never hides the report.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from jobpipe_common import DEFAULT_ARGS, HOME, TASK_ENV, TZ, VENV

with DAG(
    dag_id="jobpipe_daily",
    description="Fetch postings, rebuild the warehouse, write the ranked report",
    schedule=CronTriggerTimetable("0 7 * * *", timezone=TZ),   # 07:00 Chicago
    start_date=TZ.datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["jobpipe"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        bash_command=f"{VENV}/python -m jobpipe.ingest",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
        # One retry for transient trouble (network blip). Re-fetching is
        # idempotent: the same day's files are overwritten (ADR-002).
        retries=1,
        retry_delay=timedelta(minutes=10),
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{VENV}/dbt build --project-dir dbt --profiles-dir dbt",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
        trigger_rule="all_done",
    )

    report = BashOperator(
        task_id="report",
        bash_command=f"{VENV}/python -m jobpipe.report",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
    )

    upload_raw = BashOperator(
        task_id="upload_raw",
        bash_command=f"{VENV}/python -m jobpipe.upload",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
        trigger_rule="all_done",
        # Idempotent (only new/changed files), so a retry is always safe.
        retries=1,
        retry_delay=timedelta(minutes=10),
    )

    export_curated = BashOperator(
        task_id="export_curated",
        bash_command=f"{VENV}/python -m jobpipe.export",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
    )

    upload_curated = BashOperator(
        task_id="upload_curated",
        bash_command=f"{VENV}/python -m jobpipe.upload --root data/curated --prefix curated",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
        retries=1,
        retry_delay=timedelta(minutes=10),
    )

    ingest >> upload_raw
    ingest >> dbt_build >> [report, export_curated]
    export_curated >> upload_curated
