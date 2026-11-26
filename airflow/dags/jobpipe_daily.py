"""Daily pipeline: ingest -> dbt build -> report (ADR-028).

    ingest      fetch every active board's postings into data/raw/
    dbt_build   rebuild staging, SCD2 history, features and scores; run tests
    report      write data/reports/dt=<as_of>/top_matches.{csv,html}

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

    ingest >> dbt_build >> report
