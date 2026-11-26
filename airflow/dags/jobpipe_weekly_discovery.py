"""Weekly discovery: find new job boards and append them to config/companies.csv
(ADR-007, ADR-008, ADR-028).

Runs Monday 06:00 Chicago, an hour before the daily run, so new boards are
fetched the same morning. Needs BRAVE_API_KEY in the repo's .env (ADR-009);
without it the task fails with a clear message.

It edits a version-controlled file in your working tree: review the change
with `git diff config/companies.csv` and commit it.
"""
from __future__ import annotations

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from jobpipe_common import DEFAULT_ARGS, HOME, TASK_ENV, TZ, VENV

with DAG(
    dag_id="jobpipe_weekly_discovery",
    description="Find boards hiring for the discovery titles (Brave Search)",
    schedule=CronTriggerTimetable("0 6 * * 1", timezone=TZ),   # Monday 06:00 Chicago
    start_date=TZ.datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    doc_md=__doc__,
    tags=["jobpipe"],
) as dag:
    BashOperator(
        task_id="discover",
        bash_command=f"{VENV}/python -m jobpipe.discover",
        cwd=HOME,
        env=TASK_ENV,
        append_env=True,
    )
