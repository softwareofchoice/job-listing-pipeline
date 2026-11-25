"""Shared settings for the jobpipe DAGs (ADR-027, ADR-028).

Tasks run the pipeline's own interpreter from a separate virtualenv inside the
image (/opt/jobpipe/venv), in the bind-mounted repo (JOBPIPE_HOME), exactly
like the commands in CLAUDE.md. Airflow only schedules and observes them.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum

HOME = os.environ.get("JOBPIPE_HOME", "/opt/jobpipe/project")
VENV = "/opt/jobpipe/venv/bin"
TZ = pendulum.timezone("America/Chicago")

# Passed to every BashOperator. append_env=True keeps the container's own
# environment (PATH etc.) and adds these.
TASK_ENV = {"PYTHONPATH": f"{HOME}/src"}

# One-slot pool created by airflow-init: tasks that share companies.csv or the
# DuckDB file never overlap, even across DAGs.
POOL = "jobpipe"

DEFAULT_ARGS = {
    "owner": "jobpipe",
    "pool": POOL,
    "retries": 0,
    "execution_timeout": timedelta(minutes=30),
}
