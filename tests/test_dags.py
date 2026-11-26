"""Structure of the Airflow DAGs (ADR-028).

Skipped in the project venv, which deliberately doesn't install Airflow
(ADR-027). Run it with any Python that has apache-airflow==3.3.2, e.g. a
throwaway venv:

    python3 -m venv /tmp/af && /tmp/af/bin/pip install pytest "apache-airflow==3.3.2" \\
      --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt
    PYTHONPATH=airflow/dags /tmp/af/bin/pytest -q tests/test_dags.py
"""
import os
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


@pytest.fixture(scope="module")
def bag(tmp_path_factory):
    os.environ.setdefault("AIRFLOW_HOME", str(tmp_path_factory.mktemp("airflow_home")))
    from airflow.dag_processing.dagbag import DagBag
    b = DagBag(dag_folder=str(DAGS))
    assert not b.import_errors, b.import_errors
    return b


def test_only_the_two_dags_load(bag):
    assert sorted(bag.dags) == ["jobpipe_daily", "jobpipe_weekly_discovery"]


def test_daily_order_and_failure_handling(bag):
    dag = bag.dags["jobpipe_daily"]
    t = {task.task_id: task for task in dag.tasks}
    assert t["ingest"].downstream_task_ids == {"dbt_build", "upload_raw"}
    assert t["dbt_build"].downstream_task_ids == {"report"}
    assert t["dbt_build"].trigger_rule == "all_done"     # partial ingest failure still builds
    # S3 is a side branch: its failure never blocks dbt or the report (ADR-030)
    assert t["upload_raw"].downstream_task_ids == set()
    assert t["upload_raw"].trigger_rule == "all_done"
    assert t["report"].trigger_rule == "all_success"     # failed data tests: no report
    assert t["ingest"].retries == 1


def test_no_catchup_one_run_at_a_time_one_pool(bag):
    for dag in bag.dags.values():
        assert dag.catchup is False
        assert dag.max_active_runs == 1
        assert {task.pool for task in dag.tasks} == {"jobpipe"}


def test_tasks_use_the_pipeline_venv_not_airflows_python(bag):
    for dag in bag.dags.values():
        for task in dag.tasks:
            assert task.bash_command.startswith("/opt/jobpipe/venv/bin/")
