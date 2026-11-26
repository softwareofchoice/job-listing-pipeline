# Airflow image with the pipeline's dependencies in a SEPARATE virtualenv
# (ADR-027). Airflow and dbt-core pin conflicting versions of shared libraries,
# so they never share one Python environment. Tasks call the pipeline's own
# interpreter: /opt/jobpipe/venv/bin/python, /opt/jobpipe/venv/bin/dbt.
#
# Only dependencies are baked in. The code, config, dbt project and data are
# bind-mounted from the repo (docker-compose.yaml), so editing code or your
# profile seeds needs no rebuild; changing requirements.txt does.
FROM apache/airflow:3.3.2-python3.12

USER root
RUN python -m venv /opt/jobpipe/venv && chown -R airflow:0 /opt/jobpipe
USER airflow

COPY --chown=airflow:0 requirements.txt /opt/jobpipe/requirements.txt
RUN /opt/jobpipe/venv/bin/pip install --no-cache-dir -r /opt/jobpipe/requirements.txt
