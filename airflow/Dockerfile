# Airflow version is declared once and reused below.
ARG AIRFLOW_VERSION=3.3.2
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12
ARG AIRFLOW_VERSION

COPY --chown=airflow:root airflow/requirements.txt /tmp/requirements.txt
COPY --chown=airflow:root pyproject.toml README.md /opt/lakehouse/
COPY --chown=airflow:root src /opt/lakehouse/src

# Pin apache-airflow to the image's version so resolving providers can never move it.
RUN pip install --no-cache-dir \
      "apache-airflow==${AIRFLOW_VERSION}" \
      -r /tmp/requirements.txt \
      /opt/lakehouse

# DAGs last: they change most often, so rebuilds stay fast.
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
