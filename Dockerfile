FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Airflow
ENV AIRFLOW_HOME=/opt/project/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/opt/project/airflow/dags
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV PYTHONPATH=/opt/project/airflow

# Dependências
COPY requirements.txt /requirements.txt

RUN apt-get update \
  && apt-get install -y --no-install-recommends gcc libpq-dev \
  && pip install --no-cache-dir -r /requirements.txt \
  && apt-get remove -y gcc \
  && apt-get autoremove -y \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/project/airflow