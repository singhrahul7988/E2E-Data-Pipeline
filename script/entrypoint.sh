#!/bin/bash
set -e

if [ -e "/opt/airflow/requirements.txt" ]; then
  python -m pip install --upgrade pip
  python -m pip install --user -r /opt/airflow/requirements.txt
fi

airflow db upgrade

if ! airflow users list | grep -q "admin@example.com"; then
  airflow users create \
    --username admin \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password admin
fi

exec airflow webserver
