#!/usr/bin/env bash

AIRFLOW_HOME=/opt/airflow
AIRFLOW_DOCKER_IMAGE=airflow-local:dev
LOCAL_HOME=/home/giovannor
LOCAL_AIRFLOW_REPO=${LOCAL_HOME}/repo/data-engineering-airflow
LOCAL_GCP_ADC=${LOCAL_HOME}/.config/gcloud/application_default_credentials.json
LOCAL_GCP_SAKEY=${LOCAL_HOME}/Documents/creds/sakey-de.json
LOCAL_SCRIPTS=${LOCAL_HOME}/repo/rnd/scripts/

echo "Deploying Airflow in Docker container..."

docker run --rm -d -it \
  --name airflow-local \
  --platform linux/amd64 \
  -p 8081:8080 \
  -v $LOCAL_AIRFLOW_REPO/dags:${AIRFLOW_HOME}/dags \
  -v $LOCAL_AIRFLOW_REPO/scripts/bootstrap.sh:${AIRFLOW_HOME}/scripts/bootstrap.sh \
  -v $LOCAL_GCP_ADC:${AIRFLOW_HOME}/application_default_credentials.json \
  -v $LOCAL_GCP_SAKEY:${AIRFLOW_HOME}/sakey-de.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=${AIRFLOW_HOME}/application_default_credentials.json \
  -e AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT="google-cloud-platform://?key_path=${AIRFLOW_HOME}/sakey-de.json" \
  -e AIRFLOW__CORE__DAGS_FOLDER=${AIRFLOW_HOME}/dags \
  -e AIRFLOW__CORE__LOAD_EXAMPLES=false \
  -e AIRFLOW_ENVIRONMENT=dev \
  -e PYTHONPATH=$AIRFLOW_HOME/dags \
  -w $AIRFLOW_HOME/ \
  $AIRFLOW_DOCKER_IMAGE \
  bash scripts/bootstrap.sh
