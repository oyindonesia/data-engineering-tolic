#!bin/sh

DBT_HOME=/home/data-engineering-dbt
DBT_PROJECT_DIR=/home/data-engineering-dbt/data
DBT_PROFILES_DIR="../profiles/dev" # be careful on what profile you use for testing
DBT_DOCKER_IMAGE=dbt-local:dev
GCP_SAKEY=~/Documents/creds/sakey-de.json
GCP_CREDS=${DBT_HOME}/sakey-de.json

echo "Executing DBT command: $1"

docker run --rm -it \
  --name dbt-local \
  -v ${GCP_SAKEY}:${DBT_HOME}/sakey-de.json \
  -e DBT_PROFILES_DIR=${DBT_PROFILES_DIR} \
  -e GOOGLE_APPLICATION_CREDENTIALS=${GCP_CREDS} \
  -w $DBT_PROJECT_DIR/ \
  $DBT_DOCKER_IMAGE \
  $1
