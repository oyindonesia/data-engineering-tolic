#!usr/bin/env bash

echo "Initiating Airflow DB..."
airflow db migrate

echo "Creating Airflow admin user..."
airflow users create \
    --username airflow \
    --password airflow \
    --firstname Airflow \
    --lastname Airflow \
    --role Admin \
    --email admin@example.com

echo "Starting Airflow webserver..."
airflow webserver &

echo "Deploying Airflow scheduler..."
exec airflow scheduler
