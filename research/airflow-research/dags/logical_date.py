from __future__ import annotations

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="logical_date_check",
    catchup=False,
) as dag:

    def get_yesterdays_date(**context):
        yesterdays_date = (
            context["logical_date"]
            .in_timezone("Asia/Jakarta")
            .subtract(days=1)
            .strftime("%Y%m%d")
        )

        return yesterdays_date

    # def print_dates(**context):
    #     logical_datetime = context["logical_date"]
    #     jakarta_datetime = logical_datetime.in_timezone("Asia/Jakarta")
    #     yesterday = jakarta_datetime.subtract(days=1)
    #
    #     print(f"LOGICAL_DATE: {logical_datetime}")
    #     print(f"jakarta_datetime: {jakarta_datetime}")
    #     print(f"yesterday: {yesterday}")
    #
    #     return f"LOGICAL_DATE: {logical_datetime}, jakarta_datetime: {jakarta_datetime}, yesterday: {yesterday}"

    PythonOperator(task_id="print_dates", python_callable=get_yesterdays_date)

    BashOperator(
        task_id="echo_dates",
        bash_command="echo {{ (logical_date.in_timezone('Asia/Jakarta') - macros.timedelta(days=1)).strftime('%Y%m%d') }}",
    )
