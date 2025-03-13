from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from tasks.extract import extract
from tasks.transform import transform
from tasks.load import load

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 1
}

with DAG("etl_csv_to_dw",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=lambda: transform(*extract())  # Passa os dataframes extraídos
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=lambda: load(
            clientes_transformados=transform(*extract())[0],
            acessos_transformados=transform(*extract())[1],
            wellhub_transformados=transform(*extract())[2],
            calendario_criado =transform(*extract())[3]
        )
    )

    extract_task >> transform_task >> load_task
