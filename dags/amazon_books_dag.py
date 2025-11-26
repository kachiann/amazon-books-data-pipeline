from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from etl.extract import extract_books
from etl.transform import clean_books
from etl.load import load_books

default_args = {
    "owner": "data_engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="amazon_books_etl",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["amazon", "etl", "books", "data_engineering"]
) as dag:

    def extract_task(**kwargs):
        raw = extract_books()
        kwargs["ti"].xcom_push(key="raw_books", value=raw)

    def transform_task(**kwargs):
        raw = kwargs["ti"].xcom_pull(key="raw_books", task_ids="extract_data")
        cleaned = clean_books(raw)
        kwargs["ti"].xcom_push(key="cleaned_books", value=cleaned)

    def load_task(**kwargs):
        cleaned = kwargs["ti"].xcom_pull(key="cleaned_books", task_ids="transform_data")
        run_id = load_books(cleaned)
        print(f"Inserted scrape run ID: {run_id}")

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_task
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_task
    )

    load_data = PythonOperator(
        task_id="load_data",
        python_callable=load_task
    )

    extract_data >> transform_data >> load_data
