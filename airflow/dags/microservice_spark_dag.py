from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime
import requests

# 🔹 Step 1: Check external API
def check_api():
    response = requests.get("http://ec2-65-0-89-157.ap-south-1.compute.amazonaws.com:8080/download/users")

    if response.status_code == 200:
        print("API Success. Triggering Spark...")
        return "trigger_spark_task"
    else:
        return "api_failed_task"


# 🔹 Step 2: Print failure message
def print_failed():
    print("API fetching failed")


with DAG(
    dag_id="conditional_spark_trigger",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    # Branching task
    check_api_task = BranchPythonOperator(
        task_id="check_api_task",
        python_callable=check_api
    )

    # Spark trigger task (calls your Spark EC2 HTTP server)
    trigger_spark_task = SimpleHttpOperator(
        task_id="trigger_spark_task",
        http_conn_id="spark_api",
        endpoint="/trigger",
        method="POST"
    )

    # Failure task
    api_failed_task = PythonOperator(
        task_id="api_failed_task",
        python_callable=print_failed
    )

    # Flow
    check_api_task >> [trigger_spark_task, api_failed_task]
