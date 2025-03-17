import sys
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp

# Add the include/scripts directory to the Python path
sys.path.append(os.path.abspath('/opt/airflow/include/scripts'))

# Import the functions
from api_to_cassandra import process_bank_transactions
from move_data_from_cassandra_to_psql import spark_AllcleanData_cassandra_to_psql

# Define notification functions
def notify_on_failure(context):
    task_instance = context['task_instance']
    error_message = f"Task {task_instance.task_id} failed with exception: {context['exception']}"
    send_email_smtp(
        to='saheedjimoh338@gmail.com',
        subject='Airflow Task Failed',
        html_content=error_message
    )

def notify_on_success(context):
    task_instance = context['task_instance']
    success_message = f"Task {task_instance.task_id} completed successfully."
    send_email_smtp(
        to='saheedjimoh338@gmail.com',
        subject='Airflow Task Succeeded',
        html_content=success_message
    )

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': notify_on_failure,
    'retries': 1,
}

dag = DAG(
    'bank_transactions_dag',
    default_args=default_args,
    description='A DAG to process bank transactions and load them into Cassandra',
    schedule_interval='@daily',
    catchup=False,
)

start_task = DummyOperator(
    task_id="start_task",
    dag=dag,
)

# Define the task for processing bank transactions
process_transactions_task = PythonOperator(
    task_id='process_bank_transactions',
    python_callable=process_bank_transactions,
    dag=dag,
    on_success_callback=notify_on_success,
)

# Define the task for Spark data processing
spark_clean_data_task = PythonOperator(
    task_id='move_data_from_cassandra_to_psql',
    python_callable=spark_AllcleanData_cassandra_to_psql,
    dag=dag,
    on_success_callback=notify_on_success,
)

end_task = DummyOperator(
    task_id="end_task",
    dag=dag,
)

# Set task dependencies
start_task >> process_transactions_task >> spark_clean_data_task >> end_task