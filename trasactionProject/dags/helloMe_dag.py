from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator  # Use EmptyOperator instead of DummyOperator
from datetime import datetime, timedelta  # Import timedelta

# Define the function to print "Hello, World!"
def print_hello_world():
    print("Hello, World!")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,  # Number of retries in case of failure
    'retry_delay': timedelta(minutes=5),  # Delay between retries
}

# Define the DAG
dag = DAG(
    'hello_world_dag',  # DAG ID
    description='A simple DAG to print Hello, World!',
    schedule_interval='@once',  # Run once
    default_args=default_args,  # Pass default arguments
    catchup=False,  # Disable catchup
    tags=['example'],  # Add tags for better organization
)

# Define tasks
start_task = EmptyOperator(
    task_id="start_task",
    dag=dag,
)

hello_world_task = PythonOperator(
    task_id='hello_world_task',  # Task ID
    python_callable=print_hello_world,  # Function to execute
    dag=dag,  # Assign the DAG
)

end_task = EmptyOperator(
    task_id="end_task",
    dag=dag,
)


# Set task dependencies
start_task >> hello_world_task >> end_task