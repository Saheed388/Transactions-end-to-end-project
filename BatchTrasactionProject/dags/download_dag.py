from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def download_kaggle_dataset():
    from kaggle.api.kaggle_api_extended import KaggleApi
    api = KaggleApi()
    api.authenticate()  # Authenticate using kaggle.json
    api.dataset_download_files('dataset-name', path='/usr/local/airflow/data/', unzip=True)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'kaggle_download_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

download_task = PythonOperator(
    task_id='download_kaggle_dataset',
    python_callable=download_kaggle_dataset,
    dag=dag,
)

download_task