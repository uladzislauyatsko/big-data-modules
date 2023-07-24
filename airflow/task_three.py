import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'Uladzislau',
    'start_date': days_ago(2),
    'depends_on_past': False,
    'email': ['yatsko_vladislav@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'greenhouse_gas_analysis',
    default_args=default_args,
    description='Download CSV and perform pivot analysis',
    schedule_interval=timedelta(days=1),
)

task_one = BashOperator(
    task_id='download_csv',
    bash_command='curl --output /usr/local/airflow/greenhouse-gas-emissions.csv -O '
                 'https://www.stats.govt.nz/assets/Uploads/Greenhouse-gas-emissions-by-region-industry-and-household/'
                 'Greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018/Download-data/'
                 'greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv',
    dag=dag,
)

task_two = BashOperator(
    task_id='install_pandas',
    bash_command='pip install pandas',
    dag=dag,
)


def create_pivot_table():
    input_csv_path = '/usr/local/airflow/greenhouse-gas-emissions.csv'
    output_csv_path = '/usr/local/airflow/greenhouse-gas-pivot-analyzed.csv'
    df = pd.read_csv(input_csv_path)
    grouped_df = df.groupby(['region', 'year']).sum().reset_index()
    grouped_df.to_csv(output_csv_path)


task_three = PythonOperator(
    task_id='create_pivot_table',
    python_callable=create_pivot_table,
    dag=dag,
)

task_one >> task_two >> task_three
