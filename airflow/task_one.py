from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Uladzislau',
    'start_date': airflow.utils.dates.days_ago(2),
    'depends_on_past': False,
    'email': ['yatsko_vladislav@gmail.com'],
    'email_on_failure': False,
    'email-on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'say_daytime',
    default_args=default_args,
    description='Simply says welcomes you with day period',
    schedule_interval=timedelta(days=1),
)

task_one = BashOperator(
    task_id='say_good_morning',
    bash_command='echo "Good Morning"',
    dag=dag
)

task_two = BashOperator(
    task_id='say_good_day',
    bash_command='echo "Good Day"',
    depends_on_past=False,
    dag=dag
)

task_three = BashOperator(
    task_id='say_good_evening',
    bash_command='echo "Good Evening"',
    depends_on_past=False,
    dag=dag
)