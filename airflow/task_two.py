import os
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

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
    'file_downloader',
    default_args=default_args,
    description='works with csv file',
    schedule_interval=timedelta(days=1),
)

task_one = BashOperator(
    task_id='download_csv',
    bash_command='curl --output /usr/local/airflow/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv -O '
                 'https://www.stats.govt.nz/assets/Uploads/'
                 'Greenhouse-gas-emissions-by-region-industry-and-household/'
                 'Greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018/Download-data/'
                 'greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv',
    dag=dag,
)

task_two = BashOperator(
    task_id='line_counter',
    bash_command='wc -l /usr/local/airflow/greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv',
    dag=dag,
)

def csv_to_avro(csv_file, avro_file):
    import csv
    import struct
    if os.path.exists('greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.avro'):
        os.remove('greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.avro')
    with open(csv_file, "r", encoding="utf-8") as csv_input:
        with open(avro_file, "wb") as avro_output:
            csv_reader = csv.DictReader(csv_input)
            with avro_output as avro_writer:
                for row in csv_reader:
                    region_bytes = row["region"].encode('utf-8')
                    anzsic_bytes = row["anzsic_descriptor"].encode('utf-8')
                    gas_bytes = row["gas"].encode('utf-8')
                    units_bytes = row["units"].encode('utf-8')
                    magnitude_bytes = row["magnitude"].encode('utf-8')
                    year_bytes = struct.pack('i', int(row["year"]))
                    data_val_bytes = struct.pack('f', float(row["data_val"]))
                    
                    record_bytes = (
                            region_bytes +
                            anzsic_bytes +
                            gas_bytes +
                            units_bytes +
                            magnitude_bytes +
                            year_bytes +
                            data_val_bytes
                    )
                    
                    avro_writer.write(record_bytes)
    os.remove('greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv')


task_three = BashOperator(
    task_id='fastavro_installer',
    bash_command='pip install fastavro',
    dag=dag,
)

task_four = PythonOperator(
    task_id='format_converter',
    python_callable=csv_to_avro,
    op_kwargs={'csv_file': 'greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.csv',
               'avro_file': 'greenhouse-gas-emissions-by-region-industry-and-household-year-ended-2018-csv.avro'},
    dag=dag,
)

task_one >> task_two >> task_three >> task_four
