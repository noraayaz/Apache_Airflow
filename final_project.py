from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd

default_args = {
    'owner': 'blue eyes',
    'start_date': days_ago(0),
    'email': ['blueyes@email.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

# Task-unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/finalassignment/staging/tolldata.tgz -C /home/project/airflow/dags/finalassignment/',
    dag=dag,
)

def extract_data_from_csv():
    input_file = '/home/project/airflow/dags/finalassignment/vehicle-data.csv'
    output_file = '/home/project/airflow/dags/finalassignment/csv_data.csv'
    df = pd.read_csv(input_file, usecols=['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'])
    df.to_csv(output_file, index=False)

# Task-extract_data_from_csv
extract_data_from_csv_task = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv,
    dag=dag,
)

def extract_data_from_tsv():
    input_file = '/home/project/airflow/dags/finalassignment/tollplaza-data.tsv'
    output_file = '/home/project/airflow/dags/finalassignment/tsv_data.csv'
    df = pd.read_csv(input_file, sep='\t', usecols=['Number of axles', 'Tollplaza id', 'Tollplaza code'])
    df.to_csv(output_file, index=False)

# Task-extract_data_from_tsv
extract_data_from_tsv_task = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv,
    dag=dag,
)

def extract_data_from_fixed_width():
    input_file = '/home/project/airflow/dags/finalassignment/payment-data.txt'
    output_file = '/home/project/airflow/dags/finalassignment/fixed_width_data.csv'
    colspecs = [(0, 5), (5, 10)]  
    df = pd.read_fwf(input_file, colspecs=colspecs, names=['Type of Payment code', 'Vehicle Code'])
    df.to_csv(output_file, index=False)

# Task-extract_data_from_fixed_width
extract_data_from_fixed_width_task = PythonOperator(
    task_id='extract_data_from_fixed_width',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)

# Task-consolidate_data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste -d, /home/project/airflow/dags/finalassignment/csv_data.csv \
               /home/project/airflow/dags/finalassignment/tsv_data.csv \
               /home/project/airflow/dags/finalassignment/fixed_width_data.csv \
    > /home/project/airflow/dags/finalassignment/extracted_data.csv
    """,
    dag=dag,
)

# Task-transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    cut -d, -f1,2,3,4,5,6,7 /home/project/airflow/dags/finalassignment/extracted_data.csv > /home/project/airflow/dags/finalassignment/extracted_data_part.csv && \
    cut -d, -f4 /home/project/airflow/dags/finalassignment/extracted_data.csv | tr '[a-z]' '[A-Z]' > /home/project/airflow/dags/finalassignment/vehicle_type_upper.csv && \
    paste -d, /home/project/airflow/dags/finalassignment/extracted_data_part.csv /home/project/airflow/dags/finalassignment/vehicle_type_upper.csv > /home/project/airflow/dags/finalassignment/transformed_data.csv
    """,
    dag=dag,
)

# Define task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
