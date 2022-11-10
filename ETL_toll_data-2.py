# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#Task 1.1 - Define DAG arguments
default_args = {
    'owner': 'Shineka B.',
    'start_date': days_ago(0),
    'email': ['owner@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

#Task 1.2 - Define the DAG
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)

#Task 1.3 - Create a task to unzip data
# define the first task named unzip_data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -zxvf tolldata.tgz',
    dag=dag,
)

#Task 1.4 - Create a task to extract data from csv file
# define the second task named extract_data_from_csv
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d":" -f1,2,3,4 /vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv',
    dag=dag,
)

#Task 1.5 - Create a task to extract data from tsv file
# define the third task named extract_data_from_tsv
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -d":" -f1,2,3 /tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag,
)

#Task 1.6 - Create a task to extract data from fixed width file
# define the fourth task named extract_data_from_fixed_width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -d":" -f1,2 /payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag,
)

#Task 1.7 - Create a task to consolidate data extracted from previous tasks
# define the fifth task named consolidate_data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='"paste csv_data.csv tsv_data.csvfixed_width_data.csv > extracted_data.csv"',
    dag=dag,
)

#Task 1.8 - Transform and load the data
# define the sixth task named transform_data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='echo ${vehicle_type^^} /extracted_data.csv > transformed_data.csv',
    dag=dag,
)

#Task 1.9 - Define the task pipeline
#task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
