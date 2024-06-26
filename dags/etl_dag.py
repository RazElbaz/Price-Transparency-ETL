from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import os



print("All Dag modules are ok ......")

start_of_day = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
default_args = {
    'owner': 'airflow',
    'start_date': start_of_day,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def transform_data(**kwargs):
    from ETL_functions.transform_functions import transform_data
    transform_data(**kwargs)

def extract_data():
    from ETL_functions.extract_functions import extract_data
    extract_data()

def create_postgres_table():
    from ETL_functions.load_functions import create_postgres_table
    create_postgres_table()

# def extract_postgres_data_to_file(output_filepath):
#     from dags.ETL_functions.load_functions import extract_postgres_data_to_file
#     extract_postgres_data_to_file(output_filepath)

def load_data(**kwargs):
    from ETL_functions.load_functions import load_to_postgres
    load_to_postgres(**kwargs)

def clear_xml_files_directory():
    xml_dir = "/usr/local/airflow/dags/xml_files"
    for filename in os.listdir(xml_dir):
        file_path = os.path.join(xml_dir, filename)
        try:
            if os.path.isfile(file_path) and filename.endswith('.xml'):
                os.remove(file_path)
                print(f"Deleted {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")


with DAG('shufersal_branches_extraction', 
         default_args=default_args,
         schedule_interval='*/30 * * * *',
         catchup=False, 
          max_active_runs=1 
         ) as dag:  
    
    create_table_task = PythonOperator(
        task_id='create_postgres_table',
        python_callable=create_postgres_table,
        trigger_rule='one_success' 
    )

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data,
        provide_context=True  
    )

    clear_xml_files_task = PythonOperator(
        task_id='clear_xml_files_directory',
        python_callable=clear_xml_files_directory,
        trigger_rule='all_done'  # Run after all tasks are done
    )

    create_table_task >> extract_task >> transform_task >> load_task  >> clear_xml_files_task
 