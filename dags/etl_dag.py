from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
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

def create_postgres_table():
    from ETL_functions.load_functions import create_postgres_table
    create_postgres_table()

def transform_data_shufersal(**kwargs):
    from ETL_functions.transform_functions_shufersal import transform_data
    transform_data(**kwargs)

def transform_data_victory(**kwargs):
    from ETL_functions.transform_functions_victory import transform_data
    transform_data(**kwargs)

def extract_data_shufersal():
    from ETL_functions.extract_functions_shufersal import extract_data
    extract_data()

def extract_data_victory():
    from ETL_functions.extract_functions_victory import extract_data
    extract_data()

def load_data(**kwargs):
    from ETL_functions.load_functions import load_to_postgres
    load_to_postgres(**kwargs)

def get_common_products_and_cheapest_basket():
    from ETL_functions.load_functions import get_common_products_and_cheapest_basket
    get_common_products_and_cheapest_basket()

def clear_xml_files_directory_shufersal():
    xml_dir = "/usr/local/airflow/dags/xml_files_shufersal"
    for filename in os.listdir(xml_dir):
        file_path = os.path.join(xml_dir, filename)
        try:
            if os.path.isfile(file_path) and filename.endswith('.xml'):
                os.remove(file_path)
                print(f"Deleted {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

def clear_xml_files_directory_victory():
    xml_dir = "/usr/local/airflow/dags/xml_files_victory"
    for filename in os.listdir(xml_dir):
        file_path = os.path.join(xml_dir, filename)
        try:
            if os.path.isfile(file_path) and filename.endswith('.xml'):
                os.remove(file_path)
                print(f"Deleted {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

with DAG('branches_extraction', 
         default_args=default_args,
         schedule_interval='0 */2 * * *',  # Run once every 2 hours
         catchup=False, 
          max_active_runs=1 
         ) as dag:  
    
    create_table_task = PythonOperator(
        task_id='create_postgres_table',
        python_callable=create_postgres_table,
        trigger_rule='one_success' 
    )

    extract_task_shufersal = PythonOperator(
        task_id='extract_data_shufersal',
        python_callable=extract_data_shufersal
    )
    extract_task_victory = PythonOperator(
        task_id='extract_data_victory',
        python_callable=extract_data_victory
    )

    transform_task_shufersal = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_shufersal,
        provide_context=True
    )

    transform_task_victory = PythonOperator(
        task_id='transform_data_victory',
        python_callable=transform_data_victory,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_data,
        provide_context=True  
    )


    get_common_products_and_cheapest_basket_task = PythonOperator(
        task_id='get_common_products_and_cheapest_basket',
        python_callable=get_common_products_and_cheapest_basket
    )

    clear_xml_files_task_victory = PythonOperator(
        task_id='clear_xml_files_directory_victory',
        python_callable=clear_xml_files_directory_victory
    )
    clear_xml_files_task_shufersal = PythonOperator(
        task_id='clear_xml_files_directory_shufersal',
        python_callable=clear_xml_files_directory_shufersal,
        trigger_rule='all_done'  # Run after all tasks are done
    )

    create_table_task >> extract_task_shufersal >> extract_task_victory >> transform_task_shufersal >> transform_task_victory >> load_task  >> get_common_products_and_cheapest_basket_task >> clear_xml_files_task_victory >> clear_xml_files_task_shufersal

 