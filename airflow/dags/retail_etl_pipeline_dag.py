from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'rishabh',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='retail_data_pipeline',
    default_args=default_args,
    description='ETL pipeline to process and load retail data from Azure to SQL',
    schedule='@daily',  # <-- updated here
    start_date=datetime(2024, 4, 20),
    catchup=False,
    tags=['retail', 'azure', 'ETL'],
) as dag:


    upload_to_azure = BashOperator(
        task_id='upload_raw_data_to_azure',
        bash_command='python3 /Users/rishabhsaudagar/Desktop/NCI\\ CLG/SEMESTER\\ 2/Data\\ Intensive\\ Scalable\\ System/OnlineEcommerce/Scripts/Azure/upload_to_azure.py'
    )

    clean_data = BashOperator(
        task_id='clean_data',
        bash_command='python3 /Users/rishabhsaudagar/Desktop/NCI\\ CLG/SEMESTER\\ 2/Data\\ Intensive\\ Scalable\\ System/OnlineEcommerce/Scripts/Azure/azure_data_cleaning.py'
    )

    transform_data = BashOperator(
        task_id='transform_data',
        bash_command='python3 /Users/rishabhsaudagar/Desktop/NCI\\ CLG/SEMESTER\\ 2/Data\\ Intensive\\ Scalable\\ System/OnlineEcommerce/Scripts/Azure/data_transformation.py'
    )

    validate_data = BashOperator(
        task_id='validate_data',
        bash_command='python3 /Users/rishabhsaudagar/Desktop/NCI\\ CLG/SEMESTER\\ 2/Data\\ Intensive\\ Scalable\\ System/OnlineEcommerce/Scripts/Azure/validate_data.py'
    )

    load_to_sql = BashOperator(
        task_id='load_to_azure_sql',
        bash_command='python3 /Users/rishabhsaudagar/Desktop/NCI\\ CLG/SEMESTER\\ 2/Data\\ Intensive\\ Scalable\\ System/OnlineEcommerce/Scripts/Azure/db_loader.py'
    )

    # Task dependencies
    upload_to_azure >> clean_data >> transform_data >> validate_data >> load_to_sql
