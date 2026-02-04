from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from etl_utilities.utils import my_failure_callback


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['business.intelligence@remindermedia.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'on_failure_callback': my_failure_callback
}

with DAG(
        'Gather_Sales_Data_Ask_Data_Import',
        default_args=default_args,
        schedule='45 10 * * 1',
        start_date=datetime(2022, 10, 12),
        doc_md="""
        # Gather Sales Data Import and Processing Script

        ## Overview
        This script automates the import and processing of Gather Sales Data files returned via email, streamlining the workflow of fetching emails, extracting attachments, and updating databases with processed data.

        ## Workflow and Key Features

        ### Email Fetching and Processing
        - Utilizes the exchangelib library to connect to an email account, filter unread emails based on specific criteria, and download Excel file attachments for processing.

        ### File Processing
        - Employs pandas for data cleaning and processing, including renaming columns, filling missing values, and adjusting data types for database compatibility.

        ### Data Comparison and Update
        - Compares processed file data against existing MySQL database records to identify updates needed for company information, contact-office relationships, and workflow management system statuses.

        ### Database Updates
        - Executes updates on relevant database tables and records based on the comparison results, ensuring data integrity and accuracy.

        ## Benefit
        This script enhances the efficiency of data import and processing tasks for Gather Sales Data files, supporting streamlined updates to databases and facilitating accurate data management with minimal manual intervention.

        ## Confluence Documentation
        https://remindermediadevelopers.atlassian.net/wiki/spaces/BD/pages/1927118908/Gather+Sales+Data+Import+Active

        """,
        max_active_runs=1,
        max_active_tasks=1,
        catchup=False) as dag:

    t1 = BashOperator(
    task_id='Gather_Sales_Data_Ask_Data_Import',
    bash_command='python3 /opt/airflow/etl_scripts/GatherSalesDataImport/gather_sales_data_ask_data_import.py',
    dag=dag)




