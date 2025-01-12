# imports
from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.BashOperator import BashOperator

import requests


def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    with requests.get(url, steam=True) as response:
        response.raise_for_status()
        with open(input_file, 'wb') as file:
            for chunck in response.iter_content(chunk_size=8192):
                file.write(chunck)
    print(f"File download successfully: {input_file}")

def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")


def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:          
            processed_line = line.upper()
            outfile.write(processed_line + '\n')


def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')

def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)


# dag default arguments
default_args = {
    'owner': 'Raheleh',
    'start_date': days_ago(0),
    'email': 'raheleh.zarei@gmail.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# dag definition

dag = DAG(
    'ETL-Server-Access-Log-processing-dag',
    default_args=default_args,
    description='ETL server access log processing',
    schedule_interval=timedelta(days=1),
)

# task1- download the file

download = PythonOperator(
    task_id='download',
    python_callable=download,
    dag=dag,
)

# task2 - read the file
extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

# task3 - extractfields
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=extract,
    dag=dag,
)

# task4 load
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# tas5 check
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)


# pipeline
download >> execute_extract >> execute_transform >> execute_load >> execute_check
