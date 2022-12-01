import requests
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator

from airflow.utils.task_group import TaskGroup

S3_CONN_ID = 's3_conn'
BUCKET = 'careflow-stage-bucket'

endpoints = ['states', 'counties', 'cbsas']
endpoint_suffix = '.csv?apiKey='
date = '{{ ds_nodash }}'
email_to = Variable.get('jtuma_secret_email')
api_key = Variable.get("covid_act_now_api_key_secret")

def upload_to_s3(endpoint, endpoint_suffix, api_key, date):

    # Instantiate S3

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    print("Created S3 Connection")
    print(s3_hook.get_session())
    print(s3_hook)

    # Base API URL
    
    url = 'https://api.covidactnow.org/v2/'

    res = requests.get(url + endpoint + endpoint_suffix + api_key)

    # Take string, upload to S3 using predefined method
    
    s3_hook.load_string(res.text, '{0}_{1}.csv'.format(endpoint, date), bucket_name=BUCKET, replace=True)

@dag("covid_data_to_s3_dag",
	description='A DAG that calls to the COVID Act Now API and loads the requested data into an S3 bucket',
	schedule_interval="@daily",
	start_date=datetime(2022, 11, 27),
	catchup=False,
	default_args={"retries": 2},
	tags=['S3 data load']
	)

def covid_data_to_s3_dag():

	start = DummyOperator(task_id="start")

	@task
	def check_api():
		
		response = requests.get(f'https://api.covidactnow.org/v2/states.csv?apiKey={api_key}')
		if response.status_code == 200:
			print("successfully fetched the data")
		else:
			print(f"Hey Jaden, there's a {response.status_code} error with your request")

	send_email = EmailOperator(
    	task_id='send_email',
    	to=email_to,
    	subject='Covid to S3 DAG',
    	html_content='<p>The Covid to S3 DAG completed successfully. Files can now be found on S3. <p>'
	)

	with TaskGroup('extract_and_load') as extract_and_load:
		for endpoint in endpoints:
			generate_files = PythonOperator(
				task_id='generate_file_{0}'.format(endpoint),
				python_callable=upload_to_s3,
				op_kwargs={'endpoint': endpoint, 'endpoint_suffix': endpoint_suffix, 'api_key': api_key, 'date': date})

	end = DummyOperator(task_id="end")

	start >> check_api() >> extract_and_load >> send_email >> end

covid_data_to_s3_dag = covid_data_to_s3_dag()



