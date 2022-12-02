from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import os
import requests

S3_CONN_ID = 's3_conn'
BUCKET = 'careflow-stage-bucket'

endpoints = ['states', 'counties', 'cbsas']
date = '{{ ds_nodash }}'

@dag("s3_to_snowflake_dag",
	description="A DAG that takes COVID csv's and loads them into a Snowflake database",
	schedule_interval="@daily",
	start_date=datetime(2022, 11, 30),
	catchup=False,
	default_args={'retries': 1},
	tags=['s3_to_snowflake']
	)

def s3_to_snowflake_dag():

	start = DummyOperator(task_id="start")

	with TaskGroup('transfer_to_snowflake') as transfer_to_snowflake:
		for endpoint in endpoints:
			snowflake = S3ToSnowflakeOperator(
				task_id='upload_{0}_to_snowflake'.format(endpoint),
				s3_keys=['{0}_{1}.csv'.format(endpoint, date)],
				stage='COVID_S3_STAGE',
				file_format='covid_csv',
				table='{0}'.format(endpoint),
				snowflake_conn_id='snowflake_covid_raw',
				pattern=".*[.]csv"
				)

	with TaskGroup('remove_row_headers') as remove_row_headers:
		for endpoint in endpoints:
			remove_row_header = SnowflakeOperator(
				task_id='remove_row_header_in_{0}'.format(endpoint),
				sql="delete from {0} where fips = 'fips'".format(endpoint),
				snowflake_conn_id='snowflake_covid_raw'
				)

	end = DummyOperator(task_id="end")

	start >> transfer_to_snowflake >> remove_row_headers >> end

s3_to_snowflake_dag = s3_to_snowflake_dag()




