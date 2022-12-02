from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from datetime import datetime, timedelta
import os

endpoints = ['states', 'counties', 'cbsas']

@dag("truncate_raw_covid_tables_dag",
	description="A DAG that deletes all data from the raw COVID tables",
	schedule_interval=None,
	start_date=datetime(2022, 11, 30),
	catchup=False,
	default_args={'retries': 1},
	tags=['s3_to_snowflake']
	)

def truncate_raw_covid_tables_dag():

	start = DummyOperator(task_id='start')

	
	with TaskGroup('truncate_tables') as truncate_tables:
		for endpoint in endpoints:
			truncate = SnowflakeOperator(
					task_id='truncate_{0}'.format(endpoint),
					sql="truncate table {0}".format(endpoint),
					snowflake_conn_id='snowflake_covid_raw'
					)
	
	end = DummyOperator(task_id='end')


	start >> truncate_tables >> end

truncate_raw_covid_tables_dag = truncate_raw_covid_tables_dag()



