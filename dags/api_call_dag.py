import requests
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task

from airflow.operators.dummy import DummyOperator

@dag("api_call_dag",
	description="A dag that calls to the COVID Act Now API",
	schedule_interval="@daily",
	start_date=datetime(2022, 11, 27),
	catchup=False,
	default_args={"retries": 2},
	tags=['api call']
	)

def api_call_dag():

	start = DummyOperator(task_id="start")
	
	@task()
	def extract():
		api_key = Variable.get("covid_act_now_api_key_secret")
		response = requests.get(f'https://api.covidactnow.org/v2/states.csv?apiKey={api_key}')
		if response.status_code == 200:
			print("successfully fetched the data")
		else:
			print(f"Hey Jaden, there's a {response.status_code} error with your request")
		
	
	stop = DummyOperator(task_id='stop')

	start >> extract() >> stop

api_call_dag = api_call_dag()


