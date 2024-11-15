from airflow.decorators import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime
import requests
from requests import Response
import json
import logging

@dag(
    'dag_template_api_request',
    start_date=datetime(2024, 11, 12),
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    schedule_interval="0/30 * * * *",
    tags=[
        "TaskflowAPI", 
        "Requests", 
        "Json",  
    ]
)
def init():
  start = EmptyOperator(
    task_id='start',
    doc_md="Marks the beginning of the DAG execution."
  )

  @task(
    retries=3,
    retry_delay=timedelta(minutes=5),
    doc_md="Extracts data from an API."
  )
  def extract() -> json:
    data: Response = requests.get('https://randomuser.me/api/')
    data: json = json.loads(data.text)
    return data

  @task(
    doc_md="Transforms the API response data."
  )
  def transform(data: json) -> str:
    return data["results"][0]["email"]

  @task(
    doc_md="Displays the collected data."
  )
  def load(data: str):
    logger = logging.getLogger('airflow.task')
    logger.info(f"Email: {data}")
  
  @task_group(
    group_id="etl", 
    tooltip="Tasks for Extract, Transform, and Load"
  )
  def etl():
    extracted = extract()
    transformed = transform(extracted)
    load(transformed)

  end = EmptyOperator(
    task_id='end',
    doc_md="Marks the end of the DAG execution."
  )
  start >> etl() >> end

dag = init()

dag.doc_md = """
#### DAG: **dag_template_api_request**

*Template for API Integration*

This DAG template demonstrates an API integration using Airflow. 

**Task Descriptions**

- `start`: Marks the beginning of the DAG execution.
- `extract`: Extracts data from an API.
- `transform`: Transforms the API response data.
- `load`: Displays the collected data.
- `end`: Marks the end of the DAG execution.

© Developed by Jefferson Alves - GitHub Profile: [https://github.com/j3ffbruce](https://github.com/j3ffbruce)
"""
