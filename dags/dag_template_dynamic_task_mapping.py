#TODO: Development Dag Dog

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from datetime import datetime
import logging

data = [
    {"file": "data_01", "uri": "s3://..../data_01"},
    {"file": "data_02", "uri": "s3://..../data_02"},
    {"file": "data_03", "uri": "s3://..../data_03"},
]

@dag(
    dag_id="dag_template_dynamic_task_mapping",
    start_date=datetime(2024, 11, 15),
    schedule_interval=None,
    concurrency=1,
    tags=["TaskflowAPI", "DynamicTask"]
)
def init():
    start = EmptyOperator(
        task_id='start',
        doc_md="Marks the start of the DAG execution.")

    @task(
        doc_md="A dynamic task that processes each item in the data list and logs relevant information.",
        map_index_template="{{file}}"
    )
    def dynamic_task_mapping(data: dict):
        context = get_current_context()
        context["file"] = data["file"]
        logger = logging.getLogger('airflow.task')
        logger.info(f"Processing file: {data['file']}, URI: {data['uri']}")
        logger.info(f"Task Instance Context: {context}")

    end = EmptyOperator(
        task_id='end',
        doc_md="Marks the end of the DAG execution.")

    start >> dynamic_task_mapping.expand(data=data) >> end

dag = init()

dag.doc_md = """
#### DAG: **dag_template_dynamic_task_mapping**

*Template for Dynamic Task Mapping*

This DAG demonstrates the use of dynamic task mapping in Airflow/

**Task Description**

- `start`: Marks the start of the DAG execution.
- `dynamic_task_mapping`: A dynamic task that processes each item in the data list and logs relevant information.
- `end`: Marks the end of the DAG execution.

© Developed by Jefferson Alves - Git Profile: [https://github.com/j3ffbruce](https://github.com/j3ffbruce)
"""
