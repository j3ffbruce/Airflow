from airflow.decorators import dag, task, task_group, setup, teardown
from datetime import timedelta, datetime
import json

@dag(
    'dag_template_task_architecture_pattern',
    start_date=datetime(2024, 11, 12),
    dagrun_timeout=timedelta(minutes=5),
    catchup=False,
    schedule_interval=None,
    tags=[
        "TaskflowAPI", 
        "Architecture", 
        "Yaml",
        "Api"
    ]
)
def init():
    
    @setup
    def start():
        from dags.dag_template_task_architecture_pattern.tasks import getConfig
        return getConfig(environment="dev")

    start = start()

    @task_group(
        group_id="etl", 
        tooltip="Tasks for Extract, Transform, and Load"
    )
    def etl():
        @task(
            retries=3,
            retry_delay=timedelta(minutes=5),
            doc_md="Extracts data from an API."
        )
        def extract(config: dict) -> json:
            from dags.dag_template_task_architecture_pattern.tasks import extract_api
            return extract_api(config)

        extract = extract(start)

        @task(doc_md="Transforms the API response data.")
        def transform(data: json) -> str:
            from  dags.dag_template_task_architecture_pattern.tasks import transform
            return transform(data)

        transform = transform(extract)

        @task(doc_md="Displays the collected data.")
        def load(data: str) -> None:
            from dags.dag_template_task_architecture_pattern.tasks import load
            load(data)

        load = load(transform)
    
    @teardown
    def end():
        pass

    end = end()

    start >> etl() >> end

dag = init()

dag.doc_md = """
#### DAG: **dag_template_task_architecture_pattern**

*Demonstration Template of an Architecture Pattern with Airflow*

This DAG is an example implementation using Airflow to demonstrate best practices in applying architecture patterns. 

**Task Descriptions**

- `start`: Initializes the environment and loads necessary configurations.
- `etl`:
  - `extract`: Extracts data from an API.
  - `transform`: Processes the extracted data, applying transformations.
  - `load`: Logs the transformed data.
- `end`: Marks the completion of the workflow, releasing resources if necessary.

**Objective**

This template is ideal for applications involving API integrations and ETL processes, serving as a foundation for implementing scalable and reusable architecture patterns.

© Developed by Jefferson Alves - GitHub Profile: [https://github.com/j3ffbruce](https://github.com/j3ffbruce)
"""
