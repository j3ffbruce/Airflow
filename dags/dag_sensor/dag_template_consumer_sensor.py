from airflow.decorators import dag
from datetime import datetime, timedelta
from airflow.sensors.time_sensor import TimeSensor
from dags.dag_sensor.tasks import __sensor

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'dag_template_consumer_sensor',
    default_args=default_args,
    description='Template for External TaskSensor Usage',
    schedule_interval='*/10 * * * *',
    is_paused_upon_creation=True,
    start_date=datetime(year=2025, month=5, day=14, hour=15, minute=35),
    catchup=False,
    tags=[
        "TaskflowAPI",
        "Consumer",
        "Sensor",
        "EmptyTask",
    ]
)
def init():

    sensor = __sensor(
        task_id="sensor2",
        external_dag="dag_template_producer_sensor",
        external_task="end",
        cron_expression="*/5 * * * *"
    )

    task_time_sensor = TimeSensor(
        task_id="wait_task_second",
        target_time=(datetime.now() + timedelta(minutes=2)).time(),
        doc_md="Pauses execution until a specified target time."
    )

    sensor >> task_time_sensor


dag = init()

dag.doc_md = """
#### DAG: **dag_template_sensor**

*Template for External TaskSensor Usage*

This DAG demonstrates the use of an `ExternalTaskSensor` to monitor the success status of an external task in another DAG (`dag_template_api_request`), and includes a waiting task controlled by a `TimeSensor`.

**Task Description**

- `sensor`: Monitors the success status of an external task.
- `wait_task_second`: Pauses execution until a specified target time.

© Developed by Jefferson Alves - Git Profile: [https://github.com/j3ffbruce](https://github.com/j3ffbruce)
"""
