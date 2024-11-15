from airflow.decorators import dag
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.sensors.time_sensor import TimeSensor
from croniter import croniter
from airflow.utils.state import DagRunState

def get_last_execution(dt):
    cron_expression = '*/10 * * * *'
    cron = croniter(cron_expression, dt)
    last_execution = cron.get_current(datetime)
    print("[Airflow LOG]: dt:", dt)
    print("[Airflow LOG]: last_execution:", last_execution)
    return last_execution 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    'dag_template_sensor',
    default_args=default_args,
    description='Template for External TaskSensor Usage',
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 10, 25),
    catchup=False,
    tags=[
        "ExternalTaskSensor",
        "TimeSensor",
    ]
)
def init():
    
    sensor = ExternalTaskSensor(
        task_id='sensor',
        external_dag_id='dag_template_api_request',
        external_task_id="wait_task_first",
        allowed_states=['success'],
        failed_states=[DagRunState.FAILED],
        timeout=600,
        mode='poke',
        deferrable=True,
        execution_date_fn=lambda execution_date: get_last_execution(execution_date),
        poke_interval=10,
        doc_md="Monitors the success status of an external task."
    )

    task_time_sensor = TimeSensor(
        task_id="wait_task_second",
        target_time=(datetime.now() + timedelta(minutes=1)).time(),
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
