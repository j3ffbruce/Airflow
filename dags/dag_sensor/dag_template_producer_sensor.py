from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from datetime import timedelta, datetime
from airflow.sensors.time_sensor import TimeSensor

@dag(
    dag_id='dag_template_producer_sensor',
    start_date=datetime(year=2025, month=5, day=14, hour=15, minute=35),
    dagrun_timeout=timedelta(minutes=5),
    is_paused_upon_creation=True,
    catchup=False,
    schedule_interval="*/5 * * * *",
    tags=[
        "TaskflowAPI", 
        "Producer",
        "Sensor",
        "EmptyTask",
    ]
)
def init():
  start = EmptyOperator(
    task_id='start',
    doc_md="Marks the beginning of the DAG execution."
  )

  task_time_sensor = TimeSensor(
      task_id="wait_task_second",
      target_time=(datetime.now() + timedelta(minutes=2)).time(),
      doc_md="Pauses execution until a specified target time."
  )

  end = EmptyOperator(
    task_id='end',
    doc_md="Marks the end of the DAG execution."
  )
  start >> task_time_sensor >>  end

dag = init()

dag.doc_md = """
#### DAG: **dag_producer_sensor**

*Template Producer DAG for a Consumer Sensor DAG*

This DAG template demonstrates DAG to be used by a Consumer DAG using Sensor. 

**Task Descriptions**

- `start`: Marks the beginning of the DAG execution.
- `empty_task`: Execute a empty Task.
- `end`: Marks the end of the DAG execution.

© Developed by Jefferson Alves - GitHub Profile: [https://github.com/j3ffbruce](https://github.com/j3ffbruce)
"""
