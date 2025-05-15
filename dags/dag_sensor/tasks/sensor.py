from croniter import croniter
from datetime import datetime, timedelta
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.state import DagRunState

def get_last_execution(dt, cron_expression):
    cron = croniter(cron_expression, dt)
    last_execution = cron.get_current(datetime)
    print("[Airflow LOG]: execution_date:", dt)
    print(f"[Airflow LOG]: last_execution by cron ({cron_expression}):", last_execution)
    return last_execution

def __sensor(task_id: str, external_dag: str, cron_expression: str, external_task: str=None):
    return (
        ExternalTaskSensor(
            task_id=task_id,
            external_dag_id=external_dag,
            external_task_id=external_task,
            allowed_states=['success'],
            failed_states=[DagRunState.FAILED],
            timeout=10,
            mode='poke',
            deferrable=True,
            execution_date_fn=lambda execution_date: get_last_execution(execution_date, cron_expression=cron_expression),
            poke_interval=10,
            doc_md="Monitors the success status of an external task."
        )
    )