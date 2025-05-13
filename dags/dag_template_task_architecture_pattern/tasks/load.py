import logging

def load(data :str) -> None:
    logger = logging.getLogger('airflow.task')
    logger.info(f"Email: {data}")

