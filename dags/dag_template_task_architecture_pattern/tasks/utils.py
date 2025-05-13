import yaml
from pathlib import Path
from typing import Dict
from airflow.exceptions import AirflowException


def getConfig(environment: str) -> Dict:
    """
    Return configuration for a specified environment from a YAML file.
    :environment: (str) accept values `prd` or `dev`
    """  
    config_path = Path("./dags/dag_template_task_architecture_pattern/configs/config.yaml")
    try:
        with open(config_path) as f:
            config = yaml.safe_load(f)
            return config[environment]
    except Exception as e:
        raise AirflowException(f"Failed to load config: {str(e)}")
