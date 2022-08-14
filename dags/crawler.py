"""Crawler DAG
"""

from airflow import DAG
from datetime import datetime, timedelta
from pathlib import Path
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

DAG_ID = Path(__file__).stem
DAG_START_DATE = datetime(2022, 8, 14, 0, 0)
DAG_SCHEDULE_INTERVAL = "0 */12 * * *"
DAG_CONFIG = {
    "default_args": {
        "owner": "koksang",
        "depends_on_past": False,
        "email": "koksanggl@gmail.com",
        "email_on_failure": False,
        "email_on_retry": False,
        "on_failure_callback": None,
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "task_concurrency": 2,
    },
    "params": {},
    "start_date": DAG_START_DATE,
    "schedule_interval": DAG_SCHEDULE_INTERVAL,
    "max_active_runs": 1,
    "concurrency": 2,
    "catchup": False,
}
IMAGE = ""

with DAG(DAG_ID, **DAG_CONFIG) as dag:
    produce = DockerOperator(image=IMAGE)
    consume = DockerOperator(image=IMAGE)
