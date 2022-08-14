"""
# Crawler DAG
- produce events
- consume events
"""

from textwrap import dedent
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)

DAG_ID = Path(__file__).stem
DOC_MD = dedent(__doc__)
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
    "params": {"start_date": None, "end_date": None},
    "start_date": DAG_START_DATE,
    "schedule_interval": DAG_SCHEDULE_INTERVAL,
    "max_active_runs": 1,
    "concurrency": 2,
    "catchup": False,
}
IMAGE = ""
PRODUCER_CMD = """
{%- if params.start_date -%}
    python src/main.py producer=backfill producer.start_date={{ params.start_date }} producer.end_date={{ params.end_date }}
{%- else -%}
    python src/main.py
{%- endif -%}
"""
CONSUMER_CMD = "python src/main.py run_mode=consumer"

with DAG(dag_id=DAG_ID, doc_md=DOC_MD, **DAG_CONFIG) as dag:
    produce = DockerOperator(
        task_id="produce", image=IMAGE, cpus=12, command=PRODUCER_CMD
    )
    consume = DockerOperator(
        task_id="consume", image=IMAGE, cpus=3, command=CONSUMER_CMD
    )

    produce >> consume
