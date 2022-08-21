"""
# DBT DAG
- generate all tables
- generate all views
"""

from textwrap import dedent
from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from operators import GcloudDockerOperator
from crawler import DAG_ID as UPSTREAM_DAG_ID

DAG_ID = Path(__file__).stem
DOC_MD = dedent(__doc__)
DAG_START_DATE = datetime(2022, 8, 21, 0, 0)
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
    "params": {"full_refresh": False},
    "start_date": DAG_START_DATE,
    "schedule_interval": DAG_SCHEDULE_INTERVAL,
    "max_active_runs": 1,
    "concurrency": 2,
    "catchup": False,
}

IMAGE = "social-media-analysis:latest"
DBT_BASE_PATH = "dbt"
DBT_RUN_CMD = "dbt run --profiles-dir . {extra_cmds}"

with DAG(dag_id=DAG_ID, doc_md=DOC_MD, **DAG_CONFIG) as dag:
    upstream_sensor = ExternalTaskSensor(
        task_id="upstream_sensor", external_dag_id=UPSTREAM_DAG_ID
    )

    dbt_fct_models = GcloudDockerOperator(
        task_id="dbt_fct_models",
        image=IMAGE,
        cpus=1,
        command=DBT_RUN_CMD.format(
            extra_cmds="-m marts.fct {{ --full-refresh if params.full_refresh else '' }}"
        ),
    )

    dbt_downstream_models = GcloudDockerOperator(
        task_id="dbt_downstream_models_non_incremental",
        image=IMAGE,
        cpus=1,
        command=DBT_RUN_CMD.format(extra_cmds="--exclude marts.fct"),
    )

    upstream_sensor >> dbt_fct_models >> dbt_downstream_models
