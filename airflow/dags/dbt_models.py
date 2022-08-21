"""
# DBT DAG
- generate all tables
- generate all views
"""

import os
import json
from textwrap import dedent
from datetime import datetime, timedelta
from pathlib import Path
from dateutil.parser import parse
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor

DAG_ID = Path(__file__).stem
DOC_MD = dedent(__doc__)
DAG_START_DATE = datetime(2022, 8, 21, 0, 0)
DAG_SCHEDULE_INTERVAL = None  # "0 */12 * * *"
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

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
DBT_BASE_PATH = os.path.join(AIRFLOW_HOME, "dbt")
DBT_MANIFEST_FILENAME = str(Path(DBT_BASE_PATH, "target", "manifest.json"))
DBT_EXTRA_CMD = "--profiles-dir ."
DBT_MODEL_CMD = "-m {dbt_model}"
DBT_MANIFEST_CMD = " ".join(["dbt compile", DBT_EXTRA_CMD])
DBT_RUN_CMD = " ".join(["dbt run", DBT_EXTRA_CMD, DBT_MODEL_CMD])


def load_dbt_manifest(manifest_filename: str = DBT_MANIFEST_FILENAME) -> dict:
    """Load dbt manifest.json

    :param str manifest_filename: _description_, defaults to DBT_MANIFEST_FILENAME
    :return _type_: _description_
    """
    with open(manifest_filename) as f:
        data = json.load(f)
    return data


def sense_manifest_updates(
    compare_datetime: datetime, manifest_filename: str = DBT_MANIFEST_FILENAME
) -> bool:
    """Sense manifest generated_at is >= compare_time, indicates latest updates

    :param datetime compare_time: _description_
    :param str manifest_filename: _description_, defaults to DBT_MANIFEST_FILENAME
    :return _type_: _description_
    """
    data = load_dbt_manifest(manifest_filename)
    if parse(data["metadata"]["generated_at"]) >= compare_datetime:
        return True
    else:
        return False


with DAG(dag_id=DAG_ID, doc_md=DOC_MD, **DAG_CONFIG) as dag:
    dbt_manifest = BashOperator(
        task_id="dbt_manifest",
        bash_command=DBT_MANIFEST_CMD,
    )

    sense_manifest = PythonSensor(
        task_id="sense_manifest",
        python_callable=sense_manifest_updates,
        op_kwargs={"compare_datetime": "{{ execution_datetime }}"},
    )

    manifest_data = load_dbt_manifest()
    dbt_runs, depends_on = {}, {}
    node_keys = load_dbt_manifest()["nodes"].keys()
    for node in node_keys:
        node_value = manifest_data["nodes"][node]
        model = node_value["name"]

        dbt_runs[node] = BashOperator(
            task_id=f"dbt_run_{model}",
            bash_command=DBT_RUN_CMD.format(dbt_model=model),
        )
        depends_on[node] = node_value["depends_on"]["nodes"]
        dbt_manifest >> sense_manifest >> dbt_runs[node]

    # NOTE: set upstream dependencies
    for node, upstream_nodes in depends_on.items():
        (
            [
                dbt_runs[upstream_node]
                for upstream_node in upstream_nodes
                if upstream_node in node_keys
            ]
            >> dbt_runs[node]
        )
