# /dags/vpc_amn_mun_management_dag.py
import pendulum
import logging
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param

# Import from our custom package
from snapshot_pro_etl import asset_management
from snapshot_pro_etl.mappers import vpc_amn_mun
from snapshot_pro_etl.pod_helpers import get_pod_override_config

log = logging.getLogger(__name__)

SCHEMA_NAME = "vpc_amn_mun"

def _run_management_task(**context):
    """Callable function that runs the selected management operation."""
    operation = context["params"].get("operation")
    log.info(f"Executing operation: {operation} for schema: {SCHEMA_NAME}")

    if operation.upper() in ["CREATE", "UPDATE"]:
        asset_management.create_or_update_asset(SCHEMA_NAME, vpc_amn_mun.ATTRIBUTE_MAPPER, operation)
    elif operation.upper() == "CLEAR_ALL_DATA":
        asset_management.clear_all_asset_data(SCHEMA_NAME)
    else:
        raise ValueError(f"Unknown operation: {operation}")

with DAG(
    dag_id=f"{SCHEMA_NAME}_01_asset_management",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Riyadh"),
    schedule=None,
    catchup=False,
    tags=["snapshot-pro", "management", SCHEMA_NAME],
    params={"operation": Param("CREATE", type="string", enum=["CREATE", "UPDATE", "CLEAR_ALL_DATA"], title="Asset Operation")},
) as dag:
    run_asset_task = PythonOperator(
        task_id="run_asset_task",
        python_callable=_run_management_task,
        # Call the new helper function with no arguments
        executor_config=get_pod_override_config(),
    )