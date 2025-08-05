# dags/all_incidents_dag/dag_manage_asset.py

import sys
import os
from pendulum import datetime
from datetime import timedelta
import logging

# Ensure we can import from include/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.exceptions import AirflowException
from airflow.models.variable import Variable

from include.all_incidents_dag.constants import (
    VIEW_VAR, CONFIG_VAR, ASSET_ID_VAR, TOKEN_VAR,
    DB_CONN_ID, API_CONN_ID
)
from include.all_incidents_dag.helpers import (
    bootstrap_variables,
    build_schema_from_db,
    build_payload,
)
from plugins.hooks.pro_hook import ProHook

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

@dag(
    dag_id="all_incidents_manage_asset",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["urbi_pro", "all_incidents", "asset_management"],
    doc_md="""
    ### Manage Urbi Pro Dynamic Asset
    This DAG supports three operations:
    - **CREATE**: bootstraps .env → Variables, then creates the dynamic asset.
    - **UPDATE**: rebuilds schema & filters and updates the existing asset.
    - **CLEAR_ALL_DATA**: deletes all asset data and resets the sync state.
    """,
    params={
        "operation": Param("CREATE", type="string", enum=["CREATE", "UPDATE", "CLEAR_ALL_DATA"]),
        "asset_id": Param(None, type=["null","string"], description="Optional ID for UPDATE/CLEAR; falls back to Variable."),
        "db_conn_id": Param(DB_CONN_ID, type="string"),
        "api_conn_id": Param(API_CONN_ID, type="string"),
    },
)
def manage_asset_dag():

    @task
    def execute_operation(**context):
        params = context["params"]
        op = params.get("operation")
        if op not in ["CREATE", "UPDATE", "CLEAR_ALL_DATA"]:
            raise AirflowException(f"Unsupported operation: {op}")

        # On CREATE, load .env into Airflow Variables
        if op == "CREATE":
            bootstrap_variables()

        # Prepare hook
        api_hook = ProHook(http_conn_id=params["api_conn_id"])

        # Determine the asset ID
        asset_id = params.get("asset_id")
        if op in ["UPDATE", "CLEAR_ALL_DATA"] and not asset_id:
            asset_id = Variable.get(ASSET_ID_VAR, default_var=None)
            if not asset_id:
                raise AirflowException("Asset ID is required for UPDATE/CLEAR_ALL_DATA but was not found.")

        if op in ["CREATE", "UPDATE"]:
            # Build schema from DB
            attributes, filters = build_schema_from_db()
            asset_cfg = Variable.get(CONFIG_VAR, deserialize_json=True)

            # Create or update asset
            payload = build_payload(asset_cfg, attributes, filters)
            response = api_hook.create_or_update_asset(payload=payload, asset_id=asset_id)

            if op == "CREATE":
                new_id    = response.get("asset_id")
                new_token = response.get("access_token")
                if new_id and new_token:
                    Variable.set(ASSET_ID_VAR, new_id)
                    Variable.set(TOKEN_VAR,  new_token, serialize_json=False)
                    log.info(f"Asset created: ID={new_id}")
                else:
                    raise AirflowException("CREATE response missing 'asset_id' or 'access_token'.")
            else:
                log.info(f"Asset updated: ID={asset_id}")

        else:  # CLEAR_ALL_DATA
            api_hook.clear_all_asset_data(asset_id)
            Variable.set(ASSET_ID_VAR, "", serialize_json=False)
            log.info(f"Cleared all data for asset: {asset_id}")

        return {"operation": op}

    execute_operation()

manage_asset = manage_asset_dag()
