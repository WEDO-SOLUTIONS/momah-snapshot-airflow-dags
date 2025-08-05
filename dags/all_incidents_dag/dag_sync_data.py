# dags/all_incidents_dag/dag_sync_data.py

import sys
import os
import logging
from pendulum import datetime
from datetime import timedelta
from typing import List, Dict, Any, Optional

# Ensure include/ is on PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

from include.all_incidents_dag.constants import (
    VIEW_VAR,
    CONFIG_VAR,
    ASSET_ID_VAR,
    TOKEN_VAR,
    KNOWN_IDS_VAR,
    DB_CONN_ID,
    API_CONN_ID,
    DB_FETCH_SIZE,
    API_PUSH_SIZE,
)
from include.all_incidents_dag.helpers import validate_and_convert_row
from plugins.hooks.pro_hook import ProHook

log = logging.getLogger(__name__)

# Default args for retries, timeouts, etc.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=12),
}

@dag(
    dag_id="all_incidents_sync_data",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["urbi_pro", "all_incidents", "data_sync"],
    doc_md="""
    ### All Incidents Data Sync
    - **Step 1**: Update asset schema via the manage_asset DAG.
    - **Step 2**: Delete removed records.
    - **Step 3**: Upsert new/updated records using keyset pagination.
    - **Step 4**: Persist the new set of known IDs.
    """,
)
def sync_data_dag():
    # 1️⃣ Trigger schema update
    update_schema = TriggerDagRunOperator(
        task_id="update_asset_schema",
        trigger_dag_id="all_incidents_manage_asset",
        conf={"operation": "UPDATE"},
        wait_for_completion=True,
        deferrable=True,
        failed_states=["failed"],
    )

    @task
    def process_deletes() -> List[str]:
        # Fetch last-known IDs
        last_known = set(Variable.get(
            KNOWN_IDS_VAR,
            default_var=[],
            deserialize_json=True
        ))

        # Fetch current IDs from DB
        oracle = OracleHook(oracle_conn_id=DB_CONN_ID)
        view = Variable.get(VIEW_VAR)
        with oracle.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f'SELECT "id" FROM {view}')
                records = cur.fetchall()
        current = {str(r[0]) for r in records}

        # Identify deletes
        to_delete = list(last_known - current)
        if to_delete:
            log.info(f"Deleting {len(to_delete)} removed records...")
            hook = ProHook(http_conn_id=API_CONN_ID)
            asset_id = Variable.get(ASSET_ID_VAR)
            token    = Variable.get(TOKEN_VAR)
            for i in range(0, len(to_delete), API_PUSH_SIZE):
                batch = to_delete[i : i + API_PUSH_SIZE]
                hook.push_data(asset_id, token, batch, is_delete=True)

        return list(current)

    @task
    def sync_upserts() -> None:
        oracle = OracleHook(oracle_conn_id=DB_CONN_ID)
        view = Variable.get(VIEW_VAR)
        asset_id = Variable.get(ASSET_ID_VAR)
        token    = Variable.get(TOKEN_VAR)
        asset_cfg = Variable.get(CONFIG_VAR, deserialize_json=True)
        primary   = asset_cfg.get("primary_name_column", "").lower()
        hook = ProHook(http_conn_id=API_CONN_ID)

        last_id: Optional[Any] = None
        while True:
            if last_id is None:
                sql = f'SELECT * FROM {view} ORDER BY "id" FETCH NEXT :limit ROWS ONLY'
                params = {"limit": DB_FETCH_SIZE}
            else:
                sql = (
                    f'SELECT * FROM {view} WHERE "id" > :last_id '
                    f'ORDER BY "id" FETCH NEXT :limit ROWS ONLY'
                )
                params = {"last_id": last_id, "limit": DB_FETCH_SIZE}

            with oracle.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, **params)
                    cols = [d[0].lower() for d in cur.description]
                    recs = cur.fetchall()

            if not recs:
                log.info("No more rows to process; upserts complete.")
                break

            features: List[Dict[str, Any]] = []
            for row in recs:
                row_dict = dict(zip(cols, row))
                feat = validate_and_convert_row(row_dict, primary)
                if feat:
                    features.append(feat)

            if features:
                log.info(f"Upserting batch of {len(features)} features...")
                for i in range(0, len(features), API_PUSH_SIZE):
                    batch = features[i : i + API_PUSH_SIZE]
                    hook.push_data(asset_id, token, batch, is_delete=False)

            # Advance keyset
            last_id = recs[-1][cols.index("id")]

    @task
    def update_known_ids(current_ids: List[str]) -> None:
        Variable.set(KNOWN_IDS_VAR, current_ids, serialize_json=True)
        log.info(f"Updated known IDs variable with {len(current_ids)} entries.")

    # Orchestrate tasks
    deleted_ids = process_deletes()
    upserts    = sync_upserts()
    updated    = update_known_ids(deleted_ids)

    update_schema >> deleted_ids >> upserts >> updated

sync_data = sync_data_dag()
