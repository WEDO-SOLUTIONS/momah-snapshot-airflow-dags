import sys
import os
from pendulum import datetime
from typing import Any, Dict, List, Optional
import logging

# Add the project's root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.exceptions import AirflowException
from dateutil.parser import parse as date_parse

from include.vpamnmun_dag.attribute_mapper import ATTRIBUTE_MAPPER
from plugins.hooks.urbi_pro_hook import UrbiProHook

log = logging.getLogger(__name__)

# --- Reusable Helper Functions ---

def validate_and_convert_row(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Validates a row and converts it to a GeoJSON feature."""
    if not all(row.get(key) for key in ["id", "last_modified_date", "latitude", "longitude"]):
        log.warning(f"Skipping record due to missing core data: {row.get('id')}")
        return None
    
    properties = {}
    for db_col, map_info in ATTRIBUTE_MAPPER.items():
        value = row.get(db_col)
        if map_info.get("mandatory") and value is None:
            log.warning(f"Skipping record {row.get('id')} due to missing mandatory attribute: {db_col}")
            return None
        properties[db_col] = value
    
    try:
        lon, lat = float(properties["longitude"]), float(properties["latitude"])
    except (ValueError, TypeError):
        log.warning(f"Skipping record {row.get('id')} due to invalid coordinates.")
        return None
        
    return {
        "type": "Feature",
        "id": str(row["id"]),
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": properties
    }


# --- DAG Definition ---
@dag(
    dag_id="vpamnmun_sync_data",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["urbi_pro", "vpamnmun", "data_sync"],
    doc_md="Hourly sync of MOMRAH priority areas data to the Urbi Pro dynamic asset."
)
def sync_data_dag():

    @task
    def get_last_run_state(**context) -> Dict[str, Any]:
        """Gets state from the previous run or initializes it for the first run."""
        prev_run_ts = context.get("data_interval_end")
        
        if prev_run_ts:
            start_sync_timestamp = prev_run_ts.to_iso8601_string()
        else: # First run
            start_sync_timestamp = "1970-01-01T00:00:00+00:00"
            log.warning("No previous successful run found. Syncing from the beginning of time.")

        last_known_ids = Variable.get("vpamnmun_known_ids", default_var=[], deserialize_json=True)
        log.info(f"Starting sync for data since: {start_sync_timestamp}")
        log.info(f"Found {len(last_known_ids)} known IDs from previous run.")
        
        return {
            "start_sync_timestamp": start_sync_timestamp,
            "last_known_ids": last_known_ids,
            "current_run_timestamp": context["ts"]
        }

    @task
    def process_upserts(state: Dict[str, Any]) -> Dict[str, List[Dict]]:
        """Fetches and processes new/updated records from the database."""
        oracle_hook = OracleHook(oracle_conn_id="oracle_db_conn")
        db_view = Variable.get("vpamnmun_db_view_name")
        sql_path = os.path.join(os.path.dirname(__file__), '..', '..', 'include/vpamnmun_dag/sql/get_records_since.sql')

        with open(sql_path) as f:
            sql = f.read().replace(":db_view", db_view)

        records = oracle_hook.get_records(sql, parameters={"ts": state["start_sync_timestamp"]})
        
        features_to_upsert = []
        if records:
            for row in records:
                feature = validate_and_convert_row(dict(row))
                if feature:
                    features_to_upsert.append(feature)
        
        log.info(f"Found {len(features_to_upsert)} records to add/update.")
        return {"features": features_to_upsert}
        
    @task
    def push_upserts_to_urbi(data: Dict[str, List[Dict]]):
        """Pushes data chunks to the Urbi Pro API."""
        features = data["features"]
        if not features:
            log.info("No records to upsert. Skipping.")
            return

        api_hook = UrbiProHook(http_conn_id="urbi_pro_api_conn")
        asset_id = Variable.get("vpamnmun_dynamic_asset_id")
        token = Variable.get("vpamnmun_push_data_access_token")

        # In a real scenario with massive data, you would chunk this list
        # and use .expand() for parallel pushes. For simplicity here, we push as one.
        api_hook.push_data(asset_id, token, features, is_delete=False)
        log.info(f"Successfully pushed {len(features)} upserts.")
        
    @task
    def process_deletes(state: Dict[str, Any]) -> Dict[str, List[str]]:
        """Compares current DB state with the last known state to find deletions."""
        last_known_ids_set = set(state["last_known_ids"])
        
        oracle_hook = OracleHook(oracle_conn_id="oracle_db_conn")
        db_view = Variable.get("vpamnmun_db_view_name")
        sql_path = os.path.join(os.path.dirname(__file__), '..', '..', 'include/vpamnmun_dag/sql/get_all_current_ids.sql')

        with open(sql_path) as f:
            sql = f.read().replace(":db_view", db_view)

        current_ids_list = oracle_hook.get_first(sql)
        current_ids_set = set(str(item[0]) for item in current_ids_list) if current_ids_list else set()

        ids_to_delete = list(last_known_ids_set - current_ids_set)
        log.info(f"Found {len(ids_to_delete)} records to delete.")
        return {"ids_to_delete": ids_to_delete, "all_current_ids": list(current_ids_set)}
    
    @task
    def push_deletes_to_urbi(data: Dict[str, List[str]]):
        """Pushes deletion requests to the Urbi Pro API."""
        ids = data["ids_to_delete"]
        if not ids:
            log.info("No records to delete. Skipping.")
            return

        api_hook = UrbiProHook(http_conn_id="urbi_pro_api_conn")
        asset_id = Variable.get("vpamnmun_dynamic_asset_id")
        token = Variable.get("vpamnmun_push_data_access_token")
        
        # In a real scenario with massive data, you would chunk this list
        api_hook.push_data(asset_id, token, ids, is_delete=True)
        log.info(f"Successfully pushed {len(ids)} deletions.")

    @task
    def update_final_state(delete_data: Dict[str, List[str]]):
        """Saves the new complete list of object IDs as an Airflow Variable for the next run."""
        final_id_list = delete_data["all_current_ids"]
        Variable.set("vpamnmun_known_ids", final_id_list, serialize_json=True)
        log.info(f"Successfully updated final state with {len(final_id_list)} IDs.")

    # --- Task Dependencies ---
    run_state = get_last_run_state()
    
    upsert_data = process_upserts(run_state)
    push_upserts_to_urbi(upsert_data)
    
    delete_data = process_deletes(run_state)
    push_deletes_to_urbi(delete_data)
    
    # The final state update depends on the deletion process, which gives the full current ID list
    update_final_state(delete_data)


sync_data_dag()