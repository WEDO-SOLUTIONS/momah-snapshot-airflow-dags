import sys
import os
from pendulum import datetime
from typing import Any, Dict, List, Optional
import logging
import math

# Add the project's root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from dateutil.parser import parse as date_parse

from include.vpamnmun_dag.attribute_mapper import ATTRIBUTE_MAPPER
from plugins.hooks.urbi_pro_hook import UrbiProHook


log = logging.getLogger(__name__)


DB_CONN_ID = "oracle_db_conn_momrah"
API_CONN_ID = "snapshot_pro_api_conn"

DB_FETCH_CHUNK_SIZE = 50000
API_PUSH_CHUNK_SIZE = 100

def validate_and_convert_row(row: Dict[str, Any], primary_name_column: str) -> Optional[Dict[str, Any]]:
    if not all(row.get(key) for key in ["id", "last_modified_date"]):
        log.warning(f"Skipping record due to missing id or last_modified_date.")

        return None
    
    properties = {}

    for db_col, map_info in ATTRIBUTE_MAPPER.items():
        value = row.get(db_col)

        is_mandatory = map_info.get("mandatory", False)

        if is_mandatory and value is None:
            log.warning(f"Skipping record {row.get('id')} due to missing mandatory attribute: {db_col}")

            return None
        if value is None:
            properties[db_col] = None

            continue
        if map_info["type"] == "date_time":
            try:
                dt_obj = date_parse(value, dayfirst=True) if isinstance(value, str) else value

                properties[db_col] = dt_obj.isoformat()
            except (ValueError, TypeError):
                log.warning(f"Invalid date format for record {row.get('id')}, attribute '{db_col}'. Setting to null.")

                properties[db_col] = None
        else:
            properties[db_col] = value
            
    if primary_name_column and (primary_name_val := row.get(primary_name_column)):
        properties[f"{primary_name_column}_ns"] = str(primary_name_val)

    lon = properties.get('longitude')
    lat = properties.get('latitude')

    if lon is None or lat is None:
        log.warning(f"Skipping record {row.get('id')} due to missing longitude/latitude.")

        return None
    
    try:
        lon_float, lat_float = float(lon), float(lat)
    except (ValueError, TypeError):
        log.warning(f"Skipping record {row.get('id')} because longitude/latitude are not valid numbers.")

        return None
    
    if not (-180 <= lon_float <= 180):
        log.warning(f"Skipping record {row.get('id')} due to out-of-bounds longitude: {lon_float}")

        return None
    
    if not (-90 <= lat_float <= 90):
        log.warning(f"Skipping record {row.get('id')} due to out-of-bounds latitude: {lat_float}")

        return None
    
    return {

        "type": "Feature", "id": str(row["id"]),
        "geometry": {"type": "Point", "coordinates": [lon_float, lat_float]},
        "properties": properties

    }

@dag (
        
    dag_id = "vpamnmun_sync_data",
    start_date = datetime(2025, 1, 1),
    schedule = "0 */6 * * *",
    catchup = False,
    max_active_runs = 1,
    tags = ["urbi_pro", "vpamnmun", "data_sync"],
    doc_md = """
    ### Vpamnmun Full Data Sync
    Performs a full data sync from the Oracle view to the Urbi Pro dynamic asset.
    - **Step 1**: Triggers the `vpamnmun_manage_asset` DAG to update the asset schema.
    - **Step 2**: If the schema update succeeds, it proceeds to sync all data.
    """

)
def sync_data_dag():
    update_asset_schema = TriggerDagRunOperator (

        task_id = "update_asset_schema",
        trigger_dag_id = "vpamnmun_manage_asset",
        wait_for_completion = True,
        conf = {"operation": "UPDATE"},
        deferrable = True,
        failed_states = ["failed"],

    )

    @task
    def get_record_count_and_generate_chunks() -> List[Dict]:
        oracle_hook = OracleHook(oracle_conn_id=DB_CONN_ID)

        db_view = Variable.get("vpamnmun_db_view_name")

        sql = f'SELECT COUNT(*) FROM {db_view}'

        total_records = oracle_hook.get_first(sql)[0] or 0

        log.info(f"Found {total_records} total records in the source view.")

        num_chunks = math.ceil(total_records / DB_FETCH_CHUNK_SIZE)

        chunks = [{"limit": DB_FETCH_CHUNK_SIZE, "offset": i * DB_FETCH_CHUNK_SIZE} for i in range(num_chunks)]

        log.info(f"Generated {len(chunks)} chunks to process in parallel.")

        return chunks

    @task
    def process_and_push_chunk(chunk_info: Dict):
        limit = chunk_info["limit"]

        offset = chunk_info["offset"]

        log.info(f"Processing DB chunk: offset={offset}, limit={limit}")
        
        oracle_hook = OracleHook(oracle_conn_id=DB_CONN_ID)

        db_view = Variable.get("vpamnmun_db_view_name")

        asset_config = Variable.get("vpamnmun_asset_config", deserialize_json=True)

        primary_name_col = asset_config.get("primary_name_column", "")
        
        sql = f'SELECT * FROM {db_view} ORDER BY "id" OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY'
        
        features_to_upsert = []
        
        with oracle_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, offset=offset, limit=limit)

                # Get column names directly from the active cursor
                column_names = [d[0].lower() for d in cursor.description]

                log.info(f"Column names retrieved: {column_names}")

                records = cursor.fetchall()

        if records:
            for row_tuple in records:
                row_dict = dict(zip(column_names, row_tuple))

                feature = validate_and_convert_row(row_dict, primary_name_col)

                if feature:
                    features_to_upsert.append(feature)

        if not features_to_upsert:
            log.info("No valid features found in this DB chunk. Skipping.")

            return

        log.info(f"DB chunk processing complete. Found {len(features_to_upsert)} valid records to push.")

        api_hook = UrbiProHook(http_conn_id=API_CONN_ID)

        asset_id = Variable.get("vpamnmun_dynamic_asset_id")

        token = Variable.get("vpamnmun_push_data_access_token")

        for i in range(0, len(features_to_upsert), API_PUSH_CHUNK_SIZE):
            batch = features_to_upsert[i:i + API_PUSH_CHUNK_SIZE]

            log.info(f"Pushing an API batch of {len(batch)} records...")

            api_hook.push_data(asset_id, token, batch, is_delete=False)
        
        log.info(f"All API batches for this DB chunk have been pushed successfully.")

    @task
    def process_and_push_deletes() -> List[str]:
        last_known_ids = Variable.get("vpamnmun_known_ids", default_var=[], deserialize_json=True)

        last_known_ids_set = set(last_known_ids)
        
        oracle_hook = OracleHook(oracle_conn_id=DB_CONN_ID)

        db_view = Variable.get("vpamnmun_db_view_name")

        sql = f'SELECT "id" FROM {db_view}'

        records = oracle_hook.get_records(sql)

        current_ids_set = {str(item[0]) for item in records} if records else set()

        ids_to_delete = list(last_known_ids_set - current_ids_set)
        
        if ids_to_delete:
            log.info(f"Found {len(ids_to_delete)} records to delete. Pushing to API in batches...")

            api_hook = UrbiProHook(http_conn_id=API_CONN_ID)

            asset_id = Variable.get("vpamnmun_dynamic_asset_id")

            token = Variable.get("vpamnmun_push_data_access_token")
            
            for i in range(0, len(ids_to_delete), API_PUSH_CHUNK_SIZE):
                batch = ids_to_delete[i:i + API_PUSH_CHUNK_SIZE]

                log.info(f"Pushing an API batch of {len(batch)} deletions...")

                api_hook.push_data(asset_id, token, batch, is_delete=True)
        else:
            log.info("No records to delete.")
        
        return list(current_ids_set)

    @task
    def update_final_state(all_current_ids: List[str]):
        Variable.set("vpamnmun_known_ids", all_current_ids, serialize_json=True)

        log.info(f"Successfully updated final state with {len(all_current_ids)} IDs.")

    chunks_to_process = get_record_count_and_generate_chunks()

    current_ids_list = process_and_push_deletes()

    update_asset_schema >> [chunks_to_process, current_ids_list]

    process_and_push_chunk.expand(chunk_info=chunks_to_process)

    update_final_state(current_ids_list)

sync_data_dag()