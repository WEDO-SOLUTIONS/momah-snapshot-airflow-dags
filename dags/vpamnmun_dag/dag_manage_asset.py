import sys
import os
from pendulum import datetime
from typing import Any, Dict, List, Tuple
import logging

# Add the project's root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.exceptions import AirflowException
from dateutil.parser import parse as date_parse

from include.vpamnmun_dag.attribute_mapper import ATTRIBUTE_MAPPER
from plugins.hooks.urbi_pro_hook import UrbiProHook

log = logging.getLogger(__name__)

# --- Helper Function to Build Schema ---
def _build_schema_from_db(oracle_hook: OracleHook) -> Tuple[List[Dict], List[Dict]]:
    """
    Connects to the database and dynamically builds the attributes and filters
    lists based on the view's schema and data analysis.
    """
    db_view = Variable.get("vpamnmun_db_view_name")
    asset_config = Variable.get("vpamnmun_asset_config", deserialize_json=True)
    primary_name_col = asset_config.get("primary_name_column", "")
    
    attributes = []
    filters = []

    # Get column names
    sql_get_cols = f'SELECT * FROM {db_view} WHERE ROWNUM = 1'
    with oracle_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(sql_get_cols)
            db_columns = [desc[0].lower() for desc in cursor.description]

    log.info(f"Detected columns from view: {db_columns}")

    for col_name in db_columns:
        if col_name not in ATTRIBUTE_MAPPER:
            log.warning(f"Column '{col_name}' not in ATTRIBUTE_MAPPER. Skipping.")
            continue

        map_info = ATTRIBUTE_MAPPER[col_name]
        
        # 1. Build the Attribute object
        attribute = {
            "id": col_name,
            "type": map_info["type"],
            "caption": map_info["en"],
            "localizations": {"caption": {"en": map_info["en"], "ar": map_info["ar"]}}
        }
        attributes.append(attribute)

        # If it's the primary name column, add the special "_ns" attribute
        if col_name == primary_name_col:
             attributes.append({
                "id": f"{col_name}_ns", "type": "name", "caption": map_info["en"],
                "localizations": {"caption": {"en": map_info["en"], "ar": map_info["ar"]}}
            })

        # 2. Build the Filter object by analyzing data
        filter_obj = {"attribute_id": col_name}
        
        try:
            if map_info["type"] == "string":
                # Analyze distinct string values for checklist vs text box
                sql_distinct = f'SELECT COUNT(DISTINCT "{col_name}") FROM {db_view}'
                distinct_count = oracle_hook.get_first(sql_distinct)[0]
                
                if 0 < distinct_count <= 50:
                    filter_obj["control_type"] = "check_box_list"
                    sql_values = f'SELECT DISTINCT "{col_name}" FROM {db_view} WHERE "{col_name}" IS NOT NULL'
                    rows = oracle_hook.get_records(sql_values)
                    filter_obj["items"] = [{"value": str(r[0]), "caption": str(r[0])} for r in rows]
                else:
                    filter_obj["control_type"] = "text_box"

            elif map_info["type"] in ["number", "date_time"]:
                # Analyze min/max for range filters
                sql_minmax = f'SELECT MIN("{col_name}"), MAX("{col_name}") FROM {db_view}'
                min_val, max_val = oracle_hook.get_first(sql_minmax)

                if min_val is not None and max_val is not None:
                    if map_info["type"] == "date_time":
                        filter_obj["control_type"] = "date_time_range"
                        min_dt = date_parse(min_val) if isinstance(min_val, str) else min_val
                        max_dt = date_parse(max_val) if isinstance(max_val, str) else max_val
                        filter_obj["min"] = int(min_dt.timestamp() * 1000)
                        filter_obj["max"] = int(max_dt.timestamp() * 1000)
                    else: # number
                        filter_obj["control_type"] = "range"
                        filter_obj["min"] = float(min_val)
                        filter_obj["max"] = float(max_val)
        
        except Exception as e:
            log.error(f"Analysis failed for column '{col_name}', defaulting to text_box. Error: {e}")
            filter_obj["control_type"] = "text_box"

        if "control_type" in filter_obj:
            filters.append(filter_obj)

    return attributes, filters


# --- DAG Definition ---
@dag(
    dag_id="vpamnmun_manage_asset",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["urbi_pro", "vpamnmun", "asset_management"],
    doc_md="""
    ### Manage Urbi Pro Dynamic Asset
    This DAG allows you to CREATE, UPDATE, or CLEAR data from a dynamic asset.
    - **CREATE**: Creates a new asset and saves the ID/Token as Airflow Variables.
    - **UPDATE**: Updates an existing asset's schema. If `asset_id` is left blank, it will use the one from the 'vpamnmun_dynamic_asset_id' Variable.
    - **CLEAR_ALL_DATA**: Deletes all data from an asset. If params are left blank, it will use the values from Airflow Variables.
    """,
    params={
        "operation": Param("CREATE", type="string", enum=["CREATE", "UPDATE", "CLEAR_ALL_DATA"]),
        "asset_id": Param("", type=["null", "string"], description="Optional. If blank, the DAG will use the 'vpamnmun_dynamic_asset_id' Variable."),
        "push_data_access_token": Param("", type=["null", "string"], description="Optional. If blank, the DAG will use the 'vpamnmun_push_data_access_token' Variable."),
        "db_conn_id": Param("oracle_db_conn", type="string"),
        "api_conn_id": Param("urbi_pro_api_conn", type="string"),
    },
)
def manage_asset_dag():
    
    @task
    def execute_operation(**context):
        params = context["params"]
        op = params["operation"]
        api_hook = UrbiProHook(http_conn_id=params["api_conn_id"])

        # --- NEW: Automatically get credentials from Variables if not provided ---
        asset_id = params.get("asset_id")
        token = params.get("push_data_access_token")

        if op in ["UPDATE", "CLEAR_ALL_DATA"] and not asset_id:
            log.info("Asset ID not provided, fetching from Airflow Variable...")
            asset_id = Variable.get("vpamnmun_dynamic_asset_id", default_var=None)
            if not asset_id:
                raise AirflowException("Asset ID is required for this operation and was not found in params or Variables.")
        
        if op == "CLEAR_ALL_DATA" and not token:
            log.info("Push token not provided, fetching from Airflow Variable...")
            token = Variable.get("vpamnmun_push_data_access_token", default_var=None)
            if not token:
                raise AirflowException("Push data access token is required for this operation and was not found in params or Variables.")
        # --- End of new logic ---

        if op in ["CREATE", "UPDATE"]:
            oracle_hook = OracleHook(oracle_conn_id=params["db_conn_id"])
            attributes, filters = _build_schema_from_db(oracle_hook)
            
            asset_config = Variable.get("vpamnmun_asset_config", deserialize_json=True)
            payload = {
                "name": asset_config["name"],
                "description": asset_config["description"],
                "geometry_dimension": asset_config["geometry_dimension"],
                "localizations": {
                    "name": {"en": asset_config["name"], "ar": asset_config["name_ar"]},
                    "description": {"en": asset_config["description"], "ar": asset_config["description_ar"]}
                },
                "attribute_groups": [{
                    "name": asset_config["attribute_group_name"],
                    "localizations": {"name": {"en": asset_config["attribute_group_name"], "ar": asset_config["attribute_group_name_ar"]}},
                    "attributes": attributes
                }],
                "filters": filters
            }

            response = api_hook.create_or_update_asset(payload, asset_id=(asset_id if op == 'UPDATE' else None))

            if op == "CREATE":
                new_asset_id = response.get("asset_id")
                new_token = response.get("access_token")
                if new_asset_id and new_token:
                    Variable.set("vpamnmun_dynamic_asset_id", new_asset_id)
                    Variable.set("vpamnmun_push_data_access_token", new_token, serialize_json=False)
                    log.info("SUCCESS: Asset created. New ID and Token saved as Airflow Variables.")
                else:
                    raise ValueError("API response for CREATE did not contain asset_id and access_token.")
            else:
                 log.info(f"SUCCESS: Asset {asset_id} updated.")
            
            return {"status": "SUCCESS", "operation": op, "response": response}

        elif op == "CLEAR_ALL_DATA":
            api_hook.clear_all_asset_data(asset_id, token)
            return {"status": "SUCCESS", "operation": op}

    execute_operation()

manage_asset_dag()