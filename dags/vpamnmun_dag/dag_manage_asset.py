import sys
import os
from pendulum import datetime
from typing import Any, Dict, List

# Add the project's root directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.models.variable import Variable
from airflow.providers.oracle.hooks.oracle import OracleHook

from include.vpamnmun_dag.attribute_mapper import ATTRIBUTE_MAPPER
from plugins.hooks.urbi_pro_hook import UrbiProHook

# --- DAG Definition ---
@dag(
    dag_id="vpamnmun_manage_asset",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # This DAG is manually triggered
    catchup=False,
    tags=["urbi_pro", "vpamnmun", "asset_management"],
    doc_md="""
    ### Manage Urbi Pro Dynamic Asset
    This DAG allows you to CREATE, UPDATE, or CLEAR data from a dynamic asset.
    - **CREATE**: Creates a new asset based on the DB view schema and saves the new ID and Token as Airflow Variables.
    - **UPDATE**: Updates the schema of an existing asset. Requires `asset_id` param.
    - **CLEAR_ALL_DATA**: Deletes all data from an existing asset. Requires `asset_id` and `push_data_access_token` params.
    """,
    params={
        "operation": Param("CREATE", type="string", enum=["CREATE", "UPDATE", "CLEAR_ALL_DATA"]),
        "asset_id": Param("", type=["null", "string"], description="Required for UPDATE and CLEAR_ALL_DATA operations."),
        "push_data_access_token": Param("", type=["null", "string"], description="Required for CLEAR_ALL_DATA operation."),
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

        if op in ["CREATE", "UPDATE"]:
            # 1. Connect to DB and build the schema payload
            oracle_hook = OracleHook(oracle_conn_id=params["db_conn_id"])
            db_view = Variable.get("vpamnmun_db_view_name")
            sql = f'SELECT * FROM {db_view} WHERE ROWNUM = 1'
            
            with oracle_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    db_columns = [desc[0].lower() for desc in cursor.description]

            attributes = []
            for col_name in db_columns:
                if col_name in ATTRIBUTE_MAPPER:
                    map_info = ATTRIBUTE_MAPPER[col_name]
                    attributes.append({
                        "id": col_name,
                        "type": map_info["type"],
                        "caption": map_info["en"],
                        "localizations": {"caption": {"en": map_info["en"], "ar": map_info["ar"]}}
                    })
            
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
                "filters": [] # Add filter generation logic here if needed
            }

            # 2. Execute API call
            asset_id_to_update = params["asset_id"] if op == "UPDATE" else None
            response = api_hook.create_or_update_asset(payload, asset_id=asset_id_to_update)

            # 3. If CREATE, save new credentials as Variables
            if op == "CREATE":
                new_asset_id = response.get("asset_id")
                new_token = response.get("access_token")
                if new_asset_id and new_token:
                    Variable.set("vpamnmun_dynamic_asset_id", new_asset_id)
                    Variable.set("vpamnmun_push_data_access_token", new_token, serialize_json=False)
                    print(f"SUCCESS: Asset created. New ID and Token saved as Airflow Variables.")
                else:
                    raise ValueError("API response for CREATE did not contain asset_id and access_token.")
            
            return {"status": "SUCCESS", "operation": op, "response": response}

        elif op == "CLEAR_ALL_DATA":
            asset_id = params["asset_id"]
            token = params["push_data_access_token"]
            if not asset_id or not token:
                raise ValueError("For CLEAR_ALL_DATA, asset_id and push_data_access_token params are required.")
            
            api_hook.clear_all_asset_data(asset_id, token)
            return {"status": "SUCCESS", "operation": op}

    execute_operation()

manage_asset_dag()