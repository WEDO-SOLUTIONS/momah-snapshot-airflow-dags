# /snapshot_pro_etl/asset_management.py
import logging
import json
from airflow.providers.http.hooks.http import HttpHook
from .common import config as Cfg

logger = logging.getLogger(__name__)

def create_or_update_asset(schema_name: str, attribute_mapper: dict, operation: str):
    """Builds and executes CREATE or UPDATE API calls."""
    api_config = Cfg.get_config(schema_name)
    api_hook = HttpHook(method='POST', http_conn_id='snapshot_pro_api_http_connector')

    attributes = []
    for col_name, map_info in attribute_mapper.items():
        attributes.append({
            "id": col_name,
            "type": map_info["type"],
            "caption": map_info["en"],
            "localizations": {"caption": {"en": map_info["en"], "ar": map_info["ar"]}}
        })
        if col_name == api_config["primary_name_column"]:
            attributes.append({
                "id": f"{col_name}_ns",
                "type": "name",
                "caption": map_info["en"],
                "localizations": {"caption": {"en": map_info["en"], "ar": map_info["ar"]}}
            })

    payload = {
        "name": api_config["asset_name"],
        "description": api_config["asset_description"],
        "geometry_dimension": "point",
        "localizations": {
            "name": {"en": api_config["asset_name"], "ar": api_config["asset_name_ar"]},
            "description": {"en": api_config["asset_description"], "ar": api_config["asset_description_ar"]}
        },
        "attribute_groups": [{
            "name": api_config["attribute_group_name"],
            "localizations": {"name": {"en": api_config["attribute_group_name"], "ar": api_config["attribute_group_name_ar"]}},
            "attributes": attributes
        }],
        "filters": []
    }

    http_method = "POST"
    endpoint = "/dynamic_asset"
    if operation.upper() == "UPDATE":
        http_method = "PUT"
        if not api_config["dynamic_asset_id"]:
            raise ValueError(f"Cannot perform UPDATE. Variable '{schema_name.upper()}_DYNAMIC_ASSET_ID' is not set.")
        payload["id"] = api_config["dynamic_asset_id"]

    logger.info(f"Sending {http_method} request to {endpoint} for operation {operation}...")
    response = api_hook.run(endpoint=endpoint, json=payload, extra_options={"check_response": True})
    response_data = response.json()
    logger.info(f"Asset successfully {operation.lower()}d.")
    logger.info(f"Response: {json.dumps(response_data, indent=2, ensure_ascii=False)}")

    if operation.upper() == "CREATE":
        new_asset_id = response_data.get("asset_id")
        logger.info("----------------------------------------------------------------")
        logger.info(f"ACTION REQUIRED: Asset Created. Please update the Airflow Variable '{schema_name.upper()}_DYNAMIC_ASSET_ID' with this value: {new_asset_id}")
        logger.info("----------------------------------------------------------------")

def clear_all_asset_data(schema_name: str):
    """Clears all data from an asset for a given schema."""
    api_config = Cfg.get_config(schema_name)
    api_hook = HttpHook(method='DELETE', http_conn_id='snapshot_pro_api_http_connector')

    push_token = api_hook.get_extra().get("push_data_access_token")
    asset_id = api_config["dynamic_asset_id"]

    if not asset_id or not push_token:
        raise ValueError("Cannot clear data. DYNAMIC_ASSET_ID variable or push_data_access_token in connection extra is not set.")

    endpoint = f"/dynamic_asset/{asset_id}/data/all"
    headers = {'Authorization': f'Bearer {push_token}'}

    logger.info(f"Sending DELETE request to {endpoint}...")
    api_hook.run(endpoint=endpoint, headers=headers, extra_options={"check_response": True})
    logger.info(f"All data successfully cleared from asset '{asset_id}'.")