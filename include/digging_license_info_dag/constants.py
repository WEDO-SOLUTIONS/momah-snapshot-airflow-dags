# include/digging_license_info_dag/constants.py

# Centralized constants for digging_license_info DAGs
PREFIX        = "digging_license_info"

# Airflow Variable names
VIEW_VAR      = f"{PREFIX}_db_view_name"
CONFIG_VAR    = f"{PREFIX}_asset_config"
ASSET_ID_VAR  = f"{PREFIX}_dynamic_asset_id"
TOKEN_VAR     = f"{PREFIX}_push_data_access_token"
KNOWN_IDS_VAR = f"{PREFIX}_known_ids"

# Airflow Connection IDs
DB_CONN_ID    = "oracle_db_conn_amanat_intgr"
API_CONN_ID   = "snapshot_pro_api_conn"

# Fetch and push batch sizes
DB_FETCH_SIZE = 100000
API_PUSH_SIZE = 100