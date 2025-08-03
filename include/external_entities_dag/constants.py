# include/external_entities_dag/constants.py

# Centralized constants for external_entities DAGs
PREFIX        = "external_entities"

# Airflow Variable names
VIEW_VAR      = f"{PREFIX}_db_view_name"
CONFIG_VAR    = f"{PREFIX}_asset_config"
ASSET_ID_VAR  = f"{PREFIX}_dynamic_asset_id"
TOKEN_VAR     = f"{PREFIX}_push_data_access_token"
KNOWN_IDS_VAR = f"{PREFIX}_known_ids"

# Airflow Connection IDs
DB_CONN_ID    = "oracle_db_conn_momrah"
API_CONN_ID   = "snapshot_pro_api_conn"

# Fetch and push batch sizes
DB_FETCH_SIZE = 50000
API_PUSH_SIZE = 100