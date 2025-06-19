from airflow.models import Variable

def get_config(schema_name: str) -> dict:
    """Fetches all configuration for a GIVEN SCHEMA from Airflow Variables."""
    prefix = f"{schema_name.upper()}_"
    return {
        "asset_name": Variable.get(f"{prefix}ASSET_NAME"),
        "asset_description": Variable.get(f"{prefix}ASSET_DESCRIPTION"),
        "asset_name_ar": Variable.get(f"{prefix}ASSET_NAME_AR"),
        "asset_description_ar": Variable.get(f"{prefix}ASSET_DESCRIPTION_AR"),
        "primary_name_column": Variable.get(f"{prefix}PRIMARY_NAME_COLUMN"),
        "attribute_group_name": Variable.get(f"{prefix}ATTRIBUTE_GROUP_NAME"),
        "attribute_group_name_ar": Variable.get(f"{prefix}ATTRIBUTE_GROUP_NAME_AR"),
        "db_view_name": Variable.get(f"{prefix}DB_VIEW_NAME"),
        "dynamic_asset_id": Variable.get(f"{prefix}DYNAMIC_ASSET_ID", default_var=None),
        "s3_reports_bucket": Variable.get(f"{prefix}S3_REPORTS_BUCKET"),
        "s3_reports_prefix": Variable.get(f"{prefix}S3_REPORTS_PREFIX"),
        "etl_chunk_size": int(Variable.get(f"{prefix}ETL_CHUNK_SIZE", 50000)),
    }