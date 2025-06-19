import os
import importlib
from airflow.models.dag import DAG

# This import now works correctly because of the new dags/__init__.py file
from factories.urbi_pro_dag_factory import create_asset_management_dag, create_hourly_sync_dag

# This path assumes the dags folder is at the root of the synced repo
# We go up one level ('..') to get to the repo root, then into etl_scripts/mappers
MAPPERS_DIR = os.path.join(os.path.dirname(__file__), '..', 'etl_scripts', 'mappers')
MAPPER_MODULE_PREFIX = "etl_scripts.mappers"

if os.path.exists(MAPPERS_DIR):
    for filename in os.listdir(MAPPERS_DIR):
        if filename.endswith('.py') and not filename.startswith('__'):
            schema_name = filename[:-3]
            mapper_path = f"{MAPPER_MODULE_PREFIX}.{schema_name}"
            try:
                mapper_module = importlib.import_module(mapper_path)

                mgmt_dag_id = f"{schema_name}_01_asset_management"
                globals()[mgmt_dag_id] = create_asset_management_dag(schema_name, mapper_module)

                sync_dag_id = f"{schema_name}_02_hourly_sync"
                globals()[sync_dag_id] = create_hourly_sync_dag(schema_name, mapper_module)
            except ImportError as e:
                print(f"Failed to generate DAGs for schema '{schema_name}': Could not import module '{mapper_path}'. Error: {e}")
else:
    print(f"Mappers directory not found at {MAPPERS_DIR}. No DAGs will be generated.")