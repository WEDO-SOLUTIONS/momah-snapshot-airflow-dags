import os
import sys

# This adds the parent directory of this 'dags' folder (which is the root of your
# git repository) to Python's path. This allows imports like
# 'from snapshot_pro_etl...' to work correctly from any DAG file.
# This is the definitive fix for all import-related errors.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))