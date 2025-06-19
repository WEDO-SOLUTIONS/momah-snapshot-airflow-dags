import os
import sys

# Add the parent directory of this 'dags' folder (i.e., the repository root)
# to Python's path. This is the definitive fix for all import issues.
# This ensures that when Airflow parses any DAG, it knows where to find
# the 'dags' and 'etl_scripts' packages.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))