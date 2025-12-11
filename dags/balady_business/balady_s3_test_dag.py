import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# ======================================================
# CONNECTIONS (update if needed)
# ======================================================
AWS_CONN_ID = "oci_s3_conn"                 # Your configured OCI S3-compatible connection
TEST_BUCKET = "balady_business_test"        # A small test bucket
TEST_KEY = "airflow_test/test_file.json"    # Test file path inside bucket


def write_test_file_to_s3():
    """Writes a simple JSON file to OCI S3 to confirm access + permissions."""

    hook = S3Hook(aws_conn_id=AWS_CONN_ID)

    # Check if bucket exists
    existing_buckets = hook.list_buckets()
    print("Existing buckets:", existing_buckets)

    if TEST_BUCKET not in existing_buckets:
        print(f"Bucket {TEST_BUCKET} does not exist. Creating it...")
        hook.create_bucket(bucket_name=TEST_BUCKET)

    # Create test payload
    payload = {"status": "success", "message": "Airflow can write to OCI S3!"}
    body = json.dumps(payload)

    # Upload file
    hook.load_string(
        string_data=body,
        key=TEST_KEY,
        bucket_name=TEST_BUCKET,
        replace=True
    )

    print(f"Successfully wrote test file to s3://{TEST_BUCKET}/{TEST_KEY}")


# ======================================================
# DAG DEFINITION
# ======================================================
with DAG(
    dag_id="oci_s3_write_test",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["s3", "test", "oci"],
    description="Tests whether Airflow can connect & write to OCI S3",
):

    test_s3_write = PythonOperator(
        task_id="write_test_file",
        python_callable=write_test_file_to_s3
    )

    test_s3_write
