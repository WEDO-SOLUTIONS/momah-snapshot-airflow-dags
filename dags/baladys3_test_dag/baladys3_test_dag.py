import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


AWS_CONN_ID = "oci_s3_conn"                 # your OCI-compatible S3 connection
TEST_BUCKET = "balady_business_test"        # adjustable
TEST_KEY = "airflow_test/test_file.json"


def write_test_file_to_s3():
    """Test writing into OCI Object Storage using Airflow's S3Hook + boto3."""
    
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    # boto3 client from Airflow connection
    session = hook.get_session()
    s3_client = session.client(
        "s3",
        endpoint_url=hook.conn_config.endpoint_url,  # required for OCI
    )

    # 1. Check whether bucket exists
    try:
        s3_client.head_bucket(Bucket=TEST_BUCKET)
        print(f"Bucket {TEST_BUCKET} exists.")
    except Exception:
        print(f"Bucket {TEST_BUCKET} does NOT exist — creating it.")
        s3_client.create_bucket(Bucket=TEST_BUCKET)

    # 2. Upload a JSON test file
    payload = {"status": "SUCCESS", "message": "Airflow wrote to OCI S3!"}
    body = json.dumps(payload)

    s3_client.put_object(
        Bucket=TEST_BUCKET,
        Key=TEST_KEY,
        Body=body,
        ContentType="application/json",
    )

    print(f"Uploaded test file: s3://{TEST_BUCKET}/{TEST_KEY}")


with DAG(
    dag_id="oci_s3_write_test",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test", "s3", "oci"],
):
    test_s3_write = PythonOperator(
        task_id="write_test_file",
        python_callable=write_test_file_to_s3,
    )
