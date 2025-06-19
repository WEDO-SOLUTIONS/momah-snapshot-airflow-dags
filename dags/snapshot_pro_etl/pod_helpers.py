# /snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns the executor_config for a pod that installs the project
    by first copying it to a writable directory to solve permission errors.
    """
    # This robust shell command sequence is the final fix.
    install_and_run_command = (
        # 1. Create a temporary, writable directory for our source code.
        "mkdir -p /tmp/build_source && "
        
        # 2. Copy the read-only repo code to the writable directory.
        "cp -r /opt/airflow/dags/repo/. /tmp/build_source/ && "

        # 3. Create a target directory for the final installed packages.
        "mkdir -p /tmp/packages && "

        # 4. Install the package FROM THE WRITABLE COPY into the target directory.
        "pip install --no-cache-dir --target /tmp/packages /tmp/build_source/. && "

        # 5. Add the new packages directory to Python's path.
        "export PYTHONPATH=${PYTHONPATH}:/tmp/packages && "
        
        # 6. Finally, execute the Airflow task.
        "exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
    )

    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        # This name 'base' is required by Airflow to override the main container
                        name="base",
                        # Use a lightweight, public Python image
                        image="python:3.9-slim",
                        # The command to run inside the container
                        command=["/bin/sh", "-c"],
                        args=[install_and_run_command]
                    )
                ]
            )
        )
    }