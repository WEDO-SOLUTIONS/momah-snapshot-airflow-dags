# /snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns a simplified pod_override.
    It ONLY overrides the main container's image and command, assuming
    the system's default git-sync is already providing the code volume.
    """
    # This command creates a writable directory for pip, installs the project
    # from the git-synced repo, adds it to the path, and then runs the airflow task.
    # The system git-sync places the repo at /opt/airflow/dags/repo.
    install_and_run_command = (
        "mkdir -p /tmp/packages && "
        "pip install --no-cache-dir --target /tmp/packages /opt/airflow/dags/repo/. && "
        "export PYTHONPATH=${PYTHONPATH}:/tmp/packages && "
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
                        args=[install_and_run_command],
                    )
                ]
            )
        )
    }