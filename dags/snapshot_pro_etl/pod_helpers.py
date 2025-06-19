# /snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns a simplified pod_override.

    It ONLY overrides the main container's image and command. It assumes
    the system's default git-sync is already providing the code volume.
    """
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        # This name 'base' is required by Airflow
                        name="base",

                        # Use a lightweight, public Python image
                        image="python:3.9-slim",

                        # This command runs first inside the container
                        command=["/bin/sh", "-c"],
                        
                        # It installs our entire project via setup.py, then runs the Airflow task
                        args=[
                            "pip install --no-cache-dir /opt/airflow/dags/repo/. && exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
                        ]
                    )
                ]
            )
        )
    }