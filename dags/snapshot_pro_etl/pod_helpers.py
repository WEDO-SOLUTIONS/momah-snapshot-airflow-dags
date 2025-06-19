# /dags/snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns the final, correct executor_config.
    It uses the main Airflow image, runs as root to install system build
    dependencies, and requests sufficient memory/cpu to avoid being killed.
    """
    # This command installs OS packages, then our Python package.
    install_and_run_command = (
        "set -ex && "
        "apt-get update -y && "
        "apt-get install -y --no-install-recommends pkg-config libxmlsec1-dev && "
        "apt-get clean && rm -rf /var/lib/apt/lists/* && "
        # The path /opt/airflow/dags/ is correct due to the `subPath: dags` setting.
        "pip install --no-cache-dir /opt/airflow/dags/. && "
        "exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
    )

    # Define resource requirements for the pod.
    # This is the crucial fix for the OOMKill (Out of Memory) issue.
    resources = k8s.V1ResourceRequirements(
        requests={"cpu": "1000m", "memory": "2Gi"},
        limits={"cpu": "2000m", "memory": "4Gi"}
    )

    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="registry.momrah.gov.sa/urbi-omar/momah-airflow:latest",
                        security_context=k8s.V1SecurityContext(run_as_user=0),
                        command=["/bin/sh", "-c"],
                        args=[install_and_run_command],
                        # Add the resource requests and limits to the container
                        resources=resources,
                    )
                ]
            )
        )
    }