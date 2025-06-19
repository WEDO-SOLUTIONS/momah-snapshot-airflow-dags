# /dags/snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns the final, correct executor_config.
    It uses the main Airflow image, runs as root to install system build tools,
    and then installs the custom package from the correct path.
    """
    # This final command installs OS packages, then our Python package.
    install_and_run_command = (
        "set -ex && "

        # 1. Install system-level build tools for the 'xmlsec' library.
        "apt-get update -y && "
        "apt-get install -y --no-install-recommends pkg-config libxmlsec1-dev && "
        "apt-get clean && rm -rf /var/lib/apt/lists/* && "

        # 2. Install our custom Python package. The path /opt/airflow/dags/
        #    is now correct because of the `subPath` setting.
        "pip install --no-cache-dir /opt/airflow/dags/. && "

        # 3. Execute the Airflow task.
        "exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
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
                    )
                ]
            )
        )
    }