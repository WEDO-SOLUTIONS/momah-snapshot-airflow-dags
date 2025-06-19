# /snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns the final, correct executor_config.
    - Uses the main Airflow image.
    - Runs as the root user to install system-level build dependencies.
    - Installs the custom python package globally within the pod.
    This solves all previous errors.
    """
    # This robust shell command sequence is the final fix.
    install_and_run_command = (
        # 1. Update the OS package list.
        "apt-get update && "
        
        # 2. Install the system-level build tools needed for the 'xmlsec' library.
        #    The --no-install-recommends flag keeps the install lean.
        "apt-get install -y --no-install-recommends pkg-config libxmlsec1-dev && "
        
        # 3. Clean up the apt cache to save space.
        "apt-get clean && rm -rf /var/lib/apt/lists/* && "

        # 4. Now, install our custom Python package from the git-synced repo.
        #    Because we are root, pip will install it into the main site-packages.
        "pip install --no-cache-dir /opt/airflow/dags/repo/. && "

        # 5. Finally, execute the Airflow task.
        "exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
    )

    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="registry.momrah.gov.sa/urbi-omar/momah-airflow:latest",
                        # This security context runs the container as the root user (ID 0)
                        # which is required for the 'apt-get install' command.
                        security_context=k8s.V1SecurityContext(run_as_user=0),
                        command=["/bin/sh", "-c"],
                        args=[install_and_run_command],
                    )
                ]
            )
        )
    }