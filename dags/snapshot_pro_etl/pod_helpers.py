# /snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config() -> dict:
    """
    Returns the final, robust executor_config.
    This version includes verbose logging (set -ex) to ensure we see every step
    and installs all dependencies before running the task.
    """
    # This robust shell command sequence is the final fix.
    install_and_run_command = (
        # Use 'set -ex' to print each command and exit immediately on any error.
        "set -ex && "

        # 1. Update OS packages and install build tools.
        "echo 'INFO: Step 1/5 - Updating OS packages...' && "
        "apt-get update -y && "
        "apt-get install -y --no-install-recommends pkg-config libxmlsec1-dev && "
        "apt-get clean && rm -rf /var/lib/apt/lists/* && "

        # 2. Copy the read-only repo to a writable location to solve build errors.
        "echo 'INFO: Step 2/5 - Copying source code to /tmp/build_source...' && "
        "cp -r /opt/airflow/dags/repo /tmp/build_source && "
        
        # 3. Install all Python packages from our setup.py file.
        #    Because we are root, pip will install into the global site-packages.
        "echo 'INFO: Step 3/5 - Installing Python packages...' && "
        "pip install --no-cache-dir /tmp/build_source/. && "

        # 4. Verify the airflow command is available.
        "echo 'INFO: Step 4/5 - Verifying airflow command...' && "
        "which airflow && "

        # 5. Execute the Airflow task.
        "echo 'INFO: Step 5/5 - All dependencies installed. Executing Airflow task...' && "
        "exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
    )

    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        # This name 'base' is required by Airflow
                        name="base",
                        # Use your main Airflow image, which has the 'airflow' command
                        image="registry.momrah.gov.sa/urbi-omar/momah-airflow:latest",
                        # Run as root (user 0) to allow apt-get install
                        security_context=k8s.V1SecurityContext(run_as_user=0),
                        # The command to run inside the container
                        command=["/bin/sh", "-c"],
                        args=[install_and_run_command],
                    )
                ]
            )
        )
    }