# /snapshot_pro_etl/pod_helpers.py
from kubernetes.client import models as k8s

def get_pod_override_config(git_repo_url: str, git_branch: str) -> dict:
    """Returns the executor_config for a pod that installs the project via setup.py."""
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                init_containers=[
                    k8s.V1Container(
                        name="git-sync-init",
                        image="registry.k8s.io/git-sync/git-sync:v4.3.0",
                        args=[
                            f"--repo={git_repo_url}",
                            f"--branch={git_branch}",
                            "--root=/repo",
                            "--one-time",
                            "--depth=1",
                        ],
                        volume_mounts=[
                            k8s.V1VolumeMount(name="repo-storage", mount_path="/repo")
                        ],
                    )
                ],
                containers=[
                    k8s.V1Container(
                        name="base",
                        image="python:3.9-slim",
                        command=["/bin/sh", "-c"],
                        args=[
                            "pip install --no-cache-dir /repo/. && exec airflow tasks run {{ ti.dag_id }} {{ ti.task_id }} {{ ti.run_id }} --local"
                        ],
                        volume_mounts=[
                            k8s.V1VolumeMount(name="repo-storage", mount_path="/repo")
                        ],
                    )
                ],
                volumes=[
                    k8s.V1Volume(name="repo-storage", empty_dir=k8s.V1EmptyDirVolumeSource())
                ],
            )
        )
    }