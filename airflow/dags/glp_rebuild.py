"""Rebuild marts from retained RAW on a schedule, as a Kubernetes pod per run.

The work runs in the pipeline image (KubernetesPodOperator), so Airflow only orchestrates:
scheduling, retries, one run at a time, and logs. Credentials come from Kubernetes Secrets by
reference; none are stored in Airflow or in this file.
"""
import os
from datetime import datetime, timedelta

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import DAG
from kubernetes.client import models as k8s

IMAGE = os.environ.get("GLP_IMAGE", "ghcr.io/llernandez/game-log-pipeline:0.3.0")
PULL_POLICY = os.environ.get("GLP_IMAGE_PULL_POLICY", "IfNotPresent")
NAMESPACE = os.environ.get("GLP_NAMESPACE", "glp")


def secret_env(name, secret, key, optional=False):
    return k8s.V1EnvVar(name=name, value_from=k8s.V1EnvVarSource(
        secret_key_ref=k8s.V1SecretKeySelector(name=secret, key=key, optional=optional)))


# Snowflake is an optional second target: absent Secret -> variables unset -> PostgreSQL marts only.
ENV = [secret_env("GLP_POSTGRES_DSN", "glp-db-app", "uri")] + [
    secret_env("GLP_SNOWFLAKE_" + key.upper(), "glp-snowflake", key, optional=True)
    for key in ("account", "user", "password", "warehouse", "database", "schema")]

with DAG(
    dag_id="glp_rebuild",
    description="RAW (Kafka -> PostgreSQL landing) -> CLEAN/quarantine/anomaly marts",
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["game-log-pipeline"],
) as dag:
    KubernetesPodOperator(
        task_id="rebuild_marts",
        name="glp-rebuild",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy=PULL_POLICY,
        arguments=["rebuild"],
        env_vars=ENV,
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        security_context=k8s.V1PodSecurityContext(run_as_non_root=True, run_as_user=10001),
    )
