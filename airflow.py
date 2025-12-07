"""
Airflow DAG: p6 simple oci dataflow

Reads config from p6_simple_dataflow_config.yaml and runs a single
OCI Data Flow application, polling until it finishes.
"""

import time
from datetime import datetime, timedelta

import yaml
import oci
from oci.data_flow.data_flow_client import DataFlowClient
from oci.config import from_file
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

CONFIG_FILE = "/opt/airflow/dags/p6_simple_dataflow_config.yaml"
OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"


def load_yaml_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_oci_config():
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)


def trigger_p6_simple_dataflow(**context):

    cfg = load_yaml_config(CONFIG_FILE)

    dag_cfg = cfg["dag"]
    shared_dag_cfg = cfg["shared_dag_config"]
    df_shared = cfg["shared_dataflow_config"]
    app_cfg = cfg["dataflow_application"]

    application_id = app_cfg["application_id"]
    display_name = app_cfg["display_name"]
    arguments = app_cfg.get("arguments", [])
    compartment_id = cfg["compartment_id"]

    logs_bucket_uri = df_shared["logs_bucket_uri"]

    oci_cfg = load_oci_config()
    df_client = DataFlowClient(oci_cfg)

    driver_cfg = oci.data_flow.models.ShapeConfig(
        ocpus=df_shared["driver_shape_config"]["ocpus"],
        memory_in_gbs=df_shared["driver_shape_config"]["memory_in_gbs"],
    )

    executor_cfg = oci.data_flow.models.ShapeConfig(
        ocpus=df_shared["executor_shape_config"]["ocpus"],
        memory_in_gbs=df_shared["executor_shape_config"]["memory_in_gbs"],
    )

    create_run_details = oci.data_flow.models.CreateRunDetails(
        application_id=application_id,
        compartment_id=compartment_id,
        display_name=display_name,
        arguments=arguments,
        logs_bucket_uri=logs_bucket_uri,
        driver_shape=df_shared["driver_shape"],
        executor_shape=df_shared["executor_shape"],
        driver_shape_config=driver_cfg,
        executor_shape_config=executor_cfg,
        num_executors=df_shared["num_executors"],
    )

    response = df_client.create_run(create_run_details)
    run_id = response.data.id
    print(f"[p6_simple_dataflow] run created: {run_id}")

    poll_interval = dag_cfg.get("run_poll_interval", shared_dag_cfg.get("run_poll_interval", 60))

    while True:
        run = df_client.get_run(run_id).data
        state = run.lifecycle_state
        print(f"[p6_simple_dataflow] run {run_id} state: {state}")

        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            if state != "SUCCEEDED":
                raise AirflowException(f"dataflow run {run_id} finished with state {state}")
            print(f"[p6_simple_dataflow] run {run_id} succeeded")
            break

        time.sleep(poll_interval)


cfg_root = load_yaml_config(CONFIG_FILE)
dag_cfg = cfg_root["dag"]
shared_dag_cfg = cfg_root["shared_dag_config"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": dag_cfg["retries"],
    "retry_delay": timedelta(seconds=dag_cfg["retry_delay"]),
}

start_date = datetime.fromisoformat(shared_dag_cfg["start_date"])

dag = DAG(
    dag_id=dag_cfg["dag_id"],
    description=dag_cfg["description"],
    schedule_interval=dag_cfg["schedule_interval"],
    default_args=default_args,
    start_date=start_date,
    catchup=dag_cfg["catchup"],
    max_active_runs=dag_cfg["max_active_runs"],
)

run_p6_simple_dataflow = PythonOperator(
    task_id=dag_cfg["task_id"],
    python_callable=trigger_p6_simple_dataflow,
    dag=dag,
)
