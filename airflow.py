"""
Simple Airflow DAG to run a single OCI Data Flow application
using p6_data_pipeline_details_config.yaml.

- YAML: /opt/airflow/dags/p6_data_pipeline_details_config.yaml
- OCI config: /opt/airflow/.oci/config (profile: DEFAULT)
"""

import time
from datetime import datetime, timedelta

import yaml
import oci
from oci.data_flow.data_flow_client import DataFlowClient
from oci.config import from_file
from oci.exceptions import TransientServiceError

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

# --------------------------------------
# CONSTANTS / PATHS
# --------------------------------------
CONFIG_FILE = "/opt/airflow/dags/p6_data_pipeline_details_config.yaml"
OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"


# --------------------------------------
# HELPERS
# --------------------------------------
def load_yaml_config(path: str) -> dict:
    """Load YAML config from the given path."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_oci_config():
    """Load OCI configuration using config file + profile."""
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)


def trigger_dataflow_run(**context):
    """
    Trigger a single OCI Data Flow application run and poll until it finishes.
    Uses the 'simple' dag_config and the first task in its 'tasks' list.
    """
    cfg = load_yaml_config(CONFIG_FILE)

    # --- DAG runtime config ---
    dag_cfg = cfg["dag_configs"]["simple"]
    shared_dag_cfg = cfg.get("shared_dag_config", {})

    tasks = dag_cfg.get("tasks", [])
    if not tasks:
        raise AirflowException("No 'tasks' found in dag_configs.simple.tasks")
    app_key = tasks[0]  # e.g. 'p6_simple_app'

    # --- Application details ---
    app_details_all = cfg["Applications"]["details"]
    if app_key not in app_details_all:
        raise AirflowException(f"Application key '{app_key}' not found under Applications.details")

    app_details = app_details_all[app_key]

    application_id = app_details["application_id"]
    application_display_name = app_details.get("display_name", f"DataFlowRun-{app_key}")
    arguments = app_details.get("arguments", [])

    # --- DataFlow config ---
    shared_df_cfg = cfg["shared_dataflow_config"]
    df_app_list = cfg.get("dataflow_app", [])
    df_app_cfg = next(
        (a for a in df_app_list if a.get("application_key") == app_key),
        {}
    )

    logs_bucket_uri = df_app_cfg.get("logs_bucket_uri", shared_df_cfg.get("logs_bucket_uri"))

    compartment_id = app_details.get("compartment_id", cfg["compartment_id"])

    # --- OCI clients ---
    oci_config = load_oci_config()
    df_client = DataFlowClient(oci_config)

    # Build Shape configs
    driver_shape_cfg = oci.data_flow.models.ShapeConfig(
        ocpus=shared_df_cfg["driver_shape_config"]["ocpus"],
        memory_in_gbs=shared_df_cfg["driver_shape_config"]["memory_in_gbs"],
    )

    executor_shape_cfg = oci.data_flow.models.ShapeConfig(
        ocpus=shared_df_cfg["executor_shape_config"]["ocpus"],
        memory_in_gbs=shared_df_cfg["executor_shape_config"]["memory_in_gbs"],
    )

    # Create run details (no pool / private endpoint)
    create_run_details = oci.data_flow.models.CreateRunDetails(
        application_id=application_id,
        compartment_id=compartment_id,
        display_name=application_display_name,
        arguments=arguments,
        logs_bucket_uri=logs_bucket_uri,
        driver_shape=shared_df_cfg["driver_shape"],
        executor_shape=shared_df_cfg["executor_shape"],
        driver_shape_config=driver_shape_cfg,
        executor_shape_config=executor_shape_cfg,
        num_executors=shared_df_cfg["num_executors"],
    )

    # --- Create run ---
    try:
        response = df_client.create_run(create_run_details)
    except TransientServiceError as e:
        raise AirflowException(f"Transient error while creating Data Flow run: {e}") from e
    except Exception as e:
        raise AirflowException(f"Error while creating Data Flow run: {e}") from e

    run_id = response.data.id
    print(f"[DataFlow] Created run with ID: {run_id}")

    # --- Poll for completion ---
    poll_interval = dag_cfg.get(
        "run_poll_interval",
        shared_dag_cfg.get("run_poll_interval", 60),
    )

    while True:
        run = df_client.get_run(run_id).data
        state = run.lifecycle_state
        print(f"[DataFlow] Run {run_id} state: {state}")

        if state in ("SUCCEEDED", "FAILED", "CANCELED"):
            if state != "SUCCEEDED":
                raise AirflowException(f"Data Flow run {run_id} finished with state {state}")
            print(f"[DataFlow] Run {run_id} SUCCEEDED")
            break

        time.sleep(poll_interval)


# --------------------------------------
# BUILD DAG FROM CONFIG
# --------------------------------------
_cfg = load_yaml_config(CONFIG_FILE)
_dag_cfg = _cfg["dag_configs"]["simple"]
_shared_dag_cfg = _cfg.get("shared_dag_config", {})

default_args = {
  "owner": "airflow",
  "depends_on_past": False,
  "retries": _dag_cfg.get("retries", _shared_dag_cfg.get("retries", 0)),
  "retry_delay": timedelta(
      seconds=_dag_cfg.get("retry_delay", _shared_dag_cfg.get("retry_delay", 300))
  ),
}

start_date_str = _shared_dag_cfg.get("start_date", "2024-11-20T00:00:00")
start_date = datetime.fromisoformat(start_date_str)

dag = DAG(
    dag_id=_dag_cfg["dag_id"],
    description=_dag_cfg.get("description", "Simple OCI Data Flow DAG (P6)"),
    default_args=default_args,
    schedule_interval=_dag_cfg.get("schedule_interval", None),
    start_date=start_date,
    catchup=_dag_cfg.get("catchup", _shared_dag_cfg.get("catchup", False)),
    max_active_runs=_dag_cfg.get(
        "max_active_runs", _shared_dag_cfg.get("max_active_runs", 1)
    ),
)

run_p6_simple_app = PythonOperator(
    task_id="run_p6_simple_app",
    python_callable=trigger_dataflow_run,
    provide_context=True,
    dag=dag,
)

# Single-task DAG, no dependencies to define
