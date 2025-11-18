"""
Airflow DAG: P6 PDS → OCI Object Storage + ADW via Data Flow

- Driven entirely by the YAML config file.
- Uses only the `aggregation` block from `dag_configs`.
- No curation / pipeline_hierarchy logic.
- Pool handling is disabled (no pool_id, no start_pool_if_stopped).
"""

import time
from datetime import datetime, timedelta

import oci
import yaml
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from oci.data_flow.data_flow_client import DataFlowClient
from oci.config import from_file
from oci.exceptions import TransientServiceError

# ---------------------------------------------------------------------------
# CONFIG PATHS (adjust if your layout is different)
# ---------------------------------------------------------------------------

# Path to your YAML file (the one you pasted to me)
CONFIG_FILE_PATH = "/opt/airflow/dags/p6_pds_to_oci_adw_config.yaml"

# OCI config used by the Airflow worker/Container
OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"

DEFAULT_EXECUTION_MODE = "sequential"

# ---------------------------------------------------------------------------
# LOAD YAML CONFIG
# ---------------------------------------------------------------------------

with open(CONFIG_FILE_PATH, "r") as f:
    config_data = yaml.safe_load(f)

compartment_id = config_data["compartment_id"]
shared_dag_config = config_data["shared_dag_config"]
dag_configs = config_data["dag_configs"]
shared_dataflow_config = config_data["shared_dataflow_config"]
dataflow_app_configs = config_data.get("dataflow_app", [])
app_details = config_data["Applications"]["details"]
run_poll_interval = shared_dag_config["run_poll_interval"]


# ---------------------------------------------------------------------------
# HELPER: LOAD OCI CONFIG
# ---------------------------------------------------------------------------

def load_oci_config():
    """Load OCI configuration."""
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)


# ---------------------------------------------------------------------------
# HELPER: MERGE DAG CONFIGS
# ---------------------------------------------------------------------------

def merge_configs(shared_config, pipeline_specific_config):
    """
    Merge shared DAG config with pipeline-specific DAG config.
    Specific config values override shared ones.
    """
    merged = shared_config.copy()
    merged.update(pipeline_specific_config)
    return merged


# ---------------------------------------------------------------------------
# HELPER: MERGE DATA FLOW CONFIGS
# ---------------------------------------------------------------------------

def get_merged_dataflow_config(app_key: str) -> dict:
    """
    Merge shared_dataflow_config with specific configurations for a given app_key.
    Specific configurations override shared configurations.

    app_key is something like "application_id_p6_adw_ops".
    """
    # Validate shared config
    if not isinstance(shared_dataflow_config, dict):
        raise ValueError("shared_dataflow_config must be a dictionary.")

    # Find specific config for this application
    specific_df_config = next(
        (
            app_config
            for app_config in dataflow_app_configs
            if app_config.get("application_id") == app_key
        ),
        {},
    )

    if not isinstance(specific_df_config, dict):
        raise ValueError(f"Specific configuration for {app_key} must be a dictionary.")

    merged_df_config = shared_dataflow_config.copy()
    merged_df_config.update(specific_df_config)
    return merged_df_config


# ---------------------------------------------------------------------------
# DATA FLOW RUN HELPERS
# ---------------------------------------------------------------------------

def initiate_dataflow_run(data_flow_client, compartment_id, application_id, config):
    """
    Initiate a Data Flow run using the config dict.

    config keys:
        - display_name
        - driver_shape, driver_ocpus, driver_memory
        - executor_shape, executor_ocpus, executor_memory
        - logs_bucket_uri
        - num_executors
        - configuration (optional dict passed to Data Flow run)
    """
    driver_shape = config["driver_shape"]
    driver_ocpus = config["driver_ocpus"]
    driver_memory = config["driver_memory"]
    executor_shape = config["executor_shape"]
    executor_ocpus = config["executor_ocpus"]
    executor_memory = config["executor_memory"]
    logs_bucket_uri = config["logs_bucket_uri"]
    num_executors = config["num_executors"]
    display_name = config["display_name"]
    extra_configuration = config.get("configuration")  # may be None

    # Prepare CreateRunDetails (no pool_id here – pool handling disabled)
    create_run_details = oci.data_flow.models.CreateRunDetails(
        compartment_id=compartment_id,
        application_id=application_id,
        display_name=display_name,
        driver_shape=driver_shape,
        driver_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=driver_ocpus,
            memory_in_gbs=driver_memory,
        ),
        executor_shape=executor_shape,
        executor_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=executor_ocpus,
            memory_in_gbs=executor_memory,
        ),
        logs_bucket_uri=logs_bucket_uri,
        num_executors=num_executors,
        type="BATCH",
        configuration=extra_configuration,
    )

    return data_flow_client.create_run(create_run_details=create_run_details)


def wait_for_run_completion(data_flow_client, run_id, task_context):
    """
    Poll the Data Flow run until it reaches a terminal state.
    """
    while True:
        time.sleep(run_poll_interval)

        run_details = data_flow_client.get_run(run_id)
        lifecycle_state = run_details.data.lifecycle_state
        print(f"Run {run_id} is in state: {lifecycle_state}")

        # Push status to XCom for inspection
        task_context["ti"].xcom_push(
            key=f"dataflow_status_{run_id}", value=lifecycle_state
        )

        if run_details.status != 200:
            raise AirflowException("Failed to retrieve Data Flow run information")

        if lifecycle_state in ("SUCCEEDED", "FAILED", "CANCELED"):
            if lifecycle_state != "SUCCEEDED":
                raise AirflowException(
                    f"Data Flow run {run_id} ended with state: {lifecycle_state}"
                )
            print(f"Data Flow run {run_id} completed successfully.")
            break


def handle_service_error(e, retry_delay, backoff_factor):
    """
    Basic handler for OCI ServiceError with exponential backoff for some codes.
    """
    if e.status == 409:  # Incorrect State
        print(f"HTTP 409 (Incorrect State): {e.message}")
    elif e.status == 429:  # Too Many Requests
        print(f"HTTP 429 (Throttling): {e.message}")
    elif 500 <= e.status < 600 and e.status != 501:
        print(f"HTTP {e.status} (Server Error): {e.message}")
    else:
        raise AirflowException(f"Unhandled HTTP error {e.status}: {e.message}") from e

    time.sleep(retry_delay)
    return retry_delay * backoff_factor


def handle_transient_service_error(error, attempt, max_retries, retry_delay):
    """
    Handles transient service errors with exponential backoff.
    Currently only special cases 429; others are re-raised.
    """
    if error.status == 429:
        print(
            f"429 TooManyRequests: Retry {attempt + 1}/{max_retries}. "
            f"Waiting {retry_delay} seconds..."
        )
        time.sleep(retry_delay)
    else:
        raise AirflowException(f"Failed due to: {error}")


def run_dataflow_application_with_backoff(application_id, config, **kwargs):
    """
    Run OCI Data Flow application with retries and exponential backoff.

    NOTE:
    - Pool logic is disabled (no pool_id, no start_pool_if_stopped call).
    """
    max_retries = shared_dag_config["retries"]
    retry_delay = shared_dag_config["retry_delay"]
    backoff_factor = 2

    for attempt in range(max_retries):
        try:
            oci_config = load_oci_config()
            data_flow_client = DataFlowClient(oci_config)

            response = initiate_dataflow_run(
                data_flow_client, compartment_id, application_id, config
            )

            if response.status != 200:
                raise AirflowException(
                    f"Failed to start Data Flow run. Status: {response.status}"
                )

            print(f"Data Flow Run ID: {response.data.id}")
            wait_for_run_completion(
                data_flow_client, response.data.id, kwargs
            )
            return True

        except oci.exceptions.ServiceError as e:
            retry_delay = handle_service_error(e, retry_delay, backoff_factor)
        except TransientServiceError as e:
            handle_transient_service_error(e, attempt, max_retries, retry_delay)
        except (ValueError, TypeError, KeyError) as e:
            # Specific config/parameter issues: don't keep retrying forever
            print(f"Specific error occurred: {e}")
            raise AirflowException(f"Specific error occurred: {e}") from e
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            raise AirflowException(f"Unexpected error occurred: {e}") from e

    raise AirflowException("Exceeded maximum retries for Data Flow run.")


# ---------------------------------------------------------------------------
# TASK FACTORY
# ---------------------------------------------------------------------------

def create_pipeline_tasks(dag_instance, pipeline_applications, execution_strategy):
    """
    Create tasks for each application_id_* in pipeline_applications list.
    Supports 'sequential' or 'parallel' execution.
    """
    tasks = []
    previous_task = None

    for app_key in pipeline_applications:
        # Get actual Data Flow app OCID and display name
        app_id = app_details[app_key]
        display_name_key = app_key.replace("application_id_", "") + "_display_name"
        display_name = app_details.get(
            display_name_key, f"Unknown Display Name for {app_key}"
        )

        merged_df_config = get_merged_dataflow_config(app_key)

        # Build config dict passed into Data Flow run helper
        config = {
            "driver_shape": merged_df_config["driver_shape"],
            "driver_ocpus": merged_df_config["driver_shape_config"]["ocpus"],
            "driver_memory": merged_df_config["driver_shape_config"]["memory_in_gbs"],
            "executor_shape": merged_df_config["executor_shape"],
            "executor_ocpus": merged_df_config["executor_shape_config"]["ocpus"],
            "executor_memory": merged_df_config["executor_shape_config"]["memory_in_gbs"],
            "logs_bucket_uri": merged_df_config["logs_bucket_uri"],
            "num_executors": merged_df_config["num_executors"],
            # Pool is intentionally not used:
            # "pool_id": merged_df_config.get("pool_id", ""),
            "display_name": display_name,
            "configuration": merged_df_config.get("configuration"),
        }

        task = PythonOperator(
            task_id=f"run_{app_key}_dataflow_application",
            python_callable=run_dataflow_application_with_backoff,
            op_args=[app_id, config],
            provide_context=True,
            dag=dag_instance,
        )

        tasks.append(task)

        if execution_strategy == "sequential" and previous_task is not None:
            previous_task >> task

        previous_task = task

    return tasks


# ---------------------------------------------------------------------------
# DAG DEFINITION (AGGREGATION ONLY)
# ---------------------------------------------------------------------------

PIPELINE_TYPE = "aggregation"  # only type we care about for this DAG

specific_config = dag_configs[PIPELINE_TYPE]
merged_config = merge_configs(shared_dag_config, specific_config)

execution_mode = specific_config.get("execution_mode", DEFAULT_EXECUTION_MODE)
pipeline_apps = specific_config.get("pipeline_apps", [])

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.fromisoformat(merged_config["start_date"]),
    "retries": merged_config["retries"],
    "retry_delay": timedelta(seconds=merged_config["retry_delay"]),
}

dag = DAG(
    dag_id=merged_config["dag_id"],
    default_args=default_args,
    description=merged_config["description"],
    schedule_interval=merged_config["schedule_interval"],
    max_active_runs=merged_config["max_active_runs"],
    catchup=merged_config["catchup"],
)

# Attach tasks to the DAG
create_pipeline_tasks(dag, pipeline_apps, execution_mode)
