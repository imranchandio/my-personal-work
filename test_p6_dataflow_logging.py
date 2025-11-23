"""
Test DAG: Hard-coded OCI DataFlow run with heavy logging

- No YAML, no Variables – everything is hard-coded.
- Logs:
    * OCI config (user, tenancy, region – no keys)
    * DataFlow application metadata (via get_application)
    * Full "config" dict like the screenshot
    * CreateRunDetails payload (shapes, logs bucket, etc.)
    * Run lifecycle states until terminal
    * Detailed ServiceError info (status, code, opc-request-id, message)
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta

import oci
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from oci.config import from_file
from oci.data_flow.data_flow_client import DataFlowClient

# ---------------------------------------------------------------------------
# HARD-CODED VALUES (adjust if needed)
# ---------------------------------------------------------------------------

OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"

COMPARTMENT_ID = (
    "ocid1.compartment.oc1..aaaaaaaaly6cyhdxvvnldnu5hfk53hqxxedwc5kxh5ailrbtsmgtt3apguja"
)

APPLICATION_ID = (
    "ocid1.dataflowapplication.oc1.me-jeddah-1."
    "anvgkljrsvwgetyaw7iyguspejmebw6yafzn4ss7yckzvfohhqwze7rnrc7q"
)

LOGS_BUCKET_URI = (
    "oci://bkt-neom-enowa-des-dev-data-landing@axjj8sdvrg1w/Logs/"
)

# Shapes
DRIVER_SHAPE = "VM.Standard.E4.Flex"
EXECUTOR_SHAPE = "VM.Standard.E4.Flex"
DRIVER_OCPUS = 2
DRIVER_MEMORY_GB = 32
EXECUTOR_OCPUS = 2
EXECUTOR_MEMORY_GB = 32
NUM_EXECUTORS = 2

POLL_INTERVAL = 60  # seconds

# ---------------------------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------------------------

def load_oci_config():
    """Load OCI config from the Airflow VM/container and log high-level info."""
    logging.info("Loading OCI config from %s (profile=%s)", OCI_CONFIG_PATH, OCI_PROFILE)
    cfg = from_file(OCI_CONFIG_PATH, OCI_PROFILE)

    # Log non-sensitive config fields
    safe_cfg = {
        "tenancy": cfg.get("tenancy"),
        "user": cfg.get("user"),
        "region": cfg.get("region"),
        "fingerprint": cfg.get("fingerprint"),
        "key_file": cfg.get("key_file"),
    }
    logging.info("OCI CONFIG (safe view): %s", json.dumps(safe_cfg, indent=2))
    return cfg


def build_run_config():
    """
    Build a config dict for logging & for CreateRunDetails – formatted similar
    to the JSON block in your screenshot.
    """
    config_block = {
        "compartment_id": COMPARTMENT_ID,
        "application_id": APPLICATION_ID,
        "config": {
            "DRIVER_SHAPE": DRIVER_SHAPE,
            "DRIVER_OCPUS": DRIVER_OCPUS,
            "DRIVER_MEMORY_GB": DRIVER_MEMORY_GB,
            "EXECUTOR_SHAPE": EXECUTOR_SHAPE,
            "EXECUTOR_OCPUS": EXECUTOR_OCPUS,
            "EXECUTOR_MEMORY_GB": EXECUTOR_MEMORY_GB,
            "NUM_EXECUTORS": NUM_EXECUTORS,
            "LOGS_BUCKET_URI": LOGS_BUCKET_URI,
            "ENV": os.environ.get("ENV", "dev"),
        },
        "defined_tags": {
            "Oracle-Tags": {
                "CreatedBy": "airflow-test-dag",
            }
        },
        "display_name": "p6-pds-oci-adw-test-run",
        "freeform_tags": {},
    }

    logging.info(
        "RUN CONFIG SNAPSHOT (similar to screenshot JSON):\n%s",
        json.dumps(config_block, indent=2),
    )

    return config_block


# ---------------------------------------------------------------------------
# TASK 1: LOG ENV + APPLICATION DETAILS
# ---------------------------------------------------------------------------

def log_environment_and_application(**_):
    """Log OS env and DataFlow application metadata for debugging."""
    cfg = load_oci_config()
    df_client = DataFlowClient(cfg)

    # Log a few environment variables (limited)
    env_sample = {
        "HOSTNAME": os.environ.get("HOSTNAME"),
        "AIRFLOW__CORE__EXECUTOR": os.environ.get("AIRFLOW__CORE__EXECUTOR"),
        "PYTHONPATH": os.environ.get("PYTHONPATH"),
    }
    logging.info("ENVIRONMENT SAMPLE: %s", json.dumps(env_sample, indent=2))

    # Fetch the application details (will also 404 if not visible!)
    logging.info("Calling DataFlow get_application(%s)", APPLICATION_ID)
    try:
        app_resp = df_client.get_application(APPLICATION_ID)
        logging.info("get_application HTTP status: %s", app_resp.status)

        app = app_resp.data
        app_info = {
            "id": app.id,
            "display_name": app.display_name,
            "lifecycle_state": app.lifecycle_state,
            "compartment_id": app.compartment_id,
            "time_created": str(app.time_created),
        }
        logging.info("DataFlow APPLICATION INFO:\n%s", json.dumps(app_info, indent=2))
    except oci.exceptions.ServiceError as e:
        logging.error(
            "ServiceError in get_application: status=%s, code=%s, "
            "opc-request-id=%s, message=%s",
            getattr(e, "status", None),
            getattr(e, "code", None),
            getattr(e, "opc_request_id", None),
            getattr(e, "message", None),
        )
        # Re-raise so we clearly see this in Airflow UI
        raise


# ---------------------------------------------------------------------------
# TASK 2: START DATAFLOW RUN WITH HEAVY LOGGING
# ---------------------------------------------------------------------------

def start_dataflow_run(**context):
    cfg = load_oci_config()
    df_client = DataFlowClient(cfg)

    run_cfg = build_run_config()
    display_name = run_cfg["display_name"]

    # Log the payload about to be sent to OCI
    payload = {
        "compartment_id": COMPARTMENT_ID,
        "application_id": APPLICATION_ID,
        "display_name": display_name,
        "driver_shape": DRIVER_SHAPE,
        "driver_shape_config": {
            "ocpus": DRIVER_OCPUS,
            "memory_in_gbs": DRIVER_MEMORY_GB,
        },
        "executor_shape": EXECUTOR_SHAPE,
        "executor_shape_config": {
            "ocpus": EXECUTOR_OCPUS,
            "memory_in_gbs": EXECUTOR_MEMORY_GB,
        },
        "logs_bucket_uri": LOGS_BUCKET_URI,
        "num_executors": NUM_EXECUTORS,
        "type": "BATCH",
        "configuration": run_cfg["config"],
    }

    logging.info(
        "CreateRunDetails PAYLOAD (what we send to OCI DataFlow):\n%s",
        json.dumps(payload, indent=2),
    )

    try:
        create_run_details = oci.data_flow.models.CreateRunDetails(
            compartment_id=payload["compartment_id"],
            application_id=payload["application_id"],
            display_name=payload["display_name"],
            driver_shape=payload["driver_shape"],
            driver_shape_config=oci.data_flow.models.ShapeConfig(
                ocpus=payload["driver_shape_config"]["ocpus"],
                memory_in_gbs=payload["driver_shape_config"]["memory_in_gbs"],
            ),
            executor_shape=payload["executor_shape"],
            executor_shape_config=oci.data_flow.models.ShapeConfig(
                ocpus=payload["executor_shape_config"]["ocpus"],
                memory_in_gbs=payload["executor_shape_config"]["memory_in_gbs"],
            ),
            logs_bucket_uri=payload["logs_bucket_uri"],
            num_executors=payload["num_executors"],
            type=payload["type"],
            configuration=payload["configuration"],
        )

        resp = df_client.create_run(create_run_details)

    except oci.exceptions.ServiceError as e:
        logging.error(
            "ServiceError in create_run: status=%s, code=%s, opc-request-id=%s, "
            "message=%s",
            getattr(e, "status", None),
            getattr(e, "code", None),
            getattr(e, "opc_request_id", None),
            getattr(e, "message", None),
        )
        raise AirflowException(
            f"OCI ServiceError during create_run: status={getattr(e, 'status', None)}, "
            f"code={getattr(e, 'code', None)}, message={getattr(e, 'message', None)}"
        )

    logging.info("create_run HTTP status: %s", resp.status)
    if resp.status != 200:
        raise AirflowException(f"Failed to start DataFlow run. HTTP {resp.status}")

    run_id = resp.data.id
    logging.info("✅ DataFlow RUN ID: %s", run_id)

    # Build a final JSON block similar to your screenshot
    summary = {
        "compartment_id": COMPARTMENT_ID,
        "application_id": APPLICATION_ID,
        "run_id": run_id,
        "logs_bucket_uri": LOGS_BUCKET_URI,
        "config": payload["configuration"],
        "display_name": display_name,
    }
    logging.info("RUN SUMMARY JSON:\n%s", json.dumps(summary, indent=2))

    # Push to XCom so the next task can monitor it
    context["ti"].xcom_push(key="dataflow_run_id", value=run_id)


# ---------------------------------------------------------------------------
# TASK 3: MONITOR RUN
# ---------------------------------------------------------------------------

def monitor_dataflow_run(**context):
    cfg = load_oci_config()
    df_client = DataFlowClient(cfg)

    run_id = context["ti"].xcom_pull(key="dataflow_run_id", task_ids="start_dataflow")
    if not run_id:
        raise AirflowException("No run_id found in XCom – cannot monitor run.")

    logging.info("Starting monitor loop for run_id=%s", run_id)

    while True:
        try:
            resp = df_client.get_run(run_id)
        except oci.exceptions.ServiceError as e:
            logging.error(
                "ServiceError in get_run: status=%s, code=%s, opc-request-id=%s, "
                "message=%s",
                getattr(e, "status", None),
                getattr(e, "code", None),
                getattr(e, "opc_request_id", None),
                getattr(e, "message", None),
            )
            raise

        run = resp.data
        state = run.lifecycle_state
        logging.info(
            "Run %s current state: %s (HTTP=%s)",
            run_id,
            state,
            resp.status,
        )

        # Pretty JSON dump of the run details (trimmed)
        run_snapshot = {
            "id": run.id,
            "display_name": run.display_name,
            "compartment_id": run.compartment_id,
            "application_id": run.application_id,
            "lifecycle_state": run.lifecycle_state,
            "time_created": str(run.time_created),
            "time_updated": str(run.time_updated),
        }
        logging.info(
            "RUN SNAPSHOT:\n%s", json.dumps(run_snapshot, indent=2)
        )

        if state == "SUCCEEDED":
            logging.info("🎉 DataFlow run %s completed successfully.", run_id)
            return True

        if state in ("FAILED", "CANCELED"):
            raise AirflowException(
                f"❌ DataFlow run {run_id} ended with state={state}"
            )

        time.sleep(POLL_INTERVAL)


# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="test_p6_dataflow_logging",
    default_args=default_args,
    description="Hard-coded test DAG for OCI DataFlow with heavy logging",
    schedule_interval=None,  # manual trigger only
    catchup=False,
    max_active_runs=1,
)

log_env_task = PythonOperator(
    task_id="log_env_and_application",
    python_callable=log_environment_and_application,
    dag=dag,
)

start_task = PythonOperator(
    task_id="start_dataflow",
    python_callable=start_dataflow_run,
    provide_context=True,
    dag=dag,
)

monitor_task = PythonOperator(
    task_id="monitor_dataflow",
    python_callable=monitor_dataflow_run,
    provide_context=True,
    dag=dag,
)

log_env_task >> start_task >> monitor_task
