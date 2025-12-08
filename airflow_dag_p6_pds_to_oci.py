import time
from datetime import datetime, timedelta
import yaml
import oci
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from oci.data_flow.data_flow_client import DataFlowClient
from oci.config import from_file

CONFIG_FILE_PATH = "/opt/airflow/dags/p6_pds_to_oci_config.yaml"
OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"

# Load config once at import
with open(CONFIG_FILE_PATH, "r") as f:
    config = yaml.safe_load(f)

compartment_id = config["compartment_id"]
shared_dag_config = config["shared_dag_config"]
dataflow_shared = config["shared_dataflow_config"]
pipeline_config = config["dag_configs"]["aggregation"]
app_details = config["Applications"]["details"]
dataflow_app_conf = config["dataflow_app"][0]


def load_oci():
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)


def start_dataflow_run(application_id, cfg):
    df = DataFlowClient(load_oci())

    run_details = oci.data_flow.models.CreateRunDetails(
        compartment_id=compartment_id,
        application_id=application_id,
        display_name=cfg["display_name"],
        driver_shape=cfg["driver_shape"],
        driver_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=cfg["driver_ocpus"],
            memory_in_gbs=cfg["driver_memory"],
        ),
        executor_shape=cfg["executor_shape"],
        executor_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=cfg["executor_ocpus"],
            memory_in_gbs=cfg["executor_memory"],
        ),
        logs_bucket_uri=cfg["logs_bucket_uri"],
        num_executors=cfg["num_executors"],
        type="BATCH",
        configuration=cfg.get("configuration"),
    )

    resp = df.create_run(run_details)
    if resp.status != 200:
        raise AirflowException(f"Failed to start DataFlow run: HTTP {resp.status}")

    run_id = resp.data.id
    print(f"[P6_PDS_TO_OCI] Started DataFlow run: {run_id}")
    return run_id


def wait_for_run(dataflow_run_id):
    """
    Poll the OCI Data Flow run until it completes.
    Note: parameter name is dataflow_run_id (not run_id) to avoid clashes with Airflow context.
    """
    df = DataFlowClient(load_oci())
    poll_interval = shared_dag_config["run_poll_interval"]

    while True:
        run = df.get_run(dataflow_run_id).data
        state = run.lifecycle_state
        print(f"[P6_PDS_TO_OCI] Run {dataflow_run_id} state: {state}")

        if state == "SUCCEEDED":
            print("[P6_PDS_TO_OCI] Run completed successfully.")
            return True

        if state in ["FAILED", "CANCELED"]:
            raise AirflowException(f"[P6_PDS_TO_OCI] Run ended in {state}")

        time.sleep(poll_interval)


def run_p6_pipeline(**kwargs):
    """
    Main task: start the DataFlow run and wait for completion.
    kwargs contains Airflow context (including 'run_id'), which we do NOT pass into wait_for_run.
    """
    print("[P6_PDS_TO_OCI] Starting pipeline with context keys:", list(kwargs.keys()))

    # First (and only) app in this pipeline
    app_key = pipeline_config["pipeline_apps"][0]  # e.g. "application_id_p6_pds_to_oci"
    app_id = app_details[app_key]

    # e.g. app_key = "application_id_p6_pds_to_oci"
    # display name key = "p6_pds_to_oci_display_name"
    display_key = app_key.replace("application_id_", "") + "_display_name"
    display_name = app_details[display_key]

    cfg = {
        "display_name": display_name,
        "driver_shape": dataflow_shared["driver_shape"],
        "driver_ocpus": dataflow_shared["driver_shape_config"]["ocpus"],
        "driver_memory": dataflow_shared["driver_shape_config"]["memory_in_gbs"],
        "executor_shape": dataflow_shared["executor_shape"],
        "executor_ocpus": dataflow_shared["executor_shape_config"]["ocpus"],
        "executor_memory": dataflow_shared["executor_shape_config"]["memory_in_gbs"],
        "logs_bucket_uri": dataflow_app_conf["logs_bucket_uri"],
        "num_executors": dataflow_shared["num_executors"],
        "configuration": dataflow_app_conf.get("configuration"),
    }

    print(f"[P6_PDS_TO_OCI] Using application_id: {app_id}")
    print(f"[P6_PDS_TO_OCI] Using logs_bucket_uri: {cfg['logs_bucket_uri']}")

    dataflow_run_id = start_dataflow_run(app_id, cfg)
    wait_for_run(dataflow_run_id)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.fromisoformat(shared_dag_config["start_date"]),
    "retries": shared_dag_config["retries"],
    "retry_delay": timedelta(seconds=shared_dag_config["retry_delay"]),
}

dag = DAG(
    dag_id=pipeline_config["dag_id"],
    default_args=default_args,
    description=pipeline_config["description"],
    schedule_interval=pipeline_config["schedule_interval"],
    catchup=False,
    max_active_runs=1,
)

PythonOperator(
    task_id="run_p6_pds_oci_adw",
    python_callable=run_p6_pipeline,
    dag=dag,
)
