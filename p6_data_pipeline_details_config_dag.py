"""
Single DF DAG Builder for df-app-ingestion_p6_data

This file:
- Loads YAML config: p6_data_pipeline_details_config.yml
- Builds ONE DAG for the 'ingestion' pipeline only
"""

import os
from datetime import datetime, timedelta
import yaml

from airflow import DAG
from oci.config import from_file
from utils.common_utils import merge_configs, create_pipeline_tasks


# --------------------------------------------------------------------
# Constants / Paths
# --------------------------------------------------------------------

PIPELINE_CONFIG_FILE = "/opt/airflow/dags/p6_data_pipeline_details_config.yml"
OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"


def load_oci_config():
    """Load OCI configuration if needed by downstream utils."""
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)


# --------------------------------------------------------------------
# Core DAG Builder (INGESTION ONLY)
# --------------------------------------------------------------------

def build_dag(config_data: dict) -> DAG:
    """
    Creates and returns an Airflow DAG for the 'ingestion' pipeline.
    """

    shared_dag_config = config_data["shared_dag_config"]
    dag_configs = config_data["dag_configs"]
    shared_dataflow_config = config_data["shared_dataflow_config"]

    ingestion_config = dag_configs["ingestion"]

    # Merge shared + ingestion-specific configs
    merged_config = merge_configs(shared_dag_config, ingestion_config)

    execution_mode = ingestion_config.get("execution_mode", "sequential")
    pipeline_apps = ingestion_config.get("pipeline_apps", [])

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

    app_details = config_data["Applications"]["details"]

    # Create tasks for ingestion pipeline
    create_pipeline_tasks(
        dag=dag,
        pipeline_apps=pipeline_apps,
        execution_mode=execution_mode,
        app_details=app_details,
        config_data=config_data,
        shared_dag_config=shared_dag_config,
    )

    return dag


# --------------------------------------------------------------------
# Load YAML + Register DAG
# --------------------------------------------------------------------

if not os.path.exists(PIPELINE_CONFIG_FILE):
    raise FileNotFoundError(f"Pipeline config file not found: {PIPELINE_CONFIG_FILE}")

with open(PIPELINE_CONFIG_FILE, "r", encoding="utf-8") as file:
    config_data = yaml.safe_load(file)

# Build the ingestion DAG
try:
    ingestion_dag = build_dag(config_data)
    globals()[ingestion_dag.dag_id] = ingestion_dag
except Exception as e:
    print(f"Failed to build ingestion DAG from {PIPELINE_CONFIG_FILE}: {e}")
