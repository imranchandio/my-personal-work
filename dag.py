"""
WSR DAG Builder
Shared module to dynamically create DAGs for Aggregation, Curation, or Consumption across multiple regions.
"""
import os
import time
from datetime import datetime, timedelta
import yaml
from airflow import DAG
from oci.config import from_file
from utils.common_utils import merge_configs, create_pipeline_tasks, create_curation_dag


# Constants
PIPELINES_CONFIG_FILE = '/opt/airflow/dags/airflow_pipelines_config.yml'
OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"


def load_oci_config():
    """Load OCI configuration."""
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)


def build_dag(pipeline_type: str, config_data: dict) -> DAG:
    """Creates and returns an Airflow DAG for the given pipeline type using config_data."""
    
    shared_dag_config = config_data['shared_dag_config']
    dag_configs = config_data['dag_configs']
    shared_dataflow_config = config_data['shared_dataflow_config']
    
    specific_config = dag_configs[pipeline_type]
    merged_config = merge_configs(shared_dag_config, specific_config)
    execution_mode = specific_config.get('execution_mode', 'sequential')
    pipeline_apps = specific_config.get('pipeline_apps', [])

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime.fromisoformat(merged_config['start_date']),
        'retries': merged_config['retries'],
        'retry_delay': timedelta(seconds=merged_config['retry_delay']),
    }

    dag = DAG(
        dag_id=merged_config['dag_id'],
        default_args=default_args,
        description=merged_config['description'],
        schedule_interval=merged_config['schedule_interval'],
        max_active_runs=merged_config['max_active_runs'],
        catchup=merged_config['catchup'],
    )

    app_details = config_data['Applications']['details']

    # Use specialized curation logic if needed
    if pipeline_type == "curation":
        pipeline_hierarchy = dag_configs['curation']['pipeline_hierarchy']
        create_curation_dag(dag, pipeline_hierarchy, app_details, config_data,  shared_dag_config)
    else:
        create_pipeline_tasks(dag, pipeline_apps, execution_mode, app_details,config_data,shared_dag_config)

    return dag


def load_pipeline_config_paths(config_file_path: str) -> dict:
    """Load mapping of pipeline names to config file paths."""
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Pipeline config file not found: {config_file_path}")
    with open(config_file_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config.get("pipeline_config", {})

# Load external airflow_pipelines_config.yml
pipeline_config_files = load_pipeline_config_paths(PIPELINES_CONFIG_FILE)

# Register DAGs from all pipeline config files
for pipeline, config_path in pipeline_config_files.items():
    with open(config_path, 'r', encoding='utf-8') as file:
        config_data = yaml.safe_load(file)

    for pipeline_type in ["aggregation", "curation", "consumption"]:
        if pipeline_type not in config_data.get("dag_configs", {}):
            continue
        try:
            dag_obj = build_dag(pipeline_type, config_data)
            dag_id = config_data['dag_configs'][pipeline_type]['dag_id']
            globals()[dag_id] = dag_obj
        except Exception as e:
            print(f"Failed to build DAG for pipeline {pipeline}, type {pipeline_type}: {e}")
