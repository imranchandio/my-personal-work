"""
Dynamic DAG Creation for ALBADA Pipelines (Aggregation, Curation, Consumption)

This script dynamically creates DAGs for the ALBADA pipelines based on shared
and pipeline-specific configurations. Only one pipeline type (aggregation, curation, or consumption)
will be triggered per DAG execution.
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


# Load configuration YAML
CONFIG_FILE_PATH = '/opt/airflow/dags/ALBADA_Aggregation_data_pipelines_details_config.yaml'

OCI_CONFIG_PATH = "/opt/airflow/.oci/config"
OCI_PROFILE = "DEFAULT"
DEFAULT_EXECUTION_MODE = "sequential"

# Helper to load OCI configuration
def load_oci_config():
    """Load OCI configuration."""
    return from_file(OCI_CONFIG_PATH, OCI_PROFILE)

with open(CONFIG_FILE_PATH, 'r', encoding='utf-8') as file:
    config_data = yaml.safe_load(file)


# Extract shared and specific DAG configurations
shared_dag_config = config_data['shared_dag_config']
dag_configs = config_data['dag_configs']
app_details = config_data['Applications']['details']
run_poll_interval = shared_dag_config['run_poll_interval']
pipeline_hierarchy = dag_configs['curation']['pipeline_hierarchy']
shared_dataflow_config = config_data['shared_dataflow_config']

def get_merged_dataflow_config(app_key):
    """
    Merge shared_dataflow_config with specific configurations for the given app_key.
    Specific configurations override shared configurations.

    Args:
        app_key (str): The application key for which the configuration is needed.

    Returns:
        dict: Merged configuration for the application.
    """
    # Fetch shared and specific dataflow configurations
    shared_config = config_data['shared_dataflow_config']

    dataflow_app_configs = config_data['dataflow_app']

    # Validate shared_config is a dictionary
    if not isinstance(shared_config, dict):
        raise ValueError("shared_dataflow_config must be a dictionary.")

    # Find specific configuration for the given app_key in dataflow_app
    specific_df_config = next(
        (
            app_config
            for app_config in dataflow_app_configs
            if app_config.get('application_id') == app_key
        ),
        {}
    )

    # Validate specific_config is a dictionary
    if not isinstance(specific_df_config, dict):
        raise ValueError(f"Specific configuration for {app_key} must be a dictionary.")

    # Merge shared and specific configurations (specific overrides shared)
    merged_df_config = shared_config.copy()
    merged_df_config.update(specific_df_config)

    return merged_df_config

def start_pool_if_stopped(pool_id):
    """
    Checks the state of an OCI Data Flow pool and starts it if it is in a STOPPED state.

    Args:
        pool_id (str): The OCID of the pool to check and start.
        config_file (str): Path to the OCI configuration file. Defaults to "~/.oci/config".
        profile (str): Profile name in the OCI configuration file. Defaults to "DEFAULT".

    Returns:
        str: A message indicating the pool's status or the action taken.
    """
    try:
        # Load OCI configuration
        config = load_oci_config()
        # Initialize the Data Flow client
        data_flow_client = oci.data_flow.DataFlowClient(config)

        # Get the current state of the pool
        pool_details = data_flow_client.get_pool(pool_id)
        pool_state = pool_details.data.lifecycle_state

        # Default message
        message = ""

        # Check the pool's state
        if pool_state == "STOPPED":
            print(f"Pool {pool_id} is in STOPPED state. Starting the pool...")

            # Start the pool
            data_flow_client.start_pool(pool_id)
            message = f"Pool {pool_id} has been started successfully."
        elif pool_state == "ACTIVE":
            message = f"Pool {pool_id} is already ACTIVE."
        else:
            message = f"Pool {pool_id} is in {pool_state} state. No action taken."

        print(message)
        return message


    except oci.exceptions.ServiceError as e:
        # Specific handling for OCI service errors
        return f"OCI Service Error: {e}"
    except oci.exceptions.ConfigFileNotFound as e:
        # Specific handling for missing config file
        return f"OCI Config File Not Found: {e}"
    except oci.exceptions.InvalidConfig as e:
        # Specific handling for invalid config
        return f"OCI Invalid Config: {e}"
    except FileNotFoundError as e:
        # Handle file not found errors for the config file
        return f"File Not Found: {e}"
    except ValueError as e:
        # Handle value errors, e.g., invalid profile names
        return f"Value Error: {e}"
    except Exception as e:
        # Log unexpected errors but avoid suppressing them
        print(f"Unexpected error occurred: {e}")
        raise  # Re-raise the exception to avoid suppressing it

# Helper function to merge configurations
def merge_configs(shared_config, pipeline_specific_config):
    """
    Merge shared configurations with specific configurations.
    Specific configurations take precedence.
    """
    merged = shared_config.copy()
    merged.update(pipeline_specific_config)
    return merged

def run_dataflow_application_with_backoff(application_id, config, **kwargs):
    """
    Run an OCI Data Flow application with exponential backoff for retries.
    """
    pool_id = config["pool_id"]
    compartment_id = config_data["compartment_id"]
    max_retries = shared_dag_config["retries"]
    retry_delay = shared_dag_config["retry_delay"]
    backoff_factor = 2


    # Ensure the pool is active before running
    start_pool_if_stopped(pool_id)

    for attempt in range(max_retries):
        try:
            # Initiate and monitor the Data Flow application run
            return initiate_and_monitor_run(compartment_id, application_id, config, kwargs)
        except oci.exceptions.ServiceError as e:
            retry_delay = handle_service_error(e, retry_delay, backoff_factor)
        except TransientServiceError as e:
            retry_delay = handle_transient_error(
                e,
                attempt,
                max_retries,
                retry_delay,
                backoff_factor
            )
        except AirflowException as e:
            if not handle_airflow_exception(e, retry_delay):
                raise
        except (ValueError, TypeError, KeyError) as e:
            # Catch specific common errors
            print(f"Specific error occurred: {e}")
            raise AirflowException(f"Specific error occurred: {e}") from e
        except Exception as e:
            print(f"Unexpected error occurred: {e}")
            raise AirflowException(f"An unexpected error occurred: {e}") from e

    raise AirflowException(
        "Exceeded maximum retries due to repeated errors or pool resource issues."
    )


def initiate_and_monitor_run(compartment_id, application_id, config, kwargs):
    """
    Initiates and monitors an OCI Data Flow run.

    This function starts a Data Flow application with the given configuration and monitors
    its progress until completion. If the application starts successfully, it waits for the
    run to complete and logs the progress.

    Args:
        compartment_id (str): The OCID of the compartment where the application resides.
        application_id (str): The OCID of the Data Flow application to run.
        config (dict): Dictionary containing the configuration parameters for the run.
            - display_name (str): Display name for the Data Flow run.
            - driver_shape (str): Shape of the driver node.
            - driver_ocpus (float): Number of OCPUs for the driver node.
            - driver_memory (float): Memory (in GB) for the driver node.
            - executor_shape (str): Shape of the executor nodes.
            - executor_ocpus (float): Number of OCPUs for the executor nodes.
            - executor_memory (float): Memory (in GB) for the executor nodes.
            - logs_bucket_uri (str): URI of the OCI Object Storage bucket for logs.
            - num_executors (int): Number of executor nodes to use.
            - pool_id (str): OCID of the pool for the run.
        kwargs (dict): Additional context passed to monitor the run.

    Returns:
        bool: True if the run is successfully initiated and completed, False otherwise.

    Raises:
        AirflowException: If an unexpected error occurs or the run fails to start.
    """
    oci_config = load_oci_config()
    data_flow_client = DataFlowClient(oci_config)

    # Initiates and monitors the Data Flow run.
    create_run_response = initiate_dataflow_run(
        data_flow_client, compartment_id, application_id, config
    )
    if create_run_response.status == 200:
        print(f"Data Flow Run ID: {create_run_response.data.id}")
        wait_for_run_completion(data_flow_client, create_run_response.data.id, kwargs)
        return True
    return False


def handle_service_error(e, retry_delay, backoff_factor):
    """Handles OCI ServiceError exceptions."""
    if e.status == 409:  # HTTP 409: Incorrect State
        log_retry_message("HTTP 409 (Incorrect State)", e.message, retry_delay)
    elif e.status == 429:  # HTTP 429: Too Many Requests
        log_retry_message("HTTP 429 (Throttling)", e.message, retry_delay)
    elif 500 <= e.status < 600 and e.status != 501:
        log_retry_message(f"HTTP {e.status} (Server Error)", e.message, retry_delay)
    else:
        raise AirflowException(f"Unhandled HTTP error {e.status}: {e.message}") from e
    time.sleep(retry_delay)
    return retry_delay * backoff_factor


def handle_transient_error(e, attempt, max_retries, retry_delay, backoff_factor):
    """Handles transient service errors."""
    print(f"Transient error on attempt {attempt + 1}/{max_retries}: {e}")
    time.sleep(retry_delay)
    return retry_delay * backoff_factor


def handle_airflow_exception(e, retry_delay):
    """Handles AirflowException for specific cases."""
    error_message = str(e).lower()
    if "does not have enough resources" in error_message:
        print(f"Pool resource issue detected: {e}. Retrying after {retry_delay} seconds...")
        time.sleep(retry_delay)
        return True
    return False


def handle_unexpected_error(e):
    """Handles unexpected exceptions."""
    print(f"Unexpected error occurred: {e}")
    raise AirflowException(f"An unexpected error occurred: {e}") from e


def log_retry_message(error_type, message, retry_delay):
    """Logs retry messages for ServiceError."""
    print(f"{error_type}: {message}. Retrying after {retry_delay} seconds...")


def initiate_dataflow_run(data_flow_client, compartment_id, application_id, config):
    """
    Initiates a Data Flow application run.

    Args:
        data_flow_client: OCI Data Flow client
        compartment_id (str): Compartment OCID
        application_id (str): Application OCID
        config (dict): Dictionary containing the configuration details for the run.
            - display_name (str): Display name for the run.
            - driver_shape (str): Driver shape.
            - driver_ocpus (int): Number of CPUs for the driver.
            - driver_memory (int): Memory in GBs for the driver.
            - executor_shape (str): Executor shape.
            - executor_ocpus (int): Number of CPUs for the executor.
            - executor_memory (int): Memory in GBs for the executor.
            - logs_bucket_uri (str): URI for the logs bucket.
            - num_executors (int): Number of executors.
            - pool_id (str): Pool ID.

    Returns:
        object: The result of the `data_flow_client.create_run` call.
    """
    # Extract configuration from the provided dictionary
    driver_shape = config["driver_shape"]
    driver_ocpus = config["driver_ocpus"]
    driver_memory = config["driver_memory"]
    executor_shape = config["executor_shape"]
    executor_ocpus = config["executor_ocpus"]
    executor_memory = config["executor_memory"]
    logs_bucket_uri = config["logs_bucket_uri"]
    num_executors = config["num_executors"]
    pool_id = config["pool_id"]
    display_name = config["display_name"]

    # Prepare the CreateRunDetails object
    create_run_details = oci.data_flow.models.CreateRunDetails(
        compartment_id=compartment_id,
        application_id=application_id,
        display_name=display_name,
        driver_shape=driver_shape,
        driver_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=driver_ocpus,
            memory_in_gbs=driver_memory
        ),
        executor_shape=executor_shape,
        executor_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=executor_ocpus,
            memory_in_gbs=executor_memory
        ),
        logs_bucket_uri=logs_bucket_uri,
        num_executors=num_executors,
        pool_id=pool_id,
        type="BATCH"
    )

    return data_flow_client.create_run(create_run_details=create_run_details)


def wait_for_run_completion(data_flow_client, run_id, run_context):
    """
    Waits for the completion of a Data Flow run.
    """
    while True:
        time.sleep(run_poll_interval)

        run_details = data_flow_client.get_run(run_id)
        lifecycle_state = run_details.data.lifecycle_state
        print(f"Run {run_id} is in state: {lifecycle_state}")

        # Push the status to XCom
        run_context["ti"].xcom_push(key=f"dataflow_status_{run_id}", value=lifecycle_state)

        if run_details.status != 200:
            raise AirflowException("Failed to load Run information")

        if lifecycle_state == "SUCCEEDED":
            print(f"Run {run_id} completed successfully.")
            run_context["ti"].xcom_push(key=f"dataflow_status_{run_id}", value="SUCCESS")
            return
        elif lifecycle_state in ["FAILED", "CANCELED"]:
            print(f"Run {run_id} failed or was canceled.")
            run_context["ti"].xcom_push(key=f"dataflow_status_{run_id}", value="FAILURE")
            raise AirflowException(
                 f"Data Flow Run {run_id} failed: {run_details.data.lifecycle_details}"
              )
        elif lifecycle_state in ["ACCEPTED", "IN_PROGRESS"]:
            continue
        else:
            raise AirflowException(f"Unexpected Data Flow Run state: {lifecycle_state}")

def handle_transient_service_error(error, attempt, max_retries, retry_delay):
    """
    Handles transient service errors with exponential backoff.
    """
    if error.status == 429:  # TooManyRequests
        print(
            f"429 TooManyRequests: Retry {attempt + 1}/{max_retries}. "
            f"Waiting {retry_delay} seconds..."
        )
        time.sleep(retry_delay)
    else:
        raise AirflowException(f"Failed due to: {error}")

# Function to create tasks for a specific pipeline type
def create_pipeline_tasks(dag_instance, pipeline_applications, execution_strategy):
    """
    Create tasks for the given pipeline applications in the DAG.
    Supports sequential or parallel execution.
    """
    tasks = []
    previous_task = None

    for app_key in pipeline_applications:
        # Retrieve app details
        app_id = app_details[app_key]
        display_name_key = app_key.replace("application_id_", "") + "_display_name"
        display_name = app_details.get(display_name_key, f"Unknown Display Name for {app_key}")

        # Retrieve application-specific configuration
        merged_dataflow_config = get_merged_dataflow_config(app_key)

        # Group related configuration parameters into a dictionary
        config = {
            "driver_shape": merged_dataflow_config["driver_shape"],
            "driver_ocpus": merged_dataflow_config["driver_shape_config"]["ocpus"],
            "driver_memory": merged_dataflow_config["driver_shape_config"]["memory_in_gbs"],
            "executor_shape": merged_dataflow_config["executor_shape"],
            "executor_ocpus": merged_dataflow_config["executor_shape_config"]["ocpus"],
            "executor_memory": merged_dataflow_config["executor_shape_config"]["memory_in_gbs"],
            "logs_bucket_uri": merged_dataflow_config["logs_bucket_uri"],
            "num_executors": merged_dataflow_config["num_executors"],
            "pool_id": merged_dataflow_config.get("pool_id", ""),
             "display_name": display_name,
        }

        # Define the task using PythonOperator
        task = PythonOperator(
            task_id=f"run_{app_key}_dataflow_application",
            python_callable=run_dataflow_application_with_backoff,
            op_args=[app_id,config],
            provide_context=True,
            dag=dag_instance,
        )

        tasks.append(task)

        # If execution_strategy is sequential, chain tasks
        if execution_strategy == "sequential" and previous_task:
            previous_task >> task

        previous_task = task

    return tasks


# Main function to create the DAG for curation
def create_curation_dag(dag_instance, hierarchy):
    """
    Creates the DAG for the curation pipeline using the hierarchical structure.
    """
    # Store tasks for each batch
    batch_tasks = {}

    for batch_name, batch_details in hierarchy.items():
        tasks = batch_details['tasks']
        batch_execution_mode = batch_details.get('execution_mode', 'sequential')
        depends_on = batch_details.get('depends_on', [])

        # Create tasks for the current batch
        batch_tasks[batch_name] = create_pipeline_tasks(dag_instance, tasks, batch_execution_mode)

        # Handle dependencies between batches
        for dependency in depends_on:
            if dependency in batch_tasks:
                for parent_task in batch_tasks[dependency]:
                    for child_task in batch_tasks[batch_name]:
                        parent_task >> child_task

    return dag_instance

# Select the pipeline type (aggregation, curation, or consumption)
PIPELINE_TYPE = "aggregation"  # Change to "aggregation" or "consumption" as needed

# Get the specific DAG configuration
specific_config = dag_configs[PIPELINE_TYPE]
merged_config = merge_configs(shared_dag_config, specific_config)
execution_mode = specific_config.get('execution_mode', 'sequential')  # Default to sequential
pipeline_apps = specific_config.get('pipeline_apps', [])

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.fromisoformat(merged_config['start_date']),
    'retries': merged_config['retries'],
    'retry_delay': timedelta(seconds=merged_config['retry_delay']),
}

# Create the DAG
dag = DAG(
    dag_id=merged_config['dag_id'],
    default_args=default_args,
    description=merged_config['description'],
    schedule_interval=merged_config['schedule_interval'],
    max_active_runs=merged_config['max_active_runs'],
    catchup=merged_config['catchup'],
)

# Add tasks to the DAG
create_pipeline_tasks(dag, pipeline_apps, execution_mode)
