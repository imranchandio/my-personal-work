"""
Dynamic DAG Creation for WSR Pipelines (Aggregation, Curation, Consumption)

This script dynamically creates DAGs for the WSR pipelines based on shared
and pipeline-specific configurations. Only one pipeline type (aggregation, curation, or consumption)
will be triggered per DAG execution.
"""
"""
Aggregation DAG using shared WSR DAG builder.
"""
from dag_builder import build_dag

# Only change pipeline_type here
PIPELINE_TYPE = "aggregation"
dag = build_dag(PIPELINE_TYPE)
