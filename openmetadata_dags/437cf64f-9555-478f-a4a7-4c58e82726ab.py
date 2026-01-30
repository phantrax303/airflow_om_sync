"""
This file has been generated from dag_runner.j2
"""
from airflow import DAG
from openmetadata_managed_apis.workflows import workflow_factory

workflow = workflow_factory.WorkflowFactory.create("/opt/airflow/dag_generated_configs/437cf64f-9555-478f-a4a7-4c58e82726ab.json")
workflow.generate_dag(globals())
dag = workflow.get_dag()