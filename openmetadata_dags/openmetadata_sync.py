import logging
import pendulum
import requests
import json
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

from airflow.decorators import task, dag
from airflow.models import Variable
from airflow.utils.session import provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import get_current_context

# Import custom utilities
from utils.delete_xcom import delete_xcom_function
from utils.slack_alert import failure_callback_slack
from utils.get_clickhouse_client import get_clickhouse_client
from utils.support_functions import clickhouseToDict

# --- CONFIGURATION ---
CONFIG = {
    "interval_int": 24,
    "offset": 1,
    "num_worker_tasks": 3,
    "num_threads": 3,
    "dag_id": 'open_metadata_sync',
    "clickhouse_conn_id": 'clickhouse_default'
}

SCHEDULE_CRON = f'0 {(CONFIG["interval_int"] - CONFIG["offset"]) % 24} * * *'

@dag(
    dag_id=CONFIG["dag_id"],
    schedule=SCHEDULE_CRON,
    start_date=pendulum.datetime(2026, 1, 8, tz='UTC'),
    catchup=False,
    tags=['openmetadata', 'clickhouse', 'cloud', 'portfolio'],
    description='Syncs OpenMetadata descriptions back to ClickHouse comments to ensure metadata consistency.',
    # on_failure_callback=failure_callback_slack # Uncomment for production use
)
def sync_dag():
    """
    ### Metadata Synchronization DAG
    This DAG fetches metadata (descriptions) from OpenMetadata and propagates them 
    as COMMENTs to ClickHouse tables, columns, and databases.
    """

    @task()
    def get_clickhouse_resources():
        """
        Fetches the current schema and comments from ClickHouse system tables.
        Returns a list of dictionaries containing database, table, and column metadata.
        """
        columns = ['active_type', 'database', 'table', 'column', 'description']
        client = get_clickhouse_client(CONFIG["clickhouse_conn_id"])
        
        sql = """
            SELECT 'columns' active_type, c.database, c.table, c.name column, c.comment as description
            FROM system.columns c WHERE match(database, 'staging|landing|access|analytics')
            UNION ALL
            SELECT 'tables' active_type, t.database, t.table, Null column, t.comment as description 
            FROM system.tables t WHERE match(database, 'staging|landing|access|analytics')
            UNION ALL
            SELECT 'databases' active_type, d.name database, Null table, Null column, d.comment as description 
            FROM system.databases d WHERE match(name, 'staging|landing|access|analytics')
        """
        logging.info("Fetching current metadata from ClickHouse...")
        rows = client.query(sql).result_rows
        return clickhouseToDict(rows, columns)

    @task()
    def get_om_resources():
        """
        Connects to the OpenMetadata API to retrieve documented descriptions for 
        databases, tables, and columns.
        """
        token = Variable.get("open_metadata_token", default_var="your_token_here")
        url_root = Variable.get("url_root", default_var='http://localhost:8585/api/v1')
        headers = {'Authorization': f'Bearer {token}'}
        return_limit = 100000

        om_resources = {}

        logging.info(f"Connecting to OpenMetadata at {url_root}")
        
        # 1. Fetch Databases
        db_resp = requests.get(f'{url_root}/databaseSchemas/?limit={return_limit}', headers=headers)
        for db in db_resp.json().get('data', []):
            key = f"database|{db['name']}|None|None"
            om_resources[key] = {
                'active_type': 'database', 'database': db['name'], 
                'table': None, 'column': None, 'description': db.get('description')
            }

        # 2. Fetch Tables and Columns
        table_resp = requests.get(f'{url_root}/tables/?limit={return_limit}', headers=headers)
        for table in table_resp.json().get('data', []):
            fqn_parts = table['fullyQualifiedName'].split('.')
            db_name = fqn_parts[-2]
            table_name = fqn_parts[-1]

            # Table Entry
            om_resources[f"tabela|{db_name}|{table_name}|None"] = {
                'active_type': 'tabela', 'database': db_name, 
                'table': table_name, 'column': None, 'description': table.get('description')
            }

            # Column Entries
            for col in table.get('columns', []):
                om_resources[f"coluna|{db_name}|{table_name}|{col['name']}"] = {
                    'active_type': 'coluna', 'database': db_name, 
                    'table': table_name, 'column': col.get('name'), 'description': col.get('description')
                }

        logging.info(f"Retrieved {len(om_resources)} items from OpenMetadata.")
        return om_resources

    @task()
    def filter_resources(ch_resources, om_resources, num_workers):
        """
        Compares CH and OM metadata. Only items with differing descriptions are queued for update.
        Chunks the resulting list for parallel worker execution.
        """
        to_update = []
        for res in ch_resources:
            # Map 'columns' -> 'coluna' and 'tables' -> 'tabela' to match OM logic
            type_map = {'columns': 'coluna', 'tables': 'tabela', 'databases': 'database'}
            mapped_type = type_map.get(res['active_type'], res['active_type'])
            
            key = f"{mapped_type}|{res['database']}|{res['table']}|{res['column']}"
            om_item = om_resources.get(key)

            if om_item and om_item.get('description') != res.get('description'):
                to_update.append(om_item)

        logging.info(f"Found {len(to_update)} items requiring comment updates.")
        
        # Split into chunks for workers
        k, m = divmod(len(to_update), num_workers)
        return [to_update[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(num_workers)]

    @task()
    def sync_to_clickhouse(om_chunk, num_threads):
        """
        Executes ALTER statements in ClickHouse using a ThreadPoolExecutor.
        """
        if not om_chunk:
            logging.info("No resources to update in this chunk.")
            return

        def run_alter(resource):
            client = get_clickhouse_client(CONFIG["clickhouse_conn_id"])
            desc = (resource.get('description') or "").replace("'", "\\'")
            
            if resource['active_type'] == 'coluna':
                sql = f"ALTER TABLE {resource['database']}.{resource['table']} COMMENT COLUMN {resource['column']} '{desc}'"
            elif resource['active_type'] == 'tabela':
                sql = f"ALTER TABLE {resource['database']}.{resource['table']} MODIFY COMMENT '{desc}'"
            else:
                sql = f"ALTER DATABASE {resource['database']} MODIFY COMMENT '{desc}'"

            try:
                client.command(sql)
                return ("OK", f"{resource['database']}.{resource['table']}")
            except Exception as e:
                return ("FAIL", f"{resource['database']}.{resource['table']} | Error: {str(e)}")

        results = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(run_alter, res) for res in om_chunk]
            for f in as_completed(futures):
                results.append(f.result())

        successes = [r for r in results if r[0] == "OK"]
        failures = [r for r in results if r[0] == "FAIL"]

        logging.info(f"Chunk Sync Complete: {len(successes)} succeeded, {len(failures)} failed.")
        if failures:
            for fail in failures[:5]: # Log first 5 errors
                logging.error(f"Sync Failure Detail: {fail[1]}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    @provide_session
    def cleanup_xcoms(session=None):
        """
        Cleans up XCom values generated during the task execution to keep the DB lean.
        """
        context = get_current_context()
        dr = context.get('dag_run')
        logging.info(f"Cleaning up XComs for run: {dr.run_id}")
        delete_xcom_function(dag_id=dr.dag_id, run_id=dr.run_id, session=session)

    ch_inv = get_clickhouse_resources()
    om_inv = get_om_resources()

    chunks = filter_resources(
        ch_resources=ch_inv, 
        om_resources=om_inv, 
        num_workers=CONFIG["num_worker_tasks"]
    )

    sync_tasks = sync_to_clickhouse.partial(num_threads=CONFIG["num_threads"]).expand(om_chunk=chunks)
    
    sync_tasks >> cleanup_xcoms()

sync_dag()