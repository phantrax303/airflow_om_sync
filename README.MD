# OpenMetadata to ClickHouse Synchronization DAG

---

This project provides an automated pipeline to synchronize data documentation. It fetches descriptions (metadata) authored in **OpenMetadata** and propagates them as `COMMENT` attributes in **ClickHouse** databases, tables, and columns.

## 🚀 Overview

In modern data stacks, documentation often lives in a metadata platform while the actual database remains undocumented. This DAG ensures that the hard work put into data catalogs is visible to anyone querying ClickHouse directly via CLI or BI tools.

### Key Features

- **Hybrid Synchronization**: Supports databases, tables, and columns.
- **Parallel Execution**: Uses Airflow's Dynamic Task Mapping (`.expand`) and Python’s `ThreadPoolExecutor` for high-performance updates.
- **Efficient Filtering**: Only executes `ALTER` commands if the metadata has actually changed.
- **Automatic Cleanup**: Integrated XCom cleanup to maintain Airflow database performance.
- **Slack Integration**: Native support for failure alerts with detailed error logs and ClickHouse logging.

---

## 🛠 Setup & Permissions

To ensure the Airflow workers can manage the DAG files and the OpenMetadata ingestion bot has the necessary write access to mapped volumes, apply the following permissions:

Bash

`# 1. Change ownership to the Airflow user (50000) and root group (0)
sudo chown -R 50000:0 ./openmetadata_dags

# 2. Ensure the group has write permissions for OM ingestion
sudo chmod -R 775 ./openmetadata_dags`

---

## ⚙️ Environment Configuration

This POC relies on specific service configurations and Airflow environment variables. The provided `.env` files for each service contain the minimum necessary parameters to enable connectivity and functionality.

### 1. Airflow Variables

You must create the following variables in your Airflow environment (UI -> Admin -> Variables):

| **Variable Key** | **Description** |
| --- | --- |
| `open_metadata_token` | Your OpenMetadata JWT Token for API authentication. |
| `url_root` | The API endpoint for your OM instance (e.g., `http://openmetadata:8585/api/v1`). |
| `slack_webhook` | (Optional) The Webhook URL for Slack notifications. |

### 2. ClickHouse Service

The POC is configured to communicate with ClickHouse using the following defaults:

- **Host**: `clickhouse`
- **Port**: `8123`
- **Connection ID**: `clickhouse_default` (Must be created in Airflow Connections).

### 3. OpenMetadata Ingestion Bot

Ensure the OpenMetadata Ingestion Bot is properly configured to crawl your ClickHouse instance. This DAG acts as the **reverse-sync** mechanism, taking descriptions documented by users in the OM UI and pushing them back to the source database.

---

## 📂 Project Structure

- `openmetadata_sync.py`: The main DAG file using TaskFlow API.
- `utils/get_clickhouse_client.py`: Wrapper for `clickhouse-connect` with Airflow Hook integration.
- `utils/delete_xcom.py`: Utility to purge XComs after successful runs.
- `utils/slack_alert.py`: Logic for sending Block Kit alerts to Slack on failure and logging errors to ClickHouse.
- `utils/support_functions.py`: Data transformation helpers to convert result sets to dictionaries.

---

## 📈 Technical Logic

1. **Extract**: Queries `system.columns`, `system.tables`, and `system.databases` from ClickHouse.
2. **Fetch**: Calls OpenMetadata API to get all documented descriptions for those schemas.
3. **Filter**: Compares the two datasets and identifies mismatches.
4. **Load (Sync)**: Splits the work into chunks. Each chunk runs in parallel across Airflow workers, and within each worker, threads execute `ALTER` statements simultaneously.
5. **Cleanup**: Once all tasks are complete, the `delete_xcom_task` clears the metadata from the Airflow database to prevent bloat.