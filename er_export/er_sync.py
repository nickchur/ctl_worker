"""
Synchronization DAG for ER export metadata.
Syncs configuration from the ClickHouse `export.er_wf_meta` table to Airflow Variables/S3.
"""
from __future__ import annotations

import logging
import pendulum
from airflow.decorators import dag, task

from er_export.er_config import CH_ID, DEF_ARGS
from er_export.er_core import get_dict
from plugins.ctl_utils import ctl_obj_save

logger = logging.getLogger(__name__)

VAR_NAME = "datalab_er_wfs"
BUCKET   = "datalab-er"


@dag(
    dag_id="er_sync__datalab_er_wfs",
    description="Sync export.er_wf_meta → Airflow Variable datalab_er_wfs",
    default_args=DEF_ARGS,
    start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone("UTC")),
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["DataLab", "CI02420667", "ER", "sync"],
    is_paused_upon_creation=True,
)
def er_sync_dag():

    @task(task_id="sync")
    def sync():
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
        rows = get_dict(
            hook,
            "SELECT * FROM export.er_wf_meta FINAL WHERE is_active = 1",
        )

        if not rows:
            raise ValueError("No active workflows found in export.er_wf_meta — aborting to avoid overwriting Variable with empty dict")

        wfs = {}
        for row in rows:
            table_key = f"{row['db_name']}.{row['extract_name']}"
            sql_key   = "sql_stmt_export_recent" if row["is_recent"] else "sql_stmt_export_delta"
            sql_val   = {"from": row["sql_from"]}
            if row["sql_where"]:
                sql_val["where"] = row["sql_where"]

            entry = {
                "replica":  row["replica"],
                "schema":   row["schema_name"],
                "format":   row["format"],
                "strategy": row["strategy"],
                "PK":       row["pk"],
                "UK":       row["uk"],
                "increment": row["increment"],
                "selfrun_timeout": row["selfrun_timeout"],
                "auto_confirm_delta": row["auto_confirm_delta"],
                sql_key:    sql_val,
            }
            if row["fields"]:
                entry["fields"] = row["fields"]
            if row["description"]:
                entry["description"] = row["description"]

            wfs[table_key] = entry

        logger.info("Loaded %d workflow(s) from export.er_wf_meta", len(wfs))
        ctl_obj_save(VAR_NAME, wfs, var=True, s3_id="s3", bucket=BUCKET)

    sync()


er_sync_dag()
