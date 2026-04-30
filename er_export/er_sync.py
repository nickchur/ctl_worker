from __future__ import annotations

import logging
import pendulum
from airflow.decorators import dag, task

from er_export.er_config import CH_ID, DEFAULT_ARGS
from er_export.er_core import select_dic
from plugins.ctl_utils import ctl_obj_save

logger = logging.getLogger(__name__)

VARIABLE_NAME = "datalab_er_wfs"
BUCKET        = "datalab-er"


@dag(
    dag_id="er_sync__datalab_er_wfs",
    description="Sync export.er_wf_meta → Airflow Variable datalab_er_wfs",
    default_args=DEFAULT_ARGS,
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
        rows = select_dic(
            hook,
            "SELECT * FROM export.er_wf_meta FINAL WHERE is_active = 1",
        )

        wfs = {}
        for row in rows:
            table_key = f"{row['db_name']}.{row['extract_name']}"
            sql_key   = "sql_stmt_export_recent" if row["is_recent"] else "sql_stmt_export_delta"
            sql_val: dict = {"from": row["sql_from"]}
            if row["sql_where"]:
                sql_val["where"] = row["sql_where"]

            entry: dict = {
                "replica":  row["replica"],
                "schema":   row["schema_name"],
                "format":   row["format"],
                "strategy": row["strategy"],
                "PK":       list(row["pk"]),
                "UK":       list(row["uk"]),
                sql_key:    sql_val,
            }
            if row["fields"]:
                entry["fields"] = list(row["fields"])

            wfs[table_key] = entry

        logger.info("Loaded %d workflow(s) from export.er_wf_meta", len(wfs))
        ctl_obj_save(VARIABLE_NAME, wfs, var=True, s3_id="s3", bucket=BUCKET)

    sync()


er_sync_dag()
