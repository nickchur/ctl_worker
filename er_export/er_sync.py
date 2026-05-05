"""
Synchronization DAG for ER export metadata.
Syncs configuration from the ClickHouse `export.er_wf_meta` table to Airflow Variables/S3.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from er_export.er_config import get_config, get_dict

_cfg      = get_config()
CH_ID     = _cfg['CH_ID']
DEF_ARGS  = _cfg['DEF_ARGS']
MODE      = _cfg['MODE']
VAR_NAME  = _cfg['VAR_NAME']
POOL_NAME = _cfg['POOL_NAME']
POOL_SLOTS = _cfg['POOL_SLOTS']
from plugins.ctl_utils import ctl_obj_save

from  logging import getLogger
logger = getLogger("airflow.task")


def _ensure_pool() -> None:
    from airflow.models import Pool
    from airflow.utils.session import create_session
    with create_session() as session:
        if not session.query(Pool).filter(Pool.pool == POOL_NAME).first():
            session.add(Pool(pool=POOL_NAME, slots=POOL_SLOTS, description='ER export pool', include_deferred=False))


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

    @task(task_id="sync", pool=POOL_NAME)
    def sync():
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        # Ensure Airflow Pool exists (called here to avoid overhead during DAG parsing)
        _ensure_pool()

        hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

        if MODE == 'test':
            hook.execute("""
                CREATE TABLE IF NOT EXISTS export.er_wf_meta
                (
                    extract_name    String                    COMMENT 'Имя выгрузки (table name без схемы)',
                    db_name         String                    COMMENT 'База данных источника в ClickHouse (левая часть "db.table")',
                    replica         String                    COMMENT 'Реплика-маршрутизатор TFS (ключ в TFS_OUT_CONFIG_MAP)',
                    schema_name     String                    COMMENT 'Целевая схема в .meta-файле для TFS',
                    format          String        DEFAULT 'TSVWithNames' COMMENT 'Формат выгрузки ClickHouse',
                    strategy        String        DEFAULT 'FULL_UK'      COMMENT 'Стратегия merge: FULL_UK, FULL_PK, DELTA_UK и др.',
                    pk              Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа',
                    uk              Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа',
                    fields          Array(String) DEFAULT []             COMMENT 'SELECT-выражения (export_time, ctl_action, ctl_validfrom добавляются автоматически)',
                    sql_from        String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос',
                    sql_where       String        DEFAULT ''             COMMENT 'WHERE-условие; пустая строка — без фильтра; {condition} подставляется рантаймом',
                    increment       Int32         DEFAULT 60             COMMENT 'Инкремент дельты (сек)',
                    selfrun_timeout Int32         DEFAULT 10             COMMENT 'Таймаут перед авто-запуском следующей дельты (мин)',
                    auto_confirm    UInt8         DEFAULT 1              COMMENT '1 = авто-подтверждение дельты, 0 = ждать уведомления в Kafka',
                    confirm_timeout Int32         DEFAULT 3600           COMMENT 'Таймаут ожидания подтверждения из Kafka (сек)',
                    description     String        DEFAULT ''             COMMENT 'Описание DAG-а (отображается в Airflow UI)',
                    is_recent       UInt8         DEFAULT 0             COMMENT '0 = delta (sql_stmt_export_delta), 1 = recent (sql_stmt_export_recent)',
                    is_active       UInt8         DEFAULT 1             COMMENT '0 = запись игнорируется при синхронизации в Variable',
                    updated_at      DateTime      DEFAULT now()         COMMENT 'Версия строки для ReplacingMergeTree'
                )
                ENGINE = ReplacingMergeTree(updated_at)
                ORDER BY (db_name, extract_name)
            """)
            logger.info("Test mode: ensured export.er_wf_meta exists")

        rows = get_dict(
            hook,
            "SELECT * FROM export.er_wf_meta FINAL WHERE is_active = 1",
        )

        if not rows:
            if MODE == 'test':
                logger.warning("export.er_wf_meta is empty — skipping Variable update in test mode")
                return
            raise ValueError("No active workflows found in export.er_wf_meta — aborting to avoid overwriting Variable with empty dict")

        # batch-fetch CH table comments for rows without description
        no_desc = [(r["db_name"], r["extract_name"]) for r in rows if not r["description"]]
        ch_comments: dict[tuple, str] = {}
        if no_desc:
            cond = " OR ".join(f"(database='{db}' AND name='{tbl}')" for db, tbl in no_desc)
            ch_comments = {
                (r["database"], r["name"]): r["comment"]
                for r in get_dict(hook, f"SELECT database, name, comment FROM system.tables WHERE {cond}")
            }

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
                "auto_confirm": row["auto_confirm"],
                "confirm_timeout": row.get("confirm_timeout", 3600),
                sql_key:    sql_val,
            }
            if row["fields"]:
                entry["fields"] = row["fields"]
            desc = row["description"] or ch_comments.get((row["db_name"], row["extract_name"]), "")
            if desc:
                entry["description"] = desc

            wfs[table_key] = entry

        logger.info("Loaded %d workflow(s) from export.er_wf_meta", len(wfs))
        ctl_obj_save(VAR_NAME, wfs, var=True)

    sync()


er_sync_dag()
