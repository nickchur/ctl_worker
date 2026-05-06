"""
DAG синхронизации метаданных ER-выгрузок.

Читает таблицу export.er_wf_meta из ClickHouse и сохраняет активные записи
в Airflow Variable `datalab_er_wfs` (JSON-словарь), который используется
фабрикой er_export.py для динамической генерации DAG-ов.

Структура записи в Variable:
  {
    "db_name.extract_name": {
      "replica":   str,           # ключ в TFS_MAP
      "schema":    str,           # целевая схема TFS
      "PK":        list[str],
      "UK":        list[str],
      "params":    str,           # JSON с переопределёнными DEFAULT_PARAMS
      "sql_stmt_export_delta" | "sql_stmt_export_recent": {
          "from":     str,        # обязательно
          "with":     str,        # опционально — WITH-блок (CTE)
          "joins":    str,        # опционально — JOIN-clause
          "where":    str,        # опционально — WHERE-условие
          "settings": str,        # опционально — SETTINGS-блок ClickHouse
      },
      "fields":    list[str],     # опционально
      "description": str,         # опционально
    }
  }

Расписание: каждые 5 минут. При пустой таблице в SIGMA-режиме падает,
чтобы не затереть Variable пустым значением.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task
from logging import getLogger

try:
    from CI06932748.analytics.datalab.export_er.er_config import get_config, get_dict
except ImportError:
    from er_export.er_config import get_config, get_dict
from plugins.ctl_utils import ctl_obj_save

_cfg       = get_config()
CH_ID      = _cfg['CH_ID']
DEF_ARGS   = _cfg['DEF_ARGS']
MODE       = _cfg['MODE']
VAR_NAME   = _cfg['VAR_NAME']
POOL_NAME  = _cfg['POOL_NAME']
POOL_SLOTS = _cfg['POOL_SLOTS']

logger = getLogger("airflow.task")


def _ensure_pool() -> None:
    """Создаёт Airflow Pool для ER-выгрузок, если он ещё не существует.

    Вызывается внутри таска (не при парсинге DAG), чтобы не создавать
    сессию БД при каждом обходе scheduler-ом.
    """
    from airflow.models import Pool
    from airflow.utils.session import create_session
    with create_session() as session:
        if not session.query(Pool).filter(Pool.pool == POOL_NAME).first():
            session.add(Pool(pool=POOL_NAME, slots=POOL_SLOTS, description='Пул для ER-выгрузок', include_deferred=False))


@dag(
    dag_id="export_er_sync",
    description="Синхронизация export.er_wf_meta → Airflow Variable datalab_er_wfs",
    default_args=DEF_ARGS,
    start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone("UTC")),
    schedule_interval="*/5 * * * *",
    max_active_runs=1,
    catchup=False,
    tags=["DataLab", "CI02420667", "ER", "sync"],
    is_paused_upon_creation=True,
)
def er_sync_dag():

    @task(task_id="sync", pool="default_pool")
    def sync():
        """Читает er_wf_meta, собирает словарь выгрузок и сохраняет в Airflow Variable.

        В ALPHA-режиме дополнительно создаёт таблицу er_wf_meta если её нет,
        и пропускает обновление Variable при пустой таблице.
        В SIGMA-режиме пустая таблица — ошибка (защита от затирания Variable).
        """
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        _ensure_pool()

        hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

        if MODE == 'ALPHA':
            # В тестовом окружении таблица может отсутствовать — создаём на лету
            hook.execute("""
                CREATE TABLE IF NOT EXISTS export.er_wf_meta
                (
                    extract_name    String                    COMMENT 'Имя выгрузки (table name без схемы)',
                    db_name         String                    COMMENT 'База данных источника в ClickHouse (левая часть "db.table")',
                    replica         String                    COMMENT 'Реплика-маршрутизатор TFS (ключ в TFS_OUT_CONFIG_MAP)',
                    schema_name     String                    COMMENT 'Целевая схема в .meta-файле для TFS',
                    pk              Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа',
                    uk              Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа',
                    fields          Array(String) DEFAULT []             COMMENT 'SELECT-выражения (export_time, ctl_action, ctl_validfrom добавляются автоматически)',
                    sql_from        String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос',
                    sql_where       String        DEFAULT ''             COMMENT 'WHERE-условие; пустая строка — без фильтра; {condition} подставляется рантаймом',
                    sql_join        String        DEFAULT ''             COMMENT 'JOIN-clause (полное выражение: JOIN t ON ...); вставляется между FROM и WHERE',
                    sql_with        String        DEFAULT ''             COMMENT 'WITH-блок (CTE); вставляется перед SELECT',
                    sql_settings    String        DEFAULT ''             COMMENT 'SETTINGS-блок ClickHouse; вставляется в конец запроса',
                    params          String        DEFAULT '{}'           COMMENT 'JSON с параметрами выгрузки (см. DEFAULT_PARAMS в er_config)',
                    description     String        DEFAULT ''             COMMENT 'Описание DAG-а (отображается в Airflow UI)',
                    is_recent       UInt8         DEFAULT 0              COMMENT '0 = delta (sql_stmt_export_delta), 1 = recent (sql_stmt_export_recent)',
                    is_active       UInt8         DEFAULT 1              COMMENT '0 = запись игнорируется при синхронизации в Variable',
                    updated_at      DateTime      DEFAULT now()          COMMENT 'Версия строки для ReplacingMergeTree'
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
            if MODE == 'ALPHA':
                logger.warning("export.er_wf_meta is empty — skipping Variable update in test mode")
                return
            raise ValueError("No active workflows found in export.er_wf_meta — aborting to avoid overwriting Variable with empty dict")

        # Для строк без явного description подтягиваем комментарий таблицы из system.tables
        # одним батч-запросом, чтобы не делать N отдельных DESCRIBE.
        no_desc = [(r["db_name"], r["extract_name"]) for r in rows if not r["description"]]
        ch_comments: dict[tuple[str, str], str] = {}
        if no_desc:
            cond = " OR ".join(f"(database='{db}' AND name='{tbl}')" for db, tbl in no_desc)
            ch_comments = {
                (r["database"], r["name"]): r["comment"]
                for r in get_dict(hook, f"SELECT database, name, comment FROM system.tables WHERE {cond}")
            }

        wfs = {}
        for row in rows:
            table_key = f"{row['db_name']}.{row['extract_name']}"

            # is_recent определяет ключ SQL-запроса: фабрика er_export.py проверяет наличие одного из двух
            sql_key = "sql_stmt_export_recent" if row["is_recent"] else "sql_stmt_export_delta"
            sql_val = {"from": row["sql_from"]}
            if row["sql_with"]:     sql_val["with"]     = row["sql_with"]
            if row["sql_join"]:     sql_val["joins"]    = row["sql_join"]
            if row["sql_where"]:    sql_val["where"]    = row["sql_where"]
            if row["sql_settings"]: sql_val["settings"] = row["sql_settings"]

            entry = {
                "replica": row["replica"],
                "schema":  row["schema_name"],
                "PK":      row["pk"],
                "UK":      row["uk"],
                "params":  row.get("params", "{}"),
                sql_key:   sql_val,
            }
            if row["fields"]:
                entry["fields"] = row["fields"]

            desc = row["description"] or ch_comments.get((row["db_name"], row["extract_name"]), "")
            if desc:
                entry["description"] = desc

            wfs[table_key] = entry

        logger.info("Загружено %d выгрузок из export.er_wf_meta", len(wfs))
        # ctl_obj_save пропускает запись если данные не изменились (сравнение JSON)
        ctl_obj_save(VAR_NAME, wfs, var=True)

    sync()


er_sync_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
