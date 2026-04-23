"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами.

| Параметр           | Описание                                                          |
|--------------------|-------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес)*       |
| 🔍 `dry_run`        | Только подсчёт без удаления *(default: `True`)*                   |
| 📋 `tables`         | Список таблиц для очистки *(default: все)*                        |

---

#### Таблицы очистки и порядок удаления

| # | Таблица                         | Критерий                              |
|---|---------------------------------|---------------------------------------|
| 1 | `callback_request`              | `created_at < cutoff`                 |
| 2 | `celery_taskmeta`               | `date_done < cutoff`                  |
| 3 | `celery_tasksetmeta`            | `date_done < cutoff`                  |
| 4 | `session`                       | `expiry < NOW()`                      |
| 5 | `import_error`                  | `timestamp < cutoff`                  |
| 6 | `sla_miss`                      | `timestamp < cutoff`                  |
| 7 | `log`                           | `dttm < cutoff`                       |
| 8 | `job`                           | `latest_heartbeat < cutoff`           |
| 9 | `xcom`                          | `timestamp < cutoff`                  |
| 10| `rendered_task_instance_fields` | via `dag_run.execution_date < cutoff` |
| 11| `task_instance_history`         | `updated_at < cutoff`                 |
| 12| `task_instance`                 | `start_date < cutoff`                 |
| 13| `trigger`                       | `created_date < cutoff`, без активных task_instance |
| 14| `dag_run`                       | `execution_date < cutoff`             |
| 15| `dataset_event`                 | `timestamp < cutoff`                  |
| 16| `dataset`                       | `is_orphaned = TRUE`                  |
| 17| `dataset_alias`                 | не в DAG-расписаниях и не в алиасах   |
> **dry_run=True** по умолчанию — первый запуск всегда безопасен.
> Минимальный порог `retention_days` — 30 дней.
"""

from airflow.decorators import task, dag
from airflow.models import Param
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.exceptions import AirflowFailException, AirflowSkipException

from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from psycopg2 import OperationalError, InterfaceError, DatabaseError

import time
import pendulum
import os

from logging import getLogger
logger = getLogger("airflow.task")


# Порядок важен: сначала независимые, потом дочерние, dag_run — последним
CLEANABLE_TABLES = [
    'callback_request',
    'celery_taskmeta',
    'celery_tasksetmeta',
    'session',
    'import_error',
    'sla_miss',
    'log',
    'job',
    'xcom',
    'rendered_task_instance_fields',
    'task_instance_history',
    'task_instance',
    'trigger',
    'dag_run',
    'dataset_event',
    'dataset',
    'dataset_alias',
]

DATE_COLUMNS = {
    'callback_request':              'created_at',
    'celery_taskmeta':               'date_done',
    'celery_tasksetmeta':            'date_done',
    'session':                       None,
    'import_error':                  'timestamp',
    'sla_miss':                      'timestamp',
    'log':                           'dttm',
    'job':                           'latest_heartbeat',
    'xcom':                          'timestamp',
    'rendered_task_instance_fields': None,
    'task_instance_history':         'updated_at',
    'task_instance':                 'start_date',
    'trigger':                       'created_date',
    'dag_run':                       'execution_date',
    'dataset_event':                 'timestamp',
    'dataset':                       None,
    'dataset_alias':                 None,
}

# DELETE SQL для каждой таблицы; {cutoff} подставляется при выполнении
DELETE_SQLS = {
    'callback_request':   "DELETE FROM callback_request WHERE created_at < '{cutoff}'",
    'celery_taskmeta':    "DELETE FROM celery_taskmeta WHERE date_done < '{cutoff}'",
    'celery_tasksetmeta': "DELETE FROM celery_tasksetmeta WHERE date_done < '{cutoff}'",
    'session':            "DELETE FROM session WHERE expiry < NOW()",
    'import_error':       "DELETE FROM import_error WHERE timestamp < '{cutoff}'",
    'sla_miss':           "DELETE FROM sla_miss WHERE timestamp < '{cutoff}'",
    'log':                "DELETE FROM log WHERE dttm < '{cutoff}'",
    'job':                "DELETE FROM job WHERE latest_heartbeat < '{cutoff}'",
    'xcom':               "DELETE FROM xcom WHERE timestamp < '{cutoff}'",
    'rendered_task_instance_fields': """
        DELETE FROM rendered_task_instance_fields
        USING dag_run
        WHERE rendered_task_instance_fields.dag_id = dag_run.dag_id
          AND rendered_task_instance_fields.run_id = dag_run.run_id
          AND dag_run.execution_date < '{cutoff}'
    """,
    'task_instance_history': "DELETE FROM task_instance_history WHERE updated_at < '{cutoff}'",
    'task_instance':      "DELETE FROM task_instance WHERE start_date < '{cutoff}'",
    'trigger':            """
        DELETE FROM trigger
        WHERE created_date < '{cutoff}'
          AND id NOT IN (
              SELECT trigger_id FROM task_instance WHERE trigger_id IS NOT NULL
          )
    """,
    'dag_run':            "DELETE FROM dag_run WHERE execution_date < '{cutoff}'",
    'dataset_event':      "DELETE FROM dataset_event WHERE timestamp < '{cutoff}'",
    'dataset':            "DELETE FROM dataset WHERE is_orphaned = TRUE",
    'dataset_alias':      """
        DELETE FROM dataset_alias
        WHERE id NOT IN (SELECT alias_id FROM dag_schedule_dataset_alias_reference)
          AND id NOT IN (SELECT alias_id FROM dataset_alias_dataset)
    """,
}

# Кастомные COUNT для таблиц без колонки даты (не используют cutoff)
COUNT_SQLS = {
    'session': "SELECT COUNT(*) FROM session WHERE expiry < NOW()",
    'rendered_task_instance_fields': """
        SELECT COUNT(*) FROM rendered_task_instance_fields
        JOIN dag_run ON rendered_task_instance_fields.dag_id = dag_run.dag_id
          AND rendered_task_instance_fields.run_id = dag_run.run_id
        WHERE dag_run.execution_date < '{cutoff}'
    """,
    'trigger': """
        SELECT COUNT(*) FROM trigger
        WHERE created_date < '{cutoff}'
          AND id NOT IN (
              SELECT trigger_id FROM task_instance WHERE trigger_id IS NOT NULL
          )
    """,
    'dataset':       "SELECT COUNT(*) FROM dataset WHERE is_orphaned = TRUE",
    'dataset_alias': """
        SELECT COUNT(*) FROM dataset_alias
        WHERE id NOT IN (SELECT alias_id FROM dag_schedule_dataset_alias_reference)
          AND id NOT IN (SELECT alias_id FROM dataset_alias_dataset)
    """,
}


def _get_hook():
    return PostgresHook(postgres_conn_id='airflowdb')


def _setup_session(cursor, timeout=300):
    cursor.execute("SET search_path = main")
    if timeout:
        cursor.execute(f"SET statement_timeout TO {timeout * 1000}")


def query_to_dict(gp_hook, sql, timeout=300):
    with gp_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            _setup_session(cursor, timeout)
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]


def execute_dml(gp_hook, sql, timeout=300):
    with gp_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            _setup_session(cursor, timeout)
            cursor.execute(sql)
            rowcount = cursor.rowcount
        conn.commit()
    return rowcount


def log_retry_attempt(retry_state):
    logger.warning(
        f"⚠️ Попытка {retry_state.attempt_number} не удалась. "
        f"Ошибка: {retry_state.outcome.exception()}. "
        f"Следующая попытка через {retry_state.next_action.sleep} сек..."
    )


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((OperationalError, InterfaceError, ConnectionError)),
    before_sleep=log_retry_attempt,
    reraise=True,
)
def pg_exe(sql='select 1'):
    hook = _get_hook()
    ts_start = time.time()
    try:
        records = query_to_dict(hook, sql)
        logger.info(f"✅ {len(records)} records in {time.time() - ts_start:.2f}s")
        return records
    except (OperationalError, InterfaceError) as e:
        logger.error(f"📡 Ошибка подключения к Postgres: {e}")
        raise
    except DatabaseError as e:
        logger.error(f"❌ Ошибка SQL: {e}")
        raise AirflowFailException(f"PG SQL Error: {e}")
    except Exception as e:
        logger.error(f"💥 Непредвиденная ошибка pg_exe: {e}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((OperationalError, InterfaceError, ConnectionError)),
    before_sleep=log_retry_attempt,
    reraise=True,
)
def pg_delete(sql):
    hook = _get_hook()
    ts_start = time.time()
    try:
        rowcount = execute_dml(hook, sql)
        logger.info(f"🗑️  Удалено {rowcount} строк за {time.time() - ts_start:.2f}s")
        return rowcount
    except (OperationalError, InterfaceError) as e:
        logger.error(f"📡 Ошибка подключения к Postgres: {e}")
        raise
    except DatabaseError as e:
        logger.error(f"❌ Ошибка SQL: {e}")
        raise AirflowFailException(f"PG SQL Error: {e}")
    except Exception as e:
        logger.error(f"💥 Непредвиденная ошибка pg_delete: {e}")
        raise


params = {
    'retention_days': Param(
        180,
        type='integer',
        minimum=30,
        description='Хранить записи не старше N дней (минимум 30)',
    ),
    'dry_run': Param(
        True,
        type='boolean',
        description='True — только подсчёт, False — реальное удаление',
    ),
}
for table in CLEANABLE_TABLES:
    params[table] = Param(True, type='boolean', title=f'Очистить {table}')


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
    },
    start_date=pendulum.datetime(2025, 8, 7, tz=pendulum.UTC),
    tags=['DataLab', 'tools', 'maintenance'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    schedule_interval='@weekly',
    params=params,
)
def tools_db_cleanup():

    @task
    def check_retention(**context):
        params = context['params']
        retention_days = params['retention_days']
        tables = [t for t in CLEANABLE_TABLES if params[t]]

        if retention_days < 30:
            raise AirflowFailException(f"retention_days={retention_days} меньше минимума (30)")

        cutoff = pendulum.now('UTC').subtract(days=retention_days).format('YYYY-MM-DD HH:mm:ss')
        logger.info(f"📝 Граница очистки: {cutoff} (retention_days={retention_days})")

        for table in tables:
            date_col = DATE_COLUMNS.get(table)
            custom_sql = COUNT_SQLS.get(table)
            if date_col is not None:
                sql = f"SELECT COUNT(*) FROM {table} WHERE {date_col} < '{cutoff}'"
            elif custom_sql is not None:
                sql = custom_sql.format(cutoff=cutoff)
            else:
                logger.info(f"⚠️  {table}: подсчёт не поддерживается")
                continue
            try:
                result = pg_exe(sql)
                count = result[0]['count']
                logger.info(f"🔍 {table}: {count} записей к удалению")
            except Exception as e:
                logger.warning(f"❌ {table}: ошибка подсчёта — {e}")

    @task
    def clean_metadata(**context):
        params = context['params']
        retention_days = params['retention_days']
        dry_run = params['dry_run']
        tables = [t for t in CLEANABLE_TABLES if params[t]]

        if dry_run:
            raise AirflowSkipException('🔒 dry_run=True — удаление пропущено')

        if retention_days < 30:
            raise AirflowFailException(f"retention_days={retention_days} меньше минимума (30)")

        cutoff = pendulum.now('UTC').subtract(days=retention_days).format('YYYY-MM-DD HH:mm:ss')
        logger.info(f"🗑️  Начало очистки. Граница: {cutoff}")

        for table in tables:
            sql = DELETE_SQLS[table].format(cutoff=cutoff)
            logger.info(f"[УДАЛЕНИЕ] {table}")
            try:
                pg_delete(sql)
            except Exception as e:
                logger.error(f"❌ {table}: ошибка удаления — {e}")

    check_retention() >> clean_metadata()


ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

if ENV_STAND == "prom":
    logger.warning("DAG tools_db_cleanup is disabled on 'prom' stand. Skipping DAG registration.")
else:
    tools_db_cleanup()
