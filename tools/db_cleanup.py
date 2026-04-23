"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами.

| Параметр           | Описание                                                          |
|--------------------|-------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес)*       |
| 🔍 `dry_run`        | Только подсчёт без удаления *(default: `True`)*                   |
| 📋 `tables`         | Список таблиц для очистки *(default: все)*                        |

---

#### Таблицы очистки и порядок удаления

| #  | Таблица                         | Критерий                              | Заметки                                                      |
|----|---------------------------------|---------------------------------------|--------------------------------------------------------------|
| 1  | `callback_request`              | `created_at < cutoff`                 | Обработанные колбэки планировщика                            |
| 2  | `celery_taskmeta`               | `date_done < cutoff`                  | Результаты Celery-задач (result backend)                     |
| 3  | `celery_tasksetmeta`            | `date_done < cutoff`                  | Результаты Celery-групп (result backend)                     |
| 4  | `session`                       | `expiry < current_date`                      | Веб-сессии UI; не зависит от cutoff                          |
| 5  | `import_error`                  | `timestamp < cutoff`                  | Ошибки парсинга DAG-файлов                                   |
| 6  | `sla_miss`                      | `timestamp < cutoff`                  | Нарушения SLA                                                |
| 7  | `log`                           | `dttm < cutoff`                       | Audit-лог событий (не логи тасков)                           |
| 8  | `job`                           | `latest_heartbeat < cutoff`           | Записи SchedulerJob / LocalTaskJob                           |
| 9  | `xcom`                          | `timestamp < cutoff`                  | ↳ каскад от `task_instance`                                  |
| 10 | `rendered_task_instance_fields` | via `dag_run.execution_date < cutoff` | ↳ каскад от `task_instance`; нет своей колонки даты          |
| 11 | `task_instance_history`         | `updated_at < cutoff`                 | ↳ каскад от `task_instance`; чистим до TI чтобы управлять объёмом |
| 12 | `task_instance`                 | `start_date < cutoff`                 | ↳ каскад от `dag_run`                                        |
| 13 | `trigger`                       | `created_date < cutoff`               | Deferred tasks; только без активных `task_instance`          |
| 14 | `dag_run`                       | `execution_date < cutoff`             | Каскадно удаляет TI, xcom, rtif, dagrun_dataset_event и др.  |
| 15 | `dataset_event`                 | `timestamp < cutoff`                  | Каскадно удаляет `dagrun_dataset_event`, `dataset_alias_dataset_event` |
| 16 | `dataset`                       | `is_orphaned = TRUE`                  | Флаг выставляется Airflow при удалении DAG                   |
| 17 | `dataset_alias`                 | не в DAG-расписаниях и не в алиасах   | Каскадно удаляет `dag_schedule_dataset_alias_reference`      |
> **dry_run=True** по умолчанию — первый запуск всегда безопасен.
> Минимальный порог `retention_days` — 30 дней.
"""

from airflow.decorators import task, dag
from airflow.models import Param
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.utils.session import create_session
from sqlalchemy import text

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
    'session':            "DELETE FROM session WHERE expiry < current_date",
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
    'session': "SELECT COUNT(*) FROM session WHERE expiry < current_date",
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


def db_count(sql, timeout=300):
    with create_session() as session:
        session.execute(text("SET LOCAL search_path = main"))
        session.execute(text(f"SET LOCAL statement_timeout = '{timeout}s'"))
        return session.execute(text(sql)).scalar()


def db_delete(sql, timeout=600):
    import time
    ts = time.time()
    with create_session() as session:
        session.execute(text("SET LOCAL search_path = main"))
        session.execute(text(f"SET LOCAL statement_timeout = '{timeout}s'"))
        result = session.execute(text(sql))
        rowcount = result.rowcount
    logger.info(f"🗑️  Удалено {rowcount} строк за {time.time() - ts:.2f}s")
    return rowcount


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
    max_active_tasks=len(CLEANABLE_TABLES),
    schedule_interval='@weekly',
    params=params,
)
def tools_db_cleanup():

    def _cutoff(retention_days):
        return pendulum.now('UTC').subtract(days=retention_days).format('YYYY-MM-DD HH:mm:ss')

    def _make_check(tbl):
        @task(task_id=f'check_{tbl}')
        def _check(_tbl=tbl, **context):
            params = context['params']
            if not params.get(_tbl, True):
                raise AirflowSkipException(f'Таблица {_tbl} отключена')
            retention_days = params['retention_days']
            if retention_days < 30:
                raise AirflowFailException(f'retention_days={retention_days} меньше минимума (30)')
            cutoff = _cutoff(retention_days)
            date_col = DATE_COLUMNS.get(_tbl)
            custom_sql = COUNT_SQLS.get(_tbl)
            if date_col is not None:
                sql = f"SELECT COUNT(*) FROM {_tbl} WHERE {date_col} < '{cutoff}'"
            elif custom_sql is not None:
                sql = custom_sql.format(cutoff=cutoff)
            else:
                logger.info(f'⚠️  {_tbl}: подсчёт не поддерживается')
                return
            count = db_count(sql)
            logger.info(f'🔍 {_tbl}: {count} записей к удалению')
        return _check()

    def _make_clean(tbl):
        @task(task_id=f'clean_{tbl}')
        def _clean(_tbl=tbl, **context):
            params = context['params']
            if not params.get(_tbl, True):
                raise AirflowSkipException(f'Таблица {_tbl} отключена')
            if params['dry_run']:
                raise AirflowSkipException('🔒 dry_run=True — удаление пропущено')
            retention_days = params['retention_days']
            if retention_days < 30:
                raise AirflowFailException(f'retention_days={retention_days} меньше минимума (30)')
            cutoff = _cutoff(retention_days)
            sql = DELETE_SQLS[_tbl].format(cutoff=cutoff)
            logger.info(f'[УДАЛЕНИЕ] {_tbl}')
            db_delete(sql)
        return _clean()

    # --- check: все таски параллельно ---
    with TaskGroup('check') as check_group:
        for tbl in CLEANABLE_TABLES:
            _make_check(tbl)

    # --- clean: с зависимостями по каскадам ---
    with TaskGroup('clean') as clean_group:
        ct = {tbl: _make_clean(tbl) for tbl in CLEANABLE_TABLES}

        # Фаза 1 (параллельно) >> task_instance
        phase1 = [ct[t] for t in [
            'callback_request', 'celery_taskmeta', 'celery_tasksetmeta', 'session',
            'import_error', 'sla_miss', 'log', 'job',
            'xcom', 'rendered_task_instance_fields', 'task_instance_history',
        ]]
        phase1 >> ct['task_instance']

        # task_instance >> trigger, dag_run (параллельно)
        ct['task_instance'] >> [ct['trigger'], ct['dag_run']]

        # trigger, dag_run >> dataset_event, dataset, dataset_alias (параллельно)
        [ct['trigger'], ct['dag_run']] >> [ct['dataset_event'], ct['dataset'], ct['dataset_alias']]

    check_group >> clean_group


ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

if ENV_STAND == "prom":
    logger.warning("DAG tools_db_cleanup is disabled on 'prom' stand. Skipping DAG registration.")
else:
    tools_db_cleanup()
