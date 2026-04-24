"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами.

| Параметр           | Описание                                                          |
|--------------------|-------------------------------------------------------------------|
| Параметр            | Описание                                                                                      |
|---------------------|-----------------------------------------------------------------------------------------------|
| 📅 `retention_days`  | Хранить записи не старше N дней *(default: `180` = 6 мес)*                                   |
| 🔍 `dry_run`         | Только подсчёт без удаления *(default: `True`)*                                               |
| 🧹 `vacuum_mode`     | `analyze` — VACUUM ANALYZE *(default)*, `full` — VACUUM FULL ANALYZE, `skip` — пропустить    |
| ⏱️ `timeout_count`  | Таймаут подсчёта записей, мин *(default: `5`)*                                                |
| ⏱️ `timeout_delete` | Таймаут удаления записей, мин *(default: `10`)*                                               |
| ⏱️ `timeout_vacuum` | Таймаут VACUUM, мин *(default: `60`)*                                                         |
| 📋 `tables`          | Список таблиц для очистки *(default: все)*                                                    |

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

from airflow.decorators import task, dag, task_group
from airflow.models import Param
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.helpers import cross_downstream
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.session import create_session
from airflow.operators.python import get_current_context
from airflow import settings
from sqlalchemy import text

from pprint import PrettyPrinter
import pendulum
import time
import os

from logging import getLogger
logger = getLogger("airflow.task")

MAX_NOTE_LEN = 1000


def add_note(msg, context=None, level='task', add=True, title='', compact=False, duration=None):
    if not context:
        context = get_current_context()

    if isinstance(msg, dict) and len(msg) == 1:
        t, msg = next(iter(msg.items()))
        title += str(t) + (f' ({len(msg)})' if isinstance(msg, (dict, list, tuple, set)) else '')

    if duration is not None:
        msg = f"{msg} ⏱ {duration:.2f}s"

    if type(msg) is not str:
        msg = PrettyPrinter(indent=4, compact=compact).pformat(msg).replace("'", '')
        msg = '```\n' + msg + '\n```'

    logger.info(f"📝 Note added to {level} {title}:\n{msg}")

    with create_session() as session:
        for l in list(set(level.upper().split(',')))[:2]:
            new_note = msg.strip()

            if title:
                import unicodedata
                if not unicodedata.category(title[0]) == 'So':
                    title = "📝 " + title
                new_note = f"{title}\n---\n{new_note}"

            if l == 'DAG':
                # Атомарный upsert — избегает UniqueViolation при параллельных тасках
                dag_run_id = context['dag_run'].id
                existing = session.execute(
                    text("SELECT content FROM main.dag_run_note WHERE dag_run_id = :id"),
                    {'id': dag_run_id}
                ).scalar()
                if existing and existing.startswith(new_note[:MAX_NOTE_LEN]):
                    continue
                if add and existing:
                    new_note = f"{new_note}\n\n---\n{existing}"
                session.execute(text("""
                    INSERT INTO main.dag_run_note (dag_run_id, user_id, content, created_at, updated_at)
                    VALUES (:id, NULL, :content, NOW(), NOW())
                    ON CONFLICT (dag_run_id) DO UPDATE
                    SET content = EXCLUDED.content, updated_at = NOW()
                """), {'id': dag_run_id, 'content': new_note[:MAX_NOTE_LEN]})
            else:
                obj = session.merge(context['task_instance'])
                session.expire(obj)
                if obj.note and obj.note.startswith(new_note[:MAX_NOTE_LEN]):
                    continue
                if add:
                    new_note = f"{new_note}\n\n---\n{obj.note if obj.note else ''}"
                obj.note = new_note[:MAX_NOTE_LEN]


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
    ts = time.time()
    with create_session() as session:
        session.execute(text("SET LOCAL search_path = main"))
        session.execute(text(f"SET LOCAL statement_timeout = '{timeout}s'"))
        result = session.execute(text(sql))
        rowcount = result.rowcount
    logger.info(f"🗑️  Удалено {rowcount} строк за {time.time() - ts:.2f}s")
    return rowcount


def db_vacuum(table, full=False, timeout=3600):
    ts = time.time()
    mode = 'FULL ANALYZE' if full else 'ANALYZE'
    with settings.engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"SET statement_timeout = '{timeout}s'"))
        conn.execute(text(f"VACUUM {mode} main.{table}"))
    logger.info(f"✅ VACUUM {mode} main.{table} за {time.time() - ts:.2f}s")


def readable_size(size_bytes, base=1024):
    if base == 1024:
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
    else:
        units = ["ед", "тыс", "млн", "млрд", "трлн", "птлн"]
    if not size_bytes or size_bytes == 0:
        return f"0 {units[0]}"
    import math
    sign = "-" if size_bytes < 0 else ""
    size_bytes = abs(size_bytes)
    i = int(math.floor(math.log(size_bytes, base)))
    if i >= len(units): i = len(units) - 1
    if i < 0: i = 0
    return f"{sign}{round(size_bytes / (base ** i), 2)} {units[i]}"


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
    'vacuum_mode': Param(
        'analyze',
        type='string',
        enum=['analyze', 'full', 'skip'],
        description='analyze — VACUUM ANALYZE, full — VACUUM FULL ANALYZE, skip — пропустить',
    ),
    'timeout_count': Param(
        5,
        type='integer',
        minimum=1,
        description='Таймаут подсчёта записей, мин',
    ),
    'timeout_delete': Param(
        10,
        type='integer',
        minimum=1,
        description='Таймаут удаления записей, мин',
    ),
    'timeout_vacuum': Param(
        60,
        type='integer',
        minimum=1,
        description='Таймаут VACUUM, мин',
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
                add_note('подсчёт не поддерживается', context=context, level='Task', title=f'⚠️ {_tbl}')
                return
            _ts = time.time()
            count = db_count(sql, timeout=params.get('timeout_count', 5) * 60)
            add_note(f'{readable_size(count, base=1000)} записей к удалению', context=context, level='Task', title=f'🔍 {_tbl}', duration=time.time() - _ts)
        return _check()

    # Таски downstream от потенциально пропущенных — none_failed чтобы не каскадить skip
    _none_failed = {
        'task_instance', 'trigger', 'dag_run',
        'dataset_event', 'dataset', 'dataset_alias',
    }

    def _make_clean(tbl):
        tr = TriggerRule.NONE_FAILED if tbl in _none_failed else TriggerRule.ALL_SUCCESS

        @task(task_id=f'clean_{tbl}', trigger_rule=tr)
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
            _ts = time.time()
            rowcount = db_delete(sql, timeout=params.get('timeout_delete', 10) * 60)
            add_note(f'удалено {readable_size(rowcount, base=1000)} строк', context=context, level='Task', title=f'🗑️ {_tbl}', duration=time.time() - _ts)
        return _clean()

    @task_group(group_id='check')
    def check_group():
        for tbl in CLEANABLE_TABLES:
            _make_check(tbl)

    @task_group(group_id='clean')
    def clean_group():
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
        cross_downstream(
            [ct['trigger'], ct['dag_run']],
            [ct['dataset_event'], ct['dataset'], ct['dataset_alias']],
        )

    def _make_vacuum(tbl):
        @task(task_id=f'vacuum_{tbl}')
        def _vacuum(_tbl=tbl, **context):
            params = context['params']
            mode = params.get('vacuum_mode', 'analyze')
            if mode == 'skip':
                raise AirflowSkipException('vacuum_mode=skip — пропущено')
            if not params.get(_tbl, True):
                raise AirflowSkipException(f'Таблица {_tbl} отключена')
            full = mode == 'full'
            label = 'VACUUM FULL ANALYZE' if full else 'VACUUM ANALYZE'
            _ts = time.time()
            db_vacuum(_tbl, full=full, timeout=params.get('timeout_vacuum', 60) * 60)
            add_note(f'{label} выполнен', context=context, level='Task', title=f'🧹 {_tbl}', duration=time.time() - _ts)
        return _vacuum()

    @task_group(group_id='vacuum')
    def vacuum_group():
        for tbl in CLEANABLE_TABLES:
            _make_vacuum(tbl)

    @task(task_id='report', trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):
        sql = """
            SELECT
                relname,
                pg_total_relation_size('main.' || relname)  AS total_bytes,
                n_live_tup,
                n_dead_tup,
                COALESCE(last_vacuum, last_autovacuum)      AS last_vacuum,
                COALESCE(last_analyze, last_autoanalyze)    AS last_analyze
            FROM pg_stat_user_tables
            WHERE schemaname = 'main'
            ORDER BY total_bytes DESC
        """
        with create_session() as session:
            rows = session.execute(text(sql)).fetchall()

        data = [
            {
                'table':        relname,
                'size':         readable_size(total_bytes or 0),
                'size_bytes':   total_bytes or 0,
                'live_rows':    readable_size(live or 0, base=1000),
                'dead_rows':    readable_size(dead or 0, base=1000),
                'last_vacuum':  str(last_vac)[:16] if last_vac else None,
                'last_analyze': str(last_ana)[:16] if last_ana else None,
            }
            for relname, total_bytes, live, dead, last_vac, last_ana in rows
        ]

        lines = [
            '| Таблица | Размер | Записей | Удалённых | Последний вакуум | Последний анализ |',
            '|---------|--------|---------|-----------|------------------|------------------|',
        ] + [
            f"| `{r['table']}` | {r['size']} | {r['live_rows']} | {r['dead_rows']}"
            f" | {r['last_vacuum'] or '—'} | {r['last_analyze'] or '—'} |"
            for r in data
        ]

        report_md = '\n'.join(lines)
        logger.info(f"📊 Отчёт по схеме main:\n{report_md}")

        # Полная таблица — только в таск-заметку
        add_note(report_md, context=context, level='Task', title='📊 Схема main')

        # Саммери — в DAG-заметку (считаем из сырых rows, не из отформатированных строк)
        total_size = sum(r[1] or 0 for r in rows)
        total_live = sum(r[2] or 0 for r in rows)
        total_dead = sum(r[3] or 0 for r in rows)
        summary = (
            f"| Таблиц | Размер | Записей | Удалённых |\n"
            f"|--------|--------|---------|----------|\n"
            f"| {readable_size(len(rows), base=1000)} | {readable_size(total_size)} "
            f"| {readable_size(total_live, base=1000)} "
            f"| {readable_size(total_dead, base=1000)} |"
        )
        add_note(summary, context=context, level='DAG', title='📊 Схема main')

        return data

    check_group() >> clean_group() >> vacuum_group() >> report()


ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

if ENV_STAND == "prom":
    logger.warning("DAG tools_db_cleanup is disabled on 'prom' stand. Skipping DAG registration.")
else:
    tools_db_cleanup()
