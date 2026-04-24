"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами.

| Параметр            | Описание                                                                                      |
|---------------------|-----------------------------------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес)*                                    |
| 🔍 `dry_run`        | Только подсчёт без удаления *(default: `True`)*                                               |
| 🧹 `vacuum_mode`    | `analyze` — VACUUM ANALYZE *(default)*, `full` — VACUUM FULL ANALYZE, `skip` — пропустить     |
| ⏱️ `timeout`        | Таймаут на каждую операцию, мин *(default: `15`)*                                             |
| 📋 `tables`         | Список таблиц для очистки *(default: все)*                                                    |

---

#### Таблицы очистки и порядок удаления

| #  | Таблица                         | Критерий                              | Заметки                                                                |
|----|---------------------------------|---------------------------------------|------------------------------------------------------------------------|
| 1  | `callback_request`              | `created_at < cutoff`                 | Обработанные колбэки планировщика                                      |
| 2  | `celery_taskmeta`               | `date_done < cutoff`                  | Результаты Celery-задач (result backend)                               |
| 3  | `celery_tasksetmeta`            | `date_done < cutoff`                  | Результаты Celery-групп (result backend)                               |
| 4  | `session`                       | `expiry < current_date`               | Веб-сессии UI; не зависит от cutoff                                    |
| 5  | `import_error`                  | `timestamp < cutoff`                  | Ошибки парсинга DAG-файлов                                             |
| 6  | `sla_miss`                      | `timestamp < cutoff`                  | Нарушения SLA                                                          |
| 7  | `log`                           | `dttm < cutoff`                       | Audit-лог событий (не логи тасков)                                     |
| 8  | `job`                           | `latest_heartbeat < cutoff`           | Записи SchedulerJob / LocalTaskJob                                     |
| 9  | `xcom`                          | `timestamp < cutoff`                  | ↳ каскад от `task_instance`                                            |
| 10 | `rendered_task_instance_fields` | via `dag_run.execution_date < cutoff` | ↳ каскад от `task_instance`; нет своей колонки даты                    |
| 11 | `task_instance_history`         | `updated_at < cutoff`                 | ↳ каскад от `task_instance`; чистим до TI чтобы управлять объёмом      |
| 12 | `task_instance`                 | `start_date < cutoff`                 | ↳ каскад от `dag_run`                                                  |
| 13 | `trigger`                       | `created_date < cutoff`               | Deferred tasks; только без активных `task_instance`                    |
| 14 | `dag_run`                       | `execution_date < cutoff`             | Каскадно удаляет TI, xcom, rtif, dagrun_dataset_event и др.            |
| 15 | `dataset_event`                 | `timestamp < cutoff`                  | Каскадно удаляет `dagrun_dataset_event`, `dataset_alias_dataset_event` |
| 16 | `dataset`                       | `is_orphaned = TRUE`                  | Флаг выставляется Airflow при удалении DAG                             |
| 17 | `dataset_alias`                 | не в DAG-расписаниях и не в алиасах   | Каскадно удаляет `dag_schedule_dataset_alias_reference`                |
> **dry_run=True** по умолчанию — первый запуск всегда безопасен.
> Минимальный порог `retention_days` — 30 дней.
"""

from airflow.decorators import task, dag, task_group
from airflow.models import Param, DagRun, XCom
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
    'task_instance':                 None,
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
    'task_instance':      """
        DELETE FROM task_instance
        USING dag_run
        WHERE task_instance.dag_id = dag_run.dag_id
          AND task_instance.run_id = dag_run.run_id
          AND dag_run.execution_date < '{cutoff}'
    """,
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
    'task_instance': """
        SELECT COUNT(*), MIN(dag_run.execution_date), MAX(dag_run.execution_date)
        FROM task_instance
        JOIN dag_run ON task_instance.dag_id = dag_run.dag_id
          AND task_instance.run_id = dag_run.run_id
        WHERE dag_run.execution_date < '{cutoff}'
    """,
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


def db_stats(sql, timeout=300):
    with create_session() as session:
        session.execute(text("SET LOCAL search_path = main"))
        session.execute(text(f"SET LOCAL statement_timeout = '{timeout}s'"))
        return session.execute(text(sql)).fetchone()


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
    sql = f"VACUUM {mode} main.{table}"
    # execution_options на engine (до connect) — единственный способ гарантировать
    # autocommit ДО того как SQLAlchemy успеет выдать BEGIN
    with settings.engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
        conn.execute(text(f"SET statement_timeout = '{timeout}s'"))
        conn.execute(text(sql))
    logger.info(f"✅ {sql} за {time.time() - ts:.2f}s")


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
    'timeout': Param(
        15,
        type='integer',
        minimum=1,
        description='Таймаут на каждую операцию, мин',
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
            _ts = time.time()
            timeout = params.get('timeout', 15) * 60
            if date_col is not None:
                sql = f"SELECT COUNT(*), MIN({date_col}), MAX({date_col}) FROM {_tbl} WHERE {date_col} < '{cutoff}'"
                count, min_date, max_date = db_stats(sql, timeout=timeout)
                min_s = str(min_date)[:16] if min_date else '—'
                max_s = str(max_date)[:16] if max_date else '—'
                msg = f'{readable_size(count, base=1000)} записей к удалению | min: {min_s} | max: {max_s}'
            elif custom_sql is not None:
                row = db_stats(custom_sql.format(cutoff=cutoff), timeout=timeout)
                count = row[0] if row else 0
                if row and len(row) == 3:
                    min_s = str(row[1])[:16] if row[1] else '—'
                    max_s = str(row[2])[:16] if row[2] else '—'
                    msg = f'{readable_size(count, base=1000)} записей к удалению | min: {min_s} | max: {max_s}'
                else:
                    min_s = max_s = '—'
                    msg = f'{readable_size(count, base=1000)} записей к удалению'
            else:
                add_note('подсчёт не поддерживается', context=context, level='Task', title=f'⚠️ {_tbl}')
                return None
            dead = db_stats(
                f"SELECT n_dead_tup FROM pg_stat_user_tables WHERE schemaname='main' AND relname='{_tbl}'",
                timeout=30,
            )
            dead_tup = (dead[0] if dead else 0) or 0
            msg += f' | мёртвых: {readable_size(dead_tup, base=1000)}'
            add_note(msg, context=context, level='Task', title=f'🔍 {_tbl}', duration=time.time() - _ts)
            return {'table': _tbl, 'count': count, 'min': min_s, 'max': max_s, 'dead': dead_tup}
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
            rowcount = db_delete(sql, timeout=params.get('timeout', 15) * 60)
            add_note(f'удалено {readable_size(rowcount, base=1000)} строк', context=context, level='Task', title=f'🗑️ {_tbl}', duration=time.time() - _ts)
            return {'table': _tbl, 'deleted': rowcount}
        return _clean()

    @task_group(group_id='check')
    def check_group():
        checks = [_make_check(tbl) for tbl in CLEANABLE_TABLES]

        @task(task_id='summary', trigger_rule=TriggerRule.ALL_DONE)
        def _check_summary(**context):
            ti = context['ti']
            results = [ti.xcom_pull(task_ids=f'check.check_{t}') for t in CLEANABLE_TABLES]
            rows = [r for r in results if r]
            if not rows:
                add_note('нет данных', context=context, level='DAG,Task', title='🔍 Итог check')
                return
            lines = [
                '| Таблица | К удалению | Мёртвых | Min | Max |',
                '|---------|-----------|---------|-----|-----|',
            ] + [f"| `{r['table']}` | {readable_size(r['count'], base=1000)} | {readable_size(r.get('dead', 0), base=1000)} | {r['min']} | {r['max']} |" for r in rows]
            total = sum(r['count'] for r in rows)
            total_dead = sum(r.get('dead', 0) for r in rows)
            lines.append(f"| **Итого** | **{readable_size(total, base=1000)}** | **{readable_size(total_dead, base=1000)}** | | |")
            add_note('\n'.join(lines), context=context, level='DAG,Task', title='🔍 Итог check')

        checks >> _check_summary()

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

        @task(task_id='summary', trigger_rule=TriggerRule.ALL_DONE)
        def _clean_summary(**context):
            ti = context['ti']
            results = [ti.xcom_pull(task_ids=f'clean.clean_{t}') for t in CLEANABLE_TABLES]
            rows = [r for r in results if r]
            if not rows:
                add_note('удалений не было', context=context, level='DAG,Task', title='🗑️ Итог clean')
                return
            lines = [
                '| Таблица | Удалено |',
                '|---------|---------|',
            ] + [f"| `{r['table']}` | {readable_size(r['deleted'], base=1000)} |" for r in rows]
            total = sum(r['deleted'] for r in rows)
            lines.append(f"| **Итого** | **{readable_size(total, base=1000)}** |")
            add_note('\n'.join(lines), context=context, level='DAG,Task', title='🗑️ Итог clean')

        [ct['dataset_event'], ct['dataset'], ct['dataset_alias']] >> _clean_summary()

    def _make_vacuum(tbl):
        @task(task_id=f'vacuum_{tbl}', trigger_rule=TriggerRule.ALL_DONE)
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
            db_vacuum(_tbl, full=full, timeout=params.get('timeout', 15) * 60)
            duration = round(time.time() - _ts, 2)
            add_note(f'{label} выполнен', context=context, level='Task', title=f'🧹 {_tbl}', duration=duration)
            return {'table': _tbl, 'mode': label, 'duration': duration}
        return _vacuum()

    @task_group(group_id='vacuum')
    def vacuum_group():
        vacuums = [_make_vacuum(tbl) for tbl in CLEANABLE_TABLES]

        @task(task_id='summary', trigger_rule=TriggerRule.ALL_DONE)
        def _vacuum_summary(**context):
            ti = context['ti']
            results = [ti.xcom_pull(task_ids=f'vacuum.vacuum_{t}') for t in CLEANABLE_TABLES]
            rows = [r for r in results if r]
            if not rows:
                add_note('вакуум не выполнялся', context=context, level='DAG,Task', title='🧹 Итог vacuum')
                return
            lines = [
                '| Таблица | Режим | Время, с |',
                '|---------|-------|---------|',
            ] + [f"| `{r['table']}` | {r['mode']} | {r['duration']} |" for r in rows]
            total = round(sum(r['duration'] for r in rows), 2)
            lines.append(f"| **Итого** | | **{total} с** |")
            mode_label = 'FULL' if any('FULL' in r['mode'] for r in rows) else 'ANALYZE'
            add_note('\n'.join(lines), context=context, level='Task', title=f'🧹 Итог vacuum ({mode_label})')
            add_note(f'{len(rows)} таблиц за {total} с', context=context, level='DAG', title=f'🧹 Итог vacuum ({mode_label})')

        vacuums >> _vacuum_summary()

    @task(task_id='report', trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):
        dag_id = context['dag_run'].dag_id
        run_id = context['dag_run'].run_id

        # Размеры из предыдущего прогона берём из XCom таска report
        with create_session() as session:
            prev_run = (
                session.query(DagRun)
                .filter(DagRun.dag_id == dag_id, DagRun.run_id != run_id)
                .order_by(DagRun.execution_date.desc())
                .first()
            )
        prev_data = None
        if prev_run:
            prev_data = XCom.get_one(
                run_id=prev_run.run_id, key='return_value',
                task_id='report', dag_id=dag_id,
            )
        before = {r['table']: r['size_bytes'] for r in prev_data} if prev_data else {}

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

        data = []
        for relname, total_bytes, live, dead, last_vac, last_ana in rows:
            after_b  = total_bytes or 0
            before_b = before.get(relname)
            delta_b  = (after_b - before_b) if before_b is not None else None
            delta_s  = (('-' if delta_b < 0 else '+') + readable_size(abs(delta_b))) if delta_b is not None else '—'
            data.append({
                'table':        relname,
                'before':       readable_size(before_b) if before_b is not None else '—',
                'after':        readable_size(after_b),
                'delta':        delta_s,
                'size_bytes':   after_b,
                'live_rows':    readable_size(live or 0, base=1000),
                'dead_rows':    readable_size(dead or 0, base=1000),
                'last_vacuum':  str(last_vac)[:16] if last_vac else None,
                'last_analyze': str(last_ana)[:16] if last_ana else None,
            })

        lines = [
            '| Таблица | До | После | Δ | Записей | Удалённых |',
            '|---------|-----|-------|---|---------|-----------|',
        ] + [
            f"| `{r['table']}` | {r['before']} | {r['after']} | {r['delta']}"
            f" | {r['live_rows']} | {r['dead_rows']} |"
            for r in data
        ]

        report_md = '\n'.join(lines)
        logger.info(f"📊 Отчёт по схеме main:\n{report_md}")
        add_note(report_md, context=context, level='Task', title='📊 Схема main')

        total_after  = sum(r[1] or 0 for r in rows)
        total_live   = sum(r[2] or 0 for r in rows)
        total_dead   = sum(r[3] or 0 for r in rows)
        if before:
            total_before = sum(before.get(r[0], r[1] or 0) for r in rows)
            total_delta  = total_after - total_before
            delta_str    = ('-' if total_delta < 0 else '+') + readable_size(abs(total_delta))
            before_str   = readable_size(total_before)
        else:
            delta_str  = '—'
            before_str = '—'
        summary = (
            f"| Таблиц | До | После | Δ | Записей | Удалённых |\n"
            f"|--------|-----|-------|---|---------|----------|\n"
            f"| {readable_size(len(rows), base=1000)}"
            f" | {before_str}"
            f" | {readable_size(total_after)}"
            f" | {delta_str}"
            f" | {readable_size(total_live, base=1000)}"
            f" | {readable_size(total_dead, base=1000)} |"
        )
        add_note(summary, context=context, level='DAG', title='📊 Схема main')

        return data

    check_group() >> clean_group() >> vacuum_group() >> report()

tools_db_cleanup()
