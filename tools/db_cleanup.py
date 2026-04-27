"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow через `airflow.utils.db_cleanup`.

| Параметр            | Описание                                                                                      |
|---------------------|-----------------------------------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес)*                                    |
| 🔍 `dry_run`        | Только подсчёт без удаления *(default: `True`)*                                               |
| 🧹 `vacuum_mode`    | `analyze` — VACUUM ANALYZE *(default)*, `skip` — пропустить                                   |
| ⏱️ `timeout`        | Таймаут на каждую операцию, мин *(default: `15`)*                                             |

> **dry_run=True** по умолчанию — первый запуск всегда безопасен.
> Минимальный порог `retention_days` — 30 дней.
"""

from airflow.decorators import task, dag
from airflow.models import Param, DagRun, XCom
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.session import create_session
from airflow.operators.python import get_current_context
from airflow import settings
from airflow.utils.db_cleanup import run_cleanup, config_dict as _cleanup_config
from sqlalchemy import text

from pprint import PrettyPrinter
import pendulum
import time

from logging import getLogger
logger = getLogger("airflow.task")

MAX_NOTE_LEN = 1000

# Таблицы для vacuum (run_cleanup знает свой список, здесь только для vacuum)
CLEANABLE_TABLES = list(_cleanup_config.keys())


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


def db_vacuum(table, full=False, timeout=3600):
    ts = time.time()
    mode = 'FULL ANALYZE' if full else 'ANALYZE'
    sql = f"VACUUM {mode} main.{table}"
    with settings.engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
        conn.execute(text(f"SET statement_timeout = '{timeout}s'"))
        conn.execute(text(sql))
        pool_proxy = conn.connection
        psy = getattr(pool_proxy, 'dbapi_connection', None) or getattr(pool_proxy, 'connection', None) or pool_proxy
        notices = list(getattr(psy, 'notices', []))
    skipped = next((n for n in notices if 'skipping' in n.lower()), None)
    if skipped:
        raise AirflowSkipException(skipped.strip())
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
        enum=['analyze', 'skip'],
        description='analyze — VACUUM ANALYZE, skip — пропустить',
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

    @task(task_id='clean')
    def clean(**context):
        p = context['params']
        retention_days = p['retention_days']
        if retention_days < 30:
            raise AirflowFailException(f'retention_days={retention_days} меньше минимума (30)')

        dry_run = p['dry_run']
        cutoff = pendulum.now('UTC').subtract(days=retention_days)
        table_names = [t for t in CLEANABLE_TABLES if p.get(t, True)]

        _ts = time.time()
        run_cleanup(
            clean_before_timestamp=cutoff,
            table_names=table_names,
            dry_run=dry_run,
            verbose=True,
            confirm=False,
        )
        duration = round(time.time() - _ts, 2)

        mode = '🔍 dry_run' if dry_run else '🗑️ удалено'
        add_note(
            f'{mode} | cutoff: {cutoff.format("YYYY-MM-DD")} | таблиц: {len(table_names)}',
            context=context, level='DAG,Task', title='🗑️ clean',
            duration=duration,
        )

    @task(task_id='vacuum', trigger_rule=TriggerRule.ALL_DONE)
    def vacuum(**context):
        p = context['params']
        if p.get('vacuum_mode', 'analyze') == 'skip':
            raise AirflowSkipException('vacuum_mode=skip — пропущено')

        timeout = p.get('timeout', 15) * 60
        tables = [t for t in CLEANABLE_TABLES if p.get(t, True)]
        results = []
        for tbl in tables:
            _ts = time.time()
            try:
                db_vacuum(tbl, full=False, timeout=timeout)
            except AirflowSkipException as e:
                logger.warning(f"⚠️ {tbl}: {e}")
                continue
            results.append({'table': tbl, 'duration': round(time.time() - _ts, 2)})

        if not results:
            add_note('нет таблиц для вакуума', context=context, level='DAG,Task', title='🧹 vacuum')
            return
        lines = [
            '| Таблица | Время, с |',
            '|---------|---------|',
        ] + [f"| `{r['table']}` | {r['duration']} |" for r in results]
        total = round(sum(r['duration'] for r in results), 2)
        lines.append(f"| **Итого** | **{total} с** |")
        add_note('\n'.join(lines), context=context, level='Task', title='🧹 vacuum')
        add_note(f'{len(results)} таблиц за {total} с', context=context, level='DAG', title='🧹 vacuum')

    @task(task_id='report', trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):
        dag_id = context['dag_run'].dag_id
        run_id = context['dag_run'].run_id

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
                n_dead_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'main'
            ORDER BY total_bytes DESC
        """
        with create_session() as session:
            rows = session.execute(text(sql)).fetchall()

        data = []
        for relname, total_bytes, live, dead in rows:
            after_b  = total_bytes or 0
            before_b = before.get(relname)
            delta_b  = (after_b - before_b) if before_b is not None else None
            delta_s  = (('-' if delta_b < 0 else '+') + readable_size(abs(delta_b))) if delta_b is not None else '—'
            data.append({
                'table':      relname,
                'after':      readable_size(after_b),
                'delta':      delta_s,
                'size_bytes': after_b,
                'live_rows':  readable_size(live or 0, base=1000),
                'dead_rows':  readable_size(dead or 0, base=1000),
            })

        lines = [
            '| Таблица | После | Δ | Записей | Удалённых |',
            '|---------|-------|---|---------|-----------|',
        ] + [
            f"| `{r['table']}` | {r['after']} | {r['delta']} | {r['live_rows']} | {r['dead_rows']} |"
            for r in data
        ]

        report_md = '\n'.join(lines)
        logger.info(f"📊 Отчёт по схеме main:\n{report_md}")
        add_note(report_md, context=context, level='Task', title='📊 Схема main')

        total_after = sum(r[1] or 0 for r in rows)
        total_live  = sum(r[2] or 0 for r in rows)
        total_dead  = sum(r[3] or 0 for r in rows)
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

    clean() >> vacuum() >> report()

tools_db_cleanup()
