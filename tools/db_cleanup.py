"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами (без CTAS-архивирования).
Для таблиц, связанных с `dag_run`, используются существующие индексы через косвенные условия.
Большие таблицы (> 50 000 строк) удаляются порциями по диапазону дат.

| Параметр            | Описание                                                                                   |
|---------------------|--------------------------------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес, минимум 30)*                    |
| 🔍 `dry_run`        | `True` — только подсчёт без удаления, `False` — реальное удаление *(default)*             |
| 🧹 `vacuum`         | `True` — VACUUM ANALYZE после очистки *(default)*, `False` — пропустить                   |
| ➕ `custom`     | `True` — включить `dag_code` и `dag_pickle` *(default)*, `False` — только стандартные     |

**Таски:**
- **clean** — подсчёт и удаление по каждой таблице; заметка обновляется после каждой таблицы
- **vacuum** — VACUUM ANALYZE по очищенным таблицам
- **report** — отчёт по размерам схемы `main` с delta к предыдущему запуску

> `dry_run=False` по умолчанию — реальное удаление. Для проверки установите `dry_run=True`.
"""

from airflow.decorators import task, dag
from airflow.models import Param, DagRun, XCom
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.session import create_session
from airflow.operators.python import get_current_context
from airflow import settings
from airflow.utils.db_cleanup import config_dict as _cleanup_config
from sqlalchemy import text

from pprint import PrettyPrinter
import pendulum
import time
import logging

logger = logging.getLogger("airflow.task")


MAX_NOTE_LEN = 1000
BATCH_SIZE = 50_000

# Дополнительные условия для использования существующих индексов без создания новых.
# {p} — префикс алиаса таблицы ('' или 'base.').
# Опираемся на idx_dag_run_execution_date: execution_date ≤ start_date всегда,
# поэтому start_date < cutoff ⟹ execution_date < cutoff (безопасное добавление).
_EXTRA_COND = {
    'dag_run': '{p}execution_date < :cutoff',
    'task_instance': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    'task_instance_history': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    'task_fail': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    'task_reschedule': (
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.dag_id = {p}dag_id AND _dr.run_id = {p}run_id'
        ' AND _dr.execution_date < :cutoff)'
    ),
    # dag_run_id — FK на dag_run.id; подзапрос использует idx_dag_run_execution_date
    'xcom': (
        '{p}dag_run_id IN'
        ' (SELECT id FROM main.dag_run WHERE execution_date < :cutoff)'
    ),
    # task_id (unique index) → external_executor_id в task_instance → dag_run
    'celery_taskmeta': (
        '{p}task_id IN ('
        'SELECT ti.external_executor_id FROM main.task_instance ti'
        ' JOIN main.dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id'
        ' WHERE dr.execution_date < :cutoff AND ti.external_executor_id IS NOT NULL)'
    ),
}

# Таблицы без индекса на recency-колонке но с integer PK —
# удаляем через ORDER BY pk LIMIT batch_size чтобы использовать PK-индекс.
_PK_BATCH = {
    'celery_tasksetmeta': 'id',
    'callback_request':   'id',
    'import_error':       'id',
}

# Таблицы вне стандартного _cleanup_config с дополнительным safety-фильтром.
_CUSTOM_TABLES = {
    # Исходники DAG-файлов — нельзя трогать то, на что ссылается serialized_dag
    'dag_code': {
        'col': 'last_updated',
        'safe_where': (
            'fileloc_hash NOT IN (SELECT fileloc_hash FROM main.serialized_dag)'
        ),
    },
    # Устаревший pickle-формат — нельзя трогать то, на что ссылается dag.pickle_id
    'dag_pickle': {
        'col': 'created_dttm',
        'safe_where': (
            'id NOT IN (SELECT pickle_id FROM main.dag WHERE pickle_id IS NOT NULL)'
        ),
    },
}


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
        units = ["", "тыс", "млн", "млрд", "трлн", "птлн"]
    if not size_bytes or size_bytes == 0:
        return f"0 {units[0]}"
    import math
    sign = "-" if size_bytes < 0 else ""
    size_bytes = abs(size_bytes)
    i = int(math.floor(math.log(size_bytes, base)))
    if i >= len(units): i = len(units) - 1
    if i < 0: i = 0
    suffix = units[i]
    return f"{sign}{round(size_bytes / (base ** i), 2)}" + (f" {suffix}" if suffix else "")


params = {
    'retention_days': Param(
        180,
        type='integer',
        minimum=30,
        description='Хранить записи не старше N дней (минимум 30)',
    ),
    'dry_run': Param(
        False,
        type='boolean',
        description='True — только подсчёт, False — реальное удаление',
    ),
    'vacuum': Param(
        True,
        type='boolean',
        description='True — VACUUM ANALYZE, False — пропустить',
    ),
    'custom': Param(
        False,
        type='boolean',
        description='True — включить dag_code и dag_pickle, False — только стандартные таблицы',
    ),
    'batch_size': Param(
        BATCH_SIZE,
        type='integer',
        minimum=1000,
        description='Максимальный размер порции при удалении (строк)',
    ),
    'lock_timeout': Param(
        '10min',
        type='string',
        description='Таймаут ожидания блокировки (например: 10min, 30s)',
    ),
}


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
    schedule_interval='0 2 * * *',
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
        batch_size = p.get('batch_size', BATCH_SIZE)
        lock_timeout = p.get('lock_timeout', '10min')
        cutoff = pendulum.now('UTC').subtract(days=retention_days)

        def _fmt_date(d):
            return str(d)[:10] if d else '—'

        def _idx_label(tbl, col, session):
            """✅ прямой индекс / ↗ косвенный / ❌ seq scan."""
            n = session.execute(text("""
                SELECT COUNT(*) FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace ns ON ns.oid = c.relnamespace
                WHERE ns.nspname = 'main' AND c.relname = :tbl AND a.attname = :col
            """), {'tbl': tbl, 'col': col}).scalar()
            if n:
                return '✅'
            if tbl in _EXTRA_COND:
                return '↗'
            if tbl in _PK_BATCH:
                return '🔑'
            return '❌'

        def _do_cleanup(tbl, session, on_batch=None):
            session.execute(text(f"SET lock_timeout = '{lock_timeout}'"))
            t = f'main.{tbl}'
            bind = {'cutoff': cutoff}

            if tbl in _CUSTOM_TABLES:
                # Таблицы вне стандартного Airflow cleanup — простой WHERE + safety-фильтр
                custom = _CUSTOM_TABLES[tbl]
                col = custom['col']
                idx = _idx_label(tbl, col, session)
                p = ''
                base_where = f"{col} < :cutoff AND {custom['safe_where']}"
                count_sql = text(f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {t} WHERE {base_where}")
                def make_delete(batch_extra=''):
                    w = base_where + (f' AND {batch_extra}' if batch_extra else '')
                    return text(f"DELETE FROM {t} WHERE {w}")
            else:
                cfg = _cleanup_config[tbl]
                col = cfg.recency_column_name
                idx = _idx_label(tbl, col, session)

                if cfg.keep_last and cfg.keep_last_group_by:
                    p = 'base.'
                    grp = cfg.keep_last_group_by[0]
                    keep_sub = (
                        f"SELECT {grp}, MAX({col}) AS _max FROM {t} "
                        f"WHERE external_trigger = false GROUP BY {grp}"
                    )
                    jc = f"base.{grp} = _l.{grp} AND base.{col} = _l._max"
                    base_where = f"base.{col} < :cutoff AND _l._max IS NULL"
                    from_clause = f"{t} base LEFT JOIN ({keep_sub}) _l ON {jc}"
                else:
                    p = ''
                    base_where = f"{col} < :cutoff"
                    from_clause = t

                extra_cond = _EXTRA_COND.get(tbl, '').format(p=p)
                if extra_cond:
                    base_where += f' AND {extra_cond}'

                if cfg.keep_last and cfg.keep_last_group_by:
                    count_sql = text(
                        f"SELECT COUNT(*), MIN(base.{col}), MAX(base.{col}) "
                        f"FROM {from_clause} WHERE {base_where}"
                    )
                    def make_delete(batch_extra=''):
                        w = base_where + (f' AND {batch_extra}' if batch_extra else '')
                        return text(f"DELETE FROM {t} WHERE id IN (SELECT base.id FROM {from_clause} WHERE {w})")
                else:
                    count_sql = text(f"SELECT COUNT(*), MIN({col}), MAX({col}) FROM {t} WHERE {base_where}")
                    def make_delete(batch_extra=''):
                        w = base_where + (f' AND {batch_extra}' if batch_extra else '')
                        return text(f"DELETE FROM {t} WHERE {w}")

            row = session.execute(count_sql, bind).fetchone()
            count, min_date, max_date = row[0] or 0, row[1], row[2]

            batches = 0
            if count and not dry_run:
                n_batches = (count + batch_size - 1) // batch_size
                pk = _PK_BATCH.get(tbl)
                if pk and n_batches > 1:
                    # Нет индекса на recency-колонке — используем PK-индекс:
                    # DELETE WHERE pk IN (SELECT pk WHERE ... ORDER BY pk LIMIT batch_size)
                    pk_delete = text(
                        f"DELETE FROM {t} WHERE {pk} IN"
                        f" (SELECT {pk} FROM {t} WHERE {base_where}"
                        f" ORDER BY {pk} LIMIT :lim)"
                    )
                    while True:
                        res = session.execute(pk_delete, {**bind, 'lim': batch_size})
                        session.commit()
                        if res.rowcount == 0:
                            break
                        batches += 1
                        if on_batch:
                            on_batch(batches, n_batches, count, min_date, idx)
                elif n_batches > 1 and min_date is not None:
                    start = pendulum.instance(min_date)
                    diff = (cutoff - start).total_seconds()
                    step = diff / n_batches
                    for j in range(n_batches):
                        b_s = start.add(seconds=step * j)
                        b_e = cutoff if j == n_batches - 1 else start.add(seconds=step * (j + 1))
                        batch_extra = f"{p}{col} >= :b_s AND {p}{col} < :b_e"
                        session.execute(make_delete(batch_extra), {**bind, 'b_s': b_s, 'b_e': b_e})
                        session.commit()
                        batches += 1
                        if on_batch:
                            on_batch(batches, n_batches, count, min_date, idx)
                else:
                    session.execute(make_delete(), bind)
                    session.commit()
                    batches = 1

            return {'count': count, 'min_date': min_date, 'max_date': max_date,
                    'idx': idx, 'batches': batches}

        def _note_rows(res):
            return [
                f"|{t}|{readable_size(r['count'], base=1000)}"
                f"|{_fmt_date(r['min_date'])}"
                f"|{r['idx']}"
                f"|{r.get('duration', '')}|"
                for t, r in res.items()
            ]

        HDR = ['|Таблица|Строк|Min|Idx|Время|',
               '|-|-|-|-|-|']

        custom = p.get('custom', True)
        table_names = list(_cleanup_config.keys()) + (list(_CUSTOM_TABLES.keys()) if custom else [])
        results = {}
        mode = '🔍 dry_run' if dry_run else '🗑️ удалено'
        _ts_total = time.time()

        for i, tbl in enumerate(table_names, 1):
            _ts = time.time()

            def _on_batch(done, total, count, min_date, idx, _tbl=tbl, _i=i):
                elapsed = round(time.time() - _ts_total, 2)
                cur = {'count': count, 'min_date': min_date, 'idx': idx,
                       'duration': f'{done}/{total}'}
                subtotal = sum(r['count'] for r in results.values()) + count
                prog = f"|*{_i}/{len(table_names)}*|*{readable_size(subtotal, base=1000)}*|||*{elapsed}*|"
                add_note('\n'.join(HDR + _note_rows(results) + _note_rows({_tbl: cur}) + [prog]),
                         context=context, level='Task',
                         title=f'🗑️ clean ({mode}, {retention_days}d)', add=False)

            try:
                with create_session() as session:
                    info = _do_cleanup(tbl, session, on_batch=_on_batch)
            except Exception as e:
                logger.warning(f"⚠️ {tbl}: {e}")
                results[tbl] = {'count': 0, 'min_date': None, 'idx': '⚠️',
                                 'duration': str(e)[:40], 'batches': 0}
                elapsed = round(time.time() - _ts_total, 2)
                subtotal = sum(r['count'] for r in results.values())
                prog = f"|*{i}/{len(table_names)}*|*{readable_size(subtotal, base=1000)}*|||*{elapsed}*|"
                add_note('\n'.join(HDR + _note_rows(results) + [prog]),
                         context=context, level='Task',
                         title=f'🗑️ clean ({mode}, {retention_days}d)', add=False)
                continue
            info['duration'] = round(time.time() - _ts, 2)
            results[tbl] = info
            logger.info(
                f"🔎 {tbl}: {info['count']} rows "
                f"[{_fmt_date(info['min_date'])}…{_fmt_date(info['max_date'])}] "
                f"idx={info['idx']} batches={info['batches']} {info['duration']}s"
            )

            subtotal = sum(r['count'] for r in results.values())
            elapsed = round(time.time() - _ts_total, 2)
            progress = f"|*{i}/{len(table_names)}*|*{readable_size(subtotal, base=1000)}*|||*{elapsed}*|"
            add_note('\n'.join(HDR + _note_rows(results) + [progress]),
                     context=context, level='Task',
                     title=f'🗑️ clean ({mode}, {retention_days}d)', add=False)

        duration = round(time.time() - _ts_total, 2)

        if results:
            total = sum(r['count'] for r in results.values())
            footer = f"|**Итого**|**{readable_size(total, base=1000)}**|||**{duration}**|"
            lines = HDR + _note_rows(results) + [footer]
            add_note('\n'.join(lines), context=context, level='Task',
                     title=f'🗑️ clean ({mode}, {retention_days}d)', duration=duration, add=False)
            add_note(
                f'{mode} {readable_size(total, base=1000)} строк | cutoff: {cutoff.format("YYYY-MM-DD")}',
                context=context, level='DAG', title='🗑️ clean', duration=duration,
            )
        else:
            add_note(
                f'{mode} | cutoff: {cutoff.format("YYYY-MM-DD")}',
                context=context, level='DAG,Task', title='🗑️ clean', duration=duration,
            )

        return list(results.keys())

    @task(task_id='vacuum', trigger_rule=TriggerRule.ALL_DONE)
    def vacuum(**context):
        p = context['params']
        if not p.get('vacuum', True):
            raise AirflowSkipException('vacuum=False — пропущено')

        timeout = 15 * 60
        tables = context['ti'].xcom_pull(task_ids='clean') or []
        if not tables:
            raise AirflowSkipException('нет таблиц из clean')
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
            '|Таблица|Current|Δ|Записей|Удалённых|',
            '|-|-|-|-|-|',
        ] + [
            f"|{r['table']}|{r['after']}|{r['delta']}|{r['live_rows']}|{r['dead_rows']}|"
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
            f"| Таблиц | Last | Current | Δ | Записей | Удалённых |\n"
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
