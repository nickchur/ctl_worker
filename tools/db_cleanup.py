"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами (без CTAS-архивирования).
Для таблиц, связанных с `dag_run`, используются существующие индексы через косвенные условия.
Большие таблицы (> 50 000 строк) удаляются порциями по диапазону дат.

| Параметр            | Описание                                                                                   |
|---------------------|--------------------------------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес, минимум 30)*                    |
| 🔍 `dry_run`        | `True` — только подсчёт без удаления, `False` — реальное удаление *(default)*             |
| 🧹 `vacuum`         | `True` — VACUUM ANALYZE после очистки *(default)*, `False` — пропустить                   |
| 🔁 `reindex`        | `True` — REINDEX TABLE CONCURRENTLY после вакуума *(default)*, `False` — пропустить       |
| ➕ `custom`     | `True` — включить `dag_code` и `dag_pickle`, `False` — только стандартные *(default)*     |

**Таски:**
- **clean** — подсчёт и удаление по каждой таблице; заметка обновляется после каждой таблицы
- **vacuum** — VACUUM ANALYZE по очищенным таблицам
- **reindex** — REINDEX TABLE CONCURRENTLY по очищенным таблицам (админский коннект из Vault)
- **report** — отчёт по размерам схемы `main` с delta к предыдущему запуску

> `dry_run=False` по умолчанию — реальное удаление. Для проверки установите `dry_run=True`.
"""

# Только то, что нужно на парсинге DAG (scheduler/dag-processor): декораторы,
# Param/TriggerRule для сигнатуры, datetime для start_date, cheap-stdlib.
# Всё runtime-only (модели, exceptions, session, settings, config_dict, pprint,
# unicodedata) импортируется внутри тасков/хелперов — грузится на воркере.
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule
from sqlalchemy import text

from datetime import date, datetime, timedelta, timezone
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
        'EXISTS (SELECT 1 FROM main.dag_run _dr'
        ' WHERE _dr.id = {p}dag_run_id AND _dr.execution_date < :cutoff)'
    ),
    # task_id — уникальный индекс; EXISTS через external_executor_id → dag_run.execution_date
    'celery_taskmeta': (
        'EXISTS (SELECT 1 FROM main.task_instance ti'
        ' JOIN main.dag_run dr ON dr.dag_id = ti.dag_id AND dr.run_id = ti.run_id'
        ' WHERE ti.external_executor_id = {p}task_id'
        ' AND dr.execution_date < :cutoff)'
    ),
}

# Таблицы без индекса на recency-колонке но с integer PK —
# удаляем через ORDER BY pk LIMIT batch_size чтобы использовать PK-индекс.
_PK_BATCH = {
    'celery_tasksetmeta': 'id',
    'celery_taskmeta':    'id',
    'callback_request':   'id',
    'import_error':       'id',
}

# Таблицы вне стандартного _cleanup_config с дополнительным safety-фильтром (opt-in через custom=True).
_CUSTOM_TABLES = {
    # Исходники DAG-файлов — нельзя трогать то, на что ссылается serialized_dag
    'dag_code': {
        'col': 'last_updated',
        'safe_where': (
            'NOT EXISTS (SELECT 1 FROM main.serialized_dag sd WHERE sd.fileloc_hash = fileloc_hash)'
        ),
    },
    # Устаревший pickle-формат — нельзя трогать то, на что ссылается dag.pickle_id
    'dag_pickle': {
        'col': 'created_dttm',
        'safe_where': (
            'NOT EXISTS (SELECT 1 FROM main.dag d WHERE d.pickle_id = id)'
        ),
    },
}


def _log_sql(sql, bind, msg="SQL"):
    """Логирует SQL с подставленными параметрами (упрощённо)."""
    try:
        from sqlalchemy.sql import text as sa_text
        if isinstance(sql, str):
            sql = sa_text(sql)
        # Берём сырой SQL с плейсхолдерами :cutoff/:b_s/:b_e/:lim.
        # Компиляция с literal_binds=True рендерит несвязанный :cutoff как NULL
        # ещё до подстановки ниже, поэтому её не используем.
        q = str(sql)
        # Подставляем значения
        for k, v in bind.items():
            if isinstance(v, (datetime, date)):
                v = f"'{v.isoformat()}'"
            elif isinstance(v, str):
                v = f"'{v}'"
            elif v is None:
                v = 'NULL'
            else:
                v = str(v)
            q = q.replace(f":{k}", v)
        logger.info(f"{msg}:\n{q}")
    except Exception as e:
        logger.warning(f"⚠️ Не удалось развернуть SQL: {sql} | Параметры: {bind}")


def add_note(msg, context=None, level='task', add=True, title='', compact=False, duration=None):
    from pprint import PrettyPrinter
    import unicodedata

    from airflow.operators.python import get_current_context
    from airflow.utils.session import create_session

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


def db_stats(tables):
    """Снимок pg_stat_user_tables по таблицам схемы main: {table: (dead, last_vacuum)}.

    Статистика наполняется коллектором асинхронно (~500 мс), поэтому читать её
    сразу после VACUUM бессмысленно — снимок «после» снимаем с паузой.
    """
    from airflow import settings

    sql = text("""
        SELECT relname, n_dead_tup, GREATEST(last_vacuum, last_autovacuum)
        FROM pg_stat_user_tables
        WHERE schemaname = 'main' AND relname = ANY(:tbls)
    """)
    with settings.engine.connect() as conn:
        rows = conn.execute(sql, {'tbls': list(tables)}).fetchall()
    return {r[0]: (r[1], r[2]) for r in rows}


def db_vacuum(table, full=False, timeout=3600, set_role=None):
    """VACUUM [FULL] ANALYZE по таблице схемы main.

    VACUUM без FULL не уменьшает файл таблицы (страницы уходят в free space map),
    поэтому судить о его работе по размеру нельзя — ориентир n_dead_tup и last_vacuum.
    set_role — роль-владелец, без неё VACUUM без прав молча пропускает таблицу.
    """
    from airflow import settings
    from airflow.exceptions import AirflowSkipException

    ts = time.time()
    mode = 'FULL ANALYZE' if full else 'ANALYZE'
    sql = f"VACUUM {mode} main.{table}"
    with settings.engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
        pool_proxy = conn.connection
        psy = getattr(pool_proxy, 'dbapi_connection', None) or getattr(pool_proxy, 'connection', None) or pool_proxy
        # Соединение берётся из пула и уже может нести чужие сообщения — чистим,
        # иначе пропуск по правам ("skipping ... only table or database owner can
        # vacuum it") теряется и таблица считается успешно обработанной.
        notices = getattr(psy, 'notices', None)
        if notices is not None:
            del notices[:]
        conn.execute(text(f"SET statement_timeout = '{timeout}s'"))
        if set_role:
            conn.execute(text('SET ROLE "{}"'.format(set_role.replace('"', '""'))))
        logger.info(f"🔧 VACUUM: {sql}" + (f" (SET ROLE {set_role})" if set_role else ""))
        conn.execute(text(sql))
        notices = list(notices or [])
    for n in notices:
        logger.info(f"📣 {table}: {n.strip()}")
    skipped = next((n for n in notices if 'skipping' in n.lower()), None)
    if skipped:
        raise AirflowSkipException(skipped.strip())
    logger.info(f"✅ {sql} за {time.time() - ts:.2f}s")


# Опции libpq, которые имеет смысл пропускать из DB_EXTRA_1 в psycopg2.connect;
# всё остальное (ключи в стиле S3/ClickHouse и т.п.) вызвало бы
# "invalid connection option".
_LIBPQ_OPTS = {
    'connect_timeout', 'application_name', 'options', 'target_session_attrs',
    'sslmode', 'sslrootcert', 'sslcert', 'sslkey', 'gssencmode', 'krbsrvname',
}


def get_af_conn():
    """Параметры psycopg2 для админского подключения к метабазе Airflow из Vault.

    Нужны для REINDEX TABLE CONCURRENTLY: требуются права владельца таблиц,
    которых нет у сессионного пользователя Airflow. Собираем kwargs напрямую,
    без Airflow Connection и AIRFLOW_CONN_* — так подключение не зависит от
    secret backend, парсинга JSON-коннекта и платформенного do_connect-хука.
    """
    import ast
    import base64
    import json

    VAULT_PATH = '/vault/secrets/application'

    with open(VAULT_PATH) as f:
        secrets = json.load(f)

    def _b64(s: str) -> str:
        """base64 → текст; если значение не base64 — возвращаем как есть.

        В /vault/secrets/application часть значений лежит в открытом виде, а
        b64decode без validate=True молча выбрасывает символы вне алфавита
        base64 ('-', '_', '#', скобки) и портит пароль вместо явной ошибки.
        """
        try:
            return base64.b64decode(s, validate=True).decode()
        except (ValueError, UnicodeDecodeError):
            return s

    raw_extra = _b64(secrets['DB_EXTRA_1']) if secrets.get('DB_EXTRA_1') else ''
    try:
        extra = json.loads(raw_extra) if raw_extra else {}
    except ValueError:
        extra = ast.literal_eval(raw_extra) if raw_extra else {}

    host, _, port = _b64(secrets['DB_HOST_1']).partition(':')
    params = {
        'host':     host,
        'port':     int(port or 5433),
        'user':     _b64(secrets['DB_ADM_USER_1_1']),
        'password': _b64(secrets['DB_ADM_PASS_1_1']),
        # 'main' — SQL-схема внутри airflowdb, в запросах она указана явно
        'dbname':   _b64(secrets['DB_NAME_1']),
        'application_name': 'airflow_db_cleanup',
        'connect_timeout': 5,
        'target_session_attrs': 'read-write',
        # DB_EXTRA_1 поверх дефолтов, но sslmode и gssencmode задаём жёстко:
        #   sslmode=prefer — DB_EXTRA_1 приносит disable, и тогда SSL даже не
        #     пробуется; в DBeaver подключение под этой учёткой идёт с prefer;
        #   gssencmode=disable — в контейнере есть Kerberos-кэш для CTL, и libpq
        #     пробует GSS-шифрование раньше пароля, падая с "Unspecified GSS failure".
        **{k: v for k, v in extra.items() if k in _LIBPQ_OPTS},
        'sslmode':    'prefer',
        'gssencmode': 'disable',
    }
    pwd = params['password']
    logger.info(
        f"🔑 Админский коннект: {params['user']}@{host}:{params['port']}/{params['dbname']}"
        f" | пароль: {len(pwd)} симв. {pwd[:2]}…{pwd[-2:]}"
        f" (b64-декодирован: {pwd != secrets['DB_ADM_PASS_1_1']})"
        f" | {', '.join(f'{k}={params[k]}' for k in sorted(_LIBPQ_OPTS & params.keys()))}"
    )

    return params


def db_owners(tables):
    """Владельцы таблиц main и признак членства текущего пользователя в роли-владельце.

    {table: (owner, can_set_role)}. Если членство есть, но роль NOINHERIT, права
    владельца не действуют автоматически — их нужно взять явным SET ROLE.
    """
    from airflow import settings

    sql = text("""
        SELECT c.relname,
               pg_get_userbyid(c.relowner)                  AS owner,
               pg_has_role(current_user, c.relowner, 'USAGE') AS can_set_role,
               current_user
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'main' AND c.relname = ANY(:tbls)
    """)
    with settings.engine.connect() as conn:
        rows = conn.execute(sql, {'tbls': list(tables)}).fetchall()
    if rows:
        logger.info(f"👤 current_user={rows[0][3]}")
        for r in rows:
            logger.info(f"👤 {r[0]}: owner={r[1]} | SET ROLE доступен: {r[2]}")
    return {r[0]: (r[1], r[2]) for r in rows}


def db_reindex(table, conn_params=None, timeout=3600, set_role=None):
    """REINDEX TABLE CONCURRENTLY по таблице схемы main.

    CONCURRENTLY не может выполняться внутри транзакционного блока — нужен autocommit.
    По умолчанию идём тем же коннектом, что и db_vacuum (прав владельца хватает —
    VACUUM требует тех же). set_role — роль-владелец, которую берём перед REINDEX,
    если членство в ней есть (см. db_owners). conn_params (см. get_af_conn)
    задаются только при ошибке прав: тогда подключаемся psycopg2 напрямую.
    """
    ts = time.time()
    sql = f"REINDEX TABLE CONCURRENTLY main.{table}"
    logger.info(
        f"🔧 REINDEX: {sql}"
        + (f" (user={conn_params['user']})" if conn_params else "")
        + (f" (SET ROLE {set_role})" if set_role else "")
    )
    # Роль приходит из pg_get_userbyid — существующее имя, но кавычим на случай
    # спецсимволов в имени роли (учётки вида CI05456832-pg-airflowadm).
    role_sql = 'SET ROLE "{}"'.format(set_role.replace('"', '""')) if set_role else None

    if conn_params:
        import psycopg2

        conn = psycopg2.connect(**conn_params)
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = '{timeout}s'")
                if role_sql:
                    cur.execute(role_sql)
                cur.execute(sql)
        finally:
            conn.close()
    else:
        from airflow import settings

        with settings.engine.execution_options(isolation_level="AUTOCOMMIT").connect() as conn:
            conn.execute(text(f"SET statement_timeout = '{timeout}s'"))
            if role_sql:
                conn.execute(text(role_sql))
            conn.execute(text(sql))

    logger.info(f"✅ {sql} за {time.time() - ts:.2f}s")


def _fmt_ts(ts):
    """'HH:MM:SS' для отметок вакуума; '—' если статистики по таблице нет."""
    return ts.strftime('%H:%M:%S') if ts else '—'


def readable_size(size_bytes, base=1024):
    if base == 1024:
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
    else:
        units = ["", "тыс", "млн", "млрд", "трлн", "птлн"]
    if not size_bytes or size_bytes == 0:
        return f"0{' ' + units[0] if units[0] else ''}"
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
    'reindex': Param(
        True,
        type='boolean',
        description='True — REINDEX TABLE CONCURRENTLY после вакуума, False — пропустить',
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
    start_date=datetime(2025, 8, 7, tzinfo=timezone.utc),
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
        from airflow.exceptions import AirflowFailException
        from airflow.utils.db_cleanup import config_dict as _cleanup_config
        from airflow.utils.session import create_session

        p = context['params']
        retention_days = p['retention_days']
        if retention_days < 30:
            raise AirflowFailException(f'retention_days={retention_days} меньше минимума (30)')

        dry_run = p['dry_run']
        batch_size = p.get('batch_size', BATCH_SIZE)
        lock_timeout = p.get('lock_timeout', '10min')
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        def _fmt_date(d):
            return str(d)[:10] if d else '—'

        def _idx_label(tbl, col, session):
            """✅ прямой индекс / ↗ косвенный / 🔑 PK-батч / ❌ seq scan."""
            if col:
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
            logger.info(f"⚙️ Параметры очистки: retention_days={retention_days}, cutoff={cutoff}, dry_run={dry_run}, batch_size={batch_size}")

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
                col = str(cfg.recency_column_name)
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

            _log_sql(count_sql, bind, f"📊 COUNT {tbl}")
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
                        _log_sql(pk_delete, {**bind, 'lim': batch_size}, f"🗑️ DELETE {tbl}")
                        res = session.execute(pk_delete, {**bind, 'lim': batch_size})
                        session.commit()
                        if res.rowcount == 0:
                            break
                        batches += 1
                        if on_batch:
                            on_batch(batches, n_batches, count, min_date, idx)
                elif n_batches > 1 and min_date is not None:
                    # min_date из БД (timestamptz → aware). Приводим к aware UTC на
                    # случай timestamp-without-tz, чтобы вычитание с cutoff не падало.
                    start = min_date if min_date.tzinfo else min_date.replace(tzinfo=timezone.utc)
                    diff = (cutoff - start).total_seconds()
                    step = diff / n_batches
                    for j in range(n_batches):
                        b_s = start + timedelta(seconds=step * j)
                        b_e = cutoff if j == n_batches - 1 else start + timedelta(seconds=step * (j + 1))
                        batch_extra = f"{p}{col} >= :b_s AND {p}{col} < :b_e"
                        _log_sql(make_delete(batch_extra), {**bind, 'b_s': b_s, 'b_e': b_e}, f"🗑️ DELETE {tbl}")
                        session.execute(make_delete(batch_extra), {**bind, 'b_s': b_s, 'b_e': b_e})
                        session.commit()
                        batches += 1
                        if on_batch:
                            on_batch(batches, n_batches, count, min_date, idx)
                else:
                    _log_sql(make_delete(), bind, f"🗑️ DELETE {tbl}")
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

        custom = p.get('custom', False)
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
                f'{mode} {readable_size(total, base=1000)} строк | cutoff: {cutoff.strftime("%Y-%m-%d")}',
                context=context, level='DAG', title='🗑️ clean', duration=duration,
            )
        else:
            add_note(
                f'{mode} | cutoff: {cutoff.strftime("%Y-%m-%d")}',
                context=context, level='DAG,Task', title='🗑️ clean', duration=duration,
            )

        return list(results.keys())

    @task(task_id='vacuum', trigger_rule=TriggerRule.ALL_DONE)
    def vacuum(**context):
        from airflow.exceptions import AirflowSkipException

        p = context['params']
        if not p.get('vacuum', True):
            raise AirflowSkipException('vacuum=False — пропущено')

        timeout = 15 * 60
        tables = context['ti'].xcom_pull(task_ids='clean') or []
        if not tables:
            raise AirflowSkipException('нет таблиц из clean')
        before = db_stats(tables)
        owners = db_owners(tables)
        results, skipped = [], []
        for tbl in tables:
            _ts = time.time()
            owner, can_set_role = owners.get(tbl, (None, False))
            try:
                db_vacuum(tbl, full=False, timeout=timeout,
                          set_role=owner if can_set_role else None)
            except AirflowSkipException as e:
                logger.warning(f"☮️ {tbl}: {e}")
                skipped.append({'table': tbl, 'duration': round(time.time() - _ts, 2),
                                'status': f'☮️ {str(e)[:60]}'})
                continue
            results.append({'table': tbl, 'duration': round(time.time() - _ts, 2), 'status': '✅'})

        if not results and not skipped:
            add_note('нет таблиц для вакуума', context=context, level='DAG,Task', title='🧹 vacuum')
            return

        # Даём коллектору статистики дописать результаты вакуума (PGSTAT_STAT_INTERVAL = 500 мс)
        if results:
            time.sleep(2)
        after = db_stats(tables) if results else {}
        for r in results:
            r['ok'] = True
        for r in results + skipped:
            dead_b = before.get(r['table'], (None, None))[0]
            dead_a, last_vac = after.get(r['table'], (None, None))
            r['dead'] = f"{dead_b} → {dead_a}" if r.get('ok') else str(dead_b)
            r['last_vacuum'] = _fmt_ts(last_vac)
            logger.info(f"🔎 {r['table']}: мёртвых {r['dead']} | last_vacuum={last_vac}")

        lines = [
            '| Таблица | Время, с | Мёртвых | last_vacuum | Статус |',
            '|---------|---------|---------|-------------|--------|',
        ] + [
            f"| `{r['table']}` | {r['duration']} | {r['dead']} | {r['last_vacuum']} | {r['status']} |"
            for r in results + skipped
        ]
        total = round(sum(r['duration'] for r in results + skipped), 2)
        lines.append(f"| **Итого** | **{total} с** | | | **{len(results)}/{len(tables)}** |")
        add_note('\n'.join(lines), context=context, level='Task', title='🧹 vacuum')
        add_note(f'{len(results)}/{len(tables)} таблиц за {total} с'
                 + (f' | ☮️ пропущено {len(skipped)}' if skipped else ''),
                 context=context, level='DAG', title='🧹 vacuum')

    @task(task_id='reindex', trigger_rule=TriggerRule.ALL_DONE)
    def reindex(**context):
        from airflow.exceptions import AirflowSkipException

        p = context['params']
        if not p.get('reindex', True):
            raise AirflowSkipException('reindex=False — пропущено')

        timeout = 15 * 60
        tables = context['ti'].xcom_pull(task_ids='clean') or []
        if not tables:
            raise AirflowSkipException('нет таблиц из clean')

        owners = db_owners(tables)
        conn_params = None  # штатный коннект Airflow; админский — только если не хватит прав
        results = []
        for tbl in tables:
            _ts = time.time()
            owner, can_set_role = owners.get(tbl, (None, False))
            # Членство в роли-владельце есть, но при NOINHERIT права не действуют
            # автоматически — берём роль явно.
            set_role = owner if can_set_role else None
            try:
                try:
                    db_reindex(tbl, conn_params, timeout=timeout, set_role=set_role)
                except Exception as e:
                    if conn_params or not any(s in str(e).lower() for s in ('must be owner', 'permission denied')):
                        raise
                    logger.warning(f"⚠️ {tbl}: {e} — пробуем админский коннект из Vault")
                    conn_params = get_af_conn()
                    db_reindex(tbl, conn_params, timeout=timeout)
            except Exception as e:
                # Прерванный REINDEX CONCURRENTLY оставляет невалидный индекс
                # (pg_index.indisvalid = false) — его нужно удалить вручную.
                logger.warning(f"⚠️ {tbl}: {e} — возможен невалидный индекс, проверьте pg_index")
                results.append({'table': tbl, 'duration': round(time.time() - _ts, 2),
                                'status': f'❌ {str(e)[:60]}'})
                continue
            results.append({'table': tbl, 'duration': round(time.time() - _ts, 2), 'status': '✅'})

        lines = [
            '| Таблица | Время, с | Статус |',
            '|---------|---------|--------|',
        ] + [f"| `{r['table']}` | {r['duration']} | {r['status']} |" for r in results]
        total = round(sum(r['duration'] for r in results), 2)
        ok = sum(1 for r in results if r['status'] == '✅')
        lines.append(f"| **Итого** | **{total} с** | **{ok}/{len(results)}** |")
        add_note('\n'.join(lines), context=context, level='Task', title='🔁 reindex')
        add_note(f'{ok}/{len(results)} таблиц за {total} с', context=context, level='DAG', title='🔁 reindex')

    @task(task_id='report', trigger_rule=TriggerRule.ALL_DONE)
    def report(**context):
        from airflow.models import DagRun, XCom
        from airflow.utils.session import create_session

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
            delta_s  = (('-' if delta_b < 0 else '+') + readable_size(abs(delta_b))) if delta_b else ''
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
            delta_str    = (('-' if total_delta < 0 else '+') + readable_size(abs(total_delta))) if total_delta else '-'
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

    clean() >> vacuum() >> reindex() >> report()

tools_db_cleanup()
