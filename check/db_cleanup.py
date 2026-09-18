"""### 🧹 Очистка метадаты Airflow
*2026-09-18 13:28 MSK · v1.10 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Удаляет устаревшие записи из метабазы Airflow прямыми SQL-запросами (без CTAS-архивирования).
Для таблиц, связанных с `dag_run`, используются существующие индексы через косвенные условия.
Большие таблицы (> 50 000 строк) удаляются порциями по диапазону дат.
Порядок таблиц строится по внешним ключам — ребёнок раньше родителя, иначе каскад
(`ON DELETE CASCADE`) утягивает детей в транзакцию родителя и батч перестаёт работать.

| Параметр            | Описание                                                                                   |
|---------------------|--------------------------------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес, минимум 30)*                    |
| 🔍 `dry_run`        | `True` — только подсчёт без удаления, `False` — реальное удаление *(default)*             |
| 🧹 `vacuum`         | `True` — VACUUM ANALYZE после очистки *(default)*, `False` — пропустить                   |
| ➕ `custom`     | `True` — включить `dag_code` и `dag_pickle`, `False` — только стандартные *(default)*     |
| ⏰ `schedule`      | Расписание DAG-а: cron или пресет `@daily`, пусто — только вручную *(default: `0 2 * * *`)* |
| 💾 `save_params`    | `True` — сохранить параметры этого запуска как значения по умолчанию, `False` *(default)* |

🔁 **`reindex` скрыт.** `REINDEX TABLE CONCURRENTLY` по очищенным таблицам — операция
тяжёлая и требует админского коннекта, поэтому по умолчанию её нет ни в форме, ни в графе.
Чтобы вернуть — добавить ключ `reindex` в переменную `tools_db_cleanup_params`
(значение `true` или `false`); тогда появятся и параметр, и таск.

Значения по умолчанию берутся из переменной `tools_db_cleanup_params`, если она задана,
иначе из кода. Записывается переменная только запуском с `save_params=True` — то есть
разовый эксперимент в UI расписание не меняет, а осознанная правка меняет, без выкладки.

`schedule` — такой же сохраняемый параметр: сам запуск идёт по старому расписанию,
новое подхватывается со следующего парсинга DAG-а. Негодное значение таск `params` не
записывает (падает), а уже записанное битым — игнорируется на парсинге в пользу кода.

**Таски:**
- **params** — сохранение параметров запуска в переменную (пропускается при `save_params=False`)
- **clean** — подсчёт и удаление по каждой таблице; заметка обновляется после каждой таблицы
- **vacuum** — VACUUM ANALYZE по очищенным таблицам
- **reindex** — REINDEX TABLE CONCURRENTLY по таблицам из `clean` (админский коннект из
  Vault). Таска нет, пока в переменной нет ключа `reindex`
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

try:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, get_af_conn, on_callback, readable_size,
        saved_params, store_params, valid_schedule,
    )
except ImportError:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, get_af_conn, on_callback, readable_size,
        saved_params, store_params, valid_schedule,
    )

logger = logging.getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)


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
    # celery_taskmeta здесь нет намеренно: до v1.9 строка удалялась только при живом
    # task_instance с тем же external_executor_id. Колонка хранит id ТОЛЬКО последней
    # попытки, поэтому результаты всех ретраев, перезапусков и удалённых вручную ранов
    # осиротевали навсегда, а сам EXISTS шёл полным сканом task_instance — индекса по
    # external_executor_id нет ни у Airflow, ни в af_config.sql. Условие возраста ему
    # задаётся отдельно, см. _BASE_WHERE.
}

# Замена стандартного условия возраста `<колонка> < :cutoff` там, где его недостаточно.
# Подставляется вместо него, а не рядом: нужен OR, а _EXTRA_COND умеет только AND.
_BASE_WHERE = {
    # У celery_taskmeta date_done появляется ТОЛЬКО при переходе задачи в терминальное
    # состояние. У строки, застрявшей в STARTED (воркер умер, не закрыв задачу), его нет
    # никогда — а это ровно те строки, ради которых чистка и затевалась. Замер 18.09.2026:
    # из 314 820 строк дата стоит у всех SUCCESS и FAILURE и отсутствует у обеих STARTED;
    # на альфе таких накопилось 91 при 94 в карточке Health. То есть v1.9 с её
    # `date_done < cutoff` не удаляла их НИ РАЗУ — мой же комментарий был неверен.
    #
    # Возраст такой строки берём по id: он serial и растёт вместе с date_done (проверено —
    # min=1, max=314823, count=314823, порядок совпадает). Граница — МИНИМУМ id среди строк,
    # которые ещё НЕ вышли за срок: всё, что вставлено раньше самой ранней «свежей» строки,
    # заведомо старое.
    #
    # Первым заходом здесь стоял максимум id среди уже старых — и он оставлял сироту, которая
    # сама оказалась верхней в старом диапазоне: став сиротой, она выпала из множества «старых
    # с датой», по которому и считается максимум, и граница ушла ниже неё. Замер на стенде
    # (симуляция полного цикла удаления, четыре подставные сироты на разной высоте): у
    # максимума оставалась верхняя, у минимума не осталось ни одной, живые STARTED не тронуты
    # в обоих вариантах.
    #
    # Своя слабость есть и здесь: задача, начатая 40 дней назад и закончившаяся 5 дней назад,
    # имеет маленький id и свежий date_done — она утянет минимум вниз, и часть сирот доживёт
    # до следующего запуска. Это недоудаление, а не переудаление: осадок уйдёт, когда более
    # новые строки перешагнут cutoff. Выбор между «иногда позже» и «никогда» очевиден.
    #
    # ⚠️ Всё это держится на том, что celery_taskmeta есть в _PK_BATCH. Там батч — это
    # `ORDER BY id LIMIT n`, и строка без даты из него не выпадает. Убрать таблицу оттуда —
    # и включится батч по диапазону дат (`date_done >= :b_s AND date_done < :b_e`), который
    # припишется через AND и отсечёт ВСЕ строки с NULL: сироты снова перестанут удаляться,
    # молча. Подзапрос границы при этом пересчитывается на каждом батче, и это безвредно:
    # удаляем снизу вверх по id, а граница держится за самую раннюю свежую строку, которую
    # мы не трогаем вовсе.
    #
    # Нет ни одной свежей строки — подзапрос даёт NULL, сравнение ложно, не удаляется ничего.
    # Это и защищает живую STARTED-строку работающей задачи: у неё date_done пуст, и без
    # границы по id её снесло бы вместе с осадком.
    #
    # Трёхзначная логика тут работает на нас, и её не надо «чинить» через COALESCE: у живой
    # строки `date_done < :cutoff` даёт NULL, второй дизъюнкт — FALSE (id больше границы),
    # и всё выражение остаётся NULL. WHERE берёт только TRUE, поэтому DELETE её не трогает.
    # Проверено на стенде откатом: старые сироты удаляются, обе живые STARTED остаются.
    'celery_taskmeta': (
        '({col} < :cutoff OR ({col} IS NULL AND {p}id < '
        '(SELECT MIN(id) FROM main.celery_taskmeta WHERE date_done >= :cutoff)))'
    ),
}

# Каскад делает батч бессмысленным: у task_instance и task_reschedule FK на dag_run
# стоит ON DELETE CASCADE (af_config.sql:806,808), у task_instance_history — на
# task_instance (879). Порядок таблиц строим по FK (дети раньше родителей), но часть
# детей в чистку не входит вовсе (task_map, rendered_task_instance_fields, заметки) —
# их каскад остаётся, поэтому у dag_run батч дополнительно уменьшаем.
_BATCH_DIV = {'dag_run': 10}

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


def _order_children_first(names, session):
    """Сортирует таблицы так, чтобы ребёнок чистился раньше родителя.

    Порядок Airflow (`config_dict`) алфавитный: dag_run пятым, task_instance
    тринадцатым, xcom последним. С ON DELETE CASCADE это значит, что батч в 50 000
    ранов тянет в одну транзакцию всех их детей — миллионы строк, то есть батчинг
    не работает ровно там, где он нужен. Связи берём из pg_constraint, а не списком:
    состав таблиц меняется с версией Airflow, а список молча устаревает.
    """
    edges = session.execute(text("""
        SELECT c.relname AS child, f.relname AS parent
        FROM pg_constraint con
        JOIN pg_class c ON c.oid = con.conrelid
        JOIN pg_class f ON f.oid = con.confrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE con.contype = 'f' AND n.nspname = 'main' AND c.relname <> f.relname
    """)).fetchall()

    children = {}
    for child, parent in edges:
        children.setdefault(parent, set()).add(child)

    ranks = {}

    def rank(tbl, seen=()):
        # Ранг = глубина дерева детей. Лист — 0, родитель всегда больше своих детей,
        # значит при сортировке по возрастанию дети идут первыми.
        if tbl in ranks:
            return ranks[tbl]
        if tbl in seen:          # цикл FK — не углубляемся, порядок тут не спасёт
            return 0
        kids = children.get(tbl, ())
        ranks[tbl] = 1 + max((rank(k, seen + (tbl,)) for k in kids), default=-1)
        return ranks[tbl]

    # sorted устойчив: внутри одного ранга остаётся алфавитный порядок Airflow
    return sorted(names, key=rank)


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


def db_vacuum(table, conn_id, full=False, timeout=3600):
    """VACUUM [FULL] ANALYZE по таблице схемы main под коннектом conn_id.

    VACUUM без FULL не уменьшает файл таблицы (страницы уходят в free space map),
    поэтому судить о его работе по размеру нельзя — ориентир n_dead_tup и last_vacuum.
    Без прав владельца VACUUM не падает, а молча пропускает таблицу с warning'ом —
    его и ловим, чтобы пропуск был виден.
    """
    from airflow.exceptions import AirflowSkipException
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    ts = time.time()
    mode = 'FULL ANALYZE' if full else 'ANALYZE'
    sql = f"VACUUM {mode} main.{table}"

    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{timeout}s'")
            logger.info(f"🔧 VACUUM: {sql} (conn={conn_id})")
            del conn.notices[:]
            cur.execute(sql)
            notices = list(conn.notices)
    finally:
        conn.close()

    for n in notices:
        logger.info(f"📣 {table}: {n.strip()}")
    skipped = next((n for n in notices if 'skipping' in n.lower()), None)
    if skipped:
        raise AirflowSkipException(skipped.strip())
    logger.info(f"✅ {sql} за {time.time() - ts:.2f}s")


def db_reindex(table, conn_id, timeout=900):
    """REINDEX TABLE CONCURRENTLY по одной таблице схемы main под коннектом conn_id.

    По таблицам, а не по схеме целиком (так было до v1.10). Список берётся из того, что
    только что чистил clean: раздувает индексы именно массовый DELETE, а REINDEX SCHEMA
    проходил ВСЕ индексы схемы, включая таблицы, которых чистка не касается, — одной
    командой на час без обратной связи. По таблицам видно время каждой, а пропуск по правам
    попадает в свою строку отчёта, а не теряется в общем потоке notices.

    Про обрыв важно не обольщаться: прерванный REINDEX CONCURRENTLY оставляет невалидный
    индекс (PostgreSQL 16 на стенде), и сам он не исчезает — его удаляют вручную. По
    таблицам проще только тем, что испорчена одна таблица, а не «что-то в схеме»; «следующий
    запуск догонит» — неверно, поэтому таск с таким отказом обязан краснеть.

    CONCURRENTLY не может выполняться внутри транзакционного блока — нужен autocommit.
    Таблицу, которой пользователь не владеет, PostgreSQL пропускает с warning'ом
    ("skipping ..."), не прерывая команду, — его и ловим, как в db_vacuum.
    """
    from airflow.exceptions import AirflowSkipException
    from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore

    ts = time.time()
    sql = f"REINDEX TABLE CONCURRENTLY main.{table}"

    conn = PostgresHook(postgres_conn_id=conn_id).get_conn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = '{timeout}s'")
            logger.info(f"🔧 REINDEX: {sql} (conn={conn_id})")
            del conn.notices[:]
            cur.execute(sql)
            notices = list(conn.notices)
    finally:
        conn.close()

    for n in notices:
        logger.info(f"📣 {table}: {n.strip()}")
    skipped = next((n for n in notices if 'skipping' in n.lower()), None)
    if skipped:
        raise AirflowSkipException(skipped.strip())
    logger.info(f"✅ {sql} за {time.time() - ts:.2f}s")


def _migrate_params(context=None) -> list:
    """Переводит сохранённую переменную на PARAMS_VERSION, выкидывая устаревшие ключи.

    Возвращает список выкинутого. Версия совпала или переменной нет — не трогаем ничего.
    Ошибку записи проглатываем: миграция — уборка, а не работа DAG-а, и ронять из-за неё
    очистку метабазы незачем; в следующий раз повторится.
    """
    from airflow.models import Variable

    if not SAVED or SAVED.get('params_version') == PARAMS_VERSION:
        return []

    drop = []
    for version, keys in sorted(_DROP_ON_MIGRATE.items()):
        if version <= SAVED.get('params_version', 0):
            continue
        drop += [k for k in keys if k in SAVED]

    fresh = {k: v for k, v in SAVED.items() if k not in drop}
    fresh['params_version'] = PARAMS_VERSION
    try:
        Variable.set(PARAMS_VAR, fresh, serialize_json=True,
                     description=f'миграция параметров до v{PARAMS_VERSION}')
    except Exception as e:
        logger.warning(f"⚠️ {PARAMS_VAR}: миграция до v{PARAMS_VERSION} не записана: {e}")
        return []

    logger.info(f"🔀 {PARAMS_VAR}: версия {SAVED.get('params_version', '—')} → {PARAMS_VERSION}"
                + (f", выкинуто: {', '.join(drop)}" if drop else ''))
    if context is not None and drop:
        add_note(f"переменная переведена на v{PARAMS_VERSION}, выкинуто: "
                 + ', '.join(f'`{k}`' for k in drop),
                 context=context, level='Task', title='🔀 params')
    return drop


def _fmt_ts(ts):
    """'HH:MM:SS' для отметок вакуума; '—' если статистики по таблице нет."""
    return ts.strftime('%H:%M:%S') if ts else '—'


# Значения по умолчанию для формы запуска: код задаёт запасной вариант, переменная —
# рабочий. Пишет переменную только запуск с save_params=True, см. таск params.
PARAMS_VAR = 'tools_db_cleanup_params'
SAVED = saved_params(PARAMS_VAR)


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


DEFAULT_SCHEDULE = '0 2 * * *'


def _schedule():
    """Расписание DAG-а: из переменной, если оно осмысленное, иначе из кода."""
    value = SAVED.get('schedule', DEFAULT_SCHEDULE)
    if not valid_schedule(value):
        logger.warning(f"⚠️ {PARAMS_VAR}: расписание '{value}' не разобрано — беру {DEFAULT_SCHEDULE}")
        return DEFAULT_SCHEDULE
    return None if value in (None, '', 'None') else str(value).strip()


PARAMS_VERSION = 2

# Что выкинуть из сохранённой переменной при переходе на новую версию параметров.
# Ключ есть — таск reindex создаётся; у всех, кто когда-либо жал save_params, там лежит
# reindex: true, и без миграции фича осталась бы включённой ровно у тех, кто DAG-ом
# пользуется. Удалять переменную целиком нельзя: уедут retention_days, batch_size и
# schedule — расписание молча вернулось бы к коду.
_DROP_ON_MIGRATE = {2: ('reindex',)}

params = {
    'retention_days': _param(
        'retention_days', 180,
        type='integer',
        minimum=30,
        description='Хранить записи не старше N дней (минимум 30)',
    ),
    'dry_run': _param(
        'dry_run', False,
        type='boolean',
        description='True — только подсчёт, False — реальное удаление',
    ),
    'vacuum': _param(
        'vacuum', True,
        type='boolean',
        description='True — VACUUM ANALYZE, False — пропустить',
    ),
    'custom': _param(
        'custom', False,
        type='boolean',
        description='True — включить dag_code и dag_pickle, False — только стандартные таблицы',
    ),
    'batch_size': _param(
        'batch_size', BATCH_SIZE,
        type='integer',
        minimum=1000,
        description='Максимальный размер порции при удалении (строк)',
    ),
    'lock_timeout': _param(
        'lock_timeout', '10min',
        type='string',
        description='Таймаут ожидания блокировки (например: 10min, 30s)',
    ),
    # reindex скрыт: REINDEX TABLE CONCURRENTLY — операция тяжёлая и требует админского
    # коннекта, поэтому по умолчанию её нет ни в форме, ни в графе. Вернуть — добавить ключ
    # reindex в переменную tools_db_cleanup_params (значение true/false).
    #
    # ⚠️ saved_params при любой ошибке чтения переменной возвращает пусто (plugins/utils.py),
    # поэтому «переменная недоступна» и «reindex выключен» снаружи неотличимы. Направление
    # безопасное — отсутствие ключа значит «не делать», — но пропавший таск не дефект.
    **({'reindex': _param(
        'reindex', False,
        type='boolean',
        description='True — REINDEX TABLE CONCURRENTLY по очищенным таблицам, False — пропустить',
    )} if SAVED.get('reindex') is not None else {}),
    'schedule': _param(
        'schedule', DEFAULT_SCHEDULE,
        type='string',
        description='Расписание: cron или пресет @daily; пусто — только вручную. Применяется со следующего парсинга',
    ),
    # Разовое действие, а не настройка: в переменную не сохраняется и берётся всегда из кода
    'save_params': Param(
        False,
        type='boolean',
        description='True — сохранить параметры этого запуска как значения по умолчанию',
    ),
}


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2025, 8, 7, tzinfo=timezone.utc),
    tags=['DataLab', 'tools', 'clean'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    schedule=_schedule(),
    on_failure_callback=on_callback,
    params=params,
)
def tools_db_cleanup():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет параметры запуска в переменную как значения по умолчанию.

        Перед этим — разовая миграция переменной на текущую PARAMS_VERSION, и она идёт
        НЕЗАВИСИМО от галочки save_params: иначе ключ, из-за которого фича остаётся
        включённой, дожил бы до первого, кто вручную попросит сохранить параметры.

        На парсинге такое делать нельзя: переменную писал бы каждый процесс парсера и на
        каждом проходе. Здесь же таск существует всегда и просто пропускает сам себя.
        """
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        # Выкинутые миграцией ключи убираем и из формы этого запуска. Иначе store_params
        # тут же вернул бы их в переменную: он пишет context['params'] целиком, а форма
        # собрана на парсинге, когда ключ ещё был. Версия при этом уже стояла бы новая, и
        # миграция не повторилась бы никогда — фича осталась бы в графе навсегда.
        for key in _migrate_params(context):
            context['params'].pop(key, None)

        status, msg = store_params(PARAMS_VAR, SAVED, context,
                                   extra={'params_version': PARAMS_VERSION})
        if status == 'skip':
            raise AirflowSkipException(msg)
        if status == 'fail':
            raise AirflowFailException(msg)
        return msg

    # NONE_FAILED, а не дефолтный ALL_SUCCESS: params штатно пропускает себя при
    # save_params=False, а пропуск апстрима по ALL_SUCCESS утягивает в skip всю цепочку
    @task(task_id='clean', trigger_rule=TriggerRule.NONE_FAILED)
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
                    base_where = _BASE_WHERE.get(tbl, '{col} < :cutoff').format(col=col, p=p)
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
                tbl_batch = max(1000, batch_size // _BATCH_DIV.get(tbl, 1))
                n_batches = (count + tbl_batch - 1) // tbl_batch
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
                        _log_sql(pk_delete, {**bind, 'lim': tbl_batch}, f"🗑️ DELETE {tbl}")
                        res = session.execute(pk_delete, {**bind, 'lim': tbl_batch})
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
        try:
            with create_session() as session:
                table_names = _order_children_first(table_names, session)
            logger.info(f"📐 Порядок очистки (дети раньше родителей): {', '.join(table_names)}")
        except Exception as e:
            # Не смогли прочитать связи — чистим в порядке Airflow: медленнее, но не хуже прежнего
            logger.warning(f"⚠️ Порядок по FK не построен ({e}), идём алфавитным порядком Airflow")
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
                     title=f'🗑️ clean ({mode}, {retention_days}d)', add=False)
            add_note(
                f'{mode} {readable_size(total, base=1000)} строк | cutoff: {cutoff.strftime("%Y-%m-%d")}'
                f' ⏱ {duration}s',
                context=context, level='DAG', title='🗑️ clean',
            )
        else:
            add_note(
                f'{mode} | cutoff: {cutoff.strftime("%Y-%m-%d")} ⏱ {duration}s',
                context=context, level='DAG,Task', title='🗑️ clean',
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
        # Штатный пользователь Airflow не владеет таблицами main и VACUUM их молча
        # пропускает — идём админским коннектом из Vault.
        conn_id = get_af_conn()

        results, skipped = [], []
        for tbl in tables:
            _ts = time.time()
            try:
                db_vacuum(tbl, conn_id, full=False, timeout=timeout)
            except AirflowSkipException as e:
                logger.warning(f"☮️ {tbl}: {e}")
                skipped.append({'table': tbl, 'duration': round(time.time() - _ts, 2),
                                'status': f'☮️ {str(e)[:60]}'})
                continue
            except Exception as e:
                logger.warning(f"⚠️ {tbl}: {e}")
                skipped.append({'table': tbl, 'duration': round(time.time() - _ts, 2),
                                'status': f'❌ {str(e)[:60]}'})
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
        """REINDEX TABLE CONCURRENTLY по таблицам, которые только что чистил clean.

        Список берём из XCom clean, а не из переменной: индексы раздувает массовый DELETE,
        значит чистили — есть что переиндексировать, не чистили — нечего. Из переменной
        брать нельзя ещё и потому, что имя таблицы уходит в SQL подстановкой.
        """
        from airflow.exceptions import AirflowException, AirflowSkipException

        p = context['params']

        # Ключ читаем из переменной ЗАНОВО, а не из формы. Форма собрана на парсинге, и у
        # того, кто выкатил v1.10 с прежней переменной, в ней ещё лежит reindex: true —
        # граф первого запуска соберётся с этим таском, и он отработал бы тяжёлый REINDEX
        # ровно там, где релиз обещал его выключить. Миграция в таске params к этому
        # моменту уже прошла, поэтому свежее чтение переменной её и подхватывает.
        if saved_params(PARAMS_VAR).get('reindex') is None:
            raise AirflowSkipException(f'ключа reindex в {PARAMS_VAR} нет — переиндексация выключена')
        if not p.get('reindex', False):
            raise AirflowSkipException('reindex=False — пропущено')
        # dry_run обещает прикидку без изменений. Переиндексация — не удаление, но DDL на
        # четверть часа на таблицу «прикидкой» назвать нельзя.
        if p.get('dry_run', False):
            raise AirflowSkipException('dry_run=True — переиндексацию не делаем')

        timeout = 15 * 60  # на таблицу, как у вакуума: час был на всю схему одной командой
        tables = context['ti'].xcom_pull(task_ids='clean') or []
        if not tables:
            raise AirflowSkipException('нет таблиц из clean')

        # REINDEX требует прав владельца таблиц — идём админским коннектом из Vault.
        conn_id = get_af_conn()

        results, skipped, failed = [], [], []
        for tbl in tables:
            _ts = time.time()
            try:
                db_reindex(tbl, conn_id, timeout=timeout)
            except AirflowSkipException as e:
                # Нет прав на таблицу — это не авария, PostgreSQL её просто пропускает
                logger.warning(f"☮️ {tbl}: {e}")
                skipped.append({'table': tbl, 'duration': round(time.time() - _ts, 2),
                                'status': f'☮️ {str(e)[:60]}'})
                continue
            except Exception as e:
                # Прерванный REINDEX CONCURRENTLY оставляет невалидный индекс
                # (pg_index.indisvalid = false). Он не обслуживает выборки, но СУБД обязана
                # поддерживать его при каждой записи, и сам он не рассосётся — нужен DROP
                # руками. Поэтому цикл идёт дальше (остальные таблицы ни при чём), но таск
                # обязан закончиться красным: зелёный таск с битым индексом никто не заметит.
                logger.warning(f"⚠️ {tbl}: {e} — возможен невалидный индекс, проверьте pg_index")
                failed.append({'table': tbl, 'duration': round(time.time() - _ts, 2),
                               'status': f'❌ {str(e)[:60]}'})
                continue
            results.append({'table': tbl, 'duration': round(time.time() - _ts, 2), 'status': '✅'})

        rows = results + skipped + failed
        lines = [
            '| Таблица | Время, с | Статус |',
            '|---------|---------|--------|',
        ] + [f"| `{r['table']}` | {r['duration']} | {r['status']} |" for r in rows]
        total = round(sum(r['duration'] for r in rows), 2)
        lines.append(f"| **Итого** | **{total} с** | **{len(results)}/{len(tables)}** |")
        if failed:
            lines += ['', '⚠️ После обрыва мог остаться невалидный индекс:',
                      '`select indexrelid::regclass from pg_index where not indisvalid;`',
                      'Такой индекс сам не исчезнет — его удаляют вручную.']
        add_note('\n'.join(lines), context=context, level='Task', title='🔁 reindex')
        add_note(f'{len(results)}/{len(tables)} таблиц за {total} с'
                 + (f' | ☮️ {len(skipped)}' if skipped else '')
                 + (f' | ❌ {len(failed)}' if failed else ''),
                 context=context, level='DAG', title='🔁 reindex')

        if failed:
            raise AirflowException(
                'переиндексация не прошла: ' + ', '.join(r['table'] for r in failed)
                + ' — проверьте pg_index на невалидные индексы'
            )

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

    # Звено reindex — только когда параметр есть: без него и таска в графе нет.
    # У vacuum и report trigger_rule=ALL_DONE, поэтому звено снимается без правок правил.
    _chain = save_params() >> clean() >> vacuum()
    if SAVED.get('reindex') is not None:
        _chain = _chain >> reindex()
    _chain >> report()

tools_db_cleanup()
