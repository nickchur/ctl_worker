"""⚙️ Конфигурация, утилиты и хранилище тракта Kafka ↔ ТФС.
*2026-08-12 14:31 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Живёт в `plugins`, а не рядом с дагами, по той же причине, что `ctl_utils` и `ctl_core`:
модулем пользуются ДВА каталога — `tfs_kafka` (приём и отправка) и `er_export`
(постановка в очередь, ожидание квитанций). Держать конфиг у одного из них значило бы
либо связать каталоги между собой, либо развести настройку по копиям.

🔑 Копии здесь недопустимы принципиально. Если приёмник запишет квитанцию в ClickHouse,
а `wait_confirm` пойдёт искать её в S3 — пакет зависнет до таймаута, и причина не будет
видна ниоткуда. Поэтому `STORAGE` ровно один, и он тут.

Хранилище сменное: `ch` (ClickHouse), `s3` (там же, где логи) или `pg` (Greenplum либо
PostgreSQL — провод у них общий). Все три реализации дают одинаковые сигнатуры.
"""
from __future__ import annotations

import logging
from datetime import timedelta

# Общие хелперы Airflow берём из plugins.utils, а не держим свои копии: заметки и
# колбэки должны вести себя одинаково во всех DAG-ах контура. add_note и ensure_pool
# здесь же реэкспортируются — их импортируют соседние модули этого каталога.
try:
    from plugins.utils import add_note, ensure_pool, get_dict_from_ch, on_callback, query_to_dict  # noqa: F401  # type: ignore
    from plugins.s3_utils import s3_move_s3, s3_path_parse  # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, ensure_pool, get_dict_from_ch, on_callback, query_to_dict  # noqa: F401  # type: ignore
    from CI06932748.tools.s3_utils import s3_move_s3, s3_path_parse  # type: ignore

CH_ID = 'dlab-click'

# IN/OUT в conn_id и топиках — сторона ТФС: пишем мы в его вход, читаем из его выхода.
KAFKA_SND_CONN  = 'tfs-kafka-in'
KAFKA_SND_TOPIC = 'TFS.HRPLT.IN'

# Топики квитанций общие на ВСЕ маршруты ТФС, поэтому читает их ровно один потребитель —
# даг tfs_kafka_rcv. Выгрузки сюда не ходят: они ждут строку в RECEIPTS_TABLE.
# Список, а не строка: одним коннектом слушаем несколько топиков сразу, и добавление
# нового маршрута сводится к строчке здесь.
KAFKA_RCV_CONN   = 'tfs-kafka-out'
KAFKA_RCV_TOPICS = ['TFS.HRPLT.OUT']

# 🗄️ Где держим квитанции и очередь отправки.
#
#   'ch' — ClickHouse (DDL: tfs_kafka/tfs_receipts.sql, er_export/er_sent_files.sql)
#   's3' — объекты там же, где логи: путь берётся из airflow.cfg, см. s3_base()
#   'pg' — Greenplum или PostgreSQL, один код на оба (DDL: *_pg.sql)
#
# ⚠️ Значение ОДНО на весь тракт. Разъехавшиеся значения у писателя и читателя дают
# зависший пакет без внятной причины, поэтому копировать эту константу никуда нельзя.
STORAGE = 'ch'

# 📇 Таблицы тракта — для STORAGE в ('ch', 'pg').
RECEIPTS_TABLE   = 'export.tfs_receipts'    # квитанции из Kafka, общие для всех маршрутов
SENT_FILES_TABLE = 'export.er_sent_files'   # очередь и реестр отправок ER

# Соединение для STORAGE='pg'. Нужно ЗАПИСЫВАЮЩЕЕ: alpha-adb_dev_comm-read по имени
# только на чтение. Пустое значение при STORAGE='pg' — ошибка на старте, а не тихий сбой.
PG_CONN = ''

# Префикс тракта внутри логового бакета (STORAGE='s3').
S3_PREFIX = 'tfs'

# 🚦 Лимиты ТФС на маршрут: файлов в секунду / минуту / час / сутки.
# ТФС отбивает лишние файлы, соблюдать темп должны мы сами. Значения задекларированы
# в документации маршрутов; счётчики на их стороне, предположительно, считают сообщения
# в Kafka — гипотеза не доказана, поэтому числа держим здесь и правим по факту.
# Все окна скользящие: полночь суточный бюджет не обнуляет.
TFS_LIMITS_DEFAULT = {'sec': 10, 'min': 200, 'hour': 500, 'day': 2000}
TFS_LIMITS: dict[str, dict[str, int]] = {
    'HRPLATFORM-2100': {'sec': 1, 'min': 15, 'hour': 100, 'day': 500},
}

# Очередь старше этого возраста (мин) роняет даг-отправитель: затор должен быть виден
# в мониторинге, а не только в логе.
TFS_QUEUE_ALERT_MIN = 60

# 🔒 Пул на 1 слот: в ТФС пишет кто-то один. Его берёт tfs_kafka_snd и обязан брать любой
# даг, который шлёт в ТФС МИМО очереди.
#
# Что даёт и чего не даёт: взаимное исключение — да, соблюдение лимитов — нет. Отправитель
# мимо очереди не пишет в SENT_FILES_TABLE, поэтому его файлы не попадут в счётчики.
#
# Пул один общий, а не tfs_{scenario}: пул назначается таску при разборе файла, а
# tfs_kafka_snd — один таск на все сценарии и заранее не знает, чьи файлы попадутся.
TFS_SEND_POOL  = 'tfs_send'
TFS_SEND_SLOTS = 1

# Пул приёмника — отдельный, чтобы чтение квитанций не ждало отправку.
TFS_RCV_POOL   = 'default_pool'

logger = logging.getLogger("airflow.task")

def tfs_limits(scenario_id: str) -> dict[str, int]:
    """🚦 Лимиты маршрута: свои из TFS_LIMITS либо общие TFS_LIMITS_DEFAULT."""
    return TFS_LIMITS.get(scenario_id, TFS_LIMITS_DEFAULT)


def send_budget(counts: dict[str, int], limits: dict[str, int]) -> tuple[int, str]:
    """🧮 Сколько файлов можно отправить прямо сейчас и какой лимит упёрся первым.

    counts — уже отправлено за окно: {'sec', 'min', 'hour', 'day'}
    limits — потолки по тем же окнам

    Возвращает (сколько можно, имя упёршегося лимита или '').
    Минимум по всем окнам: свободен тот бюджет, что кончается раньше всех.
    """
    free = {w: limits[w] - counts.get(w, 0) for w in ('sec', 'min', 'hour', 'day')}
    window = min(free, key=lambda w: free[w])
    allowed = max(free[window], 0)
    return allowed, (window if allowed == 0 else '')


def parse_receipt(raw: str, partition: int = -1, offset: int = -1) -> dict:
    """📨 Разбирает XML обратной квитанции TransferFileCephRs.

    Битый XML не роняет разбор: возвращается строка со status_code = -1 и текстом
    в raw_xml. Потерять квитанцию хуже, чем сохранить её неразобранной, а застрявшее
    сообщение заблокировало бы очередь.

    findtext с '{*}' и без: у ТФС встречаются оба варианта — с неймспейсом и без.
    """
    import xml.etree.ElementTree as ET
    from datetime import datetime

    row = {
        'rq_uid': '', 'file_name': '', 'scenario_id': '',
        'status_code': -1, 'rq_tm': None, 'raw_xml': raw,
        'kafka_partition': partition, 'kafka_offset': offset,
    }

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as err:
        logger.error("❌ Квитанция не разобрана как XML (%s): %.500s", err, raw)
        return row

    def _text(tag: str) -> str:
        return (root.findtext(f'.//{{*}}{tag}') or root.findtext(f'.//{tag}') or '').strip()

    row['rq_uid']      = _text('RqUID')
    row['file_name']   = _text('Name')
    row['scenario_id'] = _text('ScenarioId')

    code = _text('StatusCode')
    try:
        row['status_code'] = int(code)
    except ValueError:
        logger.error("❌ StatusCode '%s' не число, RqUID=%s", code, row['rq_uid'])
        return row

    rq_tm = _text('RqTm')
    if rq_tm:
        try:
            row['rq_tm'] = datetime.fromisoformat(rq_tm)
        except ValueError:
            logger.warning("⚠️ RqTm '%s' не разобран, RqUID=%s", rq_tm, row['rq_uid'])

    return row

DEF_ARGS = {
    "owner":            "DataLab (CI02420667)",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
    "on_failure_callback": on_callback,
}


def ensure_pools() -> None:
    """🏊 Заводит пулы тракта, если их ещё нет.

    Вызывать из таска, а не при разборе файла: ensure_pool кэширует результат на процесс,
    но лишний SELECT на каждом обходе scheduler-ом всё равно ни к чему.

    Делает это приёмник — он ходит раз в минуту и сам сидит в default_pool, поэтому
    создаст tfs_send до того, как отправителю понадобится слот: таск с несуществующим
    пулом Airflow просто не поставит в очередь.
    """
    ensure_pool(
        TFS_SEND_POOL, slots=TFS_SEND_SLOTS,
        description=('Отправка в ТФС: не больше одного отправителя одновременно. Берёт '
                     'tfs_kafka_snd и обязан брать любой даг, шлющий в ТФС мимо очереди. '
                     'Лимиты маршрута пул НЕ соблюдает — только взаимное исключение'),
    )


# ══════════════════════════════════════════════════════════════════════════════
# 🗄️ Хранилище тракта
#
# Три реализации за одним интерфейсом. Публичные функции внизу выбирают бэкенд
# по STORAGE, вызывающему знать о нём не нужно.
#
#   save_receipts(rows)              — приёмник сложил квитанции
#   find_receipts(rq_uids)           — wait_confirm ищет свои
#   enqueue(rows)                    — файлы встали в очередь на отправку
#   pending()                        — что ещё не отправлено
#   mark_sent(rq_uid)                — отметить уход в Kafka
#   sent_counts(scenario_id)         — расход лимитов по окнам
#   queue_state(rq_uids)             — отправлено ли (для диагностики таймаута)
#
# Общая модель у всех трёх: пишем только вставками, свежая версия побеждает при чтении.
# В ClickHouse это ReplacingMergeTree + FINAL, в Postgres — DISTINCT ON по времени,
# в S3 — перенос объекта между префиксами. Так поведение одинаково везде: приём из Kafka
# идёт at-least-once, и повтор не должен ни ломать данные, ни требовать UPDATE.
# ══════════════════════════════════════════════════════════════════════════════

WINDOWS = {'sec': 1, 'min': 60, 'hour': 3600, 'day': 86400}   # секунды в окне лимита


def window_hits(sent_at, now) -> list[str]:
    """Окна лимитов, в которые попадает отправка. Все окна скользящие.

    Вынесено отдельной функцией не ради красоты: внутри счётчика время берётся из now(),
    и проверить границу окна детерминированно было бы нельзя.
    """
    from datetime import timedelta

    delta = now - sent_at
    return [w for w, sec in WINDOWS.items() if delta <= timedelta(seconds=sec)]


def _sql_str(v) -> str:
    """Экранирует одинарные кавычки для подстановки в строковый литерал SQL."""
    return str(v).replace("'", "''")


def _ts(dt) -> str:
    """datetime → литерал 'YYYY-MM-DD HH:MM:SS.mmm' для обеих СУБД."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


# ── ClickHouse ────────────────────────────────────────────────────────────────

def _ch_hook():
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    return ClickHouseHook(clickhouse_conn_id=CH_ID)


def _ch_save_receipts(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['scenario_id'])}', "
        f"{int(r['status_code'])}, "
        f"{'toDateTime64(' + chr(39) + _ts(r['rq_tm']) + chr(39) + ', 3)' if r.get('rq_tm') else 'NULL'}, "
        f"'{_sql_str(r['raw_xml'])}', '{_sql_str(r.get('kafka_topic', ''))}', "
        f"{int(r.get('kafka_partition', -1))}, {int(r.get('kafka_offset', -1))})"
        for r in rows
    )
    _ch_hook().execute(
        f"INSERT INTO {RECEIPTS_TABLE} (rq_uid, file_name, scenario_id, status_code, rq_tm, "
        f"raw_xml, kafka_topic, kafka_partition, kafka_offset) VALUES {values}"
    )


def _ch_find_receipts(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT rq_uid, file_name, status_code, toString(rq_tm) AS rq_tm
        FROM {RECEIPTS_TABLE} FINAL WHERE rq_uid IN ({uids})
    """)


def _ch_enqueue(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['replica'])}', "
        f"'{_sql_str(r['scenario_id'])}', toDateTime64('{r['package_ts']}', 3), "
        f"'{_sql_str(r.get('dag_id', ''))}', '{_sql_str(r.get('run_id', ''))}')"
        for r in rows
    )
    _ch_hook().execute(
        f"INSERT INTO {SENT_FILES_TABLE} (rq_uid, file_name, replica, scenario_id, "
        f"package_ts, dag_id, run_id) VALUES {values}"
    )


def _ch_pending() -> list[dict]:
    # FINAL обязателен: отправленная строка дописывается второй версией, без схлопывания
    # уже ушедший файл уехал бы повторно.
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT rq_uid, file_name, replica, scenario_id, package_ts, created_at
        FROM {SENT_FILES_TABLE} FINAL
        WHERE notified_at = toDateTime64(0, 3)
        ORDER BY package_ts, created_at
    """)


def _ch_mark_sent(rq_uid: str) -> None:
    _ch_hook().execute(f"""
        INSERT INTO {SENT_FILES_TABLE}
            (rq_uid, file_name, replica, scenario_id, package_ts, created_at, notified_at)
        SELECT rq_uid, file_name, replica, scenario_id, package_ts, created_at, now64(3)
        FROM {SENT_FILES_TABLE} FINAL WHERE rq_uid = '{_sql_str(rq_uid)}'
    """)


def _ch_sent_counts(scenario_id: str) -> dict:
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT
            countIf(notified_at > now64(3) - INTERVAL 1 SECOND) AS sec,
            countIf(notified_at > now64(3) - INTERVAL 1 MINUTE) AS min,
            countIf(notified_at > now64(3) - INTERVAL 1 HOUR)   AS hour,
            countIf(notified_at > now64(3) - INTERVAL 1 DAY)    AS day
        FROM {SENT_FILES_TABLE} FINAL
        WHERE scenario_id = '{_sql_str(scenario_id)}' AND notified_at > toDateTime64(0, 3)
    """)[0]


def _ch_queue_state(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT file_name, notified_at = toDateTime64(0, 3) AS pending
        FROM {SENT_FILES_TABLE} FINAL WHERE rq_uid IN ({uids})
    """)


# ── PostgreSQL / Greenplum ────────────────────────────────────────────────────
#
# Один код на обе СУБД: провод общий, различается только DDL (DISTRIBUTED BY у GP).
#
# ON CONFLICT не используем — в Greenplum 6 (ядро PG 9.4) его нет. Вместо него дубли
# допускаются при записи и снимаются при чтении через DISTINCT ON: тот же принцип, что
# FINAL в ClickHouse. Очередь по той же причине append-only — UPDATE в GP медленный
# и раздувает таблицу.

def _pg_hook():
    # Проверка ДО импорта провайдера: иначе ошибка настройки утонет в ModuleNotFoundError,
    # и разбираться будут не с тем.
    if not PG_CONN:
        raise ValueError(
            "STORAGE='pg', но PG_CONN пуст. Укажите ЗАПИСЫВАЮЩЕЕ соединение "
            "в plugins/tfs_utils.py — на чтение (…-read) не подойдёт"
        )

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=PG_CONN)


def _pg_exec(sql: str) -> None:
    hook = _pg_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def _pg_save_receipts(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['scenario_id'])}', "
        f"{int(r['status_code'])}, "
        f"{chr(39) + _ts(r['rq_tm']) + chr(39) if r.get('rq_tm') else 'NULL'}, "
        f"'{_sql_str(r['raw_xml'])}', '{_sql_str(r.get('kafka_topic', ''))}', "
        f"{int(r.get('kafka_partition', -1))}, {int(r.get('kafka_offset', -1))})"
        for r in rows
    )
    _pg_exec(
        f"INSERT INTO {RECEIPTS_TABLE} (rq_uid, file_name, scenario_id, status_code, rq_tm, "
        f"raw_xml, kafka_topic, kafka_partition, kafka_offset) VALUES {values}"
    )


def _pg_find_receipts(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return query_to_dict(_pg_hook(), f"""
        SELECT DISTINCT ON (rq_uid) rq_uid, file_name, status_code, rq_tm::text AS rq_tm
        FROM {RECEIPTS_TABLE} WHERE rq_uid IN ({uids})
        ORDER BY rq_uid, received_at DESC
    """)


def _pg_enqueue(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['replica'])}', "
        f"'{_sql_str(r['scenario_id'])}', '{r['package_ts']}', "
        f"'{_sql_str(r.get('dag_id', ''))}', '{_sql_str(r.get('run_id', ''))}')"
        for r in rows
    )
    _pg_exec(
        f"INSERT INTO {SENT_FILES_TABLE} (rq_uid, file_name, replica, scenario_id, "
        f"package_ts, dag_id, run_id) VALUES {values}"
    )


def _pg_pending() -> list[dict]:
    return query_to_dict(_pg_hook(), f"""
        SELECT * FROM (
            SELECT DISTINCT ON (rq_uid)
                   rq_uid, file_name, replica, scenario_id, package_ts, created_at, notified_at
            FROM {SENT_FILES_TABLE} ORDER BY rq_uid, updated_at DESC
        ) q
        WHERE notified_at IS NULL
        ORDER BY package_ts, created_at
    """)


def _pg_mark_sent(rq_uid: str) -> None:
    _pg_exec(f"""
        INSERT INTO {SENT_FILES_TABLE}
            (rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id,
             created_at, notified_at, updated_at)
        SELECT DISTINCT ON (rq_uid)
               rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id,
               created_at, now(), now()
        FROM {SENT_FILES_TABLE} WHERE rq_uid = '{_sql_str(rq_uid)}'
        ORDER BY rq_uid, updated_at DESC
    """)


def _pg_sent_counts(scenario_id: str) -> dict:
    return query_to_dict(_pg_hook(), f"""
        SELECT
            count(*) FILTER (WHERE notified_at > now() - interval '1 second') AS sec,
            count(*) FILTER (WHERE notified_at > now() - interval '1 minute') AS min,
            count(*) FILTER (WHERE notified_at > now() - interval '1 hour')   AS hour,
            count(*) FILTER (WHERE notified_at > now() - interval '1 day')    AS day
        FROM (
            SELECT DISTINCT ON (rq_uid) scenario_id, notified_at
            FROM {SENT_FILES_TABLE} ORDER BY rq_uid, updated_at DESC
        ) q
        WHERE scenario_id = '{_sql_str(scenario_id)}' AND notified_at IS NOT NULL
    """)[0]


def _pg_queue_state(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return query_to_dict(_pg_hook(), f"""
        SELECT file_name, notified_at IS NULL AS pending
        FROM (
            SELECT DISTINCT ON (rq_uid) rq_uid, file_name, notified_at
            FROM {SENT_FILES_TABLE} WHERE rq_uid IN ({uids})
            ORDER BY rq_uid, updated_at DESC
        ) q
    """)


# ── S3 ────────────────────────────────────────────────────────────────────────
#
# Раскладка под {логовый бакет}/{S3_PREFIX}/:
#
#   receipts/{rq_uid}.json                              — квитанция целиком
#   queue/pending/{package_ts}__{rq_uid}.json           — ждёт отправки
#   queue/sent/{scenario}/{YYYYMMDDHHMMSS}__{rq_uid}.json — отправлено
#
# Плоский receipts/{rq_uid}.json — ради wait_confirm: он знает только RqUID, и поиск
# сводится к одному GET, без обхода префикса.
#
# ⚠️ Счётчики лимитов тут заметно дороже, чем в СУБД: вместо одного countIf идёт обход
# ключей. Поэтому время отправки лежит В ИМЕНИ ключа — окно считается по именам, без
# чтения объектов, а разбиение по сценариям не даёт листать чужие маршруты.

def s3_base() -> str:
    """Корень тракта в S3 — рядом с логами, из airflow.cfg.

    remote_base_log_folder приходит как 's3://bucket/prefix', где 's3' — протокол,
    а не conn_id. В репозитории принят вид 'conn_id://bucket/key' (его разбирает
    s3_path_parse), поэтому схему подменяем на remote_log_conn_id.
    """
    from airflow.configuration import conf

    base = conf.get('logging', 'remote_base_log_folder').rstrip('/')
    conn = conf.get('logging', 'remote_log_conn_id')
    return f"{conn}://{base.split('://', 1)[1]}/{S3_PREFIX}"


def _s3_hook_key(path: str):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    parsed = s3_path_parse(path)
    return S3Hook(aws_conn_id=parsed['conn_id']), parsed['bucket'], parsed['key']


def _s3_save_receipts(rows: list[dict]) -> None:
    import json as _json

    for r in rows:
        row = {**r, 'rq_tm': _ts(r['rq_tm']) if r.get('rq_tm') else None}
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/receipts/{r['rq_uid']}.json")
        hook.load_string(string_data=_json.dumps(row, ensure_ascii=False),
                         key=key, bucket_name=bucket, replace=True)


def _s3_find_receipts(rq_uids: list[str]) -> list[dict]:
    import json as _json

    found = []
    for uid in rq_uids:
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/receipts/{uid}.json")
        if hook.check_for_key(key=key, bucket_name=bucket):
            found.append(_json.loads(hook.read_key(key=key, bucket_name=bucket)))
    return found


def _s3_enqueue(rows: list[dict]) -> None:
    import json as _json

    for r in rows:
        name = f"{str(r['package_ts']).replace(' ', 'T')}__{r['rq_uid']}.json"
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/queue/pending/{name}")
        hook.load_string(string_data=_json.dumps(r, ensure_ascii=False, default=str),
                         key=key, bucket_name=bucket, replace=True)


def _s3_pending() -> list[dict]:
    import json as _json

    hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/pending/")
    rows = []
    for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
        if key.endswith('.json'):
            rows.append(_json.loads(hook.read_key(key=key, bucket_name=bucket)))
    return sorted(rows, key=lambda r: (str(r['package_ts']), str(r.get('created_at', ''))))


def _s3_mark_sent(rq_uid: str) -> None:
    from datetime import datetime, timezone

    hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/pending/")
    src = next((k for k in hook.list_keys(bucket_name=bucket, prefix=prefix) or []
                if k.endswith(f"__{rq_uid}.json")), None)
    if not src:
        logger.warning("⚠️ %s: в очереди не найден, отметка отправки пропущена", rq_uid)
        return

    import json as _json
    row = _json.loads(hook.read_key(key=src, bucket_name=bucket))
    # Время отправки — в ИМЕНИ ключа: по нему считаются окна лимитов, без чтения объектов
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    dst = f"{s3_base()}/queue/sent/{row['scenario_id']}/{stamp}__{rq_uid}.json"
    s3_move_s3(f"{s3_base()}/queue/pending/{src.split('/')[-1]}", dst)


def _s3_sent_counts(scenario_id: str) -> dict:
    from datetime import datetime, timezone

    hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/sent/{scenario_id}/")
    now = datetime.now(timezone.utc)
    counts = dict.fromkeys(WINDOWS, 0)

    for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
        stamp = key.split('/')[-1].split('__')[0]
        try:
            sent_at = datetime.strptime(stamp, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
        except ValueError:
            logger.warning("⚠️ Ключ без разбираемой метки времени, пропущен: %s", key)
            continue
        for window in window_hits(sent_at, now):
            counts[window] += 1
    return counts


def _s3_queue_state(rq_uids: list[str]) -> list[dict]:
    hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/")
    keys = hook.list_keys(bucket_name=bucket, prefix=prefix) or []
    out = []
    for uid in rq_uids:
        match = next((k for k in keys if k.endswith(f"__{uid}.json")), None)
        if match:
            out.append({'file_name': match.split('/')[-1], 'pending': '/pending/' in match})
    return out


# ── Выбор бэкенда ─────────────────────────────────────────────────────────────

_BACKENDS = {
    'ch': (_ch_save_receipts, _ch_find_receipts, _ch_enqueue, _ch_pending,
           _ch_mark_sent, _ch_sent_counts, _ch_queue_state),
    'pg': (_pg_save_receipts, _pg_find_receipts, _pg_enqueue, _pg_pending,
           _pg_mark_sent, _pg_sent_counts, _pg_queue_state),
    's3': (_s3_save_receipts, _s3_find_receipts, _s3_enqueue, _s3_pending,
           _s3_mark_sent, _s3_sent_counts, _s3_queue_state),
}


def _backend(idx: int):
    """Функция выбранного бэкенда по позиции в _BACKENDS."""
    if STORAGE not in _BACKENDS:
        raise ValueError(f"Неизвестное STORAGE='{STORAGE}', допустимы {sorted(_BACKENDS)}")
    return _BACKENDS[STORAGE][idx]


def save_receipts(rows: list[dict]) -> None:
    """Сохраняет разобранные квитанции (приёмник)."""
    return _backend(0)(rows)


def find_receipts(rq_uids: list[str]) -> list[dict]:
    """Квитанции по списку RqUID: rq_uid, file_name, status_code, rq_tm."""
    return _backend(1)(rq_uids)


def enqueue(rows: list[dict]) -> None:
    """Ставит файлы в очередь отправки (rq_uid, file_name, replica, scenario_id, package_ts…)."""
    return _backend(2)(rows)


def pending() -> list[dict]:
    """Файлы, ещё не ушедшие в Kafka, в порядке package_ts, created_at."""
    return _backend(3)()


def mark_sent(rq_uid: str) -> None:
    """Отмечает файл отправленным."""
    return _backend(4)(rq_uid)


def sent_counts(scenario_id: str) -> dict:
    """Расход лимитов маршрута: {'sec', 'min', 'hour', 'day'}. Окна скользящие."""
    return _backend(5)(scenario_id)


def queue_state(rq_uids: list[str]) -> list[dict]:
    """file_name + pending: отличить «ещё не отправлено» от «нет квитанции»."""
    return _backend(6)(rq_uids)


def get_config() -> dict:
    """📦 Снимок констант модуля для передачи в DAG-файлы."""
    return {
        'CH_ID':            CH_ID,
        'DEF_ARGS':         DEF_ARGS,
        'KAFKA_SND_CONN':   KAFKA_SND_CONN,
        'KAFKA_SND_TOPIC':  KAFKA_SND_TOPIC,
        'KAFKA_RCV_CONN':   KAFKA_RCV_CONN,
        'KAFKA_RCV_TOPICS': KAFKA_RCV_TOPICS,
        'RECEIPTS_TABLE':   RECEIPTS_TABLE,
        'SENT_FILES_TABLE': SENT_FILES_TABLE,
        'TFS_LIMITS':       TFS_LIMITS,
        'TFS_LIMITS_DEFAULT': TFS_LIMITS_DEFAULT,
        'TFS_QUEUE_ALERT_MIN': TFS_QUEUE_ALERT_MIN,
        'TFS_SEND_POOL':    TFS_SEND_POOL,
        'TFS_SEND_SLOTS':   TFS_SEND_SLOTS,
        'TFS_RCV_POOL':     TFS_RCV_POOL,
    }
