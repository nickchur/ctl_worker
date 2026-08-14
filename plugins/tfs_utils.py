"""⚙️ Конфигурация, утилиты и хранилище тракта Kafka ↔ ТФС.
*2026-08-14 11:25 MSK · v1.4 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Живёт в `plugins`, а не рядом с дагами, по той же причине, что `ctl_utils` и `ctl_core`:
модулем пользуются ДВА каталога — `tfs_kafka` (приём и отправка) и `er_export`
(постановка в очередь, ожидание квитанций). Держать конфиг у одного из них значило бы
либо связать каталоги между собой, либо развести настройку по копиям.

🔑 Копии здесь недопустимы принципиально. Если приёмник запишет квитанцию в ClickHouse,
а `wait_confirm` пойдёт искать её в S3 — пакет зависнет до таймаута, и причина не будет
видна ниоткуда. Поэтому источник истины ровно один, и он тут.

🗄️ **S3 — источник истины.** Пишем туда всегда и читаем только оттуда: писатель и
читатель обязаны смотреть в одно место. ClickHouse и Postgres — зеркала для аналитики
и глаз: пишем в них, если задан их conn_id, и их сбой тракт не роняет. Три реализации
дают одинаковые сигнатуры, различие только в том, кто из них обязателен.
"""
from __future__ import annotations

import logging
import re
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

CH_ID = 'dlab-click'   # зеркало тракта в ClickHouse; пусто — выключено

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
# S3 (объекты там же, где логи, см. s3_base) — обязателен: туда идёт каждая запись,
# оттуда идёт КАЖДОЕ чтение. Зеркала подключаются непустым conn_id и нужны только
# чтобы смотреть на тракт запросами:
#
#   CH_ID   — ClickHouse (DDL: tfs_kafka/tfs_receipts.sql, er_export/er_sent_files.sql)
#   PG_CONN — Greenplum или PostgreSQL, один код на оба (DDL: *_pg.sql)
#
# Сбой зеркала только предупреждает: запись в S3 к этому моменту уже прошла, и ронять
# приём квитанций из-за аналитической копии нельзя. Расхождение лечится дозаливкой.

# 📇 Таблицы тракта — для зеркал в ClickHouse и Postgres.
RECEIPTS_TABLE   = 'export.tfs_receipts'    # квитанции из Kafka, общие для всех маршрутов
SENT_FILES_TABLE = 'export.er_sent_files'   # очередь и реестр отправок ER

# Соединение зеркала в Postgres. Нужно ЗАПИСЫВАЮЩЕЕ: alpha-adb_dev_comm-read по имени
# только на чтение. Пусто — зеркало выключено.
PG_CONN = ''

# Префикс тракта внутри логового бакета.
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


def parse_rq_tm(raw: str):
    """🕐 RqTm квитанции → datetime. Пусто и неразбираемое → None.

    Своя нормализация вместо голого fromisoformat, потому что `xsd:dateTime` шире того,
    что понимает Python 3.9:

      • смещение может быть записано как 'Z' — понимать его fromisoformat научился
        только в 3.11;
      • дробная часть секунд бывает любой длины, а 3.9 принимает ровно 3 или 6 знаков:
        и '…:00.12+03:00', и '…:00.123456789+03:00' роняли разбор одинаково.

    Цена ошибки тут не нулевая: метка времени квитанции терялась молча, оставаясь NULL
    при живой в остальном строке.
    """
    from datetime import datetime

    s = (raw or '').strip()
    if not s:
        return None

    if s[-1] in 'Zz':
        s = s[:-1] + '+00:00'

    # Дробную часть приводим ровно к шести знакам: короткую дополняем, длинную режем.
    # Наносекунды ТФС мы всё равно не храним — в колонке DateTime64(3).
    frac = re.match(r'^(.*?)\.(\d+)(.*)$', s)
    if frac:
        s = f"{frac.group(1)}.{(frac.group(2) + '000000')[:6]}{frac.group(3)}"

    try:
        return datetime.fromisoformat(s)
    except ValueError:
        logger.warning("⚠️ RqTm '%s' не разобран", raw)
        return None


def parse_receipt(raw: str, partition: int = -1, offset: int = -1) -> list[dict]:
    """📨 Разбирает XML обратной квитанции TransferFileCephRs — по строке на файл.

    Список, а не одна строка: по спеке ТФС агрегат `File` идёт `[1-N]`, а `Status`
    лежит ВНУТРИ `File`, то есть статус у каждого файла свой. Прежний разбор брал первый
    `Name` и первый `StatusCode` во всём документе — на квитанции с двумя файлами он
    записал бы успех первого и потерял ошибку второго, а пакет подтвердился бы, не доехав.
    Ключ таблицы квитанций — (rq_uid, file_name), несколько строк на один RqUID она держит.

    Битый XML не роняет разбор: возвращается одна строка со status_code = -1 и текстом
    в raw_xml. Потерять квитанцию хуже, чем сохранить её неразобранной, а застрявшее
    сообщение заблокировало бы очередь.

    findall/findtext с '{*}' и без: у ТФС встречаются оба варианта — с неймспейсом и без.
    """
    import xml.etree.ElementTree as ET

    base = {
        'rq_uid': '', 'file_name': '', 'scenario_id': '',
        'status_code': -1, 'status_desc': '', 'rq_tm': None, 'raw_xml': raw,
        'kafka_partition': partition, 'kafka_offset': offset,
    }

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as err:
        logger.error("❌ Квитанция не разобрана как XML (%s): %.500s", err, raw)
        return [base]

    def _text(node, tag: str) -> str:
        return (node.findtext(f'.//{{*}}{tag}') or node.findtext(f'.//{tag}') or '').strip()

    base['rq_uid']      = _text(root, 'RqUID')
    base['scenario_id'] = _text(root, 'ScenarioId')
    base['rq_tm']       = parse_rq_tm(_text(root, 'RqTm'))

    files = root.findall('.//{*}File') or root.findall('.//File')
    if not files:
        # Квитанции без File по спеке не бывает, но терять сообщение из-за этого нельзя:
        # разбираем документ целиком и берём статус там, где он найдётся.
        logger.warning("⚠️ В квитанции нет ни одного File, RqUID=%s: %.300s", base['rq_uid'], raw)
        files = [root]

    rows = []
    for node in files:
        # StatusDesc и StatusCode ищутся внутри своего File, а первым в порядке документа
        # идёт Status — AdditionalStatus (тоже с парой code/desc) лежит после него.
        row = {**base, 'file_name': _text(node, 'Name'), 'status_desc': _text(node, 'StatusDesc')}
        code = _text(node, 'StatusCode')
        try:
            row['status_code'] = int(code)
        except ValueError:
            logger.error("❌ StatusCode '%s' не число, RqUID=%s, файл '%s'",
                         code, base['rq_uid'], row['file_name'])
        rows.append(row)

    return rows


def build_message(scenario_id: str, rq_uid: str, file_name: str) -> str:
    """📤 Собирает XML-уведомление TransferFileCephRq с ГОТОВЫМ RqUID.

    RqUID приходит из очереди, а не генерируется здесь: он записан при постановке
    в очередь, и именно по нему потом ищется обратная квитанция.

    Живёт рядом с parse_receipt, а не в даге-отправителе: это формат тракта, и
    оба конца — что мы пишем, что нам отвечают — должны меняться в одном месте.
    """
    from datetime import datetime

    # isoformat(ms) воспроизводит формат pendulum 'YYYY-MM-DDTHH:mm:ss.SSSZ' (смещение с двоеточием)
    rq_tm = datetime.now().astimezone().isoformat(timespec='milliseconds')
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{rq_uid}</RqUID>
    <RqTm>{rq_tm}</RqTm>
    <ScenarioInfo><ScenarioId>{scenario_id}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{file_name}</Name></FileInfo></File>
</TransferFileCephRq>"""


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

    Делает это приёмник — он опрашивает топики постоянно и сам сидит в default_pool, поэтому
    создаст tfs_send до того, как отправителю понадобится слот: таск с несуществующим
    пулом Airflow просто не поставит в очередь.
    """
    ensure_pool(
        TFS_SEND_POOL, slots=TFS_SEND_SLOTS,
        description=('Отправка в ТФС: не больше одного отправителя одновременно. Берёт '
                     'tfs_kafka_snd и обязан брать любой даг, шлющий в ТФС мимо очереди. '
                     'Лимиты маршрута пул НЕ соблюдает — только взаимное исключение'),
    )


# 🧠 Состояние сенсора между опросами.
#
# ⚠️ XCom для этого НЕ ГОДИТСЯ, и обе очевидные попытки разбиваются об Airflow:
#
#   • ti.xcom_push — в режиме reschedule каждый опрос это отдельное исполнение таска,
#     а TaskInstance._execute_task_with_callbacks перед запуском зовёт clear_xcom_data()
#     и стирает ВСЕ XCom своего task_id за ран (исключение сделано только для отложенных
#     задач, reschedule к ним не относится). Записанное прошлым опросом следующий не видит:
#     у отправителя это давало повторную постановку файлов в очередь на КАЖДОМ опросе,
#     у приёмника — потерянный счётчик окна;
#   • запись под соседний task_id — у таблицы xcom внешний ключ на task_instance,
#     строки для несуществующего таска СУБД не принимает (ForeignKeyViolation).
#
# Поэтому состояние живёт там же, где остальной тракт, — объектом в S3. Обращение
# всегда точечное, по ключу рана; папка по дате, как у отправленных, чтобы при желании
# чистить целыми днями. Объект крошечный, пишется только когда есть что помнить.

def _state_key(context) -> str:
    from datetime import datetime, timezone

    ti = context['ti']
    safe = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in ti.run_id)
    day = datetime.now(timezone.utc).strftime('%Y%m%d')
    return f"{s3_base()}/state/{day}/{ti.dag_id}__{ti.task_id}__{safe}.json"


def run_state_get(context, key: str, default=None):
    """📤 Значение, пережившее reschedule. См. комментарий выше — почему не XCom."""
    import json as _json

    hook, bucket, obj = _s3_hook_key(_state_key(context))
    if not hook.check_for_key(key=obj, bucket_name=bucket):
        return default
    return _json.loads(hook.read_key(key=obj, bucket_name=bucket)).get(key, default)


def run_state_set(context, key: str, value) -> None:
    """📥 Кладёт значение так, чтобы его увидел следующий опрос того же рана."""
    import json as _json

    hook, bucket, obj = _s3_hook_key(_state_key(context))
    state = {}
    if hook.check_for_key(key=obj, bucket_name=bucket):
        state = _json.loads(hook.read_key(key=obj, bucket_name=bucket))
    state[key] = value
    hook.load_string(string_data=_json.dumps(state, ensure_ascii=False),
                     key=obj, bucket_name=bucket, replace=True)


# ══════════════════════════════════════════════════════════════════════════════
# 🗄️ Хранилище тракта
#
# Три реализации за одним интерфейсом. Публичные функции внизу пишут в S3 и в
# включённые зеркала, а читают всегда из S3; вызывающему знать об этом не нужно.
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
        f"{int(r['status_code'])}, '{_sql_str(r.get('status_desc', ''))}', "
        f"{'toDateTime64(' + chr(39) + _ts(r['rq_tm']) + chr(39) + ', 3)' if r.get('rq_tm') else 'NULL'}, "
        f"'{_sql_str(r['raw_xml'])}', '{_sql_str(r.get('kafka_topic', ''))}', "
        f"{int(r.get('kafka_partition', -1))}, {int(r.get('kafka_offset', -1))})"
        for r in rows
    )
    _ch_hook().execute(
        f"INSERT INTO {RECEIPTS_TABLE} (rq_uid, file_name, scenario_id, status_code, status_desc, "
        f"rq_tm, raw_xml, kafka_topic, kafka_partition, kafka_offset) VALUES {values}"
    )


def _ch_find_receipts(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT rq_uid, file_name, status_code, status_desc, toString(rq_tm) AS rq_tm
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
            "Зеркало в Postgres вызвано, но PG_CONN пуст. Укажите ЗАПИСЫВАЮЩЕЕ соединение "
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
        f"{int(r['status_code'])}, '{_sql_str(r.get('status_desc', ''))}', "
        f"{chr(39) + _ts(r['rq_tm']) + chr(39) if r.get('rq_tm') else 'NULL'}, "
        f"'{_sql_str(r['raw_xml'])}', '{_sql_str(r.get('kafka_topic', ''))}', "
        f"{int(r.get('kafka_partition', -1))}, {int(r.get('kafka_offset', -1))})"
        for r in rows
    )
    _pg_exec(
        f"INSERT INTO {RECEIPTS_TABLE} (rq_uid, file_name, scenario_id, status_code, status_desc, "
        f"rq_tm, raw_xml, kafka_topic, kafka_partition, kafka_offset) VALUES {values}"
    )


def _pg_find_receipts(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return query_to_dict(_pg_hook(), f"""
        SELECT DISTINCT ON (rq_uid, file_name)
               rq_uid, file_name, status_code, status_desc, rq_tm::text AS rq_tm
        FROM {RECEIPTS_TABLE} WHERE rq_uid IN ({uids})
        ORDER BY rq_uid, file_name, received_at DESC
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
#   receipts/{rq_uid}.json                                    — квитанция целиком
#   queue/pending/{rq_uid}.json                               — ждёт отправки
#   queue/sent/{YYYYMMDD}/{scenario}/{rq_uid}__{HHMMSS}.json  — отправлено
#
# 🔑 RqUID — ПРЕФИКС имени, а не хвост. Все три вопроса тракта задаются по RqUID
# («есть квитанция?», «ещё в очереди?», «куда переносить при отправке?»), и с ним
# в начале каждый решается точечным обращением к ключу вместо обхода префикса.
# Так уже был устроен receipts/ ради wait_confirm — очередь приведена к тому же виду.
#
# 📅 Дата в пути у sent/ идёт ПЕРЕД сценарием, и это не косметика:
#   • счётчики лимитов листают только папки дня и вчера — объём ограничен суточным
#     лимитом маршрута, а не всем архивом отправок, который никто не чистит;
#   • диагностика по RqUID (queue_state) не знает сценария, зато знает дату, и с датой
#     впереди ей хватает тех же двух папок на все маршруты сразу.
# Время отправки при этом остаётся В ИМЕНИ ключа: окна считаются по именам, без чтения
# объектов. Скользящее суточное окно живёт максимум в двух соседних датах.

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
    """Объект на RqUID, а внутри СПИСОК строк — по одной на файл квитанции.

    Список, потому что File у ТФС идёт [1-N] и все файлы приходят под одним RqUID:
    писали бы по объекту на RqUID из одной строки — второй файл затирал бы первый,
    и ошибка одного из них исчезала бы вместе с ним.
    """
    import json as _json
    from collections import defaultdict

    by_uid: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_uid[r['rq_uid']].append({**r, 'rq_tm': _ts(r['rq_tm']) if r.get('rq_tm') else None})

    for uid, uid_rows in by_uid.items():
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/receipts/{uid}.json")
        hook.load_string(string_data=_json.dumps(uid_rows, ensure_ascii=False),
                         key=key, bucket_name=bucket, replace=True)


def _s3_find_receipts(rq_uids: list[str]) -> list[dict]:
    """Читает объекты квитанций. Принимает обе формы: список строк и одиночную строку —
    объекты, записанные до перехода на [1-N], остаются читаемыми."""
    import json as _json

    found = []
    for uid in rq_uids:
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/receipts/{uid}.json")
        if not hook.check_for_key(key=key, bucket_name=bucket):
            continue
        data = _json.loads(hook.read_key(key=key, bucket_name=bucket))
        found.extend(data if isinstance(data, list) else [data])
    return found


def _s3_enqueue(rows: list[dict]) -> None:
    import json as _json
    from datetime import datetime, timezone

    # created_at проставляем здесь: в СУБД это делает колоночный DEFAULT, а объекту
    # его дать некому. Без него очередь не упорядочить — order_queue сортирует пакеты
    # по времени появления первого файла.
    stamp = _ts(datetime.now(timezone.utc))

    for r in rows:
        r = {'created_at': stamp, **{k: v for k, v in r.items() if v is not None}}
        # Имя ключа — только RqUID: package_ts лежит внутри объекта, и порядок пакетов
        # строится по содержимому (см. _s3_pending), а не по именам.
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/queue/pending/{r['rq_uid']}.json")
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


def _s3_days(now) -> list[str]:
    """Даты, в которых может лежать скользящее суточное окно: сегодня и вчера (UTC)."""
    from datetime import timedelta

    return [(now - timedelta(days=d)).strftime('%Y%m%d') for d in (0, 1)]


def _s3_mark_sent(rq_uid: str) -> None:
    import json as _json
    from datetime import datetime, timezone

    # RqUID — префикс ключа, поэтому строка очереди берётся адресно, без обхода pending/
    src = f"{s3_base()}/queue/pending/{rq_uid}.json"
    hook, bucket, key = _s3_hook_key(src)
    if not hook.check_for_key(key=key, bucket_name=bucket):
        logger.warning("⚠️ %s: в очереди не найден, отметка отправки пропущена", rq_uid)
        return

    row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
    now = datetime.now(timezone.utc)
    # Время отправки — в ИМЕНИ ключа, дата — в пути: окна считаются без чтения объектов
    dst = (f"{s3_base()}/queue/sent/{now:%Y%m%d}/{row['scenario_id']}/"
           f"{rq_uid}__{now:%H%M%S}.json")
    s3_move_s3(src, dst)


def _s3_sent_counts(scenario_id: str) -> dict:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    counts = dict.fromkeys(WINDOWS, 0)

    # Только сегодня и вчера: суточное окно дальше не тянется, а весь архив отправок
    # листать нельзя — он растёт вечно и никем не чистится.
    for day in _s3_days(now):
        hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/sent/{day}/{scenario_id}/")
        for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
            stamp = key.split('/')[-1].removesuffix('.json').split('__')[-1]
            try:
                sent_at = datetime.strptime(day + stamp, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("⚠️ Ключ без разбираемой метки времени, пропущен: %s", key)
                continue
            for window in window_hits(sent_at, now):
                counts[window] += 1
    return counts


def _s3_queue_state(rq_uids: list[str]) -> list[dict]:
    import json as _json
    from datetime import datetime, timezone

    out = []
    # Ещё в очереди — точечная проверка ключа: RqUID стоит в его начале.
    missing = []
    for uid in rq_uids:
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/queue/pending/{uid}.json")
        if hook.check_for_key(key=key, bucket_name=bucket):
            row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
            out.append({'file_name': row.get('file_name', uid), 'pending': True})
        else:
            missing.append(uid)

    if not missing:
        return out

    # Остальные уже ушли: из pending путь один — в sent. Сценарий отсюда неизвестен,
    # зато известна дата, и с ней впереди хватает двух папок на все маршруты сразу.
    # Путь редкий (диагностика таймаута), объём ограничен суточной отправкой.
    sent: dict[str, str] = {}
    hook, bucket, _ = _s3_hook_key(f"{s3_base()}/queue/")
    for day in _s3_days(datetime.now(timezone.utc)):
        _, _, prefix = _s3_hook_key(f"{s3_base()}/queue/sent/{day}/")
        for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
            sent.setdefault(key.split('/')[-1].split('__')[0], key)

    for uid in missing:
        key = sent.get(uid)
        if not key:
            continue   # ни в очереди, ни в отправленных — сказать нечего
        row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
        out.append({'file_name': row.get('file_name', uid), 'pending': False})
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


def mirrors() -> list[str]:
    """Включённые зеркала: те, у кого задан conn_id. Пустой список — только S3."""
    return [name for name, conn in (('ch', CH_ID), ('pg', PG_CONN)) if conn]


def _write(idx: int, *args) -> None:
    """Запись: S3 обязателен, зеркала — по возможности.

    Ошибка S3 прокидывается: это источник истины, без него тракт слепнет. Ошибка
    зеркала только предупреждает — данные к этому моменту уже сохранены, и ронять
    приём квитанций или отправку из-за аналитической копии нельзя.
    """
    _BACKENDS['s3'][idx](*args)

    for name in mirrors():
        try:
            _BACKENDS[name][idx](*args)
        except Exception as exc:
            logger.warning("⚠️ Зеркало %s: запись не прошла (%s). В S3 записано, "
                           "расхождение лечится дозаливкой", name, exc)


def _read(idx: int, *args):
    """Чтение всегда из S3: писатель и читатель обязаны смотреть в одно место."""
    return _BACKENDS['s3'][idx](*args)


def save_receipts(rows: list[dict]) -> None:
    """Сохраняет разобранные квитанции (приёмник)."""
    return _write(0, rows)


def find_receipts(rq_uids: list[str]) -> list[dict]:
    """Квитанции по списку RqUID: rq_uid, file_name, status_code, rq_tm."""
    return _read(1, rq_uids)


def enqueue(rows: list[dict]) -> None:
    """Ставит файлы в очередь отправки (rq_uid, file_name, replica, scenario_id, package_ts…)."""
    return _write(2, rows)


def enqueue_files(files: list[str], scenario_id: str, replica: str = '',
                  dag_id: str = '', run_id: str = '') -> list[dict]:
    """📮 Ставит в очередь готовые имена файлов из S3 — ручная досылка.

    RqUID генерируется здесь: по нему потом ищется обратная квитанция. Возвращает
    поставленные строки, чтобы вызывающий мог их показать в логе и заметке.

    Досылка идёт через очередь, а не мимо неё: только так файл попадает в учёт
    лимитов маршрута и потом находится по RqUID вместе с остальными.

    ⚠️ Каждый вызов заводит НОВЫЕ RqUID. Повторный вызов с тем же списком поставит
    файлы в очередь ещё раз — вызывающий обязан звать это ровно один раз на запуск.
    """
    from datetime import datetime, timezone
    from uuid import uuid4

    names = [str(f).strip() for f in (files or []) if str(f).strip()]
    if not names:
        return []

    scenario_id = (scenario_id or '').strip()
    if not scenario_id:
        raise ValueError("Переданы файлы, но не задан scenario_id — маршрут ТФС неизвестен")

    now = datetime.now(timezone.utc)
    rows = [{
        'rq_uid':      uuid4().hex,
        'file_name':   name,
        'replica':     (replica or scenario_id).strip(),
        'scenario_id': scenario_id,
        'package_ts':  now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'dag_id':      dag_id,
        'run_id':      run_id,
    } for name in names]

    enqueue(rows)
    return rows


def order_queue(rows: list[dict]) -> list[dict]:
    """📦 Упорядочивает очередь так, чтобы файлы одного пакета шли подряд.

    Пакеты — по времени появления (package_ts, затем created_at первого файла), внутри
    пакета — по created_at. Разрывать пакет чужими файлами нельзя: ЕР не принимает
    несколько пакетов одновременно.
    """
    packages: dict = {}
    for row in rows:
        packages.setdefault((row['replica'], row['package_ts']), []).append(row)

    ordered = []
    # get, а не [], намеренно: строка без created_at не должна ронять всю отправку —
    # она просто встанет первой в своём пакете.
    for key in sorted(packages, key=lambda k: (k[1], min(str(r.get('created_at', '')) for r in packages[k]))):
        ordered.extend(sorted(packages[key], key=lambda r: str(r.get('created_at', ''))))
    return ordered


def pending() -> list[dict]:
    """Файлы, ещё не ушедшие в Kafka, в порядке package_ts, created_at."""
    return _read(3)


def mark_sent(rq_uid: str) -> None:
    """Отмечает файл отправленным."""
    return _write(4, rq_uid)


def sent_counts(scenario_id: str) -> dict:
    """Расход лимитов маршрута: {'sec', 'min', 'hour', 'day'}. Окна скользящие."""
    return _read(5, scenario_id)


def queue_state(rq_uids: list[str]) -> list[dict]:
    """file_name + pending: отличить «ещё не отправлено» от «нет квитанции»."""
    return _read(6, rq_uids)


def get_config() -> dict:
    """📦 Снимок констант модуля для передачи в DAG-файлы."""
    return {
        'CH_ID':            CH_ID,
        'MIRRORS':          mirrors(),   # включённые зеркала; S3 обязателен и в список не входит
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
