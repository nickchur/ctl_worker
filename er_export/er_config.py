"""⚙️ Конфигурация и константы фреймворка ER-выгрузок.

CH-коннект (dlab-click) и S3 (s3-tfs-hrplt) фиксированы.

Поведение на стенде управляется ENVIRONMENT (PROM / UAT / QA / IFT / DEV).
"""
from __future__ import annotations

import json
import logging
import os
import unicodedata
from datetime import timedelta
from typing import Any

ENV_STAND = os.getenv("ENVIRONMENT", "").strip().upper()

VAR_NAME = "datalab_er_wfs"

CH_ID   = 'dlab-click'
S3_CONN = 's3-tfs-hrplt'

BUCKET          = 'tfshrplt'
# IN/OUT в conn_id и топиках — сторона TFS, и у соединения, и у топика: пишем мы в его
# вход, читаем из его выхода. Префиксы констант — наше действие, чтобы не путаться
KAFKA_SND_CONN  = 'tfs-kafka-in'    # export_er_sender: шлём уведомление
KAFKA_SND_TOPIC = 'TFS.HRPLT.IN'
# Топик квитанций общий на ВСЕ маршруты ТФС, поэтому читает его ровно один потребитель —
# даг tfs_receipts_sync. Выгрузки ER сюда не ходят: они ждут строку в RECEIPTS_TABLE.
KAFKA_RCV_CONN  = 'tfs-kafka-out'
KAFKA_RCV_TOPIC = 'TFS.HRPLT.OUT'

# 🗺️ replica → (scenario_id, s3_prefix): используется в create_export_dag для маршрутизации в TFS
TFS_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er"),
}

# 📇 Таблицы тракта ТФС
RECEIPTS_TABLE   = 'export.tfs_receipts'    # квитанции из Kafka, общие для всех маршрутов
SENT_FILES_TABLE = 'export.er_sent_files'   # очередь и реестр отправок ER

# 🚦 Лимиты ТФС на маршрут: файлов в секунду / минуту / час / сутки.
# ТФС отбивает лишние файлы, соблюдать темп должны мы сами. Значения задекларированы
# в документации маршрутов; счётчики на их стороне, предположительно, считают сообщения
# в Kafka — гипотеза не доказана, поэтому числа держим здесь и правим по факту.
TFS_LIMITS_DEFAULT = {'sec': 10, 'min': 200, 'hour': 500, 'day': 2000}
TFS_LIMITS: dict[str, dict[str, int]] = {
    'HRPLATFORM-2100': {'sec': 1, 'min': 15, 'hour': 100, 'day': 500},
}

# Очередь старше этого возраста (мин) роняет даг-отправитель: затор должен быть виден
# в мониторинге, а не только в логе.
TFS_QUEUE_ALERT_MIN = 60

POOL_NAME   = 'datalab_export_er'
POOL_SLOTS  = 20

# 🔒 Пул на 1 слот: в ТФС пишет кто-то один. Его берёт export_er_sender и обязан брать
# любой даг, который шлёт в ТФС МИМО очереди.
#
# Что он даёт и чего не даёт: взаимное исключение — да, соблюдение лимитов — нет.
# Отправитель мимо очереди не пишет в SENT_FILES_TABLE, поэтому его файлы не попадут
# в счётчики и бюджет маршрута окажется посчитан заниженным. Правильный путь для нового
# отправителя — не пул, а та же очередь.
#
# Пул один общий, а не tfs_{scenario}: пул назначается таску при разборе файла,
# а export_er_sender — один таск на все сценарии и заранее не знает, чьи файлы попадутся
# в очереди. Строже, чем нужно, но при темпе в единицы файлов в секунду это незаметно.
TFS_SEND_POOL  = 'tfs_send'
TFS_SEND_SLOTS = 1

logger = logging.getLogger("airflow.task")


def _on_callback(context, level=None):
    """Обработчик on_failure_callback/on_success_callback — формирует заметку о статусе таска/DAG в Airflow UI."""
    from airflow.utils.state import TaskInstanceState
    from datetime import datetime

    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    dag_id = ti.dag_id
    dag_state = dag_run.state

    if not level:
        if dag_state.lower() in ('success', 'failed') or not context.get('task'):
            level = 'DAG'
        else:
            level = 'task'

    if level == 'DAG':
        tis = dag_run.get_task_instances()
        finished_tis = [t for t in tis if t.end_date is not None]
        task = sorted(finished_tis, key=lambda x: x.end_date, reverse=True)[0] if finished_tis else ti
    else:
        task = ti

    task_id = task.task_id
    map_index = task.map_index
    map_ind_str = getattr(task, 'rendered_map_index', '')
    try_number = task.try_number
    state = task.state

    msg = context.get('exception')

    map_str = f"   *Map Index*: ({map_index}) **{map_ind_str}**" if str(map_index) != '-1' else ""
    try_str = f"   *Try number*: **{try_number}**" if try_number > 1 else ""
    msg_str = f"```\n{str(msg)[:1000]}\n```\n" if msg else ""

    task_msg = '❌ FAILED' if state.lower() == 'failed' else '✅ SUCCESS' if state.lower() == 'success' else state.upper()

    message = f"*{datetime.now().astimezone().strftime('%d.%m.%Y %H:%M:%S %Z')}*\n\n"

    if level == 'DAG':
        dag_msg = '❌ FAILED' if dag_state.lower() == 'failed' else '✅ SUCCESS'
        message += f"*DAG:* **{dag_id}**: **{dag_msg}**\n\n"
    else:
        message += (
            f"*Task:* **{task_id}**: **{task_msg}**\n\n"
            f"{try_str}{map_str}\n\n"
            f"{msg_str}"
        )

    add_note(message, context, level, add=True)

    if state == TaskInstanceState.SUCCESS:
        logger.info(message)
    elif state == TaskInstanceState.FAILED:
        logger.error(message)
        if level != 'DAG':
            add_note(message, context, level='DAG', add=True)
    else:
        logger.warning(message)


def on_callback(context, level=None):
    return _on_callback(context, level)


DEF_ARGS = {
    "owner":            "DataLab (CI02420667)",
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "pool":             POOL_NAME,
    "email_on_failure": False,
    "email_on_retry":   False,
    "on_failure_callback": on_callback,
    "on_success_callback": on_callback,
}

# 🔢 Лимит строк при выгрузке на стенде; 0 = без ограничений (прод)
LIMITS = {
    "PROM": 0,
    "UAT":  100,
    "QA":   100,
    "IFT":  100,
    "DEV":  100,
}

TYPE_MAP: dict[str, str] = {
    "DateTime":    "TIMESTAMP",
    "DateTime64":  "TIMESTAMP",
    "Date":        "DATE",
    "Date32":      "DATE",
    "String":      "STRING",
    "FixedString": "STRING",
    "UUID":        "STRING",
    "Int8":        "INT",
    "Int16":       "INT",
    "Int32":       "INT",
    "Int64":       "BIGINT",
    "UInt8":       "INT",
    "UInt16":      "INT",
    "UInt32":      "INT",
    "UInt64":      "BIGINT",
    "Float32":     "FLOAT",
    "Float64":     "DOUBLE",
    "Decimal":     "NUMERIC",
    "Array":       "STRING",
}

# 📎 Служебные поля: ключ sql — выражение для SELECT, остальное — метаданные колонки для .meta TFS
EXTRA_PRE = [
    {"sql": "{export_time} as export_time", "column_name": "export_time",   "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None, "description": None},
]
EXTRA_SUF = [
    {"sql": "'I' as ctl_action",            "column_name": "ctl_action",    "source_type": "VARCHAR",   "length": 10,   "notnull": False, "precision": None, "scale": None, "description": None},
    {"sql": "now64(6) as ctl_validfrom",    "column_name": "ctl_validfrom", "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None, "description": None},
]


def obj_load(key: str, default: Any = None) -> Any:
    """📥 Читает объект из Airflow Variable (JSON). При отсутствии возвращает default или {}."""
    from airflow.models import Variable
    return Variable.get(key, default_var=default if default is not None else {}, deserialize_json=True)


def obj_save(key: str, data: Any) -> None:
    """📤 Сохраняет объект в Airflow Variable (JSON).

    Пропускает запись если данные не изменились (сравнение JSON).
    Обновляет description переменной метаданными: {'ts': ..., 'len': ..., 'size': ...}.
    """
    from airflow.models import Variable
    from datetime import datetime

    # Сравниваем с текущим значением — пропускаем лишнюю запись в БД
    try:
        old_val = Variable.get(key, default_var=None, deserialize_json=True)
    except Exception:
        old_val = None

    new_json = json.dumps(data, sort_keys=True, ensure_ascii=False)
    old_json = json.dumps(old_val, sort_keys=True, ensure_ascii=False) if old_val is not None else None

    if new_json == old_json:
        return

    # Вычисляем человекочитаемый размер для description
    size_val = float(len(new_json.encode('utf-8')))
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_val < 1024.0:
            break
        size_val /= 1024.0

    size_str = f"{size_val:.1f} {unit}"
    length   = len(data) if isinstance(data, (dict, list)) else 1
    ts       = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    desc     = f"{{'ts': '{ts}', 'len': {length}, 'size': '{size_str}'}}"

    Variable.set(key, data, description=desc, serialize_json=True)


def add_note(msg, context=None, level='task', add=True, title='', compact=False):
    """📝 Добавляет структурированную заметку в Airflow UI (Task и/или DAG Run).

    level  — 'task', 'dag' или 'task,dag' (регистр не важен; макс. 2 объекта)
    add    — True = prepend к существующей заметке; False = заменить полностью
    compact — передаётся в PrettyPrinter для компактного форматирования dict/list

    Скопировано из plugins.utils для устранения внешней зависимости.
    """
    from airflow.utils.session import create_session
    from airflow.operators.python import get_current_context
    from pprint import PrettyPrinter

    MAX_NOTE_LEN = 1000

    if not context:
        try:
            context = get_current_context()
        except Exception:
            logger.warning("Could not get Airflow context for add_note")
            return

    # Если передан dict с одним ключом — распаковываем его как заголовок
    if isinstance(msg, dict) and len(msg) == 1:
        t, msg = next(iter(msg.items()))
        title += str(t) + (f' ({len(msg)})' if isinstance(msg, (dict, list, tuple, set)) else '')

    if not isinstance(msg, str):
        msg = PrettyPrinter(indent=4, compact=compact).pformat(msg).replace("'", '')
        msg = '```\n' + msg + '\n```'

    logger.info("📝 Note added to %s %s:\n%s", level, title, msg)

    with create_session() as session:
        for lvl in list(set(level.upper().split(',')))[:2]:
            new_note = msg.strip()
            obj = session.merge(context['dag_run'] if lvl == 'DAG' else context['task_instance'])
            session.expire(obj)

            if title:
                # Если title не начинается с emoji — добавляем стандартный префикс
                if not unicodedata.category(title[0]) == 'So':
                    title = "📝 " + title
                new_note = f"{title}\n---\n{new_note}"

            # Пропускаем если заметка уже начинается с того же текста (идемпотентность)
            if obj.note and obj.note.startswith(new_note[:MAX_NOTE_LEN]):
                continue

            if add:
                new_note = f"{new_note}\n\n---\n{obj.note if obj.note else ''}"

            obj.note = new_note[:MAX_NOTE_LEN]


def get_dict(ch_hook, sql: str) -> list[dict]:
    """🔍 Выполняет SQL и возвращает результат как список словарей {column: value}."""
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    return []


# 📦 Параметры уровня ПАКЕТА (группы). Задаются в строке-дефолте группы (replica заполнена,
# extract_name пуст) и относятся к дагу целиком: один тикет, одно уведомление, одно
# расписание автозапуска. В строках-таблицах игнорируются — у пакета они физически одни.
GROUP_PARAMS: dict = {
    'notify_kafka':      1,            # 1 = отправлять уведомления в Kafka; 0 = пропустить (стенд)
    'auto_confirm':      1,            # 1 = не ждать Kafka-подтверждения от TFS
    'confirm_timeout':   60,           # таймаут ожидания квитанций ТФС, мин (включая ожидание в очереди отправки)
    'selfrun_timeout':   60,           # задержка до следующего автозапуска, мин (не чаще 1 пакета/час)
    'max_active_tasks':  4,            # сколько таблиц пакета грузятся одновременно
}

# 🗂️ Параметры уровня ТАБЛИЦЫ. Наследуются из строки-дефолта группы и переопределяются
# в строке-таблице.
TABLE_PARAMS: dict = {
    # ── Дельта ───────────────────────────────────────────────────────────────
    'increment':         60,           # шаг дельты, мин: time_to = time_from + increment (не чаще 1 пакета/час по стандарту ТФС)
    'overlap':           0,            # перекрытие окна дельты назад, сек (для компенсации задержек CDC)
    'lower_bound':       '',           # нижняя граница первой дельты (bootstrap); '' → 1970-01-01
    'time_field':        'extract_time',  # поле времени в таблице-источнике
    'recent_interval':   60,           # окно для режима recent, мин (используется вместо дельты)

    # ── Стратегия ────────────────────────────────────────────────────────────
    'strategy':          'FULL_UK',    # стратегия слияния на стороне TFS
    'export_timeout':    120,          # таймаут export_to_s3, мин

    # ── Файлы ────────────────────────────────────────────────────────────────
    'max_file_size':     '',           # ограничение размера файла, байт; '' = дефолт оператора
    'send_empty':        0,            # 1 = слать пустой ZIP+Kafka при нулевой дельте

    # ── Формат и санитизация ─────────────────────────────────────────────────
    'format':            'TSVWithNames',  # формат выгрузки ClickHouse (ключ FORMAT_MAP)
    'pg_array_format':   0,            # 1 = PostgreSQL-формат массивов в TSV
    'csv_format_params': '',           # доп. параметры форматирования (dict-литерал)
    'xstream_sanitize':  0,            # 1 = экранировать спецсимволы XStream
    'sanitize_array':    0,            # 1 = санитизировать CH-массивы в строки
    'sanitize_list':     '',           # список колонок для санитизации (через запятую)
}

DEFAULT_PARAMS: dict = {**GROUP_PARAMS, **TABLE_PARAMS}

# 🔤 Поддерживаемые форматы выгрузки. Ключ — имя формата ClickHouse (как его пишут в params),
# значение — как этот формат прокинуть в оператор и как назвать файл.
#
# header для JSON обязан быть False: ветка JSON в NativeClickhouseStream при header=True
# пишет первой строкой TSV-заголовок, что в .json — мусор. Заодно это держит верным счёт
# строк: _cur_result вычитает заголовок только при header=True.
FORMAT_MAP: dict[str, dict] = {
    'TSVWithNames': {
        'fmt':         'CSV',            # значение аргумента fmt оператора
        'header':      True,
        'ext':         'csv',
        'meta_params': {"separation": "\t"},  # блок params в .meta-файле для TFS
    },
    'JSONEachRow': {
        'fmt':         'JSON',           # JSON Lines (NDJSON): объект на строку, экранирует orjson
        'header':      False,
        'ext':         'json',
        'meta_params': {"format": "JSONEachRow"},
    },
}


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


def replica_base(replica: str) -> str:
    """🔀 Базовая реплика — часть до первого '__'; остальное считается номером группы.

    Группа поставок кодируется суффиксом в replica ('hrplatform_datalab__1'), сама replica
    целиком уходит в имена архива и тикета и тем разводит пакеты по именам. А маршрут в TFS
    (scenario_id + префикс в S3) один на всю реплику, поэтому TFS_MAP ищется по базе —
    новая группа заводится строкой в er_wf_meta, без правки кода.
    """
    return replica.split('__', 1)[0]


def get_params(row: dict, group: dict | None = None) -> dict:
    """🔧 Собирает итоговые параметры: DEFAULT_PARAMS → params группы → params строки.

    row   — запись er_wf_meta (или entry из Variable) с JSON-полем params
    group — параметры строки-дефолта группы; уже разрешённый dict либо None
    """
    overrides = json.loads(row.get('params') or '{}')
    return {**DEFAULT_PARAMS, **(group or {}), **overrides}


def get_config() -> dict:
    """📦 Возвращает снимок всех констант модуля для передачи в DAG-файлы без прямого импорта."""
    return {
        'CH_ID':           CH_ID,
        'TYPE_MAP':        TYPE_MAP,
        'DEF_ARGS':        DEF_ARGS,
        'ENV_STAND':       ENV_STAND,
        'EXTRA_PRE':       EXTRA_PRE,
        'EXTRA_SUF':       EXTRA_SUF,
        'LIMITS':          LIMITS,
        'BUCKET':          BUCKET,
        'KAFKA_SND_CONN':  KAFKA_SND_CONN,
        'KAFKA_SND_TOPIC': KAFKA_SND_TOPIC,
        'KAFKA_RCV_CONN':   KAFKA_RCV_CONN,
        'KAFKA_RCV_TOPIC':  KAFKA_RCV_TOPIC,
        'TFS_MAP':         TFS_MAP,
        'S3_CONN':         S3_CONN,
        'VAR_NAME':        VAR_NAME,
        'POOL_NAME':       POOL_NAME,
        'POOL_SLOTS':      POOL_SLOTS,
        'TFS_SEND_POOL':   TFS_SEND_POOL,
        'TFS_SEND_SLOTS':  TFS_SEND_SLOTS,
        'DEFAULT_PARAMS':  DEFAULT_PARAMS,
        'GROUP_PARAMS':    GROUP_PARAMS,
        'TABLE_PARAMS':    TABLE_PARAMS,
        'FORMAT_MAP':      FORMAT_MAP,
        'RECEIPTS_TABLE':   RECEIPTS_TABLE,
        'SENT_FILES_TABLE': SENT_FILES_TABLE,
        'TFS_LIMITS':       TFS_LIMITS,
        'TFS_LIMITS_DEFAULT': TFS_LIMITS_DEFAULT,
        'TFS_QUEUE_ALERT_MIN': TFS_QUEUE_ALERT_MIN,
    }
