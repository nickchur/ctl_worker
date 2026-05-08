"""⚙️ Конфигурация и константы фреймворка ER-выгрузок.

CH-коннект и S3 определяются переменной ENV_SPACE:
  🔑 ALPHA → dlab-click-test + s3-archive, секреты из Vault.
  🏭 иначе → dlab-click + s3-tfs-hrplt.

Поведение на стенде управляется ENVIRONMENT (PROM / UAT / QA / IFT / DEV).
"""
from __future__ import annotations

import base64
import json
import logging
import os
import unicodedata
from datetime import timedelta
from typing import Any

ENV_STAND = os.getenv("ENVIRONMENT", "").strip().upper()
ENV_SPACE = os.getenv("ENV_SPACE", "").strip().upper()

VAR_NAME = "datalab_er_wfs"

if ENV_SPACE == "ALPHA":
    CH_ID = 'dlab-click-test'
    with open('/vault/secrets/application') as f:
        secrets = json.load(f)

    def _b64(s: str) -> str:
        """Декодирует base64-строку из vault в UTF-8."""
        return base64.b64decode(s).decode()

    conn_json = {
        "conn_type": "clickhouse",
        "host":      "tvlds-hrplt0429.cloud.delta.sbrf.ru",
        "port":      9440,
        "login":     _b64(secrets['SCSP_CLICKHOUSE_USERNAME']),
        "password":  _b64(secrets['SCSP_CLICKHOUSE_PASSWORD']),
        "schema":    'export',
        "extra":     {"verify": False, "secure": True},
    }
    os.environ[f'AIRFLOW_CONN_{CH_ID.upper()}'] = json.dumps(conn_json)
    S3_CONN = 's3-archive'
else:
    CH_ID   = 'dlab-click'
    S3_CONN = 's3-tfs-hrplt'

BUCKET          = 'tfshrplt'
KAFKA_OUT_TOPIC = 'TFS.HRPLT.IN'
KAFKA_OUT_CONN  = 'tfs-kafka-out'
KAFKA_IN_CONN   = 'tfs-kafka-in'
KAFKA_IN_TOPIC  = 'TFS.HRPLT.OUT'

# 🗺️ replica → (scenario_id, s3_prefix): используется в create_export_dag для маршрутизации в TFS
TFS_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er"),
}

POOL_NAME   = 'datalab_export_er'
POOL_SLOTS  = 20

logger = logging.getLogger("airflow.task")


def _on_callback(context, level=None):
    """Обработчик on_failure_callback — формирует заметку о статусе таска/DAG в Airflow UI."""
    from airflow.utils.state import TaskInstanceState
    import pendulum

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

    message = f"*{pendulum.now().format('DD.MM.YYYY HH:mm:ss zz')}*\n\n"

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
    import pendulum

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
    ts       = pendulum.now().format('YYYY-MM-DD HH:mm:ss')
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


DEFAULT_PARAMS: dict = {
    # ── Дельта / расписание ──────────────────────────────────────────────────
    'increment':         60,           # шаг дельты, мин: time_to = time_from + increment (не чаще 1 пакета/час по стандарту ТФС)
    'selfrun_timeout':   60,           # задержка до следующего автозапуска, мин (не чаще 1 пакета/час)
    'overlap':           0,            # перекрытие окна дельты назад, сек (для компенсации задержек CDC)
    'lower_bound':       '',           # нижняя граница первой дельты (bootstrap); '' → 1970-01-01
    'time_field':        'extract_time',  # поле времени в таблице-источнике
    'recent_interval':   60,           # окно для режима recent, мин (используется вместо дельты)

    # ── Стратегия и подтверждение ────────────────────────────────────────────
    'strategy':          'FULL_UK',    # стратегия слияния на стороне TFS
    'notify_kafka':      1,            # 1 = отправлять уведомления в Kafka; 0 = пропустить (стенд)
    'auto_confirm':      1,            # 1 = не ждать Kafka-подтверждения от TFS
    'confirm_timeout':   60,           # таймаут ожидания подтверждения, мин
    'export_timeout':    120,          # таймаут export_to_s3, мин
    'notify_timeout':    30,           # таймаут notify_tfs, мин

    # ── Файлы ────────────────────────────────────────────────────────────────
    'max_file_size':     '',           # ограничение размера CSV-файла, байт; '' = без ограничений
    'send_empty':        0,            # 1 = слать пустой ZIP+Kafka при нулевой дельте

    # ── Формат и санитизация ─────────────────────────────────────────────────
    'format':            'TSVWithNames',  # формат выгрузки ClickHouse
    'pg_array_format':   0,            # 1 = PostgreSQL-формат массивов в TSV
    'csv_format_params': '',           # доп. параметры форматирования (dict-литерал)
    'xstream_sanitize':  0,            # 1 = экранировать спецсимволы XStream
    'sanitize_array':    0,            # 1 = санитизировать CH-массивы в строки
    'sanitize_list':     '',           # список колонок для санитизации (через запятую)
}


def get_params(row: dict) -> dict:
    """🔧 Мёржит JSON-поле params из er_wf_meta с DEFAULT_PARAMS. Значения из row побеждают."""
    overrides = json.loads(row.get('params') or '{}')
    return {**DEFAULT_PARAMS, **overrides}


def get_config() -> dict:
    """📦 Возвращает снимок всех констант модуля для передачи в DAG-файлы без прямого импорта."""
    return {
        'CH_ID':           CH_ID,
        'TYPE_MAP':        TYPE_MAP,
        'DEF_ARGS':        DEF_ARGS,
        'ENV_STAND':       ENV_STAND,
        'ENV_SPACE':       ENV_SPACE,       # TODO: удалить после удаления ALPHA-блока
        'EXTRA_PRE':       EXTRA_PRE,
        'EXTRA_SUF':       EXTRA_SUF,
        'LIMITS':          LIMITS,
        'BUCKET':          BUCKET,
        'KAFKA_OUT_TOPIC': KAFKA_OUT_TOPIC,
        'KAFKA_OUT_CONN':  KAFKA_OUT_CONN,
        'KAFKA_IN_CONN':   KAFKA_IN_CONN,
        'KAFKA_IN_TOPIC':  KAFKA_IN_TOPIC,
        'TFS_MAP':         TFS_MAP,
        'S3_CONN':         S3_CONN,
        'VAR_NAME':        VAR_NAME,
        'POOL_NAME':       POOL_NAME,
        'POOL_SLOTS':      POOL_SLOTS,
        'DEFAULT_PARAMS':  DEFAULT_PARAMS,
    }
