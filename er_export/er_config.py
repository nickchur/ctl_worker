"""
Конфигурация и константы фреймворка ER-выгрузок.

Два режима работы (MODE):
  SIGMA — продовый CH (dlab-click) и S3 (s3-tfs-hrplt).
  ALPHA — тестовый CH из vault (dlab-click-test) и S3 (s3-archive);
          включается автоматически при наличии переменной AIRFLOW__CTL_PIN
          или принудительно через ER_MODE=ALPHA.
"""
from __future__ import annotations

import base64
import json
import os
from datetime import timedelta

VAULT_PATH = '/vault/secrets/application'
CH_BD      = 'export'
VAR_NAME   = "datalab_er_wfs"

# ER_MODE включает тестовый CH-коннект из vault; по умолчанию — SIGMA/ALPHA
MODE = os.getenv("ER_MODE", "SIGMA" if not os.getenv("AIRFLOW__CTL_PIN") else "ALPHA")

if MODE == 'ALPHA':
    CH_ID = 'dlab-click-test'
    with open(VAULT_PATH) as f:
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
        "schema":    CH_BD,
        "extra":     {"verify": False, "secure": True},
    }
    os.environ[f'AIRFLOW_CONN_{CH_ID.upper()}'] = json.dumps(conn_json)
    S3_CONN = 's3-archive'
else:
    CH_ID   = 'dlab-click'
    S3_CONN = 's3-tfs-hrplt'

BUCKET = 'tfshrplt'
TOPIC  = 'TFS.HRPLT.IN'

ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

# replica → (scenario_id, s3_prefix): используется в create_export_dag для маршрутизации в TFS
TFS_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er"),
}

DEF_ARGS = {
    "owner":              "DataLab (CI02420667)",
    "retries":            3,
    "retry_delay":        timedelta(minutes=5),
    "aws_conn_id":        S3_CONN,
    "clickhouse_conn_id": CH_ID,
    "conn_id":            CH_ID,
    "kafka_config_id":    "tfs-kafka-out",   # продюсер → TFS
    "kafka_in_conn":      "tfs-kafka-in",    # консьюмер ← TFS (подтверждения)
    "kafka_in_topic":     "TFS.HRPLT.OUT",
    "topic":              TOPIC,
}

# Лимит строк при выгрузке на стенде; 0 = без ограничений (прод)
LIMITS = {
    "prom": 0,
    "uat":  100,
    "qa":   100,
    "ift":  100,
    "dev":  100,
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

# SQL-выражения, автоматически добавляемые в SELECT каждой выгрузки
MANDATORY_PRE = ["{export_time} as export_time"]          # подставляется рантаймом из состояния дельты
MANDATORY_SUF = ["'I' as ctl_action", "now() as ctl_validfrom"]

# Описания служебных колонок для .meta-файла TFS (порядок: PRE + data + SUF)
EXTRA_COLS_PRE = [
    {"column_name": "export_time",   "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None, "description": None},
]
EXTRA_COLS_SUF = [
    {"column_name": "ctl_action",    "source_type": "VARCHAR",   "length": 10,   "notnull": False, "precision": None, "scale": None, "description": None},
    {"column_name": "ctl_validfrom", "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None, "description": None},
]
EXTRA_COLS = EXTRA_COLS_PRE + EXTRA_COLS_SUF  # полный список служебных колонок

POOL_NAME   = 'datalab_export_er'
POOL_SLOTS  = 20

def obj_load(key: str, default: any = None) -> any:
    """Читает объект из Airflow Variable (JSON). При отсутствии возвращает default или {}."""
    from airflow.models import Variable
    return Variable.get(key, default_var=default if default is not None else {}, deserialize_json=True)


def obj_save(key: str, data: any) -> None:
    """Сохраняет объект в Airflow Variable (JSON).

    Пропускает запись если данные не изменились (сравнение JSON).
    Обновляет description переменной метаданными: {'ts': ..., 'len': ..., 'size': ...}.
    """
    from airflow.models import Variable
    import json
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
    unit = 'B'
    for u in ['B', 'KB', 'MB', 'GB']:
        if size_val < 1024.0:
            unit = u
            break
        size_val /= 1024.0

    size_str = f"{size_val:.1f} {unit}"
    length   = len(data) if isinstance(data, (dict, list)) else 1
    ts       = pendulum.now().format('YYYY-MM-DD HH:mm:ss')
    desc     = f"{{'ts': '{ts}', 'len': {length}, 'size': '{size_str}'}}"

    Variable.set(key, data, description=desc, serialize_json=True)


def add_note(msg, context=None, level='task', add=True, title='', compact=False):
    """Добавляет структурированную заметку в Airflow UI (Task и/или DAG Run).

    level  — 'task', 'dag' или 'task,dag' (регистр не важен; макс. 2 объекта)
    add    — True = prepend к существующей заметке; False = заменить полностью
    compact — передаётся в PrettyPrinter для компактного форматирования dict/list

    Скопировано из plugins.utils для устранения внешней зависимости.
    """
    from airflow.utils.session import create_session
    from airflow.operators.python import get_current_context
    from pprint import PrettyPrinter
    import logging

    logger = logging.getLogger("airflow.task")
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

    logger.info(f"📝 Note added to {level} {title}:\n{msg}")

    with create_session() as session:
        for lvl in list(set(level.upper().split(',')))[:2]:
            new_note = msg.strip()
            obj = session.merge(context['dag_run'] if lvl == 'DAG' else context['task_instance'])
            session.expire(obj)

            if title:
                import unicodedata
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
    """Выполняет SQL и возвращает результат как список словарей {column: value}."""
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    return []


DEFAULT_PARAMS: dict = {
    # ── Дельта / расписание ──────────────────────────────────────────────────
    'increment':         60,           # шаг дельты, сек: time_to = time_from + increment
    'selfrun_timeout':   10,           # задержка до следующего автозапуска, мин
    'overlap':           0,            # перекрытие окна дельты назад, сек (для компенсации задержек CDC)
    'lower_bound':       '',           # нижняя граница первой дельты (bootstrap); '' → 1970-01-01
    'time_field':        'extract_time',  # поле времени в таблице-источнике
    'recent_interval':   3600,         # окно для режима recent, сек (используется вместо дельты)

    # ── Стратегия и подтверждение ────────────────────────────────────────────
    'strategy':          'FULL_UK',    # стратегия слияния на стороне TFS
    'auto_confirm':      1,            # 1 = не ждать Kafka-подтверждения от TFS
    'confirm_timeout':   3600,         # таймаут ожидания подтверждения, сек

    # ── Файлы и сжатие ───────────────────────────────────────────────────────
    'compression_type':  'none',       # тип сжатия: none | gzip | zstd
    'compression_ext':   '',           # расширение сжатого файла, если нужно
    'max_file_size':     '',           # ограничение размера CSV-файла, байт; '' = без ограничений

    # ── Формат и санитизация ─────────────────────────────────────────────────
    'format':            'TSVWithNames',  # формат выгрузки ClickHouse
    'pg_array_format':   0,            # 1 = PostgreSQL-формат массивов в TSV
    'csv_format_params': '',           # доп. параметры форматирования (dict-литерал)
    'xstream_sanitize':  0,            # 1 = экранировать спецсимволы XStream
    'sanitize_array':    0,            # 1 = санитизировать CH-массивы в строки
    'sanitize_list':     '',           # список колонок для санитизации (через запятую)
}


def get_params(row: dict) -> dict:
    """Мёржит JSON-поле params из er_wf_meta с DEFAULT_PARAMS. Значения из row побеждают."""
    overrides = json.loads(row.get('params') or '{}')
    return {**DEFAULT_PARAMS, **overrides}


def get_config() -> dict:
    """Возвращает снимок всех констант модуля для передачи в DAG-файлы без прямого импорта."""
    return {
        'CH_ID':          CH_ID,
        'TYPE_MAP':       TYPE_MAP,
        'DEF_ARGS':       DEF_ARGS,
        'ENV_STAND':      ENV_STAND,
        'EXTRA_COLS':     EXTRA_COLS,
        'EXTRA_COLS_PRE': EXTRA_COLS_PRE,
        'EXTRA_COLS_SUF': EXTRA_COLS_SUF,
        'MANDATORY_PRE':  MANDATORY_PRE,
        'MANDATORY_SUF':  MANDATORY_SUF,
        'MODE':           MODE,
        'LIMITS':         LIMITS,
        'BUCKET':         BUCKET,
        'TFS_MAP':        TFS_MAP,
        'S3_CONN':        S3_CONN,
        'VAR_NAME':       VAR_NAME,
        'POOL_NAME':      POOL_NAME,
        'POOL_SLOTS':     POOL_SLOTS,
        'DEFAULT_PARAMS': DEFAULT_PARAMS,
    }
