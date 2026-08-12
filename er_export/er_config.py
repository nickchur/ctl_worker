"""⚙️ Конфигурация и константы фреймворка ER-выгрузок.

CH-коннект (dlab-click) и S3 (s3-tfs-hrplt) фиксированы.

Поведение на стенде управляется ENVIRONMENT (PROM / UAT / QA / IFT / DEV).
"""
from __future__ import annotations

import json
import logging
import os
from datetime import timedelta
from typing import Any

# Общие хелперы Airflow берём из plugins.utils, а не держим свои копии: заметки и
# колбэки должны вести себя одинаково во всех DAG-ах контура. add_note и ensure_pool
# здесь же реэкспортируются — их импортируют соседние модули этого каталога.
try:
    from plugins.utils import add_note, ensure_pool, get_dict_from_ch, on_callback  # noqa: F401  # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, ensure_pool, get_dict_from_ch, on_callback  # noqa: F401  # type: ignore

ENV_STAND = os.getenv("ENVIRONMENT", "").strip().upper()

VAR_NAME = "datalab_er_wfs"

CH_ID   = 'dlab-click'
S3_CONN = 's3-tfs-hrplt'

BUCKET = 'tfshrplt'

# 🗺️ replica → (scenario_id, s3_prefix): используется в create_export_dag для маршрутизации в TFS
TFS_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er"),
}

POOL_NAME   = 'datalab_export_er'
POOL_SLOTS  = 20

logger = logging.getLogger("airflow.task")


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
        'TFS_MAP':         TFS_MAP,
        'S3_CONN':         S3_CONN,
        'VAR_NAME':        VAR_NAME,
        'POOL_NAME':       POOL_NAME,
        'POOL_SLOTS':      POOL_SLOTS,
        'DEFAULT_PARAMS':  DEFAULT_PARAMS,
        'GROUP_PARAMS':    GROUP_PARAMS,
        'TABLE_PARAMS':    TABLE_PARAMS,
        'FORMAT_MAP':      FORMAT_MAP,
    }
