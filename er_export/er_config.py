"""⚙️ Конфигурация и константы фреймворка ER-выгрузок.
*2026-08-12 17:35 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

CH-коннект (dlab-click) и S3 (s3-tfs-hrplt) фиксированы.

Поведение на стенде управляется ENV_STAND, при её отсутствии — ENVIRONMENT
(PROM / UAT / QA / IFT / DEV).
"""
# ⛔ Здесь НЕ место декоратору @dag — даже для маленького служебного дага.
#
# DagBag добавляет к дагам разбираемого файла ещё и DagContext.autoregistered_dags,
# куда @dag складывает всё созданное за время разбора ТЕКУЩЕГО файла. Этот модуль
# импортируют er_sync.py, er_export.py и er_wf_edit.py — заведи мы тут @dag, один и
# тот же даг приписался бы всем трём файлам сразу. Правило общее: модуль, который
# импортируют DAG-файлы, сам DAG-и создавать не должен.
#
# Подробнее и про то, почему нельзя освободить это имя переименованием, — в README.md,
# раздел «✏️ Правка настройки из UI».
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

# 🌍 Стенд. Сначала ENV_STAND — именно её читают платформенные операторы
# (hrp_operators/clickhouse_to_s3.py: os.getenv("ENV_STAND")) и соседний xs_export.
# ENVIRONMENT остаётся запасным именем: раньше конфиг читал только её, и там, где
# выставлена лишь она, поведение не должно измениться. Ни одной — пустая строка,
# то есть лимитов стенда нет и берутся дефолты.
ENV_STAND = (os.getenv("ENV_STAND") or os.getenv("ENVIRONMENT") or "").strip().upper()

VAR_NAME = "datalab_er_wfs"

# Сырые строки er_wf_meta — как они лежат в таблице, без наследования и разрешения
# параметров. Нужны дагу правки export_er_wf_edit: выпадающий список записей строится
# при разборе файла, а ходить в ClickHouse на каждом парсинге нельзя.
# Пишет тот же er_sync, он и так читает всю таблицу.
RAW_VAR_NAME = "datalab_er_wf_meta"

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

# 📏 Ограничение размера одного файла данных. Зависит от СТЕНДА, а не от таблицы:
# делить поток крупнее имеет смысл там, где канал до ТФС это выдерживает. Значение —
# строка в формате оператора ('10GB', '100MB' или просто число байт), оно же умолчание
# параметра «Max file size» в форме запуска.
MAX_FILE_SIZE = {
    "PROM": '10GB',
    "UAT":  '1GB',
    "QA":   '1GB',
    "IFT":  '1GB',
    "DEV":  '1GB',
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
    # max_file_size здесь НЕТ: он зависит от стенда, а не от таблицы — см. MAX_FILE_SIZE.
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


def raw_key(row: dict) -> str:
    """🔑 Ключ строки er_wf_meta для Variable и выпадающего списка.

    У поставки — 'db_name.extract_name', как в остальном фреймворке. У строки-дефолта
    группы extract_name пуст, а db_name равен replica, поэтому ключ превратился бы
    в 'replica.' — вместо этого помечаем её явно, чтобы в списке было понятно, что это.
    """
    if not row.get('extract_name'):
        return f"{row.get('replica', '')} (дефолты группы)"
    return f"{row.get('db_name', '')}.{row['extract_name']}"


def key_to_where(key: str) -> tuple[str, str]:
    """🔎 Ключ из выпадающего списка → (db_name, extract_name) для WHERE.

    Обратная к raw_key: у строки-дефолта extract_name пуст, а db_name равен replica.
    """
    marker = ' (дефолты группы)'
    if key.endswith(marker):
        return key[:-len(marker)], ''
    db, _, name = key.partition('.')
    return db, name


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


# 🧬 Поля строки-дефолта группы, наследуемые поставками. SQL и ключи (pk, uk, fields,
# sql_*) сюда намеренно не входят — они всегда описывают конкретную таблицу.
#
# is_recent тоже не наследуется, хотя это и не SQL: колонка UInt8 DEFAULT 0, и «не задано»
# от «явно delta» не отличить. При наследовании через `or` таблица в recent-группе не смогла
# бы вернуться к дельте — её sql_stmt_export_delta уехал бы под ключом recent.
#
# description обрабатывается отдельно (см. build_wfs): у него есть третий источник —
# комментарий таблицы в ClickHouse, и он должен быть приоритетнее группового текста.
INHERITED = ('schema_name',)

# Значение колонки schedule по умолчанию в DDL. Отличать его от осознанно заданного нельзя
# (ClickHouse возвращает дефолт, а не пустую строку), поэтому при сверке расписаний внутри
# пакета такое значение считаем «не задано».
DEFAULT_SCHEDULE = '55 0 * * *'


def parse_params(raw: str, where: str) -> dict:
    """Разбирает JSON-поле params; при битом JSON возвращает {} и пишет предупреждение."""
    try:
        return json.loads(raw or '{}')
    except json.JSONDecodeError as err:
        logger.warning("⚠️ %s: битый JSON в params (%s) — параметры проигнорированы", where, err)
        return {}


def explicit_schedule(row: dict) -> str:
    """Расписание, заданное осознанно. Колоночный дефолт трактуем как «не задано»."""
    sched = (row or {}).get('schedule') or ''
    return '' if sched == DEFAULT_SCHEDULE else sched


def check_table(row: dict, key: str, errors: list[str], params: dict) -> bool:
    """Проверяет строку-поставку. Непрошедшая запись ломает всю группу, причина — в errors.

    params — уже слитые параметры (дефолты + группа + таблица): проверять надо именно их,
    иначе опечатка в params строки-дефолта проходит синк и роняет разбор файла в фабрике.
    """
    if not row["sql_from"]:
        errors.append(f"{key}: пустой sql_from")
        return False

    base = replica_base(row["replica"])
    if base not in TFS_MAP:
        errors.append(f"{key}: базовая реплика '{base}' не найдена в TFS_MAP")
        return False

    # Без схемы фабрика падает на schema_name.replace(), а падение при разборе файла
    # уносит с собой ВСЕ пакеты, а не только этот.
    if not row.get("schema_name"):
        errors.append(f"{key}: пустой schema_name — задайте его в строке-дефолте группы или в поставке")
        return False

    # Состав полей задаётся только настройкой: иначе новая колонка источника уехала бы
    # в выгрузку и в .meta сама по себе, без единого изменения конфигурации.
    fields = row["fields"] or []
    if not fields:
        errors.append(f"{key}: пустой fields — состав колонок надо задать явно")
        return False
    star = [f for f in fields if str(f).strip() == '*' or str(f).strip().endswith('.*')]
    if star:
        errors.append(f"{key}: fields содержит {star} — звёздочка запрещена, нужен явный список")
        return False

    fmt = params.get('format', DEFAULT_PARAMS['format'])
    if fmt not in FORMAT_MAP:
        errors.append(f"{key}: неизвестный format '{fmt}', допустимы {sorted(FORMAT_MAP)}")
        return False

    return True


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
        'MAX_FILE_SIZE':   MAX_FILE_SIZE.get(ENV_STAND, '1GB'),
        'BUCKET':          BUCKET,
        'TFS_MAP':         TFS_MAP,
        'S3_CONN':         S3_CONN,
        'VAR_NAME':        VAR_NAME,
        'RAW_VAR_NAME':    RAW_VAR_NAME,
        'POOL_NAME':       POOL_NAME,
        'POOL_SLOTS':      POOL_SLOTS,
        'DEFAULT_PARAMS':  DEFAULT_PARAMS,
        'GROUP_PARAMS':    GROUP_PARAMS,
        'TABLE_PARAMS':    TABLE_PARAMS,
        'FORMAT_MAP':      FORMAT_MAP,
    }
