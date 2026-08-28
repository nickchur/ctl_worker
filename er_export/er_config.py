"""⚙️ Конфигурация, константы и сборщики фреймворка ER-выгрузок.
*2026-08-28 11:45 MSK · v1.14 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

CH-коннект (dlab-click) и S3 (s3-tfs-hrplt) фиксированы.

Поведение на стенде управляется ENV_STAND, при её отсутствии — ENVIRONMENT
(PROM / UAT / QA / IFT / DEV).

Кроме настроек здесь живёт сборка запроса и .meta (раздел «🏗️ Сборка запроса и .meta»):
её делят фабрика выгрузок er_export.py и даг настройки er_setup.py, а общий код обязан
лежать в модуле без DAG-ов.
"""
# ⛔ Здесь НЕ место декоратору @dag — даже для маленького служебного дага.
#
# DagBag добавляет к дагам разбираемого файла ещё и DagContext.autoregistered_dags,
# куда @dag складывает всё созданное за время разбора ТЕКУЩЕГО файла. Этот модуль
# импортируют er_export.py и er_setup.py — заведи мы тут @dag, один и
# тот же даг приписался бы обоим файлам сразу. Правило общее: модуль, который
# импортируют DAG-файлы, сам DAG-и создавать не должен.
#
# Подробнее и про то, почему нельзя освободить это имя переименованием, — в README.md,
# раздел «✏️ Правка настройки из UI».
from __future__ import annotations

import json
import logging
import os
import re
from datetime import timedelta
from typing import Any

# Ядро Airflow — импорт безопасен на любом стенде. Нужен сборщикам: непройденная сверка
# состава колонок ретраям не подлежит, там нечему меняться от повтора.
from airflow.exceptions import AirflowFailException

# Общие хелперы Airflow берём из plugins.utils, а не держим свои копии: заметки и
# колбэки должны вести себя одинаково во всех DAG-ах контура. add_note и ensure_pool
# здесь же реэкспортируются — их импортируют соседние модули этого каталога.
try:
    from plugins.utils import add_note, ensure_pool, get_dict_from_ch, on_callback, valid_schedule  # noqa: F401  # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, ensure_pool, get_dict_from_ch, on_callback, valid_schedule  # noqa: F401  # type: ignore

# 🌍 Стенд. Сначала ENV_STAND — именно её читают платформенные операторы
# (hrp_operators/clickhouse_to_s3.py: os.getenv("ENV_STAND")) и соседний xs_export.
# ENVIRONMENT остаётся запасным именем: раньше конфиг читал только её, и там, где
# выставлена лишь она, поведение не должно измениться. Ни одной — пустая строка,
# то есть лимитов стенда нет и берутся дефолты.
ENV_STAND = (os.getenv("ENV_STAND") or os.getenv("ENVIRONMENT") or "").strip().upper()

VAR_NAME = "datalab_er_wfs"

# Сырые строки er_wf_meta — как они лежат в таблице, без наследования и разрешения
# параметров. Нужны выпадающему списку записей в export_er_setup: он строится при разборе
# файла, а ходить в ClickHouse на каждом парсинге нельзя.
# Пишет их синк того же дага — он и так читает всю таблицу.
RAW_VAR_NAME = "datalab_er_wf_meta"

# Контрольная сумма последней успешной синхронизации: пока она сходится, синку нечего
# делать и он уходит в скип. Считается и по строкам таблицы, и по снимку конфига —
# подробности в er_setup.py, функция wf_checksum. Удалить переменную = пересчитать всё.
CKSUM_VAR_NAME = "datalab_er_wf_hash"

CH_ID   = 'dlab-click'
S3_CONN = 's3-tfs-hrplt'

BUCKET = 'tfshrplt'

# 🗺️ replica → (scenario_id, s3_prefix): используется в create_export_dag для маршрутизации в TFS
TFS_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er"),
}

POOL_NAME   = 'datalab_export_er'
POOL_SLOTS  = 20

# 🏊 Пул метки времени пакета — свой на каждую базовую реплику и ровно на один слот:
# он разводит по секундам таски make_ts разных групп одной реплики, а без этого тикеты
# групп совпали бы именем (суффикс группы в имя тикета не входит). Имя пула собирает
# ts_pool(), слот ровно один — увеличение слотов ломает саму цель пула.
TS_POOL_SLOTS = 1

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
    # ⏰ Расписание пакета. Живёт в params строки-дефолта группы, а не отдельной колонкой:
    # колонка позволяла задать его ещё и у поставки, и тогда «какое победит» зависело от
    # того, у кого поле заполнено. Умолчания у него намеренно нет — пустое значение это
    # ошибка группы, а не «раз в день»: пакет, поехавший не в своё окно, хуже непоехавшего.
    'schedule':          '',
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
    # 🔀 Режим выгрузки: 0 = дельта (окно из состояния), 1 = recent (скользящее окно).
    # Здесь, а не колонкой: у колонки UInt8 DEFAULT 0 «не задано» неотличимо от «явно
    # дельта», поэтому режим не наследовался от группы вовсе. В JSON ключ либо есть,
    # либо нет — наследование работает так же, как у остальных параметров.
    'is_recent':         0,
    'full_export':       0,            # 1 = выгружать таблицу целиком: окно дельты не подставляется, состояние не ведётся
    'increment':         60,           # шаг дельты, мин: time_to = time_from + increment (не чаще 1 пакета/час по стандарту ТФС)
    'overlap':           0,            # перекрытие окна дельты назад, сек (для компенсации задержек CDC)
    'lower_bound':       '',           # нижняя граница первой дельты (bootstrap); '' → 1970-01-01
    # Поле времени в источнике. insert_time — метка CDC, общая для источников контура
    # (её же используют выгрузки xs_export). Пустое допустимо только при full_export=1.
    # В поставках с JOIN или агрегацией задавайте его с префиксом таблицы ('s.insert_time'):
    # короткое имя там либо неоднозначно, либо затеняется одноимённым алиасом в SELECT.
    'time_field':        'insert_time',
    'recent_interval':   60,           # окно для режима recent, мин (используется вместо дельты)

    # ── Стратегия ────────────────────────────────────────────────────────────
    'strategy':          'FULL_UK',    # стратегия слияния на стороне TFS
    'export_timeout':    120,          # таймаут export_to_s3, мин

    # ── Файлы ────────────────────────────────────────────────────────────────
    # Формат — строка оператора: '500MB', '10GB' или просто число байт. Свойство таблицы:
    # ТФС ограничивает размер одинаково на всех стендах, а вот делить ли поток мельче,
    # зависит от самой выгрузки.
    'max_file_size':     '500MB',      # предел размера одного файла данных
    'send_empty':        0,            # 1 = слать пустой ZIP+Kafka при нулевой дельте

    # ── Формат и санитизация ─────────────────────────────────────────────────
    'format':            'TSVWithNames',  # формат выгрузки ClickHouse (ключ FORMAT_MAP)
    'pg_array_format':   0,            # 1 = PostgreSQL-формат массивов в TSV
    'csv_format_params': '',           # доп. параметры форматирования (dict-литерал)
    'xstream_sanitize':  0,            # 1 = экранировать спецсимволы XStream
    'sanitize_array':    0,            # 1 = санитизировать CH-массивы в строки
    'sanitize_list':     '',           # список колонок для санитизации (через запятую)

    # ── Описания для .meta ───────────────────────────────────────────────────
    # {колонка: описание}. Задаётся частично: что указано — перебивает комментарий
    # колонки источника, остальное берётся оттуда же, откуда и раньше. Единственный
    # словарный параметр, и сливается он ПО КЛЮЧАМ (см. merge_params): общие колонки
    # описываются раз в строке-дефолте группы, поставка добавляет свои.
    # Описание самой таблицы сюда не входит — для него у поставки есть колонка description.
    'descriptions':      {},
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

    'replica/extract_name' — ровно ключ сортировки таблицы. У строки-дефолта группы
    extract_name пуст, поэтому она помечается явно: в списке должно быть видно, что это
    настройка пакета, а не поставка.

    Разделитель '/' , а не '.': точка занята видом 'db.table', и на ключе с репликой
    читалась бы неоднозначно — база и реплика в именах различаются не всегда.
    """
    if not row.get('extract_name'):
        return f"{row.get('replica', '')} (дефолты группы)"
    return f"{row.get('replica', '')}/{row['extract_name']}"


def key_to_where(key: str) -> tuple[str, str]:
    """🔎 Ключ из выпадающего списка → (replica, extract_name) для WHERE.

    Обратная к raw_key: у строки-дефолта extract_name пуст.
    """
    marker = ' (дефолты группы)'
    if key.endswith(marker):
        return key[:-len(marker)], ''
    replica, _, name = key.partition('/')
    return replica, name


# Номер группы, который подставляется реплике без суффикса. Пустым он быть не может:
# имя архива строится как '{база}__{ts}__{группа}__{table}...', и реплика без суффикса дала
# бы на одно '__' меньше — принимающая сторона разбирает имя по разделителям.
DEFAULT_GROUP = '0'


# Строковые представления пустоты. Появляются, когда значение проходит через str(None)
# по дороге в ClickHouse: колонки там String, NULL в них не бывает, и в таблице оседает
# литерал 'None'. Для SQL-сборщика это обычный текст, поэтому запрос собирается вида
# `None SELECT … FROM t1 None WHERE … SETTINGS None` и падает синтаксической ошибкой
# уже в ClickHouse — с трассировкой, по которой причина не видна вовсе.
NULLISH = {'none', 'null', 'nan'}


def clean_value(value):
    """Значение настройки: 'None'/'NULL'/'NaN' строкой → пусто, остальное как есть.

    Числа, флаги и даты не трогаем: is_active = 0 после превращения в строку стал бы
    истинным и включил бы выключенную поставку.
    """
    if value is None:
        return ''
    if isinstance(value, (list, tuple)):
        return [v for v in (clean_value(x) for x in value) if v != '']
    if isinstance(value, str):
        return '' if value.strip().lower() in NULLISH else value
    return value


def clean_row(row: dict) -> dict:
    """Строка er_wf_meta без строковых 'None' — применять сразу после чтения из таблицы.

    Чистим на входе, а не при сборке SQL: 'None' в sql_from должен спотыкаться о проверку
    «пустой sql_from» с внятным текстом, а не доезжать до ClickHouse.
    """
    return {k: clean_value(v) for k, v in row.items()}


def ch_error(err) -> str:
    """Сообщение ClickHouse без стека: до 'Stack trace:'.

    Стек из двух десятков адресов в ошибке настройки не помогает никому, зато в заметке
    (add_note режет по MAX_NOTE_LEN) вытесняет собой текст самой причины.
    """
    return str(err).split('Stack trace:')[0].strip().rstrip('.')


def parse_s3_target(path: str, conn_id: str, bucket: str, prefix: str) -> dict:
    """🗺️ 'conn_id://bucket/dir' → {'conn_id', 'bucket', 'prefix'}; пусто — как настроено.

    Формат тот же, что у `s3_path_parse` в plugins/s3_utils и у путей в tools-дагах:
    слева от '://' стоит conn_id Airflow, а не протокол. Каталог необязателен —
    's3-archive://bucket' положит файлы в корень бакета.

    Разбираем сами, а не urlparse: по RFC 3986 в схеме допустимы только буквы, цифры,
    '+', '-' и '.', поэтому на 's3_minio://bucket/key' urlparse молча отдаёт пустые
    scheme и netloc — и дальше по коду улетает пустое имя бакета.
    """
    def _target(conn: str, buck: str, pref: str) -> dict:
        # key_prefix — то, что приписывается к имени файла. Пустой каталог не должен
        # давать ключ с ведущим слэшем: в S3 это объект с пустым первым сегментом пути.
        return {'conn_id': conn, 'bucket': buck, 'prefix': pref,
                'key_prefix': f"{pref}/" if pref else ''}

    path = (path or '').strip().strip('/')
    if not path:
        return _target(conn_id, bucket, prefix)

    head, sep, rest = path.partition('://')
    if not sep or not head or not rest:
        raise AirflowFailException(
            f"Путь выгрузки '{path}' не разобран. Нужен вид conn_id://bucket/dir, "
            f"например s3-archive://dataplatform-monitoring-dev/er_dump"
        )

    new_bucket, _, new_prefix = rest.partition('/')
    if not new_bucket:
        raise AirflowFailException(f"Путь выгрузки '{path}': не задан бакет")

    return _target(head, new_bucket, new_prefix.strip('/'))


def replica_full(replica: str) -> str:
    """🔢 Реплика с обязательным суффиксом группы: без него подставляется DEFAULT_GROUP.

    Нужна, чтобы у всех пакетов был один формат имени файла с одинаковым числом '__'.
    'hrplatform_datalab' → 'hrplatform_datalab__0', 'hrplatform_datalab__1' — как есть.
    """
    rep = (replica or '').strip()
    return rep if not rep or '__' in rep else f"{rep}__{DEFAULT_GROUP}"


def replica_base(replica: str) -> str:
    """🔀 Базовая реплика — часть до первого '__'; остальное считается номером группы.

    Группа поставок кодируется суффиксом в replica ('hrplatform_datalab__1'). В имена
    файлов базовая реплика идёт первой, суффикс группы — за меткой времени и только
    в имени архива; тикет пакета обходится без него вовсе. Маршрут в TFS (scenario_id +
    префикс в S3) один на всю реплику, поэтому TFS_MAP ищется по базе — новая группа
    заводится строкой в er_wf_meta, без правки кода.
    """
    return replica.split('__', 1)[0]


def replica_group(replica: str) -> str:
    """🔢 Суффикс группы — часть после первого '__'; без него подставляется DEFAULT_GROUP.

    В имени архива стоит сразу за меткой времени и разводит по именам архивы разных групп
    одной реплики. Пустым не бывает: replica_full() дописывает суффикс ещё при синхронизации,
    и число разделителей '__' в имени обязано быть одним на все пакеты.
    """
    return replica.split('__', 1)[1] if '__' in replica else DEFAULT_GROUP


def ts_pool(replica: str) -> str:
    """🏊 Имя пула метки времени — одно на БАЗОВУЮ реплику, на все её группы сразу.

    Именно на базовую: сталкиваются именами тикеты РАЗНЫХ ГРУПП одной реплики (суффикс
    из имени тикета убран, а logical_date у групп на одном cron совпадает до секунды),
    и развести их может только общий на эти группы пул с единственным слотом.
    Пул на группу был бы бесполезен — каждая заняла бы свой слот и стартовали бы разом.
    """
    return f"{POOL_NAME}_ts__{replica_base(replica)}"


# Параметры-словари, которые сливаются ПО КЛЮЧАМ, а не заменяются целиком.
DEEP_PARAMS = ('descriptions',)


def merge_params(base: dict, *overrides: dict) -> dict:
    """🔧 Слияние параметров: обычный ключ перебивается, словарь из DEEP_PARAMS сливается.

    Замена целиком годится для скаляров, но не для descriptions: групповое описание общих
    колонок исчезло бы, стоит поставке описать хоть одну свою.
    """
    out = dict(base)
    for over in overrides:
        for key, value in (over or {}).items():
            if key in DEEP_PARAMS and isinstance(value, dict) and isinstance(out.get(key), dict):
                out[key] = {**out[key], **value}
            else:
                out[key] = value
    return out


def get_params(row: dict, group: dict | None = None) -> dict:
    """🔧 Собирает итоговые параметры: DEFAULT_PARAMS → params группы → params строки.

    row   — запись er_wf_meta (или entry из Variable) с JSON-полем params
    group — параметры строки-дефолта группы; уже разрешённый dict либо None
    """
    overrides = json.loads(row.get('params') or '{}')
    return merge_params(DEFAULT_PARAMS, group or {}, overrides)


# 🧬 Поля строки-дефолта группы, наследуемые поставками. SQL и ключи (pk, uk, fields,
# sql_*) сюда намеренно не входят — они всегда описывают конкретную таблицу.
#
# db_name сюда тоже не входит: база источника у поставок пакета бывает разной, а у
# строки-дефолта её обычно нет вовсе — раньше она была занята под ключ сортировки.
#
# description обрабатывается отдельно (см. build_wfs): у него есть третий источник —
# комментарий таблицы в ClickHouse, и он должен быть приоритетнее группового текста.
#
# Остальное наследуется через params (merge_params), включая режим выгрузки is_recent
# и расписание группы.
INHERITED = ('schema_name',)

def parse_params(raw: str, where: str) -> dict:
    """Разбирает JSON-поле params; при битом JSON возвращает {} и пишет предупреждение."""
    try:
        return json.loads(raw or '{}')
    except json.JSONDecodeError as err:
        logger.warning("⚠️ %s: битый JSON в params (%s) — параметры проигнорированы", where, err)
        return {}


def explicit_schedule(row: dict) -> str:
    """Расписание, ЗАДАННОЕ В ЭТОЙ строке: ключ schedule её собственных params.

    Собственных — то есть без наследования: только так синк отличает расписание группы
    от расписания, по ошибке написанного в поставке (оно игнорируется с предупреждением).
    Пусто = не задано; умолчания у cron нет, пустое расписание ломает группу целиком.
    """
    row = row or {}
    params = parse_params(row.get('params', ''), f"строка {row.get('replica', '')}")
    return str(params.get('schedule') or '').strip()


def check_table(row: dict, key: str, errors: list[str], params: dict) -> bool:
    """Проверяет строку-поставку. Непрошедшая запись ломает всю группу, причина — в errors.

    params — уже слитые параметры (дефолты + группа + таблица): проверять надо именно их,
    иначе опечатка в params строки-дефолта проходит синк и роняет разбор файла в фабрике.
    """
    if not row["sql_from"]:
        errors.append(f"{key}: пустой sql_from")
        return False

    # Раньше пустой db_name бросался в глаза сам: он входил в ключ записи, и поставка
    # называлась '.lc_items_opened'. Теперь ключ строится из реплики, и проверка нужна
    # явная — база источника нужна и для описаний колонок в .meta, и для FROM без
    # квалификатора.
    if not row.get("db_name"):
        errors.append(f"{key}: пустой db_name — не из чего достроить имя таблицы-источника")
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

    # Поля времени у таблицы может не быть вовсе — тогда дельта невозможна физически:
    # окно строится как 'from' < time_field and time_field <= 'to'. Такую поставку
    # выгружаем целиком, и это должно быть заявлено явно, а не выясняться в рантайме
    # запросом к несуществующей колонке.
    if not params.get('full_export') and not str(params.get('time_field') or '').strip():
        errors.append(
            f"{key}: пустой time_field — без поля времени окно дельты не построить. "
            "Поставьте full_export=1, если таблицу надо выгружать целиком"
        )
        return False

    fmt = params.get('format', DEFAULT_PARAMS['format'])
    if fmt not in FORMAT_MAP:
        errors.append(f"{key}: неизвестный format '{fmt}', допустимы {sorted(FORMAT_MAP)}")
        return False

    return True


# ── 🏗️ Сборка запроса и .meta ─────────────────────────────────────────────────
#
# Здесь и только здесь собирается SQL выгрузки и описание колонок для ЕР. Раньше всё
# это лежало в er_export.py, но тем же кодом обязан пользоваться даг настройки
# export_er_setup: проверка, собирающая запрос по-своему, проверяет не то, что поедет
# ночью, — а разъезжаться две сборки начнут на первой же правке. Импортировать er_export
# ради этого нельзя: это файл с DAG-ами, и его разбор поднял бы всю фабрику.


# Hive keywords (all versions, reserved + non-reserved) — имена колонок из этого набора
# получают суффикс '_' согласно требованиям KAP/TFS (раздел 11 документации ЕР).
HIVE_RESERVED: frozenset = frozenset({
    # 1.2 non-reserved
    'add','admin','after','analyze','archive','asc','before','bucket','buckets','cascade',
    'change','cluster','clustered','clusterstatus','collection','columns','comment','compact',
    'compactions','compute','concatenate','continue','data','databases','datetime','day',
    'dbproperties','deferred','defined','delimited','dependency','desc','directories',
    'directory','disable','distribute','enable','escaped','exclusive','explain','export',
    'fields','file','fileformat','first','format','formatted','functions','hold_ddltime',
    'hour','idxproperties','ignore','index','indexes','inpath','inputdriver','inputformat',
    'items','jar','keys','limit','lines','load','location','lock','locks','logical','long',
    'mapjoin','materialized','metadata','minus','minute','month','msck','noscan','no_drop',
    'offline','option','outputdriver','outputformat','overwrite','owner','partitioned',
    'partitions','plus','pretty','principals','protection','purge','read','readonly',
    'rebuild','recordreader','recordwriter','regexp','reload','rename','repair','replace',
    'replication','restrict','rewrite','rlike','role','roles','schema','schemas','second',
    'semi','serde','serdeproperties','server','sets','shared','show','show_database',
    'skewed','sort','sorted','ssl','statistics','stored','streamtable','string','struct',
    'tables','tblproperties','temporary','terminated','tinyint','touch','transactions',
    'unarchive','undo','uniontype','unlock','unset','unsigned','uri','use','utc','view',
    'while','year',
    # 1.2 reserved
    'all','alter','and','array','as','authorization','between','bigint','binary','boolean',
    'both','by','case','cast','char','column','conf','create','cross','cube','current',
    'current_date','current_timestamp','cursor','database','date','decimal','delete',
    'describe','distinct','double','drop','else','end','exchange','exists','extended',
    'external','false','fetch','float','following','for','from','full','function','grant',
    'group','grouping','having','if','import','in','inner','insert','int','intersect',
    'interval','into','is','join','lateral','left','less','like','local','macro','map',
    'more','none','not','null','of','on','or','order','out','outer','over','partialscan',
    'partition','percent','preceding','preserve','procedure','range','reads','reduce',
    'revoke','right','rollup','row','rows','select','set','smallint','table','tablesample',
    'then','timestamp','to','transform','trigger','true','truncate','unbounded','union',
    'uniquejoin','update','user','using','utc_tmestamp','values','varchar','when','where',
    'window','with',
    # 2.0+
    'autocommit','isolation','level','offset','snapshot','transaction','work','write',
    'commit','only','rollback','start',
    # 2.1+
    'abort','key','last','norely','novalidate','nulls','rely','validate',
    'cache','constraint','foreign','primary','references',
    # 2.2+
    'days','dayofweek','dump','hours','matched','merge','minutes','months','quarter',
    'repl','seconds','status','views','week','weeks','years',
    'except','extract','floor','integer','precision',
    # 2.3+
    'detail','expression','operator','summary','vectorization','wait',
    # 3.0+
    'activate','active','alloc_fraction','check','default','do','enforced','kill',
    'management','mapping','move','path','plan','plans','pool','query',
    'query_parallelism','reoptimization','resource','scheduling_policy','unmanaged',
    'workload','zone',
    'any','application','dec','numeric','sync','time','timestamplocaltz','unique',
    # 4.0+
    'ast','at','branch','cbo','cost','cron','dcproperties','debug','disabled',
    'distributed','enabled','every','execute','executed','expire_snapshots','joincost',
    'managed','managedlocation','optimize','remote','respect','retain','retention',
    'scheduled','set_current_snapshot','snapshots','spec','system_time','system_version',
    'tag','transactional','trim','type','unknown','url','within',
    'compactionid','connector','connectors','convert','ddl','force','leading','older',
    'pkfk_join','prepare','qualify','real','some','than','trailing',
})

def build_sql(sql_meta: str | dict, indent: str = "    ") -> str:
    """Собирает SQL-запрос из словаря метаданных или возвращает строку как есть.

    Поддерживаемые ключи словаря:
      with     — CTE-блок (WITH ...)
      fields   — list[str] или str; если не задан, используется '*'
      from     — обязательный FROM-clause
      joins    — JOIN-clause (опционально)
      where    — WHERE-условие (опционально)
      settings — SETTINGS-блок ClickHouse (опционально)
    """
    if not sql_meta: return ""
    if isinstance(sql_meta, str): return sql_meta

    # Части чистим и здесь, а не только на входе синка: фабрика собирает запрос
    # по Variable, а та могла быть записана до чистки — тогда 'None' уехал бы в запрос.
    sql_meta = {k: clean_value(v) if k != 'fields' else v for k, v in sql_meta.items()}

    parts = []
    if sql_meta.get("with"): parts.append(sql_meta['with'])

    fields = sql_meta.get("fields", [])
    if isinstance(fields, list):
        fields_str = f",\n{indent}".join(fields) if fields else "*"
    else:
        fields_str = fields or "*"
    parts.append(f"SELECT\n{indent}{fields_str}\nFROM {sql_meta['from']}")

    if sql_meta.get("joins"):    parts.append(sql_meta['joins'])
    if sql_meta.get("where"):    parts.append(f"WHERE {sql_meta['where']}")
    if sql_meta.get("settings"): parts.append(f"SETTINGS {sql_meta['settings']}")

    return "\n".join(parts)


def with_condition(where: str | None, full: bool = False, elsewhere: str = '') -> str:
    """🕐 Дописывает окно дельты в WHERE, если его нет больше нигде в запросе.

    При full=True (параметр таблицы full_export) окно не подставляется вовсе: условие
    остаётся тем, что задано в sql_where, а пустое даёт запрос совсем без WHERE.
    Так выгружается таблица, у которой поля времени нет в принципе.

    Раньше плейсхолдер `{condition}` писали руками в поле sql_where таблицы. Забыл его —
    и выгрузка молча уезжала таблицей целиком на каждом ране; заметно это становилось
    только по объёму пакета. Теперь окно дописывается само, а в sql_where остаётся
    только бизнес-фильтр.

    Плейсхолдер сохраняется как отдушина: он нужен, когда окно надо положить внутрь CTE
    или подзапроса — там это бывает на порядок быстрее внешнего WHERE.

    elsewhere — остальные части запроса (sql_with, sql_from, sql_join) одной строкой.
    Плейсхолдер в них считается таким же указанием «окно поставлено вручную», как и в самом
    WHERE. Без этого окно, положенное только в CTE, дублировалось наружу: снаружи после
    GROUP BY колонки времени уже нет, и запрос падал с UNKNOWN_IDENTIFIER — обойти это
    удавалось лишь фиктивным упоминанием плейсхолдера в sql_where.
    """
    w = (where or '').strip()
    if full:
        return w
    if '{condition}' in w or '{condition}' in (elsewhere or ''):
        return w
    if not w:
        return '{condition}'
    return '{condition} AND (' + w + ')'

def safe_name(name: str) -> str:
    """Имя колонки для .meta: совпавшее с зарезервированным словом Hive получает суффикс '_'."""
    return name + '_' if name.lower() in HIVE_RESERVED else name


def field_name(expr: str) -> str | None:
    """Сырое имя колонки, которое даст выражение из fields. None — если вывести его нельзя.

    Надёжных случаев два: явный алиас в конце ('expr as name') и простая колонка, возможно
    с квалификатором ('t1.col'). Всё остальное — выражение вроде cast(x as String): наивный
    split по ' as ' дал бы 'String)', а ClickHouse назовёт колонку по-своему. Такие поля
    честно помечаем неизвестными, иначе сверка в build_meta падала бы вечно и без шансов
    на успешный ретрай.

    Имя возвращается БЕЗ hive-суффикса: по нему ищут колонку в DESCRIBE TABLE, где имена
    сырые. safe_name() навешивается уже на результат, при формировании .meta.
    """
    expr = str(expr).strip()
    alias = re.search(r'\sas\s+([A-Za-z_]\w*)\s*$', expr, re.I)
    if alias:
        return alias.group(1)
    if re.fullmatch(r'[A-Za-z_]\w*(\.[A-Za-z_]\w*)*', expr):
        return expr.rsplit('.', 1)[-1]
    return None


def cols_from_fields(fields: list, ch_cols: dict, describe_rows: list) -> list[dict]:
    """Колонки .meta по списку fields — запасной путь, когда DESCRIBE запроса недоступен.

    Работает, если sql_meta пуст (SQL задан строкой или со своим списком fields) либо если
    DESCRIBE (<запрос>) упал. JOIN-ы и вычисляемые выражения тут не учитываются: имя каждой
    колонки выводится из самого выражения.

    Выражение, у которого имя не вывести (нет алиаса, не простая колонка), — ошибка.
    Взять имя неоткуда, а .meta управляет загрузкой в КАП: молча уехавшее 'String)' вместо
    имени колонки хуже падения, и сверка состава его не поймает — для таких выражений она
    проверяет только количество.

    ch_cols       — {имя колонки источника: описание} по DESCRIBE TABLE
    describe_rows — сырые строки DESCRIBE TABLE, нужны для ветки «все колонки»
    """
    # 't1.*' — тот же «все колонки таблицы», что и '*', но с явным алиасом.
    # Через синхронизацию сюда уже не попасть (fields обязателен и звёздочка запрещена),
    # ветка оставлена для обратной совместимости со старыми записями Variable.
    if not fields or fields in (['*'], '*') or all(str(f).strip().endswith('.*') for f in fields):
        return [{**ch_cols[r[0]], "column_name": safe_name(r[0])} for r in describe_rows]

    out = []
    for f in fields:
        name = field_name(f)
        if name is None:
            raise AirflowFailException(
                f"Не удалось определить имя колонки для выражения '{f}'. "
                "Схема строится по DESCRIBE TABLE источника (запрос разобрать не удалось "
                "или он задан строкой), поэтому имя берётся из самого выражения. "
                f"Добавьте алиас в fields: '{f} as <имя_колонки>'"
            )
        base = ch_cols.get(name, {
            "column_name": name, "source_type": "STRING", "length": None,
            "notnull": False, "precision": None, "scale": None,
            "description": f"Calculated: {f}",
        })
        out.append({**base, "column_name": safe_name(base["column_name"])})
    return out


def parse_ch_type(ch_type: str, mapping: dict) -> tuple[str, bool, int | None, int | None, int | None]:
    """Раскрывает обёртки LowCardinality/Nullable и маппирует базовый CH-тип в целевой.

    Возвращает (target_type, notnull, length, precision, scale).
    FixedString(N) → length=N; Decimal(P,S) → precision=P, scale=S, length=P.
    Неизвестные базовые типы по умолчанию маппируются в STRING.
    """
    notnull = True
    length = precision = scale = None
    if ch_type.startswith("LowCardinality("): ch_type = ch_type[15:-1]
    if ch_type.startswith("Nullable("):
        ch_type = ch_type[9:-1]
        notnull = False
    base = ch_type.split("(")[0]
    if "(" in ch_type:
        args = [a.strip() for a in ch_type[len(base) + 1:-1].split(",")]
        try:
            if base == "FixedString":
                length = int(args[0])
            elif base == "Decimal" and len(args) == 2:
                precision = int(args[0])
                scale     = int(args[1])
                length    = precision
        except (ValueError, IndexError):
            pass
    return mapping.get(base, "STRING"), notnull, length, precision, scale


# Маска квалифицированного имени: db.table. Ловит и алиасы колонок (t1.person_uuid) —
# отсеются проверкой в ClickHouse, зато не нужен разбор SQL.
_QUALIFIED = re.compile(r'\b([A-Za-z_]\w*)\.([A-Za-z_]\w*)\b')
# Первое слово FROM-части: имя без базы достраивается db_name записи.
_FIRST_WORD = re.compile(r'^\s*([A-Za-z_]\w*)')


def _quote(value) -> str:
    """Строковый литерал ClickHouse."""
    return "'" + str(value).replace("'", "''") + "'"


def sql_sources(parts: dict, db_name: str = '', extract_name: str = '') -> list[tuple[str, str]]:
    """🔎 Таблицы, к которым обращается запрос, в порядке приоритета: from → with → joins.

    Приоритет решает, чьё описание достанется колонке, встречающейся в нескольких
    таблицах: побеждает основная, из FROM.

    Имена ищутся маской `db.table`; в FROM имя без базы достраивается db_name записи —
    там первое слово это гарантированно таблица, а в JOIN-ах и CTE так гадать нельзя.
    Хвостом идёт (db_name, extract_name): у выгрузки, названной по своей таблице, это
    та же таблица, а у прочих пара просто не найдётся.

    Мусорные пары (алиасы вида t1.person_uuid) не отсеиваем: разбирать SQL ради этого
    дороже, чем проверить кандидатов одним запросом в system.columns.

    where и settings не смотрим: таблиц там не бывает, а условия дают ложные пары.
    """
    out: list[tuple[str, str]] = []

    def _add(db: str, table: str) -> None:
        if db and table and (db, table) not in out:
            out.append((db, table))

    for key in ('from', 'with', 'joins'):
        text = str(parts.get(key) or '').strip()
        if not text:
            continue
        if key == 'from' and '.' not in text.split()[0]:
            if first := _FIRST_WORD.match(text):
                _add(db_name, first.group(1))
        for db, table in _QUALIFIED.findall(text):
            _add(db, table)

    _add(db_name, extract_name)
    return out


def ch_source_columns(hook, sources: list) -> tuple[dict, list]:
    """💬 {имя колонки: описание} по таблицам-источникам плюс список найденных таблиц.

    ОДИН запрос в system.columns на всех кандидатов сразу, а не DESCRIBE на каждого:
    половина кандидатов таблицами не является (это алиасы), и падающий запрос на каждого
    засорял бы лог. Строки приводятся к форме DESCRIBE, поэтому разбор остаётся общий —
    ch_columns(). Мержим по приоритету источников: первый выигрывает.
    """
    if not sources:
        return {}, []

    pairs = ", ".join(f"({_quote(db)}, {_quote(tbl)})" for db, tbl in sources)
    rows = get_dict_from_ch(hook, f"""
        SELECT database, table, name, type, comment
        FROM system.columns
        WHERE (database, table) IN ({pairs})
        ORDER BY database, table, position
    """)

    by_table: dict[tuple[str, str], list] = {}
    for r in rows:
        # (имя, тип, default_kind, default_expr, комментарий) — как отдаёт DESCRIBE TABLE
        by_table.setdefault((r['database'], r['table']), []).append(
            (r['name'], r['type'], '', '', r['comment'])
        )

    merged: dict = {}
    found: list = []
    for src in sources:
        if not (tbl_rows := by_table.get(src)):
            continue
        found.append(src)
        for name, col in ch_columns(tbl_rows).items():
            merged.setdefault(name, col)
    return merged, found


def ch_table_comments(hook, sources_by_key: dict) -> dict:
    """💬 {ключ записи: комментарий первой найденной таблицы} — батчем на все записи.

    Комментарий берётся у той же таблицы, что дала описания колонок, а не у
    db_name.extract_name: имя выгрузки таблице соответствовать не обязано.
    """
    all_pairs = {src for sources in sources_by_key.values() for src in sources}
    if not all_pairs:
        return {}

    pairs = ", ".join(f"({_quote(db)}, {_quote(tbl)})" for db, tbl in sorted(all_pairs))
    comments = {
        (r['database'], r['name']): r['comment']
        for r in get_dict_from_ch(hook, f"""
            SELECT database, name, comment FROM system.tables WHERE (database, name) IN ({pairs})
        """)
    }
    return {
        key: next((comments[src] for src in sources if comments.get(src)), '')
        for key, sources in sources_by_key.items()
    }


def ch_columns(describe_rows: list) -> dict:
    """{имя колонки: описание для .meta} по строкам `DESCRIBE TABLE <источник>`.

    Комментарий колонки берётся пятым полем DESCRIBE. У результата подзапроса комментариев
    нет вовсе, поэтому описание источника — единственное место, откуда их можно взять.
    """
    out = {}
    for row in describe_rows:
        stype, notnull, length, precision, scale = parse_ch_type(row[1], TYPE_MAP)
        comment = row[4] if len(row) > 4 else ""
        out[row[0]] = {
            "column_name": row[0], "source_type": stype, "length": length,
            "notnull": notnull, "precision": precision, "scale": scale,
            "description": comment or None,
        }
    return out


def query_columns(qrows: list, ch_cols: dict) -> list[dict]:
    """Колонки .meta по описанию колонок запроса — так же, как их видит TSVWithNames.

    qrows — то, что отдаёт `DESCRIBE (<запрос>)` либо второй элемент execute(...,
    with_column_types=True): пары (имя, тип) с необязательным хвостом.

    Единственный способ получить состав, совпадающий с заголовком файла данных при JOIN,
    алиасах и вычисляемых выражениях. description подмешивается из ch_cols по имени колонки.
    """
    out = []
    for r in qrows:
        stype, notnull, length, precision, scale = parse_ch_type(r[1], TYPE_MAP)
        out.append({
            "column_name": safe_name(r[0]), "source_type": stype, "length": length,
            "notnull": notnull, "precision": precision, "scale": scale,
            "description": ch_cols.get(r[0], {}).get("description"),
        })
    return out


def check_fields(fields: list, actual: list[str], key: str) -> list[str]:
    """🔍 Сверяет состав колонок запроса с настройкой fields. Возвращает список ошибок.

    Смысл всей проверки: новая колонка в источнике не должна доезжать до КАП сама —
    только через правку fields в er_wf_meta. Заодно ловится опечатка в выражении,
    у которого ClickHouse выведет неожиданное имя.

    Именно список, а не исключение: выгрузке нужен красный таск и она поднимает его сама,
    а дагу настройки — отчёт, где эта ошибка стоит рядом с остальными.

    actual приходит уже с hive-суффиксами (safe_name при сборке колонок), поэтому
    и ожидаемые имена приводятся к тому же виду. Позиции с невыводимым именем (выражения
    без алиаса) сверяются только по количеству: имя такой колонки определяет ClickHouse,
    и предсказать его мы не берёмся.
    """
    expected = [safe_name(n) if n is not None else None
                for n in (field_name(f) for f in fields)]

    if len(actual) != len(expected):
        return [
            f"Состав колонок {key} разошёлся с настройкой fields: "
            f"запрос вернул {len(actual)} колонок, в настройке {len(expected)}.\n"
            f"  запрос:    {actual}\n"
            f"  настройка: {fields}\n"
            "Если изменение источника ожидаемо — поправьте fields в export.er_wf_meta"
        ]

    mismatch = [(i, e, a) for i, (e, a) in enumerate(zip(expected, actual)) if e is not None and e != a]
    if not mismatch:
        return []

    # Точка в имени колонки запроса — почти всегда квалификатор таблицы, а не изменение
    # источника, и совет «поправьте fields» тут уводит не туда: список полей верен,
    # не хватает алиаса. Отдаём готовые строки замены, чтобы не разбираться заново.
    dotted = [i for i, _, a in mismatch if '.' in a]
    hint = ''
    if dotted:
        fixes = "\n".join(f"    '{fields[i]} AS {field_name(fields[i])}'" for i in dotted[:5])
        hint = (
            "\nВ именах колонок запроса остался квалификатор таблицы. При ДВУХ и более "
            "JOIN-ах ClickHouse сохраняет префикс у колонки, чьё короткое имя есть ещё "
            "в одной из соединяемых таблиц (с одним JOIN-ом префикс срезается — то есть "
            "поведение меняется от добавления третьей таблицы). Точка уедет и в заголовок "
            f"файла данных, и в .meta. Лечится алиасом в fields:\n{fixes}"
        )
    return [
        f"Состав колонок {key} разошёлся с настройкой fields.\n"
        + "\n".join(f"  позиция {i}: запрос '{a}', настройка '{e}'" for i, e, a in mismatch)
        + hint
        + "\nЕсли изменение источника ожидаемо — поправьте fields в export.er_wf_meta"
    ]


def unnamed_fields(fields: list) -> list:
    """Выражения, имя колонки для которых не вывести, — сверка их не проверяет."""
    return [f for f in fields if field_name(f) is None]


def fit_descriptions(descriptions: dict, fields: list) -> dict:
    """✂️ Оставляет из словаря описаний только колонки, которые есть в этой выгрузке.

    Нужна для ГРУППОВЫХ описаний: словарь строки-дефолта достаётся всем поставкам пакета,
    и общая колонка (person_uuid, extract_time) есть не у каждой. Без отсева строгая
    проверка check_descriptions роняла бы такие поставки — проверено живьём на стенде:
    групповой словарь из двух колонок положил поставку, где нет ни одной из них.

    Описания самой поставки не фильтруются: там лишнее имя — это опечатка, и она обязана
    всплыть ошибкой.
    """
    if not descriptions:
        return {}
    known = {safe_name(n) for n in (field_name(f) for f in fields or []) if n}
    known |= {c['column_name'] for c in EXTRA_PRE + EXTRA_SUF}
    return {k: v for k, v in descriptions.items() if safe_name(k) in known}


def check_descriptions(descriptions: dict, actual: list[str], key: str) -> list[str]:
    """🔍 Колонки из params.descriptions, которых в выгрузке нет. Возвращает список ошибок.

    Ошибка, а не предупреждение: описание, написанное для колонки с опечаткой, молча
    не доедет до КАП, и заметить это можно только глазами в готовом .meta.

    actual — колонки данных; служебные (export_time, ctl_action, ctl_validfrom) добавляются
    здесь же: их в .meta тоже можно описать, а вызывающему знать про них незачем.

    Сравниваем через safe_name: в настройке пишут имя как в fields, а в .meta оно уже
    приведено (Hive-слова получают суффикс '_').
    """
    if not descriptions:
        return []

    known = set(actual) | {c['column_name'] for c in EXTRA_PRE + EXTRA_SUF}
    unknown = [name for name in descriptions if safe_name(name) not in known]
    if not unknown:
        return []
    return [
        f"{key}: в params.descriptions описаны колонки, которых нет в выгрузке: {unknown}.\n"
        f"  колонки выгрузки: {actual}"
    ]


def build_meta(cfg: dict, data_cols: list[dict], strategy: str = '') -> dict:
    """🗂️ Готовый .meta для ЕР/TFS.

    Порядок колонок: export_time (PRE) + data_cols + ctl_action, ctl_validfrom (SUF).
    UK передаётся плоским массивом: ['id'] (стандарт ЕР).

    strategy — значение из состояния дельты; пустое означает «взять из настройки таблицы».
    """
    def _clean(cols):
        return [{k: v for k, v in c.items() if k != 'sql'} for c in cols]

    # 📝 Описания из настройки перебивают комментарии источника — но только там, где заданы.
    # Служебные колонки (export_time, ctl_action, ctl_validfrom) описать тоже можно:
    # накладываем на итоговый список, а не на data_cols.
    columns = _clean(EXTRA_PRE) + data_cols + _clean(EXTRA_SUF)
    if own := {safe_name(k): v for k, v in (cfg.get('descriptions') or {}).items()}:
        columns = [{**c, "description": own.get(c['column_name'], c['description'])}
                   for c in columns]

    return {
        "mask_file":   None,
        "schema_name": cfg['schema_name'],
        "table_name":  cfg['tbl'],
        "description": cfg.get('description') or None,
        "strategy":    strategy or cfg['strategy'],
        "PK":          clean_value(cfg['PK']),
        "UK":          clean_value(cfg['UK'] or []),
        "params":      FORMAT_MAP[cfg['format']]['meta_params'],
        "columns":     columns,
    }


def export_sql(entry: dict, params: dict, table_key: str = '') -> dict:
    """🧱 SQL поставки по записи Variable: {'sql_key', 'sql_export', 'sql_meta'}.

    entry     — запись из Variable (fields, sql_stmt_export_delta | sql_stmt_export_recent)
    params    — уже слитые параметры таблицы (get_params)
    table_key — 'db.table', только для текстов ошибок

    sql_export — то, что уедет в оператор: служебные колонки, окно дельты, лимит стенда.
    sql_meta   — тот же запрос без служебных колонок; по нему build_meta делает DESCRIBE.
                 Пуст, если запрос задан строкой или со своим списком fields.
    """
    fields = clean_value(entry.get("fields") or [])
    if not fields or any(str(f).strip() == '*' or str(f).strip().endswith('.*') for f in fields):
        raise AirflowFailException(
            f"{table_key}: fields должен быть явным списком колонок, '*' и 't1.*' запрещены"
        )

    def _prep(key):
        """Читает SQL-метадату по ключу, добавляет обязательные поля и окно дельты."""
        m = entry.get(key)
        if isinstance(m, dict) and "fields" not in m:
            m = {**m, "fields": [c['sql'] for c in EXTRA_PRE] + fields + [c['sql'] for c in EXTRA_SUF]}
        if isinstance(m, dict):
            # Плейсхолдер ищем и в остальных частях запроса: окно, положенное внутрь CTE
            # или подзапроса FROM, снаружи дописывать не надо. settings и fields сюда не
            # входят — окну там не место.
            rest = ' '.join(str(m.get(k) or '') for k in ('with', 'from', 'joins'))
            m = {**m, "where": with_condition(m.get("where"), full=bool(params['full_export']),
                                              elsewhere=rest)}
        return build_sql(m)

    def _prep_data(key):
        """Тот же запрос, но только с data-колонками — build_meta делает по нему DESCRIBE."""
        m = entry.get(key)
        return build_sql({**m, "fields": fields}) if isinstance(m, dict) and "fields" not in m else ""

    sql_delta, sql_recent = _prep('sql_stmt_export_delta'), _prep('sql_stmt_export_recent')
    if not (sql_delta or sql_recent) or (sql_delta and sql_recent):
        raise AirflowFailException(f"{table_key}: нужен ровно один из delta/recent SQL-запросов")

    sql_exp = sql_delta or sql_recent
    if LIMITS.get(ENV_STAND, 0) > 0:
        sql_exp = f"SELECT * FROM ({sql_exp}) LIMIT {LIMITS[ENV_STAND]}"

    return {
        'sql_key':    'sql_stmt_export_delta' if sql_delta else 'sql_stmt_export_recent',
        'sql_export': sql_exp,
        'sql_meta':   _prep_data('sql_stmt_export_delta') or _prep_data('sql_stmt_export_recent'),
    }


def probe_sql(sql: str) -> str:
    """🔍 Запрос проверки: тот же SQL, но заведомо без данных.

    `{condition}` подменяется ложным условием, `{export_time}` — временем: подстановка
    адресными replace, а не str.format, потому что format спотыкается о любую фигурную
    скобку в SQL (JSON-функции, map(), литерал '{}').

    Внешний LIMIT 0 — не украшение. У поставки с full_export плейсхолдера окна нет вовсе,
    и без него проверка прочитала бы таблицу целиком. ClickHouse обрывает конвейер на
    LIMIT 0 до чтения — проверено на бесконечном system.numbers, в том числе с GROUP BY.
    """
    q = sql.replace('{export_time}', 'now64(6)').replace('{condition}', '1=0')
    return f"SELECT * FROM (\n{q}\n) LIMIT 0"


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
        'RAW_VAR_NAME':    RAW_VAR_NAME,
        'CKSUM_VAR_NAME':  CKSUM_VAR_NAME,
        'POOL_NAME':       POOL_NAME,
        'POOL_SLOTS':      POOL_SLOTS,
        'TS_POOL_SLOTS':   TS_POOL_SLOTS,
        'DEFAULT_PARAMS':  DEFAULT_PARAMS,
        'GROUP_PARAMS':    GROUP_PARAMS,
        'TABLE_PARAMS':    TABLE_PARAMS,
        'FORMAT_MAP':      FORMAT_MAP,
    }
