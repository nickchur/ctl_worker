"""🚀 DAG-фабрика ER-выгрузок (ClickHouse → S3 → TFS).
*2026-08-12 16:30 MSK · v2.5 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Один DAG — один пакет — одна группа поставок — один внешний тикет. Группа задаётся
значением `replica` целиком (суффикс после '__'), внутри DAG-а по TaskGroup на таблицу:

  <db>__<tbl>: init → [build_meta, export_to_s3] → pack_zip
                                                      ↓
  make_summary → notify_tfs → wait_confirm → save_status → schedule_next

Метаданные выгрузок хранятся в Airflow Variable `datalab_er_wfs` (JSON-словарь),
который синхронизируется DAG-ом export_er_sync из таблицы export.er_wf_meta.

Поддерживаемые режимы выгрузки:
  📈 delta  — инкрементальный, окно [time_from, time_to] из export.extract_current_vw
  🔄 recent — скользящее окно [now() - recent_interval, now()], без сохранения состояния
  📅 период — ручной запуск с date_from/date_to; состояние дельты при этом не двигается
"""
from __future__ import annotations

import ast
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param
from airflow.utils.task_group import TaskGroup

try:
    from CI06932748.analytics.datalab.export_er.er_config import (  # type: ignore
        get_config, get_dict_from_ch, obj_load, add_note, get_params, replica_base,
    )
except ImportError:
    from er_export.er_config import get_config, get_dict_from_ch, obj_load, add_note, get_params, replica_base


def _tfs():
    """Слой тракта ТФС — общий с tfs_kafka: STORAGE живёт в одном месте, иначе писатель
    и читатель разъедутся и пакет зависнет без внятной причины.

    Импорт ЛЕНИВЫЙ, на уровне модуля его нет намеренно. Во-первых, транспорт нужен только
    таскам постановки в очередь и ожидания квитанции, а при `notify_kafka: 0` не нужен
    вовсе. Во-вторых, модуль этот — фабрика: жёсткий импорт при неполной выкладке ронял
    Broken DAG'ом ВСЕ пакеты ЕР разом, включая те, что в ТФС ничего не шлют.
    """
    try:
        from plugins import tfs_utils  # type: ignore
    except ImportError:
        from CI06932748.tools import tfs_utils  # type: ignore
    return tfs_utils

logger = logging.getLogger("airflow.task")

# ── Configuration & Constants ────────────────────────────────────────────────

_cfg = get_config()
CH_ID          = _cfg['CH_ID']
TYPE_MAP       = _cfg['TYPE_MAP']
DEF_ARGS       = _cfg['DEF_ARGS']
ENV_STAND      = _cfg['ENV_STAND']
EXTRA_PRE      = _cfg['EXTRA_PRE']
EXTRA_SUF      = _cfg['EXTRA_SUF']
LIMITS         = _cfg['LIMITS']
BUCKET         = _cfg['BUCKET']
TFS_MAP         = _cfg['TFS_MAP']
S3_CONN         = _cfg['S3_CONN']
VAR_NAME        = _cfg['VAR_NAME']
FORMAT_MAP      = _cfg['FORMAT_MAP']

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

# ── SQL Builders ──────────────────────────────────────────────────────────────

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


def sql_cur_delta(tbl: str) -> str:
    """SQL для получения текущего состояния дельты из export.extract_current_vw.

    Все значения возвращаются как строки-SQL-литералы ('2024-01-01' или null),
    чтобы их можно было подставлять напрямую в шаблонные SQL-запросы через str.format().
    """
    return build_sql({
        "fields": [
            "toString(a.num_state) as num_state",
            "concat('\\'', toString(a.extract_time), '\\'') as extract_time",
            "ifNull(toString(a.extract_count), 'null') as extract_count",
            "if(a.extract_count is null, 'null', concat('\\'', toString(a.loaded), '\\'')) as loaded",
            "if(a.extract_count is null, 'null', concat('\\'', toString(a.sent), '\\'')) as sent",
            "if(a.extract_count is null, 'null', concat('\\'', toString(a.confirmed), '\\'')) as confirmed",
            "toString(a.increment) as increment",
            "toString(a.overlap) as overlap",
            "concat('\\'', a.time_field, '\\'') as time_field",
            "concat('\\'', toString(a.time_from), '\\'') as time_from",
            "concat('\\'', toString(a.time_to), '\\'') as time_to",
            "concat('\\'', toString(a.time_from), '\\' < ', a.time_field, ' and ', a.time_field, ' <= \\'', toString(a.time_to), '\\'') as condition",
            "if(a.current_time = a.extract_time, 'True', 'False') as is_current",
            "toString(0) as recent_interval",
        ],
        "from": f"(SELECT * FROM export.extract_current_vw WHERE extract_name = '{tbl}') as a",
    })

# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_val(v: Any) -> str:
    """None → 'null', иначе → SQL-строковый литерал в одинарных кавычках."""
    return 'null' if v is None else f"'{v}'"


def _run_ts(context) -> datetime:
    """🕐 Единая точка отсчёта пакета — logical_date рана.

    От неё строятся имена ВСЕХ файлов пакета: и архивов по каждой таблице, и общего тикета,
    а ЕР требует у них одинаковый ts. Поэтому значение обязано быть неизменным.

    Именно logical_date, а не dag_run.start_date: последний перештамповывается при каждом
    переходе рана в RUNNING, то есть после clear упавшей таблицы её архивы получили бы новое
    время, а тикет — третье, и пакет развалился бы на разные метки. logical_date у рана одна
    навсегда. Она же стоит в ключе промежуточного файла ({{ ts_nodash }}), так что имена
    сходятся на всех этапах.
    """
    ts = context.get('logical_date') or getattr(context.get('dag_run'), 'logical_date', None)
    if ts is None:  # ручной вызов вне контекста рана — лучше отдать хоть что-то, чем упасть
        ts = getattr(context.get('dag_run'), 'start_date', None) or datetime.now(timezone.utc)
    return ts.astimezone(timezone.utc)


def _enqueue_files(gcfg: dict, files: list[str], context) -> list[dict]:
    """📮 Ставит файлы пакета в очередь отправки, выдавая каждому свой RqUID.

    Отправкой занимается отдельный даг tfs_kafka_snd — он один видит все выгрузки сразу
    и потому только он может соблюдать лимиты маршрута. Пакетный даг лишь регистрирует
    намерение.

    RqUID сохраняется здесь же: по нему потом ищется обратная квитанция.
    Куда именно ложится очередь — ClickHouse, S3 или Postgres — решает STORAGE
    в plugins/tfs_utils.py; здесь это неважно.
    """
    import uuid

    package_ts = _run_ts(context).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    run_id     = getattr(context.get('dag_run'), 'run_id', '') or ''

    rows = [{
        'rq_uid':      uuid.uuid4().hex,
        'file_name':   f,
        'replica':     gcfg['replica'],
        'scenario_id': gcfg['scenario'],
        'package_ts':  package_ts,
        'dag_id':      gcfg['dag_id'],
        'run_id':      run_id,
    } for f in files]

    _tfs().enqueue(rows)
    logger.info("📮 В очередь отправки поставлено %d файлов пакета %s", len(rows), gcfg['replica'])
    return rows


def _sql_str(s) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return str(s).replace("'", "''")


def _xcom(context, tg: str, task_id: str, key: str = 'return_value', required: bool = True):
    """📤 Тянет XCom таска внутри TaskGroup — id там составной: '<group>.<task>'.

    required=True превращает отсутствие значения в падение: молчаливый None выше по
    течению даёт не ошибку, а пустой пакет с тикетом на пустой список архивов.
    """
    full_id = f"{tg}.{task_id}" if tg else task_id
    val = context['ti'].xcom_pull(task_ids=full_id, key=key)
    if val is None and required:
        raise AirflowFailException(f"XCom '{full_id}' (key={key}) пуст — таск не отработал")
    return val


def _format_cur_state(cur: dict) -> dict:
    """Преобразует сырую строку extract_current_vw в словарь SQL-литералов.

    Нужно при bootstrap (первый запуск) или когда вью вернула сырые Python-значения
    вместо уже отформатированных строк (например, None вместо 'null').
    """
    tf = str(cur['time_field']).strip("'")
    ec = cur['extract_count']
    return {
        'num_state':       str(cur['num_state']),
        'extract_time':    _fmt_val(cur['extract_time']),
        'extract_count':   'null' if ec is None else str(ec),
        'loaded':          _fmt_val(cur['loaded']) if ec is not None else 'null',
        'sent':            _fmt_val(cur['sent']) if ec is not None else 'null',
        'confirmed':       _fmt_val(cur['confirmed']) if ec is not None else 'null',
        'increment':       str(cur['increment']),
        'overlap':         str(cur['overlap']),
        'time_field':      f"'{tf}'",
        'time_from':       _fmt_val(cur['time_from']),
        'time_to':         _fmt_val(cur['time_to']),
        'condition':       f"{_fmt_val(cur['time_from'])} < {tf} and {tf} <= {_fmt_val(cur['time_to'])}",
        'is_current':      'True' if cur.get('current_time') == cur.get('extract_time') else 'False',
        'recent_interval': str(cur.get('recent_interval', 0)),
    }


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
    import re

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


def _pre_await(context):
    """Пропускает ожидание подтверждения: auto_confirm=True, notify_kafka=False или пакет пуст.

    Вызывается первым делом в wait_confirm — так один таск заменяет
    EmptyOperator/ожидание switch.
    """
    p = context['params']
    if p.get('auto_confirm', False):
        raise AirflowSkipException("Auto confirm enabled, skipping wait")
    if not p.get('notify_kafka', True):
        raise AirflowSkipException("Kafka notification disabled (notify_kafka=0)")
    summary_tkt = context['ti'].xcom_pull(task_ids="make_summary", key='summary_tkt_name')
    if not summary_tkt:
        raise AirflowSkipException("No data exported, skipping wait")
# ── Tasks ───────────────────────────────────────────────────────────────────

class _ZipReader:
    """Адаптер stream_zip (генератор байт) → file-like object для S3 multipart upload."""
    def __init__(self, g): self._g, self._b = g, bytearray()
    def read(self, n=-1):
        if n < 0:
            self._b.extend(b''.join(self._g))
            d, self._b = bytes(self._b), bytearray()
            return d
        while len(self._b) < n:
            try: self._b.extend(next(self._g))
            except StopIteration: break
        chunk, self._b = bytes(self._b[:n]), self._b[n:]
        return chunk


@task(task_id='init')
def _er_init(cfg, **context):
    """⚙️ Инициализирует состояние выгрузки и возвращает словарь SQL-литералов для шаблонов.

    Delta-режим: читает export.extract_current_vw; при первом запуске создаёт bootstrap-состояние
    с time_from/time_to = lower_bound.
    Recent-режим: вычисляет окно [now() - recent_interval, now()] без обращения к CH.
    Период: date_from/date_to из DAG Params перебивают состояние и помечают ран как ad_hoc.

    Возвращаемый словарь (XCom "return_value") используется всеми downstream-тасками
    через _xcom(context, cfg['tg'], 'init') — внутри TaskGroup id составной.
    """
    from airflow.hooks.base import BaseHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # 🔌 Коннекты проверяем САМИ, до первого обращения. Провайдер amazon на отсутствующий
    # conn_id не падает: пишет «Unable to find AWS Connection ID …, switching to empty»
    # и переключается на дефолтную стратегию boto3 — то есть уходит в настоящий AWS
    # и валится там NoCredentialsError из глубины botocore, где ни conn_id, ни стенда
    # уже не видно. AirflowFailException, а не обычная ошибка: отсутствующий коннект
    # за четыре ретрая сам не появится, а это 20 минут ожидания на ровном месте.
    missing = []
    for conn_id in (S3_CONN, CH_ID):
        try:
            BaseHook.get_connection(conn_id)
        except Exception:
            missing.append(conn_id)
    if missing:
        raise AirflowFailException(
            f"Не найдены Airflow connection: {', '.join(missing)}. "
            f"Заведите их на стенде — выгрузка ходит в S3 '{S3_CONN}' и ClickHouse '{CH_ID}'"
        )

    s3 = S3Hook(aws_conn_id=S3_CONN)
    # 🪣 Бакет заводится заранее, вместе с коннектом, и на лету НЕ создаётся. Отсутствие
    # бакета означает ошибку в настройке контура — не тот стенд, не тот endpoint, опечатка
    # в имени. Молча созданный бакет такую ошибку прячет: выгрузка отработает «успешно»,
    # файлы лягут в пустоту, которую ТФС не читает, и обнаружится это только по
    # неприходящим квитанциям.
    if not s3.check_for_bucket(bucket_name=BUCKET):
        raise AirflowFailException(
            f"Бакет '{BUCKET}' не найден в S3 '{S3_CONN}'. Он должен существовать заранее: "
            f"проверьте endpoint коннекта и имя бакета в er_config.py"
        )
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

    tf  = cfg.get('time_field', 'extract_time')
    lb  = cfg.get('lower_bound') or '1970-01-01 00:00:00'

    # Параметры, общие для delta и recent: передаются оператору экспорта и сохраняются в историю
    reg = {
        'lower_bound':        f"'{lb}'",
        'selfrun_timeout':    str(cfg.get('selfrun_timeout', 10)),
        'max_file_size':      cfg.get('max_file_size', ''),
        'pg_array_format':    cfg.get('pg_array_format', 'False'),
        'format_params':      cfg.get('format_params', ''),
        'xstream_sanitize':   cfg.get('xstream_sanitize', 'False'),
        'sanitize_array':     cfg.get('sanitize_array', 'False'),
        'sanitize_list':      cfg.get('sanitize_list', ''),
        'increment':          str(cfg.get('increment', 60)),
        'overlap':            str(cfg.get('overlap', 0)),
        'time_field':         f"'{tf}'",
    }

    if cfg['sql_get_current']:
        cur_res = get_dict_from_ch(hook, cfg['sql_get_current'])
        if not cur_res:
            logger.warning("First execution for %s. Bootstrapping from lower_bound=%s.", cfg['tbl'], lb)
            state = {
                'num_state': 0, 'extract_time': lb, 'extract_count': None,
                'loaded': None, 'sent': None, 'confirmed': None,
                'increment': int(cfg.get('increment', 60)) * 60,
                'overlap': int(cfg.get('overlap', 0)),
                'time_field': tf,
                'time_from': lb, 'time_to': lb, 'current_time': lb,
            }
            result = {**reg, **_format_cur_state(state)}
        else:
            cur = cur_res[0]
            result = {**reg, **(cur if 'condition' in cur else _format_cur_state(cur))}
    else:
        ri  = int(cfg.get('recent_interval', 60))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        t0  = now - timedelta(minutes=ri)
        now_s, t0_s = now.isoformat(), t0.isoformat()
        reg.update({
            'extract_time':    f"'{now_s}'",
            'extract_count':   'null',
            'loaded':          'null',
            'sent':            'null',
            'confirmed':       'null',
            'time_from':       f"'{t0_s}'",
            'time_to':         f"'{now_s}'",
            'condition':       f"'{t0_s}' < {tf} and {tf} <= '{now_s}'",
            'is_current':      'True',
            'recent_interval': str(ri),
            'num_state':       '0',
        })
        result = reg

    # Переопределения из DAG Params (ручной запуск): применяются поверх состояния дельты
    p = context['params']
    key_map = {
        'extract_time':    lambda v: f"'{v}'",
        'condition':       str,
        'is_current':      lambda v: 'True' if v == 'true' else 'False',
        'increment':       str,
        'selfrun_timeout': str,
        'strategy':        str,
        'notify_kafka':    lambda v: 'True' if v else 'False',
        'auto_confirm':     lambda v: 'True' if v else 'False',
        'max_file_size':    str,
        'pg_array_format':  lambda v: 'True' if v else 'False',
        'xstream_sanitize': lambda v: 'True' if v else 'False',
        'sanitize_array':   lambda v: 'True' if v else 'False',
        'sanitize_list':    str,
        'send_empty':       lambda v: 'True' if v else 'False',
    }
    for key, transform in key_map.items():
        if p.get(key) not in (None, '', 'None'):
            result[key] = transform(p[key])

    # 📅 Ручная выгрузка за период. Задаётся на весь пакет и перебивает состояние дельты.
    date_from = str(p.get('date_from') or '').strip()
    date_to   = str(p.get('date_to') or '').strip()
    if date_from or date_to:
        if not (date_from and date_to):
            raise AirflowFailException(
                "Для выгрузки за период нужны обе даты: date_from и date_to. "
                "Одна граница «с» задаётся параметром extract_time"
            )
        if date_to <= date_from:
            raise AirflowFailException(f"date_to ({date_to}) должна быть больше date_from ({date_from})")

        result.update({
            'extract_time': f"'{date_to}'",
            'time_from':    f"'{date_from}'",
            'time_to':      f"'{date_to}'",
            'condition':    f"'{date_from}' < {tf} and {tf} <= '{date_to}'",
            # Состояние дельты не двигаем: save_status пропустит запись в extract_history,
            # а is_current=True не даст schedule_next запустить следующий цикл. Иначе разовая
            # доливка за прошлый месяц отбросила бы регулярный поток назад.
            'is_current':   'True',
            'ad_hoc':       'True',
        })
        logger.info("📅 Разовая выгрузка за период %s .. %s, состояние дельты не сохраняется", date_from, date_to)

    add_note({k: result.get(k) for k in list(key_map) + ['time_from', 'time_to', 'ad_hoc']},
             level='task', context=context, title=f"⚙️ Delta State · {cfg['db']}.{cfg['tbl']}")
    return result


@task(task_id='build_meta')
def _er_build_meta(cfg, **context):
    """🗂️ Строит .meta JSON с описанием структуры таблицы для ЕР/TFS.

    Порядок колонок: export_time (PRE) + data_cols + ctl_action, ctl_validfrom (SUF).

    Состав data_cols — два DESCRIBE:
      • имена и типы — `DESCRIBE (<итоговый запрос>)`, ровно то, что TSVWithNames
        пишет в заголовок CSV (учитывает JOIN, алиасы, вычисляемые выражения);
      • description — `DESCRIBE TABLE <источник>`, подмешивается по имени колонки,
        так как у результата подзапроса комментариев нет.
    Если запрос задан строкой или со своим списком fields, cfg['sql_meta'] пуст —
    состав берётся только по DESCRIBE TABLE источника.

    Типы: parse_ch_type → TYPE_MAP; для FixedString/Decimal извлекаются
    length/precision/scale. UK передаётся плоским массивом: ['id'] (стандарт ЕР).
    """
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    dp = _xcom(context, cfg['tg'], 'init')
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
    rows, _ = hook.execute(f"DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}", with_column_types=True)

    ch_cols = {}
    for row in rows:
        stype, notnull, length, precision, scale = parse_ch_type(row[1], TYPE_MAP)
        comment = row[4] if len(row) > 4 else ""
        ch_cols[row[0]] = {
            "column_name": row[0], "source_type": stype, "length": length,
            "notnull": notnull, "precision": precision, "scale": scale,
            "description": comment or None,
        }

    def _cols_from_query(sql: str) -> list[dict]:
        """Колонки по DESCRIBE итогового запроса — так же, как их видит TSVWithNames.

        Единственный способ получить состав, совпадающий с заголовком CSV при JOIN,
        алиасах и вычисляемых выражениях. Комментариев у результата подзапроса нет,
        поэтому description подмешивается из DESCRIBE TABLE источника по имени колонки.
        {condition} подставляем заведомо ложным: DESCRIBE запрос не выполняет, но разобрать
        его обязан.
        """
        q = sql.format(export_time='now64(6)', condition='1=0')
        qrows, _ = hook.execute(f"DESCRIBE ({q})", with_column_types=True)
        out = []
        for r in qrows:
            stype, notnull, length, precision, scale = parse_ch_type(r[1], TYPE_MAP)
            out.append({
                "column_name": safe_name(r[0]), "source_type": stype, "length": length,
                "notnull": notnull, "precision": precision, "scale": scale,
                "description": ch_cols.get(r[0], {}).get("description"),
            })
        return out

    def _cols_from_table() -> list[dict]:
        """Запасной путь: колонки по DESCRIBE TABLE источника, без учёта JOIN и выражений."""
        return cols_from_fields(cfg.get('fields', ['*']), ch_cols, rows)

    sql_meta = cfg.get('sql_meta')
    if sql_meta:
        try:
            data_cols = _cols_from_query(sql_meta)
        except Exception as err:
            # Схема из DESCRIBE TABLE лучше, чем упавший DAG, но она может разойтись с CSV
            logger.warning("DESCRIBE по запросу не удался, откат на DESCRIBE TABLE: %s", err)
            add_note(f"DESCRIBE по запросу не удался, схема взята по DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}\n\n{err}",
                     level='task', context=context, title='⚠️ build_meta fallback')
            data_cols = _cols_from_table()
    else:
        data_cols = _cols_from_table()

    # 🔍 Состав колонок обязан совпадать с настройкой. Смысл всей проверки: новая колонка
    # в источнике не должна доезжать до КАП сама — только через правку fields в er_wf_meta.
    # Заодно ловится опечатка в выражении, у которого ClickHouse выведет неожиданное имя.
    # actual уже с hive-суффиксами (safe_name при сборке), поэтому и ожидаемые имена
    # приводим к тому же виду.
    actual = [c['column_name'] for c in data_cols]
    expected = [safe_name(n) if n is not None else None
                for n in (field_name(f) for f in cfg['fields'])]

    if len(actual) != len(expected):
        raise AirflowFailException(
            f"Состав колонок {cfg['db']}.{cfg['tbl']} разошёлся с настройкой fields: "
            f"запрос вернул {len(actual)} колонок, в настройке {len(expected)}.\n"
            f"  запрос:    {actual}\n"
            f"  настройка: {cfg['fields']}\n"
            "Если изменение источника ожидаемо — поправьте fields в export.er_wf_meta"
        )

    # Позиции с невыводимым именем (выражения без алиаса) сверяем только по количеству:
    # имя такой колонки определяет ClickHouse, и предсказать его мы не берёмся.
    mismatch = [(i, e, a) for i, (e, a) in enumerate(zip(expected, actual)) if e is not None and e != a]
    if mismatch:
        raise AirflowFailException(
            f"Состав колонок {cfg['db']}.{cfg['tbl']} разошёлся с настройкой fields.\n"
            + "\n".join(f"  позиция {i}: запрос '{a}', настройка '{e}'" for i, e, a in mismatch)
            + "\nЕсли изменение источника ожидаемо — поправьте fields в export.er_wf_meta"
        )

    if unnamed := [f for f, e in zip(cfg['fields'], expected) if e is None]:
        logger.warning(
            "Выражения без алиаса, имена колонок для них не проверены: %s. "
            "Добавьте 'as <имя>', чтобы сверка работала полностью", unnamed,
        )

    meta = {
        "mask_file":   None,
        "schema_name": cfg['schema_name'],
        "table_name":  cfg['tbl'],
        "description": cfg.get('description') or None,
        "strategy":    dp.get('strategy', cfg['strategy']),
        "PK":          cfg['PK'],
        "UK":          cfg['UK'] or [],
        "params":      FORMAT_MAP[cfg['format']]['meta_params'],
        "columns":     [{k: v for k, v in c.items() if k != 'sql'} for c in EXTRA_PRE] + data_cols + [{k: v for k, v in c.items() if k != 'sql'} for c in EXTRA_SUF],
    }
    context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))
    add_note({f"🗂️ build_meta · {cfg['tbl']}": [c["column_name"] for c in meta["columns"]]},
             level='task', context=context)


@task(task_id='pack_zip')
def _er_pack_zip(cfg, **context):
    """📦 Упаковывает выгруженные файлы одной таблицы в ZIP-архивы формата ЕР.

    Каждый файл данных оборачивается в отдельный ZIP (стриминг, без буферизации в памяти):
      [replica]__[ts].tkt      — `filename;rowcount` (TKT внутри архива, стандарт ЕР)
      [schema]__[table]__[ts].meta        — JSON-схема колонок
      [schema]__[table]__[ts].csv|.json   — данные из S3, расширение по формату выгрузки

    Имена файлов — строго нижний регистр, расширение архива .zip (стандарт ЕР).
    После упаковки исходные файлы удаляются из S3.

    Общий тикет пакета здесь НЕ пишется: архивов в пакете много, тикет один, и собирает
    его make_summary после того, как отработают все таблицы группы.
    """
    from stat import S_IFREG
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from stream_zip import ZIP_32, stream_zip # type: ignore

    ti  = context["ti"]
    tg  = cfg['tg']
    ext = FORMAT_MAP[cfg['format']]['ext']

    # s3_key_list пуст при нулевой дельте — это штатно. А вот row_count_list при непустом
    # списке ключей обязан быть: без него zip() молча даст TypeError вместо внятной ошибки.
    s3_keys = _xcom(context, tg, 'export_to_s3', key='s3_key_list',   required=False)
    counts  = _xcom(context, tg, 'export_to_s3', key='row_count_list', required=bool(s3_keys))
    meta_s  = _xcom(context, tg, 'build_meta',   key='meta_json')

    base_ts = _run_ts(context)
    ts      = base_ts.strftime("%Y%m%d%H%M%S")
    mtime   = base_ts.replace(tzinfo=None)

    if not s3_keys:
        # Param по умолчанию None = «не переопределять»: у пакета много таблиц, и одно
        # умолчание в UI не должно перебивать настройку каждой из них.
        override   = context['params'].get('send_empty')
        send_empty = cfg['send_empty'] if override is None else bool(override)
        if not send_empty:
            ti.xcom_push(key="zip_name_list",   value=[])
            ti.xcom_push(key="total_row_count", value=0)
            add_note({f"📦 pack_zip · {cfg['tbl']}": "пусто, send_empty=0 — архив не создан"},
                     level='task', context=context)
            return

        # Пустой пакет по требованию ТФС. У TSV это файл с одной строкой заголовка,
        # у NDJSON заголовка нет вовсе — пустой файл нулевой длины.
        meta_obj = json.loads(meta_s)
        body     = ("\t".join(c["column_name"] for c in meta_obj["columns"]) + "\n").encode() \
                   if FORMAT_MAP[cfg['format']]['header'] else b""
        data_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__0_1_0.{ext}".lower()
        meta_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__0_1_0.meta".lower()
        tkt_n  = f"{cfg['replica']}__{ts}.tkt".lower()
        zip_n  = f"{cfg['replica']}__{ts}__{cfg['tbl']}__0_1_0.zip".lower()
        members = [
            (tkt_n,  mtime, S_IFREG | 0o600, ZIP_32, [f"{data_n};0".encode()]),
            (meta_n, mtime, S_IFREG | 0o600, ZIP_32, [meta_s.encode()]),
            (data_n, mtime, S_IFREG | 0o600, ZIP_32, [body]),
        ]
        hook_e = S3Hook(aws_conn_id=S3_CONN)
        hook_e.load_file_obj(_ZipReader(stream_zip(members)), key=f"{cfg['s3_prefix']}/{zip_n}", bucket_name=BUCKET, replace=True)
        ti.xcom_push(key="zip_name_list",   value=[zip_n])
        ti.xcom_push(key="total_row_count", value=0)
        add_note({f"📦 pack_zip · {cfg['tbl']} (empty)": [zip_n]}, title="rows=0 send_empty=True",
                 level='task', context=context)
        return

    hook, total = S3Hook(aws_conn_id=S3_CONN), len(s3_keys)
    uploaded = []

    for i, (key, rows) in enumerate(zip(s3_keys, counts)):
        data_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__{i+1}_{total}_{rows}.{ext}".lower()
        meta_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__{i+1}_{total}_{rows}.meta".lower()
        tkt_n  = f"{cfg['replica']}__{ts}.tkt".lower()
        zip_n  = f"{cfg['replica']}__{ts}__{cfg['tbl']}__{i+1}_{total}_{rows}.zip".lower()

        s3_body = hook.get_key(key=key, bucket_name=BUCKET).get()["Body"]
        members = [
            (tkt_n,  mtime, S_IFREG | 0o600, ZIP_32, [f"{data_n};{rows}".encode()]),
            (meta_n, mtime, S_IFREG | 0o600, ZIP_32, [meta_s.encode()]),
            (data_n, mtime, S_IFREG | 0o600, ZIP_32, s3_body.iter_chunks(chunk_size=8*1024*1024)),
        ]
        hook.load_file_obj(_ZipReader(stream_zip(members)), key=f"{cfg['s3_prefix']}/{zip_n}", bucket_name=BUCKET, replace=True)
        hook.delete_objects(bucket=BUCKET, keys=[key])
        uploaded.append(zip_n)

    total_rows = sum(int(r) for r in counts)
    ti.xcom_push(key="zip_name_list",   value=uploaded)
    ti.xcom_push(key="total_row_count", value=total_rows)
    add_note({f"📦 pack_zip · {cfg['tbl']}": uploaded}, title=f"rows={total_rows} files={total}",
             level='task', context=context)


@task(task_id='make_summary', trigger_rule='none_failed')
def _er_make_summary(gcfg, **context):
    """🧾 Собирает общий тикет пакета — один .tkt на все архивы группы.

    Тикет `[replica]__[ts].tkt` перечисляет ZIP-файлы всех таблиц пакета, по имени в строке.
    Его имя уникально по построению: replica включает суффикс группы, а внутри группы
    max_active_runs=1 не даёт двум пакетам родиться одновременно.

    trigger_rule=none_failed: падение любой таблицы блокирует пакет целиком — тикет
    на неполный список архивов ушёл бы в ЕР как полноценная поставка.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    ti = context["ti"]
    zips: list[str] = []
    for tg in gcfg['groups']:
        zips.extend(_xcom(context, tg, 'pack_zip', key='zip_name_list', required=False) or [])

    if not zips:
        # Пусто во всех таблицах и send_empty=0: уведомлять ЕР не о чем.
        ti.xcom_push(key="summary_tkt_name", value="")
        ti.xcom_push(key="zip_name_list",    value=[])
        add_note("пакет пуст — тикет не создан, уведомление пропущено",
                 level='task,dag', context=context, title='🧾 make_summary')
        return

    ts          = _run_ts(context).strftime("%Y%m%d%H%M%S")
    summary_tkt = f"{gcfg['replica']}__{ts}.tkt".lower()
    S3Hook(aws_conn_id=S3_CONN).load_bytes(
        "\n".join(zips).encode(), key=f"{gcfg['s3_prefix']}/{summary_tkt}", bucket_name=BUCKET, replace=True,
    )

    # 📮 Ставим файлы в очередь отправки. RqUID генерируется ЗДЕСЬ и сохраняется: именно
    # по нему потом ищется обратная квитанция. Раньше он рождался внутри produce_msg
    # и умирал там же, поэтому сопоставить отправку с квитанцией было нечем.
    files = [summary_tkt] + zips
    queued = _enqueue_files(gcfg, files, context) if context['params'].get('notify_kafka', True) else []
    if not queued:
        logger.info("📭 notify_kafka=0 — файлы в очередь отправки не ставим")

    ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
    ti.xcom_push(key="zip_name_list",    value=zips)
    ti.xcom_push(key="rq_uids",          value=[q['rq_uid'] for q in queued])
    add_note({f"🧾 {summary_tkt}": zips}, title=f"архивов в пакете: {len(zips)}, в очереди: {len(queued)}",
             level='task,dag', context=context)


@task(task_id='wait_confirm', trigger_rule='none_failed')
def _er_wait_confirm(gcfg, **context):
    """⏳ Ждёт обратные квитанции ТФС по всем файлам пакета.

    Kafka здесь больше не читается. Топик квитанций общий на все маршруты ТФС, и прежнее
    прямое чтение брало ЛЮБОЕ сообщение: пакет подтверждался чужой квитанцией, настоящий
    адресат её терял, а StatusCode не проверялся вовсе. Теперь топик вычитывает один
    даг tfs_kafka_rcv, а мы ждём появления СВОИХ строк по своим RqUID.

    Любой status_code != 0 роняет таск сразу, не дожидаясь остальных файлов: пакет уже
    не доедет целиком, ждать бессмысленно.
    """
    import time

    _pre_await(context)  # auto_confirm / notify_kafka / нет данных → skip

    rq_uids = context['ti'].xcom_pull(task_ids="make_summary", key='rq_uids') or []
    if not rq_uids:
        raise AirflowFailException(
            "В XCom make_summary нет rq_uids — файлы не встали в очередь отправки"
        )

    # Из params, а не из gcfg: значение в форме запуска должно работать. Потолок ему всё
    # равно ставит execution_timeout, посчитанный при разборе файла.
    timeout = int(context['params'].get('confirm_timeout') or gcfg['confirm_timeout'])
    deadline = time.time() + timeout * 60

    while True:
        got = _tfs().find_receipts(rq_uids)

        bad = [r for r in got if r['status_code'] != 0]
        if bad:
            add_note({"❌ Квитанции с ошибкой": [f"{r['file_name']}: код {r['status_code']}" for r in bad]},
                     level='task,dag', context=context, title='📨 TFS confirm')
            raise AirflowFailException(
                "ТФС отверг файлы пакета:\n"
                + "\n".join(f"  • {r['file_name']}: StatusCode={r['status_code']}" for r in bad)
            )

        if len(got) == len(rq_uids):
            add_note({"✅ Квитанции получены": [f"{r['file_name']} @ {r['rq_tm']}" for r in got]},
                     level='task,dag', context=context, title='📨 TFS confirm')
            return datetime.now(timezone.utc).isoformat()

        if time.time() >= deadline:
            break
        time.sleep(10)

    # Таймаут. Различаем два диагноза: очередь стоит или ТФС молчит — лечатся они разно.
    missing = list(set(rq_uids) - {r['rq_uid'] for r in got})
    queued  = _tfs().queue_state(missing)
    not_sent  = [r['file_name'] for r in queued if r['pending']]
    no_answer = [r['file_name'] for r in queued if not r['pending']]

    raise AirflowFailException(
        f"Квитанции ТФС не пришли за {timeout} мин.\n"
        + (f"  Ещё не отправлены (очередь стоит): {not_sent}\n" if not_sent else "")
        + (f"  Отправлены, ответа нет: {no_answer}\n" if no_answer else "")
        + "  Смотреть: хранилище тракта (STORAGE в plugins/tfs_utils.py), даги tfs_kafka_snd и tfs_kafka_rcv"
    )


@task(task_id='save_status', trigger_rule='none_failed')
def _er_save_status(gcfg, **context):
    """💾 Записывает результат по каждой таблице пакета в export.extract_history.

    Одна вставка на весь пакет: строк столько, сколько таблиц отработало.
    trigger_rule=none_failed: запускается при успехе или при skipped-тасках
    (wait_confirm пропускается при auto_confirm=True или отсутствии данных).
    confirmed — время квитанции ТФС из wait_confirm; null, если ждать не стали
    (auto_confirm=1) или пакет был пуст. Раньше сюда всегда писался null: подтверждение
    было неотличимо от его отсутствия.
    extract_time берётся из XCom init как SQL-литерал (уже в кавычках).

    Разовая выгрузка за период (ad_hoc) состояние не двигает — иначе следующая штатная
    дельта оттолкнулась бы от вручную заданных границ.
    """
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # wait_confirm вернул время получения квитанций; при skip (auto_confirm=1) — None
    confirmed_at = context['ti'].xcom_pull(task_ids='wait_confirm')
    confirmed = f"toDateTime64('{confirmed_at[:23].replace('T', ' ')}', 3)" if confirmed_at else 'null'

    selects, noted, skipped = [], {}, []
    for tg, tbl in gcfg['tables'].items():
        dp = _xcom(context, tg, 'init', required=False)
        if not dp:
            continue
        if str(dp.get('ad_hoc')).lower() == 'true':
            skipped.append(tbl)
            continue

        rows = _xcom(context, tg, 'pack_zip', key='total_row_count', required=False) or 0
        zips = _xcom(context, tg, 'pack_zip', key='zip_name_list',   required=False) or []
        zip_arr = "[" + ", ".join(f"'{z}'" for z in zips) + "]"

        selects.append(f"""
            SELECT
                '{tbl}', {dp['extract_time']}, {rows}, now(), now(), {confirmed},
                {dp['increment']}, {dp['overlap']}, {dp['recent_interval']},
                {dp['time_field']}, {dp['time_from']}, {dp['time_to']}, {zip_arr}
        """)
        noted[tbl] = {"time_from": dp['time_from'], "time_to": dp['time_to'], "rows": rows, "zips": zips}

    if selects:
        ClickHouseHook(clickhouse_conn_id=CH_ID).execute(f"""
            INSERT INTO export.extract_history (
                extract_name, extract_time, extract_count, loaded, sent, confirmed,
                increment, overlap, recent_interval, time_field, time_from, time_to, exported_files
            ) {' UNION ALL '.join(selects)}
        """)

    if skipped:
        add_note(f"📅 разовая выгрузка за период — состояние не сохранено: {', '.join(skipped)}",
                 level='task,dag', context=context, title='💾 save_status')
    if noted:
        add_note({"💾 save_status": noted}, level='task,dag', context=context)


@task(task_id='schedule_next')
def _er_schedule_next(gcfg, **context):
    """⏭️ Запускает следующий цикл, если хотя бы одна таблица пакета отстаёт от текущего времени.

    Пакет ходит целиком, поэтому и решение об автозапуске одно на группу: догонять
    отставшую таблицу отдельным раном нельзя — тикет формируется на весь пакет.
    Запуск откладывается на selfrun_timeout минут, чтобы избежать гонки с источником.
    Для recent-режима и для выгрузки за период is_current всегда True — автозапуск не нужен.
    """
    from airflow.api.common.trigger_dag import trigger_dag

    behind = []
    for tg, tbl in gcfg['tables'].items():
        dp = _xcom(context, tg, 'init', required=False)
        if dp and str(dp.get('is_current')).lower() not in ('true', 't', '1'):
            behind.append(tbl)

    if not behind:
        add_note("✅ delta is current — next run not scheduled", level='task,dag', context=context)
        return

    # Из params, а не из gcfg: иначе поле «Selfrun timeout» в форме запуска ничего не меняет.
    delay = context['params'].get('selfrun_timeout') or gcfg['selfrun_timeout']
    next_run = datetime.now(timezone.utc) + timedelta(minutes=int(delay))
    trigger_dag(dag_id=gcfg['dag_id'], execution_date=next_run, conf={}, replace_microseconds=False)
    add_note(f"⏭️ next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\nотстают: {', '.join(behind)}",
             level='task,dag', context=context)

# ── DAG Factory ───────────────────────────────────────────────────────────────

def _table_cfg(table_key: str, entry: dict, replica: str, prefix: str) -> dict:
    """🗂️ Конфиг одной поставки внутри пакета.

    entry — запись из Variable: schema, PK, UK, fields, params (уже разрешённые
    наследованием от строки-дефолта группы) и ровно один из sql_stmt_export_delta /
    sql_stmt_export_recent.
    """
    p = get_params(entry)
    db, tbl = table_key.split(".", maxsplit=1)

    if p['format'] not in FORMAT_MAP:
        raise AirflowFailException(
            f"{table_key}: неизвестный формат '{p['format']}', допустимы {sorted(FORMAT_MAP)}"
        )

    # Состав колонок задаётся только настройкой — иначе новая колонка источника уедет
    # в выгрузку и в КАП сама, без единой правки er_wf_meta.
    fields = entry.get("fields") or []
    if not fields or any(str(f).strip() == '*' or str(f).strip().endswith('.*') for f in fields):
        raise AirflowFailException(
            f"{table_key}: fields должен быть явным списком колонок, '*' и 't1.*' запрещены"
        )

    def _prep_sql(key):
        """Читает SQL-метадату по ключу и добавляет обязательные поля (export_time, ctl_*)."""
        m = entry.get(key)
        if isinstance(m, dict) and "fields" not in m:
            m = {**m, "fields": [c['sql'] for c in EXTRA_PRE] + fields + [c['sql'] for c in EXTRA_SUF]}
        return build_sql(m)

    def _prep_sql_data(key):
        """Тот же запрос, но только с data-колонками — build_meta делает по нему DESCRIBE."""
        m = entry.get(key)
        return build_sql({**m, "fields": fields}) if isinstance(m, dict) and "fields" not in m else ""

    sql_delta, sql_recent = _prep_sql('sql_stmt_export_delta'), _prep_sql('sql_stmt_export_recent')
    if not (sql_delta or sql_recent) or (sql_delta and sql_recent):
        raise AirflowFailException(f"{table_key}: нужен ровно один из delta/recent SQL-запросов")

    sql_exp = sql_delta or sql_recent
    if LIMITS.get(ENV_STAND, 0) > 0:
        sql_exp = f"SELECT * FROM ({sql_exp}) LIMIT {LIMITS[ENV_STAND]}"

    if p['format'] != 'TSVWithNames' and (p['pg_array_format'] or p['xstream_sanitize']):
        logger.warning(
            "%s: pg_array_format/xstream_sanitize не применяются к формату %s — "
            "orjson сериализует массивы и экранирует спецсимволы сам", table_key, p['format'],
        )

    return {
        # ── Идентификация ────────────────────────────────────────────────────
        'tg':              table_key.replace('.', '__'),   # id TaskGroup, он же префикс XCom
        'db':              db,
        'tbl':             tbl,
        'schema_name':     entry['schema'],
        'replica':         replica,
        's3_prefix':       prefix,
        # ── SQL ──────────────────────────────────────────────────────────────
        'sql_export':      sql_exp,
        'sql_get_current': sql_cur_delta(tbl) if sql_delta else None,  # None → recent-режим
        'sql_meta':        _prep_sql_data('sql_stmt_export_delta') or _prep_sql_data('sql_stmt_export_recent'),
        # ── Схема ────────────────────────────────────────────────────────────
        'fields':          fields,
        'PK':              entry.get('PK', []),
        'UK':              entry.get('UK', []),
        'description':     entry.get('description', ''),
        # ── Параметры таблицы ────────────────────────────────────────────────
        'format':          p['format'],
        'strategy':        p['strategy'],
        'increment':       p['increment'],
        'selfrun_timeout': p['selfrun_timeout'],
        'lower_bound':     p['lower_bound'],
        'time_field':      p['time_field'],
        'overlap':         p['overlap'],
        'recent_interval': p['recent_interval'],
        'export_timeout':  p['export_timeout'],
        'max_file_size':    str(p['max_file_size']),
        'format_params':    p['csv_format_params'],
        'pg_array_format':  'True' if p['pg_array_format'] else 'False',
        'xstream_sanitize': 'True' if p['xstream_sanitize'] else 'False',
        'sanitize_array':   'True' if p['sanitize_array'] else 'False',
        'sanitize_list':    p['sanitize_list'],
        'send_empty':       bool(p['send_empty']),
    }


def _dag_params(gp: dict) -> dict:
    """🎛️ DAG Params пакета.

    Групповые параметры (notify_kafka, auto_confirm, таймауты) имеют реальные умолчания —
    они одни на весь пакет. Табличные умолчаний НЕ имеют (None): пустое значение означает
    «взять из er_wf_meta», иначе умолчание одной таблицы молча перебило бы настройку всех
    остальных таблиц группы.
    """
    return {
        # ── Окно выгрузки ────────────────────────────────────────────────────
        'date_from': Param(
            None, type=['string', 'null'], title='Дата с',
            description='Разовая выгрузка за период. Задавать вместе с «Дата по». '
                        'Состояние дельты при этом НЕ сохраняется.',
        ),
        'date_to': Param(
            None, type=['string', 'null'], title='Дата по',
            description='Верхняя граница периода (включительно). Задавать вместе с «Дата с».',
        ),
        'extract_time': Param(
            None, type=['string', 'null'], title='Extract time',
            description='Переопределить время выгрузки (ISO 8601). Игнорирует состояние дельты.',
        ),
        'condition': Param(
            None, type=['string', 'null'], title='Condition',
            description='SQL WHERE-условие. Переопределяет условие из состояния дельты.',
        ),
        'is_current': Param(
            'None', type='string', enum=['None', 'true', 'false'], title='Is current',
            description='Принудительно пометить состояние как актуальное (не запускать следующий цикл).',
        ),
        'increment': Param(
            None, type=['integer', 'null'], title='Increment (мин)',
            description='Шаг дельты: time_to = time_from + increment. Пусто — из настройки таблицы.',
        ),
        # ── Групповые: одни на весь пакет ────────────────────────────────────
        'notify_kafka': Param(
            bool(gp['notify_kafka']), type='boolean', title='Notify Kafka',
            description='True = отправлять уведомление в Kafka; False = пропустить (стенд).',
        ),
        'auto_confirm': Param(
            bool(gp['auto_confirm']), type='boolean', title='Auto confirm',
            description='True = не ждать Kafka-подтверждения от TFS.',
        ),
        'confirm_timeout': Param(
            gp['confirm_timeout'], type='integer', title='Confirm timeout (мин)',
            description=(
                'Максимальное время ожидания подтверждения из Kafka. Сверху ограничено '
                f"execution_timeout таска ({gp['confirm_timeout'] + 5} мин), он считается "
                'при разборе файла и через форму запуска не меняется.'
            ),
        ),
        # Таймаута отправки здесь нет: шлёт файлы tfs_kafka_snd, у него свой темп.
        'selfrun_timeout': Param(
            gp['selfrun_timeout'], type='integer', title='Selfrun timeout (мин)',
            description='Задержка до следующего автозапуска, если дельта не догнала текущее время.',
        ),
        # ── Табличные: пусто = взять из er_wf_meta ───────────────────────────
        'strategy': Param(
            None, type=['string', 'null'], title='Strategy',
            enum=[None, 'FULL_UK', 'FULL_NO_UK', 'INC', 'APPEND'],
            description=(
                'Стратегия загрузки TFS; применяется ко ВСЕМ таблицам пакета. '
                'FULL_UK — полное обновление snp с дедубликацией по UK; строки с ctl_action=D отбрасываются TFS. '
                'FULL_NO_UK — полное обновление без дедубликации; строки с ctl_action=D отбрасываются TFS. '
                'INC — инкрементальное обновление: ctl_action=D+UK→удаление, ctl_action=D без UK→отброс, '
                'остальные+UK→обновление, остальные без UK→вставка. '
                'APPEND — только добавление; ctl_action игнорируется TFS, всегда I.'
            ),
        ),
        'max_file_size': Param(
            None, type=['integer', 'null'], title='Max file size',
            description='Ограничение размера файла данных, байт. Пусто — из настройки таблицы.',
        ),
        'send_empty': Param(
            None, type=['boolean', 'null'], title='Send Empty',
            description='True = слать пустой ZIP+Kafka при нулевой дельте (требование TFS).',
        ),
        'pg_array_format': Param(
            None, type=['boolean', 'null'], title='PG Array Format',
            description='PostgreSQL-формат массивов в TSV. Для JSON не применяется.',
        ),
        'xstream_sanitize': Param(
            None, type=['boolean', 'null'], title='XStream Sanitize',
            description='Экранировать спецсимволы XStream. Для JSON не применяется.',
        ),
        'sanitize_array': Param(
            None, type=['boolean', 'null'], title='Sanitize Array',
            description='Санитизировать CH-массивы в строки.',
        ),
        'sanitize_list': Param(
            None, type=['string', 'null'], title='Sanitize List',
            description='JSON-массив пар [[pattern, replacement], ...] для re.sub по каждой строке.',
        ),
    }


def create_export_dag(replica: str, group: dict) -> tuple[str, DAG]:
    """🏭 Создаёт Airflow DAG на одну группу поставок — то есть на один пакет ЕР.

    replica — ключ группы из Variable, он же имя в архивах и тикете
              ('hrplatform_datalab__1'); маршрут в TFS ищется по части до первого '__'
    group   — {schedule, description, params (групповые), tables: {"db.tbl": entry}}

    Возвращает (dag_id, dag) для регистрации в globals().
    """
    from hrp_operators import HrpClickNativeToS3ListOperator # type: ignore

    base = replica_base(replica)
    if base not in TFS_MAP:
        raise AirflowFailException(f"{replica}: базовая реплика '{base}' не найдена в TFS_MAP")
    scen, prefix = TFS_MAP[base]

    gp     = get_params(group)
    tables = group.get('tables') or {}
    if not tables:
        raise AirflowFailException(f"{replica}: в группе нет ни одной поставки")

    cfgs   = {tk: _table_cfg(tk, entry, replica, prefix) for tk, entry in tables.items()}
    dag_id = f"export_er__{replica}"

    gcfg = {
        'dag_id':          dag_id,
        'replica':         replica,
        'scenario':        scen,
        's3_prefix':       prefix,
        'confirm_timeout': gp['confirm_timeout'],
        'selfrun_timeout': gp['selfrun_timeout'],
        # Соответствие «TaskGroup → имя выгрузки»: по нему групповые таски собирают XCom
        'groups':          [c['tg'] for c in cfgs.values()],
        'tables':          {c['tg']: c['tbl'] for c in cfgs.values()},
    }

    schemas = sorted({c['schema_name'].replace(' ', '_').lower() for c in cfgs.values()})

    dag = DAG(
        dag_id=dag_id,
        description=group.get('description') or f"ER: пакет {replica} ({len(cfgs)} табл.)",
        doc_md=(
            f"### Пакет ЕР `{replica}` — {len(cfgs)} поставок, один тикет\n\n"
            f"Групповые параметры:\n```json\n{json.dumps(gp, indent=2, default=str)}\n```\n\n"
            + "\n".join(
                f"**{tk}** — формат `{c['format']}`, стратегия `{c['strategy']}`, "
                f"колонок {len(c['fields'])}"
                for tk, c in cfgs.items()
            )
        ),
        default_args=DEF_ARGS, start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
        schedule_interval=group.get('schedule', '55 0 * * *'),
        max_active_tasks=int(gp['max_active_tasks']), max_active_runs=1, catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'ER', replica, *schemas],
        render_template_as_native_obj=True, is_paused_upon_creation=True,
        params=_dag_params(gp),
    )

    def _make_pre_exp(tcfg):
        """pre_execute для export_to_s3: подставляет состояние дельты в SQL и параметры оператора."""
        def _pre_exp(ctx):
            dp = _xcom(ctx, tcfg['tg'], 'init')
            op = ctx['task']
            op.sql = op.sql.format(export_time=dp['extract_time'], condition=dp['condition'])

            # max_size оператор ждёт СТРОКОЙ: '100MB' либо просто число байт. Разбирает он
            # её сам, в _init_check через parse_size(), а тот сразу зовёт .strip() — int
            # роняет его с AttributeError: 'int' object has no attribute 'strip'.
            # None безопасен: _init_check подставит вместо него дефолт (10 ГБ на PROM,
            # 1 ГБ иначе), так что до сравнения размеров в execute() None не доживает.
            # Ноль тоже отдаём как None: parse_size('0') вернёт 0, а оператор считает это
            # неверным форматом и падает уже своей ошибкой.
            raw = str(dp.get('max_file_size') or '').strip().upper()
            num = re.fullmatch(r'(\d+)\s?(MB|KB|GB|B)?', raw)
            op.max_size = raw if num and int(num.group(1)) > 0 else None

            op.pg_array_format  = dp['pg_array_format'] == 'True'
            op.xstream_sanitize = dp.get('xstream_sanitize', 'False') == 'True'
            op.sanitize_array   = dp.get('sanitize_array', 'False') == 'True'
            op.sanitize_list    = dp.get('sanitize_list') or ''
            try:
                op.format_params = ast.literal_eval(dp['format_params'])
            except (ValueError, SyntaxError):
                op.format_params = {}
        return _pre_exp

    with dag:
        packed = []
        for table_key, tcfg in cfgs.items():
            fmt = FORMAT_MAP[tcfg['format']]
            with TaskGroup(group_id=tcfg['tg']):
                t_init, t_meta = _er_init(cfg=tcfg), _er_build_meta(cfg=tcfg)
                t_exp = HrpClickNativeToS3ListOperator(
                    task_id='export_to_s3', s3_bucket=BUCKET,
                    # Ключ обязан различать и таблицу, и группу: s3_prefix общий у всех групп
                    # одной базовой реплики, ts_nodash совпадает у пакетов на одном cron, а
                    # оператор лишь дописывает номер части. Только имени таблицы мало —
                    # одноимённые таблицы из разных баз (tg = db__tbl) или из разных групп
                    # писали бы в один ключ и затирали друг друга при replace=True.
                    s3_key=f"{tcfg['s3_prefix']}/{tcfg['replica']}__{tcfg['tg']}__{{{{ ts_nodash }}}}.{fmt['ext']}",
                    aws_conn_id=S3_CONN, clickhouse_conn_id=CH_ID,
                    sql=tcfg['sql_export'], fmt=fmt['fmt'], header=fmt['header'],
                    compression=None, replace=True, post_file_check=False,
                    pre_execute=_make_pre_exp(tcfg),
                    execution_timeout=timedelta(minutes=tcfg['export_timeout']),
                )
                t_zip = _er_pack_zip(cfg=tcfg)
                t_init >> [t_meta, t_exp] >> t_zip
                packed.append(t_zip)

        # Отправки здесь нет: make_summary только ставит файлы в очередь, а шлёт их
        # tfs_kafka_snd — он один видит все выгрузки и потому только он может
        # соблюдать лимиты маршрута (файлов в секунду, минуту, час и сутки).
        t_sum = _er_make_summary(gcfg=gcfg)
        # execution_timeout с запасом к собственному дедлайну таска: снимать его должен
        # он сам, с внятной ошибкой, а не Airflow по таймауту
        t_wait = _er_wait_confirm.override(
            execution_timeout=timedelta(minutes=gcfg['confirm_timeout'] + 5),
        )(gcfg=gcfg)

        packed >> t_sum >> t_wait >> _er_save_status(gcfg=gcfg) >> _er_schedule_next(gcfg=gcfg)

    return dag_id, dag

@task(task_id='config_error')
def _er_config_error(replica: str, errors: list, **context):
    """💥 Единственный таск даг-заглушки: сообщает, что пакет сломан, и падает.

    Не EmptyOperator: зелёный таск создавал бы ощущение работающей выгрузки. Причины
    дублируются в лог и в заметки, чтобы их было видно и из списка ранов, и из алертов,
    а не только в описании DAG.
    """
    for err in errors:
        logger.error("❌ %s: %s", replica, err)

    add_note({f"❌ Пакет {replica} сломан ({len(errors)})": errors},
             level='task,dag', context=context, title='🚫 Ошибки настройки er_wf_meta')

    raise AirflowFailException(
        f"Пакет {replica} не собран: {len(errors)} ошибок в настройке export.er_wf_meta.\n"
        + "\n".join(f"  • {e}" for e in errors)
        + "\nПоправьте настройку и перезапустите export_er_sync"
    )


def create_broken_dag(replica: str, errors: list, schedule=None) -> tuple[str, DAG]:
    """🚧 DAG-заглушка вместо пакета, который не удалось собрать.

    dag_id тот же, что у рабочего пакета: DAG не двоится, а подменяется — в списке сразу
    видно, что выгрузки нет. Расписание группы сохраняется, поэтому поломка продолжает
    сигналить красным раном в том же ритме, в каком должен был ходить пакет.
    """
    dag_id = f"export_er__{replica}"
    dag = DAG(
        dag_id=dag_id,
        description=f"❌ Пакет сломан: {len(errors)} ошибок в настройке er_wf_meta",
        doc_md=(
            f"## ❌ Пакет `{replica}` не собран\n\n"
            f"Выгрузка не работает: в `export.er_wf_meta` {len(errors)} ошибок.\n\n"
            + "\n".join(f"- {e}" for e in errors)
            + "\n\nПоправьте настройку и перезапустите `export_er_sync`."
        ),
        # Ретраить ошибку настройки бессмысленно; пул экспорта заглушке не нужен,
        # а on_failure_callback оставляем — он пишет штатную заметку о падении.
        default_args={
            'owner': DEF_ARGS['owner'],
            'retries': 0,
            'email_on_failure': False,
            'on_failure_callback': DEF_ARGS['on_failure_callback'],
        },
        start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
        schedule_interval=schedule, max_active_runs=1, catchup=False,
        tags=['DataLab', 'CI02420667', 'ER', replica, 'BROKEN'],
        is_paused_upon_creation=True,
    )
    with dag:
        _er_config_error(replica=replica, errors=errors)

    return dag_id, dag

# ── Dynamic DAG Registration ──────────────────────────────────────────────────

try:
    workflows = obj_load(VAR_NAME)
except Exception as e:
    logger.error("Failed to load workflows: %s", e)
    workflows = {}

for _replica, _group in workflows.items():
    # Сломанную группу подменяем заглушкой, а не роняем разбор файла: раньше одна битая
    # запись уносила ВСЕ ER-пакеты. Причины видны в описании DAG, в логе и в заметках,
    # а таск заглушки падает — молча пропущенной выгрузки не остаётся.
    try:
        if _group.get('errors'):
            dag_id, dag_obj = create_broken_dag(_replica, _group['errors'], _group.get('schedule'))
        else:
            dag_id, dag_obj = create_export_dag(_replica, _group)
    except Exception as e:
        logger.error("DAG generation failed for %s: %s", _replica, e)
        dag_id, dag_obj = create_broken_dag(_replica, [f"Ошибка сборки DAG: {e}"],
                                            _group.get('schedule'))
    globals()[dag_id] = dag_obj
