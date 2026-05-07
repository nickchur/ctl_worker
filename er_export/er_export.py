"""🚀 DAG-фабрика ER-выгрузок (ClickHouse → S3 → TFS).

Жизненный цикл одного запуска:
  init → [build_meta, export_to_s3] → pack_zip → notify_tfs → wait_confirm → save_status → schedule_next

Метаданные выгрузок хранятся в Airflow Variable `datalab_er_wfs` (JSON-словарь),
который синхронизируется DAG-ом export_er_sync из таблицы export.er_wf_meta.

Поддерживаемые режимы выгрузки:
  📈 delta  — инкрементальный, окно [time_from, time_to] из export.extract_current_vw
  🔄 recent — скользящее окно [now() - recent_interval, now()], без сохранения состояния
"""
from __future__ import annotations

import ast
import json
import logging
import time
import uuid
from datetime import timedelta
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param

try:
    from CI06932748.analytics.datalab.export_er.er_config import get_config, get_dict, obj_load, add_note, get_params
except ImportError:
    from er_export.er_config import get_config, get_dict, obj_load, add_note, get_params

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
TOPIC          = _cfg['TOPIC']
KAFKA_OUT_CONN = _cfg['KAFKA_OUT_CONN']
KAFKA_IN_CONN  = _cfg['KAFKA_IN_CONN']
KAFKA_IN_TOPIC = _cfg['KAFKA_IN_TOPIC']
TFS_MAP        = _cfg['TFS_MAP']
S3_CONN        = _cfg['S3_CONN']
VAR_NAME       = _cfg['VAR_NAME']
POOL_NAME      = _cfg['POOL_NAME']

ON_DELIVERY = f'{__name__}.on_delivery'   # dot-notation для delivery_callback

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
    fields_str = f",\n{indent}".join(fields) if isinstance(fields, list) else fields
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


def produce_msg(scenario_id: str, file_name: str, throttle_delay: int = 1):
    """Генератор Kafka-сообщений для TFS: отдаёт одно XML-уведомление о передаче файла.

    throttle_delay — пауза перед отправкой (сек), защита от перегрузки брокера.
    Функция — генератор (yield key, value), как того требует ProduceToTopicOperator.
    """
    time.sleep(throttle_delay)
    rq_uuid = str(uuid.uuid4()).replace('-', '')
    message = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{rq_uuid}</RqUID>
    <RqTm>{pendulum.now().format('YYYY-MM-DDTHH:mm:ss.SSSZ')}</RqTm>
    <ScenarioInfo><ScenarioId>{scenario_id}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{file_name}</Name></FileInfo></File>
</TransferFileCephRq>"""
    logger.info("Kafka message prepared: %s", rq_uuid)
    yield None, message


def on_delivery(err: Exception | None, msg) -> None:
    """Колбэк подтверждения доставки Kafka: падает с AirflowFailException при ошибке."""
    if err: raise AirflowFailException(f"Kafka delivery failed: {err}")
    logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def _kafka_accept_any(msg) -> bool:
    """apply_function для AwaitMessageSensor: принимает любое сообщение из топика."""
    return True


def _pre_kafka(scenario: str, notify_kafka: bool = True):
    """Фабрика pre_execute для ProduceToTopicOperator.

    Пропускает отправку если notify_kafka=False или данных нет.
    Динамически подставляет имя summary-файла в аргументы продюсера.
    """
    def pre_execute(context):
        if not notify_kafka:
            raise AirflowSkipException("Kafka notification disabled (notify_kafka=0)")
        summary_tkt = context['ti'].xcom_pull(task_ids="pack_zip", key='summary_tkt_name')
        if not summary_tkt:
            raise AirflowSkipException("No data exported, skipping notification")
        context['task'].producer_function_args = [scenario, summary_tkt]
    return pre_execute


def _pre_await(auto_confirm=False, notify_kafka: bool = True):
    """Фабрика pre_execute для AwaitMessageSensor.

    Пропускает ожидание если: auto_confirm=True, notify_kafka=False, или данных не было.
    Позволяет использовать один оператор вместо EmptyOperator/AwaitMessageSensor switch.
    """
    def pre_execute(context):
        if auto_confirm:
            raise AirflowSkipException("Auto confirm enabled, skipping wait")
        if not notify_kafka:
            raise AirflowSkipException("Kafka notification disabled (notify_kafka=0)")
        summary_tkt = context['ti'].xcom_pull(task_ids="pack_zip", key='summary_tkt_name')
        if not summary_tkt:
            raise AirflowSkipException("No data exported, skipping wait")
    return pre_execute

# ── Tasks ───────────────────────────────────────────────────────────────────

@task(task_id='init', pool=POOL_NAME)
def _er_init(cfg, **context):
    """⚙️ Инициализирует состояние выгрузки и возвращает словарь SQL-литералов для шаблонов.

    Delta-режим: читает export.extract_current_vw; при первом запуске создаёт bootstrap-состояние
    с time_from/time_to = lower_bound.
    Recent-режим: вычисляет окно [now() - recent_interval, now()] без обращения к CH.

    Возвращаемый словарь (XCom "return_value") используется всеми downstream-тасками
    через xcom_pull(task_ids="init").
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    s3 = S3Hook(aws_conn_id=S3_CONN)
    if not s3.check_for_bucket(bucket_name=BUCKET):
        s3.create_bucket(bucket_name=BUCKET)
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

    tf  = cfg.get('time_field', 'extract_time')
    lb  = cfg.get('lower_bound') or '1970-01-01 00:00:00'

    # Параметры, общие для delta и recent: передаются оператору экспорта и сохраняются в историю
    reg = {
        'lower_bound':        f"'{lb}'",
        'selfrun_timeout':    str(cfg.get('selfrun_timeout', 10)),
        'max_file_size':      cfg.get('max_file_size', ''),
        'pg_array_format':    cfg.get('pg_array_format', 'False'),
        'format_params':      cfg.get('csv_format_params', ''),
        'xstream_sanitize':   cfg.get('xstream_sanitize', 'False'),
        'sanitize_array':     cfg.get('sanitize_array', 'False'),
        'sanitize_list':      cfg.get('sanitize_list', ''),
        'increment':          str(cfg.get('increment', 60)),
        'overlap':            str(cfg.get('overlap', 0)),
        'time_field':         f"'{tf}'",
    }

    if cfg['sql_get_current']:
        cur_res = get_dict(hook, cfg['sql_get_current'])
        if not cur_res:
            logger.warning("First execution for %s. Bootstrapping from lower_bound=%s.", cfg['tbl'], lb)
            state = {
                'num_state': 0, 'extract_time': lb, 'extract_count': None,
                'loaded': None, 'sent': None, 'confirmed': None,
                'increment': int(cfg.get('increment', 60)),
                'overlap': int(cfg.get('overlap', 0)),
                'time_field': tf,
                'time_from': lb, 'time_to': lb, 'current_time': lb,
            }
            result = {**reg, **_format_cur_state(state)}
        else:
            cur = cur_res[0]
            result = {**reg, **(cur if 'condition' in cur else _format_cur_state(cur))}
    else:
        ri  = int(cfg.get('recent_interval', 3600))
        now = pendulum.now('UTC').replace(microsecond=0)
        t0  = now.subtract(seconds=ri)
        reg.update({
            'extract_time':    f"'{now}'",
            'extract_count':   'null',
            'loaded':          'null',
            'sent':            'null',
            'confirmed':       'null',
            'time_from':       f"'{t0}'",
            'time_to':         f"'{now}'",
            'condition':       f"'{t0}' < {tf} and {tf} <= '{now}'",
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
        'notify_kafka':    lambda v: 1 if v else 0,
        'auto_confirm':    lambda v: 1 if v else 0,
        'max_file_size':   str,
    }
    for key, transform in key_map.items():
        if p.get(key) not in (None, '', 'None'):
            result[key] = transform(p[key])
    
    add_note({k: result.get(k) for k in key_map}, level='Task,DAG', title='⚙️ Delta State')
    return result


@task(task_id='build_meta', pool=POOL_NAME)
def _er_build_meta(cfg, **context):
    """🗂️ Строит .meta JSON с описанием структуры таблицы для ЕР/TFS.

    Порядок колонок: export_time (PRE) + data_cols + ctl_action, ctl_validfrom (SUF).
    Типы: DESCRIBE TABLE → parse_ch_type → TYPE_MAP; для FixedString/Decimal
    извлекаются length/precision/scale. Если fields=['*'] — все колонки таблицы.
    UK оборачивается в массив массивов: ['id'] → [['id']] (стандарт ЕР).
    """
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    dp = context['ti'].xcom_pull(task_ids="init")
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

    fields = cfg.get('fields', ['*'])
    if not fields or fields in (['*'], '*'):
        data_cols = [ch_cols[r[0]] for r in rows]
    else:
        data_cols = []
        for f in fields:
            name = f.split()[-1] if ' as ' in f.lower() else f
            data_cols.append(ch_cols.get(name, {
                "column_name": name, "source_type": "STRING", "length": None,
                "notnull": False, "precision": None, "scale": None,
                "description": f"Calculated: {f}",
            }))

    meta = {
        "mask_file":   None,
        "schema_name": cfg['schema_name'],
        "table_name":  cfg['tbl'],
        "description": cfg.get('description') or None,
        "strategy":    dp.get('strategy', cfg['strategy']),
        "PK":          cfg['PK'],
        "UK":          [cfg['UK']] if cfg['UK'] else [],
        "params":      {"separation": "\t"},
        "columns":     [{k: v for k, v in c.items() if k != 'sql'} for c in EXTRA_PRE] + data_cols + [{k: v for k, v in c.items() if k != 'sql'} for c in EXTRA_SUF],
    }
    context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))
    add_note({"🗂️ build_meta": [c["column_name"] for c in meta["columns"]]}, level='task,dag', context=context)


@task(task_id='pack_zip', pool=POOL_NAME)
def _er_pack_zip(cfg, **context):
    """📦 Упаковывает CSV-файлы из S3 в ZIP-архивы формата ЕР и загружает обратно в S3.

    Каждый CSV оборачивается в отдельный ZIP (стриминг, без буферизации в памяти):
      [replica]__[ts].tkt      — `filename;rowcount` (TKT внутри архива, стандарт ЕР)
      [schema]__[table]__[ts].meta — JSON-схема колонок
      [schema]__[table]__[ts].csv  — данные из S3

    Имена файлов — строго нижний регистр, расширение архива .zip (стандарт ЕР).
    После упаковки исходные CSV удаляются из S3.
    Summary TKT (снаружи архива) перечисляет все ZIP-файлы пакета — передаётся в Kafka.
    """
    from stat import S_IFREG
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from stream_zip import ZIP_32, stream_zip # type: ignore

    ti = context["ti"]
    s3_keys = ti.xcom_pull(task_ids="export_to_s3", key='s3_key_list')
    counts  = ti.xcom_pull(task_ids="export_to_s3", key='row_count_list')
    meta_s  = ti.xcom_pull(task_ids="build_meta",   key='meta_json')

    if not s3_keys:
        ti.xcom_push(key="summary_tkt_name", value="")
        return

    hook, total = S3Hook(aws_conn_id=S3_CONN), len(s3_keys)
    base_ts, uploaded = pendulum.now("UTC"), []

    # stream_zip возвращает генератор байт, а load_file_obj ожидает file-like object
    class _Reader:
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

    for i, (key, rows) in enumerate(zip(s3_keys, counts)):
        ts_s = lambda s: base_ts.add(seconds=i*2 + s).format("YYYYMMDDHHmmss")
        csv_n  = f"{cfg['schema_name']}__{cfg['tbl']}__{ts_s(0)}__{i+1}_{total}_{rows}.csv".lower()
        meta_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts_s(0)}__{i+1}_{total}_{rows}.meta".lower()
        tkt_n  = f"{cfg['replica']}__{ts_s(1)}.tkt".lower()
        zip_n  = f"{cfg['replica']}__{ts_s(2)}__{cfg['tbl']}__{i+1}_{total}_{rows}.zip".lower()
        
        s3_body = hook.get_key(key=key, bucket_name=BUCKET).get()["Body"]
        mtime = base_ts.add(seconds=i*2).naive()
        members = [
            (tkt_n,  mtime, S_IFREG | 0o600, ZIP_32, [f"{csv_n};{rows}".encode()]),
            (meta_n, mtime, S_IFREG | 0o600, ZIP_32, [meta_s.encode()]),
            (csv_n,  mtime, S_IFREG | 0o600, ZIP_32, s3_body.iter_chunks(chunk_size=8*1024*1024)),
        ]
        hook.load_file_obj(_Reader(stream_zip(members)), key=f"{cfg['s3_prefix']}/{zip_n}", bucket_name=BUCKET, replace=True)
        hook.delete_objects(bucket=BUCKET, keys=[key])
        uploaded.append(zip_n)

    summary_tkt = f"{cfg['replica']}__{ts_s(3)}.tkt".lower()
    hook.load_bytes("\n".join(uploaded).encode(), key=f"{cfg['s3_prefix']}/{summary_tkt}", bucket_name=BUCKET, replace=True)
    
    total_rows = sum(int(r) for r in counts)
    ti.xcom_push(key="zip_name_list",    value=uploaded)
    ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
    ti.xcom_push(key="total_row_count",  value=total_rows)
    add_note({"📦 pack_zip": uploaded}, title=f"rows={total_rows} files={total}", level='task,dag', context=context)


@task(task_id='save_status', trigger_rule='none_failed', pool=POOL_NAME)
def _er_save_status(cfg, **context):
    """💾 Записывает результат выгрузки в export.extract_history.

    Запускается только при полном успехе (all_success), поэтому confirmed=null —
    ожидает Kafka-подтверждения от TFS (или остаётся null при auto_confirm).
    extract_time берётся из XCom init как SQL-литерал (уже в кавычках).
    """
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    ti, dp = context['ti'], context['ti'].xcom_pull(task_ids="init")
    if not dp: return
    
    rows = ti.xcom_pull(task_ids="pack_zip", key='total_row_count') or 0
    zips = ti.xcom_pull(task_ids="pack_zip", key='zip_name_list') or []
    zip_arr = "[" + ", ".join(f"'{z}'" for z in zips) + "]"
    
    ClickHouseHook(clickhouse_conn_id=CH_ID).execute(f"""
        INSERT INTO export.extract_history (
            extract_name, extract_time, extract_count, loaded, sent, confirmed,
            increment, overlap, recent_interval, time_field, time_from, time_to, exported_files
        ) SELECT
            '{cfg['tbl']}', {dp['extract_time']}, {rows}, now(), now(), null,
            {dp['increment']}, {dp['overlap']}, {dp['recent_interval']},
            {dp['time_field']}, {dp['time_from']}, {dp['time_to']}, {zip_arr}
    """)
    add_note(
        {"💾 save_status": {"time_from": dp['time_from'], "time_to": dp['time_to'], "rows": rows, "zips": zips}},
        level='task,dag', context=context,
    )


@task(task_id='schedule_next', pool=POOL_NAME)
def _er_schedule_next(cfg, **context):
    """⏭️ Запускает следующий цикл дельты, если time_to ещё не догнал текущее время.

    Запуск откладывается на selfrun_timeout минут, чтобы избежать гонки с источником.
    Для recent-режима is_current всегда True — автозапуск не нужен.
    """
    from airflow.models import DagBag
    from airflow.utils.types import DagRunType
    from airflow.utils.state import DagRunState
    dp = context['ti'].xcom_pull(task_ids="init")
    if str(dp.get('is_current')).lower() in ('true', 't', '1'):
        add_note("✅ delta is current — next run not scheduled", level='task,dag', context=context)
        return

    next_run = pendulum.now('UTC').add(minutes=int(dp['selfrun_timeout']))
    dag = DagBag().get_dag(cfg['dag_id'])
    dag.create_dagrun(
        run_type=DagRunType.MANUAL,
        execution_date=next_run,
        state=DagRunState.QUEUED,
        external_trigger=True,
    )
    add_note(f"⏭️ next run scheduled at {next_run.format('YYYY-MM-DD HH:mm:ss')} UTC", level='task,dag', context=context)

# ── DAG Factory ───────────────────────────────────────────────────────────────

def create_export_dag(table_key: str, params: dict) -> tuple[str, DAG]:
    """🏭 Создаёт Airflow DAG для одной ER-выгрузки на основе записи из Variable.

    table_key — ключ вида "db_name.extract_name"
    params    — словарь из Variable datalab_er_wfs (одна запись er_wf_meta):
                replica, schema, format, PK, UK, params (JSON),
                sql_stmt_export_delta | sql_stmt_export_recent, fields, description

    Возвращает (dag_id, dag) для регистрации в globals().
    """
    from hrp_operators import HrpClickNativeToS3ListOperator # type: ignore
    from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
    from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

    p = get_params(params)

    db, tbl = table_key.split(".", maxsplit=1)
    replica, schema = params['replica'], params['schema']
    if p.get('format', 'TSVWithNames') != 'TSVWithNames':
        raise AirflowFailException("Only TSVWithNames format is supported.")

    scen, prefix = TFS_MAP[replica]
    fields = params.get("fields", [])
    if not fields or fields in (['*'], '*'): fields = ['*']

    def _prep_sql(key):
        """Читает SQL-метадату по ключу и добавляет обязательные поля (export_time, ctl_*)."""
        m = params.get(key)
        if isinstance(m, dict) and "fields" not in m:
            m = {**m, "fields": [c['sql'] for c in EXTRA_PRE] + fields + [c['sql'] for c in EXTRA_SUF]}
        return build_sql(m)

    sql_delta, sql_recent = _prep_sql('sql_stmt_export_delta'), _prep_sql('sql_stmt_export_recent')
    if not (sql_delta or sql_recent) or (sql_delta and sql_recent):
        raise AirflowFailException("Must specify exactly one of delta or recent SQL statements.")

    sql_exp = sql_delta or sql_recent
    if LIMITS.get(ENV_STAND, 0) > 0:
        sql_exp = f"SELECT * FROM ({sql_exp}) LIMIT {LIMITS[ENV_STAND]}"

    cfg = {
        # ── Идентификация ────────────────────────────────────────────────────
        'db':              db,
        'tbl':             tbl,
        'dag_id':          f"export_er__{schema}__{tbl}",
        'schema_name':     schema,
        'replica':         replica,
        'scenario':        scen,
        's3_prefix':       f"{prefix}/{replica}",
        # ── SQL ──────────────────────────────────────────────────────────────
        'sql_get_current': sql_cur_delta(tbl) if sql_delta else None,  # None → recent-режим
        # ── Схема ────────────────────────────────────────────────────────────
        'fields':          fields,
        'PK':              params.get('PK', []),
        'UK':              params.get('UK', []),
        # ── Метаданные ───────────────────────────────────────────────────────
        'description':     params.get('description', ''),
        # ── Параметры из er_wf_meta.params (DEFAULT_PARAMS + overrides) ─────
        'strategy':        p['strategy'],
        'notify_kafka':    bool(p['notify_kafka']),
        'auto_confirm':    p['auto_confirm'],
        'confirm_timeout': p['confirm_timeout'],
        'increment':       p['increment'],
        'selfrun_timeout': p['selfrun_timeout'],
        'lower_bound':     p['lower_bound'],
        'time_field':      p['time_field'],
        'overlap':         p['overlap'],
        'recent_interval': p['recent_interval'],
        'max_file_size':    str(p['max_file_size']),
        'format_params':    p['csv_format_params'],
        'pg_array_format':  'True' if p['pg_array_format'] else 'False',
        'xstream_sanitize': 'True' if p['xstream_sanitize'] else 'False',
        'sanitize_array':   'True' if p['sanitize_array'] else 'False',
        'sanitize_list':    p['sanitize_list'],
    }

    dag = DAG(
        dag_id=cfg['dag_id'], description=params.get('description', f"ER: {table_key}"),
        doc_md=f"```json\n{json.dumps(p, indent=2, default=str)}\n```",
        default_args=DEF_ARGS, start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval=params.get('schedule', '55 0 * * *'), max_active_tasks=1, max_active_runs=1, catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'ER', replica, schema.replace(' ', '_').lower()],
        render_template_as_native_obj=True, is_paused_upon_creation=True,
        params={
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
                p['increment'], type=['integer', 'null'], title='Increment (сек)',
                description='Шаг дельты: time_to = time_from + increment.',
            ),
            'selfrun_timeout': Param(
                p['selfrun_timeout'], type=['integer', 'null'], title='Selfrun timeout (мин)',
                description='Задержка до следующего автозапуска, если дельта не догнала текущее время.',
            ),
            'strategy': Param(
                p['strategy'], type='string', title='Strategy',
                enum=['FULL_UK', 'FULL_NO_UK', 'INC', 'APPEND'],
                description=(
                    'Стратегия загрузки TFS. '
                    'FULL_UK — полное обновление snp с дедубликацией по UK; строки с ctl_action=D отбрасываются TFS. '
                    'FULL_NO_UK — полное обновление без дедубликации; строки с ctl_action=D отбрасываются TFS. '
                    'INC — инкрементальное обновление: ctl_action=D+UK→удаление, ctl_action=D без UK→отброс, '
                    'остальные+UK→обновление, остальные без UK→вставка. '
                    'APPEND — только добавление; ctl_action игнорируется TFS, всегда I.'
                ),
            ),
            'notify_kafka': Param(
                bool(p['notify_kafka']), type='boolean', title='Notify Kafka',
                description='True = отправлять уведомления в Kafka; False = пропустить (стенд).',
            ),
            'auto_confirm': Param(
                bool(p['auto_confirm']), type='boolean', title='Auto confirm',
                description='True = не ждать Kafka-подтверждения от TFS.',
            ),
            'confirm_timeout': Param(
                p['confirm_timeout'], type='integer', title='Confirm timeout (сек)',
                description='Максимальное время ожидания подтверждения из Kafka.',
            ),
            'max_file_size': Param(
                None, type=['integer', 'null'], title='Max file size',
                description='Ограничение размера CSV-файла, байт. None — без ограничений.',
            ),
        },
    )

    with dag:
        def _pre_exp(ctx):
            """pre_execute для export_to_s3: подставляет состояние дельты в SQL и параметры оператора."""
            dp = ctx['ti'].xcom_pull(task_ids="init")
            op = ctx['task']
            op.sql = op.sql.format(export_time=dp['extract_time'], condition=dp['condition'])
            try:
                op.max_size = int(dp.get('max_file_size'))
            except (TypeError, ValueError):
                op.max_size = None
            op.pg_array_format  = dp['pg_array_format'] == 'True'
            op.xstream_sanitize = dp.get('xstream_sanitize', 'False') == 'True'
            op.sanitize_array   = dp.get('sanitize_array', 'False') == 'True'
            op.sanitize_list    = dp.get('sanitize_list') or ''
            try:
                op.format_params = ast.literal_eval(dp['format_params'])
            except (ValueError, SyntaxError):
                op.format_params = {}

        t_init, t_meta = _er_init(cfg=cfg), _er_build_meta(cfg=cfg)
        t_exp = HrpClickNativeToS3ListOperator(
            task_id='export_to_s3', s3_bucket=BUCKET, s3_key=f"{cfg['s3_prefix']}/{{{{ ts_nodash }}}}.csv",
            aws_conn_id=S3_CONN, clickhouse_conn_id=CH_ID, conn_id=CH_ID,
            sql=sql_exp, compression=None, replace=True, post_file_check=False, pre_execute=_pre_exp, pool=POOL_NAME,
        )
        t_zip = _er_pack_zip(cfg=cfg)
        t_msg = ProduceToTopicOperator(
            task_id='notify_tfs', kafka_config_id=KAFKA_OUT_CONN, topic=TOPIC,
            producer_function=produce_msg, producer_function_args=[cfg['scenario'], ''],
            delivery_callback=ON_DELIVERY, pool=POOL_NAME, pre_execute=_pre_kafka(cfg['scenario'], cfg['notify_kafka']),
        )
        t_wait = AwaitMessageSensor(
            task_id='wait_confirm', kafka_config_id=KAFKA_IN_CONN, topics=[KAFKA_IN_TOPIC],
            apply_function="er_export.er_export._kafka_accept_any", trigger_rule='none_failed', pool=POOL_NAME,
            execution_timeout=timedelta(seconds=cfg.get('confirm_timeout', 3600)), pre_execute=_pre_await(cfg.get('auto_confirm'), cfg['notify_kafka'])
        )
        
        t_init >> [t_meta, t_exp] >> t_zip >> t_msg >> t_wait >> _er_save_status(cfg=cfg) >> _er_schedule_next(cfg=cfg)

    return cfg['dag_id'], dag

# ── Dynamic DAG Registration ──────────────────────────────────────────────────

try:
    workflows = obj_load(VAR_NAME)
except Exception as e:
    logger.error("Failed to load workflows: %s", e)
    workflows = {}

for table_key, workflow_params in workflows.items():
    try:
        dag_id, dag_obj = create_export_dag(table_key, workflow_params)
        globals()[dag_id] = dag_obj
    except Exception as e:
        logger.error("DAG generation failed for %s: %s", table_key, e)
        raise  # прерываем парсинг файла: broken DAG лучше, чем молчаливо пропущенная выгрузка
