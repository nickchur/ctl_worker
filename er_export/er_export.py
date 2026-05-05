"""
DAG Factory for ER exports.
Dynamically generates Airflow DAGs based on metadata loaded from ClickHouse.
"""
from __future__ import annotations

import ast
import json
import logging
import time
import uuid
import zipfile

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param
from airflow.utils.task_group import TaskGroup

from er_export.er_config import get_config, get_dict
from plugins.ctl_utils import ctl_obj_load
from plugins.utils import add_note

logger = logging.getLogger("airflow.task")

_cfg = get_config()
CH_ID         = _cfg['CH_ID']
TYPE_MAP      = _cfg['TYPE_MAP']
DEF_ARGS      = _cfg['DEF_ARGS']
ENV_STAND     = _cfg['ENV_STAND']
EXTRA_COLS     = _cfg['EXTRA_COLS']
EXTRA_COLS_PRE = _cfg['EXTRA_COLS_PRE']
EXTRA_COLS_SUF = _cfg['EXTRA_COLS_SUF']
MANDATORY_PRE = _cfg['MANDATORY_PRE']
MANDATORY_SUF = _cfg['MANDATORY_SUF']
MODE          = _cfg['MODE']
LIMITS        = _cfg['LIMITS']
BUCKET        = _cfg['BUCKET']
TFS_MAP       = _cfg['TFS_MAP']
S3_CONN       = _cfg['S3_CONN']
VAR_NAME      = _cfg['VAR_NAME']
POOL_NAME     = _cfg['POOL_NAME']

ON_DELIVERY = 'er_export.er_export.on_delivery'

# ── SQL Builders ──────────────────────────────────────────────────────────────

def build_sql(sql_meta: str | dict, indent: str = "    ") -> str:
    if not sql_meta:
        return ""
    if isinstance(sql_meta, str):
        return sql_meta

    sql = ""
    if sql_meta.get("with"):
        sql += f"{sql_meta['with']}\n"

    fields = sql_meta.get("fields", [])
    fields_str = f",\n{indent}".join(fields) if isinstance(fields, list) else fields
    sql += f"SELECT\n{indent}{fields_str}\nFROM {sql_meta['from']}"

    if sql_meta.get("joins"):
        sql += f"\n{sql_meta['joins']}"
    if sql_meta.get("where"):
        sql += f"\nWHERE {sql_meta['where']}"
    if sql_meta.get("settings"):
        sql += f"\nSETTINGS {sql_meta['settings']}"

    return sql

_REG_WITH = """WITH aggr AS (
    SELECT
        argMinIf(extract_name, prio, prio=1)                                       as extract_name,
        argMinIf(auto_confirm_delta, prio, prio=1)                                 as auto_confirm_delta,
        argMinIf(lower_bound, prio, prio=1)                                        as lower_bound,
        argMinIf(selfrun_timeout, prio, prio=1)                                    as selfrun_timeout,
        argMinIf(compression_type, prio, lower(compression_type) <> 'default')    as compression_type,
        argMinIf(compression_ext, prio, lower(compression_ext) <> 'default')      as compression_ext,
        argMinIf(max_file_size, prio, lower(max_file_size) <> 'default')          as max_file_size,
        argMinIf(pg_array_format, prio, prio=1)                                    as pg_array_format,
        argMinIf(csv_format_params, prio, lower(csv_format_params) <> 'default')  as csv_format_params,
        argMinIf(xstream_sanitize, prio, prio=1)                                   as xstream_sanitize,
        argMinIf(sanitize_array, prio, prio=1)                                     as sanitize_array,
        argMinIf(sanitize_list, prio, lower(sanitize_list) <> 'default')           as sanitize_list
        {extra_aggr}
    FROM (
        SELECT 1 as prio, *
        FROM export.extract_registry_vw WHERE extract_name = '{tbl}'
        UNION ALL
        SELECT 2 as prio, *
        FROM export.extract_registry_vw WHERE extract_name = 'default'
    )
)"""

_REG_FIELDS_BASE = [
    "auto_confirm_delta                                                           as auto_confirm_delta",
    "concat('\\'', toString(toDateTimeOrDefault(lower_bound)), '\\'')            as lower_bound",
    "toString(selfrun_timeout)                                                    as selfrun_timeout",
    "compression_type                                                             as compression_type",
    "compression_ext                                                              as compression_ext",
    "max_file_size                                                                as max_file_size",
    "If(pg_array_format = 1, 'True', 'False')                                   as pg_array_format",
    "csv_format_params                                                            as format_params",
    "If(xstream_sanitize = 1, 'True', 'False')                                  as xstream_sanitize",
    "If(sanitize_array = 1, 'True', 'False')                                     as sanitize_array",
    "sanitize_list                                                                as sanitize_list",
]

def sql_reg_delta(tbl: str) -> str:
    return build_sql({
        "with":     _REG_WITH.format(tbl=tbl, extra_aggr=''),
        "fields":   _REG_FIELDS_BASE,
        "from":     "aggr",
        "where":    f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1",
    })

def sql_reg_recent(tbl: str) -> str:
    extra_aggr = """,
        argMinIf(increment, prio, prio = 1)       as increment,
        argMinIf(overlap, prio, prio = 1)         as overlap,
        argMinIf(time_field, prio, prio = 1)      as time_field,
        argMinIf(recent_interval, prio, prio = 1) as recent_interval"""
    return build_sql({
        "with":   _REG_WITH.format(tbl=tbl, extra_aggr=extra_aggr),
        "fields": _REG_FIELDS_BASE + [
            "now()                                                                       as cur_time",
            "concat('\\'', toString(cur_time), '\\'')                                   as extract_time",
            "'null'                                                                      as extract_count",
            "'null'                                                                      as loaded",
            "'null'                                                                      as sent",
            "'null'                                                                      as confirmed",
            "toString(increment)                                                         as increment",
            "toString(overlap)                                                           as overlap",
            "concat('\\'', time_field, '\\'')                                            as time_field",
            "concat('\\'', toString(cur_time - recent_interval), '\\'')                 as time_from",
            "concat('\\'', toString(cur_time), '\\'')                                   as time_to",
            "concat('\\'', toString(cur_time - recent_interval), '\\' < ', time_field, ' and ', time_field, ' <= \\'', toString(cur_time), '\\'') as condition",
            "'True'                                                                      as is_current",
            "toString(recent_interval)                                                   as recent_interval",
            "toString(0)                                                                 as num_state",
        ],
        "from":     "aggr",
        "where":    f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1",
    })

def sql_cur_delta(tbl: str) -> str:
    return build_sql({
        "fields": [
            "toString(a.num_state)                                                                   as num_state",
            "concat('\\'', toString(a.extract_time), '\\'')                                          as extract_time",
            "ifNull(toString(a.extract_count), 'null')                                               as extract_count",
            "if(a.extract_count is null, 'null', concat('\\'', toString(a.loaded), '\\''))           as loaded",
            "if(a.extract_count is null, 'null', concat('\\'', toString(a.sent), '\\''))             as sent",
            "if(a.extract_count is null, 'null', concat('\\'', toString(a.confirmed), '\\''))        as confirmed",
            "toString(a.increment)                                                                    as increment",
            "toString(a.overlap)                                                                      as overlap",
            "concat('\\'', a.time_field, '\\'')                                                       as time_field",
            "concat('\\'', toString(a.time_from), '\\'')                                              as time_from",
            "concat('\\'', toString(a.time_to), '\\'')                                                as time_to",
            "concat('\\'', toString(a.time_from), '\\' < ', a.time_field, ' and ', a.time_field, ' <= \\'', toString(a.time_to), '\\'') as condition",
            "if(a.current_time = a.extract_time, 'True', 'False')                                   as is_current",
            "toString(0)                                                                              as recent_interval",
        ],
        "from": f"(SELECT * FROM export.extract_current_vw WHERE extract_name = '{tbl}') as a",
    })

# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_dt(v):
    return 'null' if v is None else f"'{v}'"

def _format_cur(cur: dict) -> dict:
    tf = str(cur['time_field']).strip("'")
    ec = cur['extract_count']
    return {
        'num_state':       str(cur['num_state']),
        'extract_time':    _fmt_dt(cur['extract_time']),
        'extract_count':   'null' if ec is None else str(ec),
        'loaded':          'null' if ec is None else _fmt_dt(cur['loaded']),
        'sent':            'null' if ec is None else _fmt_dt(cur['sent']),
        'confirmed':       'null' if ec is None else _fmt_dt(cur['confirmed']),
        'increment':       str(cur['increment']),
        'overlap':         str(cur['overlap']),
        'time_field':      f"'{tf}'",
        'time_from':       _fmt_dt(cur['time_from']),
        'time_to':         _fmt_dt(cur['time_to']),
        'condition':       f"{_fmt_dt(cur['time_from'])} < {tf} and {tf} <= {_fmt_dt(cur['time_to'])}",
        'is_current':      'True' if cur['current_time'] == cur['extract_time'] else 'False',
        'recent_interval': '0',
    }

def parse_type(ch_type: str, type_map: dict) -> tuple[str, bool]:
    notnull = True
    if ch_type.startswith("LowCardinality(") and ch_type.endswith(")"):
        ch_type = ch_type[15:-1]
    if ch_type.startswith("Nullable(") and ch_type.endswith(")"):
        ch_type = ch_type[9:-1]
        notnull = False
    base = ch_type.split("(")[0]
    return type_map.get(base, "STRING"), notnull

def produce_msg(scenario_id: str, file_name: str, throttle_delay: int = 1):
    time.sleep(throttle_delay)
    rq_uuid = str(uuid.uuid4()).replace('-', '')
    message = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <TransferFileCephRq>
        <RqUID>{rq_uuid}</RqUID>
        <RqTm>{pendulum.now().format('YYYY-MM-DDTHH:mm:ss.SSSZ')}</RqTm>
        <ScenarioInfo>
            <ScenarioId>{scenario_id}</ScenarioId>
        </ScenarioInfo>
        <File>
            <FileInfo>
                <Name>{file_name}</Name>
            </FileInfo>
        </File>
    </TransferFileCephRq>"""
    logger.info("Prepare message to send:\n%s", message)
    yield None, message

def on_delivery(err: Exception | None, msg) -> None:
    if err is not None:
        raise AirflowFailException(f"Failed to deliver message: {err}")
    logger.info(
        "Produced record to topic %s partition [%s] @ offset %s\n%s",
        msg.topic(), msg.partition(), msg.offset(), msg.value(),
    )

def _pre_kafka(scenario: str):
    def pre_execute(context):
        if MODE != 'prod':
            raise AirflowSkipException("Kafka notification skipped in test mode")
        gid = context['task'].task_group.group_id
        summary_tkt = context['ti'].xcom_pull(task_ids=f"{gid}.pack_zip", key='summary_tkt_name')
        if not summary_tkt:
            raise AirflowSkipException("No data exported, skipping notification")
        context['task'].producer_function_args = [scenario, summary_tkt]
    return pre_execute

# ── TaskGroup factory ─────────────────────────────────────────────────────────

def export_tg(gid: str, cfg: dict, sql: str) -> TaskGroup:
    from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator  # type: ignore
    from hrp_operators import HrpClickNativeToS3ListOperator  # type: ignore

    with TaskGroup(group_id=gid) as tg:

        @task(task_id='init')
        def init(cfg, **context):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            S3Hook(aws_conn_id=S3_CONN).create_bucket(bucket_name=BUCKET)
            hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
            reg_res = get_dict(hook, cfg['sql_get_registry'])
            if not reg_res:
                raise AirflowFailException(f"No registry entry found for {cfg['tbl']}")
            reg = reg_res[0]
            if reg['auto_confirm_delta']:
                hook.execute(cfg['sql_auto_confirm'])
            if cfg['sql_get_current']:
                cur_res = get_dict(hook, cfg['sql_get_current'])
                if not cur_res:
                    raise AirflowFailException(f"No delta state found for {cfg['tbl']}")
                cur = cur_res[0]
                result = {**reg, **(cur if 'condition' in cur else _format_cur(cur))}
            else:
                result = reg
            p = context['params']
            if p.get('extract_time'):
                result['extract_time'] = f"'{p['extract_time']}'"
            if p.get('condition'):
                result['condition'] = p['condition']
            if p.get('is_current') is not None:
                result['is_current'] = 'True' if p['is_current'] else 'False'
            if p.get('increment') is not None:
                result['increment'] = str(p['increment'])
            if p.get('selfrun_timeout') is not None:
                result['selfrun_timeout'] = str(p['selfrun_timeout'])
            if p.get('strategy'):
                result['strategy'] = p['strategy']
            if p.get('auto_confirm') is not None:
                result['auto_confirm'] = 1 if p['auto_confirm'] else 0
            if p.get('max_file_size') is not None:
                result['max_file_size'] = str(p['max_file_size'])
            add_note(
                {k: result.get(k) for k in (
                    'extract_time', 'condition', 'is_current', 'increment',
                    'selfrun_timeout', 'strategy', 'auto_confirm', 'max_file_size'
                )},
                level='Task,DAG', title='Delta',
            )
            return result

        t_init = init(cfg=cfg)

        @task(task_id='build_meta')
        def build_meta(cfg, **context):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            ti = context['ti']
            dp = ti.xcom_pull(task_ids=f"{gid}.init")
            hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
            rows, _ = hook.execute(f"DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}", with_column_types=True)
            data_cols = []
            for row in rows:
                source_type, notnull = parse_type(row[1], TYPE_MAP)
                data_cols.append({
                    "column_name": row[0],
                    "source_type": source_type,
                    "length":      None,
                    "notnull":     notnull,
                    "precision":   None,
                    "scale":       None,
                    "description": row[4] if len(row) > 4 and row[4] else None,
                })
            columns = EXTRA_COLS_PRE + data_cols + EXTRA_COLS_SUF
            meta = {
                "schema_name": cfg['schema_name'],
                "table_name":  cfg['tbl'],
                "strategy":    dp.get('strategy', cfg['strategy']),
                "PK":          cfg['PK'],
                "UK":          cfg['UK'],
                "params":      {"separation": "\t", "escapesymbol": "\""},
                "columns":     columns,
            }
            context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))

        t_build_meta = build_meta(cfg=cfg)

        def _pre_execute_copy(context):
            ti = context['ti']
            dp = ti.xcom_pull(task_ids=f"{gid}.init")
            op = context['task']
            op.sql = op.sql.format(export_time=dp['extract_time'], condition=dp['condition'])
            raw_max_size = dp.get('max_file_size')
            if str(raw_max_size).lower() in ('default', 'none', 'null', ''):
                op.max_size = None
            else:
                try:
                    op.max_size = int(raw_max_size)
                except (ValueError, TypeError):
                    logger.warning("Invalid max_file_size: %r, using None", raw_max_size)
                    op.max_size = None
            op.pg_array_format  = dp['pg_array_format'] == 'True'
            op.xstream_sanitize = dp.get('xstream_sanitize', 'False') == 'True'
            op.sanitize_array   = dp.get('sanitize_array', 'False') == 'True'
            op.sanitize_list    = dp.get('sanitize_list') or ''
            try:
                op.format_params = ast.literal_eval(dp['format_params']) if dp['format_params'] else {}
            except (ValueError, TypeError):
                logger.warning("Unparseable format_params: %r", dp['format_params'])
                op.format_params = {}

        export_to_s3 = HrpClickNativeToS3ListOperator(
            task_id='export_to_s3',
            s3_bucket=BUCKET,
            s3_key=f"{cfg['s3_prefix']}/{{{{ ts_nodash }}}}.csv",
            sql=sql,
            compression=None,
            replace=True,
            post_file_check=False,
            pre_execute=_pre_execute_copy,
        )

        @task(task_id='pack_zip')
        def pack_zip(cfg, **context):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            ti = context["ti"]

            s3_key_list    = ti.xcom_pull(task_ids=f"{gid}.export_to_s3", key='s3_key_list')
            row_count_list = ti.xcom_pull(task_ids=f"{gid}.export_to_s3", key='row_count_list')
            meta_json_str  = ti.xcom_pull(task_ids=f"{gid}.build_meta",   key='meta_json')

            if not s3_key_list or not row_count_list:
                logger.warning("No CSV parts exported — skipping ZIP packaging")
                ti.xcom_push(key="zip_name_list",    value=[])
                ti.xcom_push(key="summary_tkt_name", value="")
                ti.xcom_push(key="total_row_count",  value=0)
                return

            from datetime import datetime
            from stat import S_IFREG
            from stream_zip import ZIP_32, stream_zip

            meta_bytes = meta_json_str.encode()
            hook       = S3Hook(aws_conn_id=S3_CONN)
            total      = len(s3_key_list)
            base_ts    = pendulum.now("UTC")
            uploaded_zips = []

            class _Reader:
                """Wrap stream_zip generator as file-like object for load_file_obj."""
                def __init__(self, gen):
                    self._it  = gen
                    self._buf = bytearray()

                def read(self, n=-1):
                    if n < 0:
                        self._buf.extend(b''.join(self._it))
                        data, self._buf = bytes(self._buf), bytearray()
                        return data
                    while len(self._buf) < n:
                        try:
                            self._buf.extend(next(self._it))
                        except StopIteration:
                            break
                    chunk, self._buf = bytes(self._buf[:n]), self._buf[n:]
                    return chunk

            for i, (s3_key, row_count) in enumerate(zip(s3_key_list, row_count_list)):
                rows     = int(row_count)
                part     = i + 1
                inner_ts = base_ts.add(seconds=i * 2    ).format("YYYYMMDDHHmmss")
                tkt_ts   = base_ts.add(seconds=i * 2 + 1).format("YYYYMMDDHHmmss")
                outer_ts = base_ts.add(seconds=i * 2 + 2).format("YYYYMMDDHHmmss")

                csv_name  = f"{cfg['schema_name']}__{cfg['tbl']}__{inner_ts}__{part}_{total}_{rows}.csv"
                meta_name = f"{cfg['schema_name']}__{cfg['tbl']}__{inner_ts}__{part}_{total}_{rows}.meta"
                tkt_name  = f"{cfg['replica']}__{tkt_ts}.tkt"
                zip_name  = f"{cfg['replica']}__{outer_ts}__{cfg['tbl']}__{part}_{total}_{rows}.csv.zip"
                zip_s3_key = f"{cfg['s3_prefix']}/{zip_name}"

                tkt_bytes = f"{csv_name};{rows}".encode()
                s3_body   = hook.get_key(key=s3_key, bucket_name=BUCKET).get()["Body"]

                now = datetime.now()
                members = [
                    (tkt_name,  now, S_IFREG | 0o600, ZIP_32, [tkt_bytes]),
                    (meta_name, now, S_IFREG | 0o600, ZIP_32, [meta_bytes]),
                    (csv_name,  now, S_IFREG | 0o600, ZIP_32,
                     s3_body.iter_chunks(chunk_size=8 * 1024 * 1024)),
                ]
                hook.load_file_obj(
                    _Reader(stream_zip(members)),
                    key=zip_s3_key,
                    bucket_name=BUCKET,
                    replace=True,
                )
                hook.delete_objects(bucket=BUCKET, keys=[s3_key])
                uploaded_zips.append(zip_name)
                logger.info("Packaged %d/%d: %s", part, total, zip_name)

            summary_ts  = base_ts.add(seconds=(total - 1) * 2 + 3).format("YYYYMMDDHHmmss")
            summary_tkt = f"{cfg['replica']}__{summary_ts}.tkt"
            hook.load_bytes(
                "\n".join(uploaded_zips).encode(),
                key=f"{cfg['s3_prefix']}/{summary_tkt}",
                bucket_name=BUCKET,
                replace=True,
            )
            logger.info("Created summary tkt: %s", summary_tkt)

            total_rows = sum(int(r) for r in row_count_list)
            add_note({'rows': total_rows, 'parts': total, 'files': uploaded_zips}, level='Task,DAG', title='Exported')
            ti.xcom_push(key="zip_name_list",    value=uploaded_zips)
            ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
            ti.xcom_push(key="total_row_count",  value=total_rows)

        t_pack = pack_zip(cfg=cfg)

        notify_tfs = ProduceToTopicOperator(
            task_id='notify_tfs',
            topic="{{ params.topic }}",
            producer_function=produce_msg,
            producer_function_args=[cfg['scenario'], ''],
            delivery_callback=ON_DELIVERY,
            pool=POOL_NAME,
            pre_execute=_pre_kafka(cfg['scenario']),
        )

        if not cfg.get('auto_confirm', 1):
            from airflow.providers.apache.kafka.sensors.await_message import AwaitMessageSensor  # type: ignore

            def _handle_confirm(message):
                logger.info("Received confirmation from Kafka: %s", message.value())
                return True

            t_wait_confirm = AwaitMessageSensor(
                task_id='wait_confirm',
                kafka_config_id=cfg['kafka_in_conn'],
                topics=[cfg['kafka_in_topic']],
                apply_function=_handle_confirm,
                poke_interval=60,
                timeout=3600,
                mode='reschedule',
                trigger_rule='none_failed',
            )
        else:
            from airflow.operators.empty import EmptyOperator  # type: ignore
            t_wait_confirm = EmptyOperator(task_id='wait_confirm', trigger_rule='none_failed')

        @task(task_id='save_status', trigger_rule='none_failed_min_one_success')
        def save_status(cfg, **context):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            ti         = context['ti']
            dp         = ti.xcom_pull(task_ids=f"{gid}.init")
            total_rows = ti.xcom_pull(task_ids=f"{gid}.pack_zip", key='total_row_count')
            zip_names  = ti.xcom_pull(task_ids=f"{gid}.pack_zip", key='zip_name_list')
            hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
            zip_arr = "[" + ", ".join(f"'{z}'" for z in (zip_names or [])) + "]"
            hook.execute(f"""
                insert into export.extract_history (
                    extract_name, extract_time, extract_count,
                    loaded, sent, confirmed,
                    increment, overlap, recent_interval,
                    time_field, time_from, time_to, exported_files
                ) select
                    '{cfg['tbl']}',
                    {dp['extract_time']},
                    {total_rows},
                    now(), now(), null,
                    {dp['increment']}, {dp['overlap']}, {dp['recent_interval']},
                    {dp['time_field']}, {dp['time_from']}, {dp['time_to']},
                    {zip_arr}
            """)
            add_note({'rows': total_rows, 'files': zip_names}, level='Task', title='Saved')

        t_save = save_status(cfg=cfg)

        @task(task_id='schedule_next')
        def schedule_next(cfg, **context):
            from airflow.models import DagBag
            from airflow.utils.types import DagRunType
            from airflow.utils.state import DagRunState
            ti = context['ti']
            dp = ti.xcom_pull(task_ids=f"{gid}.init")
            if str(dp['is_current']).lower() in ('true', 't', '1'):
                add_note('already current', level='Task,DAG', title='Schedule')
                return
            run_date = pendulum.now('UTC').add(minutes=int(dp['selfrun_timeout']))
            dag_obj = DagBag().get_dag(cfg['dag_id'])
            dag_obj.create_dagrun(
                run_type=DagRunType.MANUAL,
                execution_date=run_date,
                state=DagRunState.QUEUED,
                external_trigger=True,
            )
            add_note(str(run_date), level='Task,DAG', title='Next run')

        t_schedule = schedule_next(cfg=cfg)

        t_init >> [t_build_meta, export_to_s3]
        [t_build_meta, export_to_s3] >> t_pack
        t_pack >> notify_tfs >> t_wait_confirm >> t_save >> t_schedule

    return tg

# ── DAG factory ───────────────────────────────────────────────────────────────

def create_export_dag(table_key: str, params: dict) -> tuple[str, DAG]:
    db, tbl = table_key.split(".", maxsplit=1)
    replica = params['replica']
    schema  = params['schema']
    fmt     = params.get('format', 'TSVWithNames')

    if fmt != 'TSVWithNames':
        raise AirflowFailException(f"Unsupported format: {fmt!r}. Only TSVWithNames is supported.")

    scen, prefix = TFS_MAP[replica]
    s3_prefix = f"{prefix}/{replica}"

    def _prepare_sql(sql_key):
        meta = params.get(sql_key)
        if isinstance(meta, dict) and "fields" not in meta:
            meta = dict(meta)
            meta["fields"] = MANDATORY_PRE + params.get("fields", []) + MANDATORY_SUF
        return build_sql(meta)

    sql_delta  = _prepare_sql('sql_stmt_export_delta')
    sql_recent = _prepare_sql('sql_stmt_export_recent')

    if not (sql_delta or sql_recent):
        raise AirflowFailException("One of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' must be specified!")
    if sql_delta and sql_recent:
        raise AirflowFailException("Only one of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' can be specified!")

    sql_export = sql_delta or sql_recent
    row_limit  = LIMITS.get(ENV_STAND, 0)
    if row_limit > 0:
        sql_export = f"select * from ({sql_export}) limit {row_limit}"

    if sql_delta:
        sql_reg, sql_cur = sql_reg_delta(tbl), sql_cur_delta(tbl)
    else:
        sql_reg, sql_cur = sql_reg_recent(tbl), None

    dag_id = f"export_er__{replica}__{tbl}"
    cfg = {
        'db':               db,
        'tbl':              tbl,
        'dag_id':           dag_id,
        'schema_name':      schema,
        'replica':          replica,
        'scenario':         scen,
        's3_prefix':        s3_prefix,
        'bucket':           BUCKET,
        'topic':            DEF_ARGS['topic'],
        'kafka_in_conn':    DEF_ARGS['kafka_in_conn'],
        'kafka_in_topic':   DEF_ARGS['kafka_in_topic'],
        'sql_auto_confirm': f"""
            insert into export.extract_history (
                extract_name, extract_time, extract_count, loaded, sent, confirmed,
                increment, overlap, recent_interval, time_field, time_from, time_to, exported_files
            )
            select
                extract_name, extract_time, extract_count, loaded, sent, now(),
                increment, overlap, recent_interval, time_field, time_from, time_to, exported_files
            from export.extract_history_vw
            where extract_name = '{tbl}'
                  and sent is not null and confirmed is null
        """,
        'sql_get_registry': sql_reg,
        'sql_get_current':  sql_cur,
        'auto_confirm':     params.get('auto_confirm', 1),
        'strategy':         params.get('strategy', 'FULL_UK'),
        'PK':               params.get('PK', []),
        'UK':               params.get('UK', []),
    }

    description = params.get('description') or f"ER-выгрузка {table_key} → S3 ZIP → TFS Kafka"

    dag = DAG(
        dag_id=dag_id,
        description=description,
        doc_md='```\n' + json.dumps({**params, 'description': description}, ensure_ascii=False, indent=2, default=str) + '\n```',
        default_args=DEF_ARGS,
        start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval='55 0 * * *',
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'ER', replica, schema.replace(' ', '_').lower()],
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
        params={
            'extract_time':    Param(None,                           type=['string', 'null'],  title='Extract time',      description='Техническое время выгрузки (правая граница дельты). Формат: 2024-01-01 00:00:00'),
            'condition':       Param(None,                           type=['string', 'null'],  title='Condition',          description='Перезаписать значение плейсхолдера {condition}'),
            'is_current':      Param(None,                           type=['boolean', 'null'], title='Is current',         description='True = данные актуальны, остановить авто-перезапуск (self-run)'),
            'increment':       Param(params.get('increment'),        type=['integer', 'null'], title='Increment (сек)',    description='Размер временного окна (шаг) одной выгрузки в секундах'),
            'selfrun_timeout': Param(params.get('selfrun_timeout'),  type=['integer', 'null'], title='Selfrun timeout (мин)', description='Пауза перед автоматическим запуском следующей итерации (мин)'),
            'strategy':        Param(params.get('strategy', 'FULL_UK'), type='string',         title='Strategy',           description='Стратегия слияния данных (FULL_UK, DELTA_UK и др.)'),
            'auto_confirm':    Param(bool(params.get('auto_confirm', 1)), type='boolean',      title='Auto confirm',       description='True = не ждать подтверждения из Kafka'),
            'max_file_size':   Param(None,                           type=['integer', 'null'], title='Max file size',      description='Максимальный размер одного CSV файла (байт). По умолчанию из реестра.'),
            'pool':            Param(POOL_NAME,                      type='string',            title='Kafka Pool',         description='Пул Airflow для задач Kafka'),
            'topic':           Param(cfg['topic'],                   type='string',            title='Kafka Topic',        description='Топик для отправки уведомлений'),
        },
    )

    with dag:
        export_tg(gid='er_export', cfg=cfg, sql=sql_export)

    return dag_id, dag


wfs = ctl_obj_load(VAR_NAME)

for table_key, params in wfs.items():
    try:
        dag_id, dag = create_export_dag(table_key, params)
        globals()[dag_id] = dag
    except Exception as e:
        logger.error(f"Failed to generate DAG for {table_key}: {e}")
