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
from typing import Any

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param

from er_export.er_config import get_config, get_dict, obj_load
from plugins.utils import add_note

logger = logging.getLogger("airflow.task")

# ── Configuration & Constants ────────────────────────────────────────────────

_cfg = get_config()
CH_ID          = _cfg['CH_ID']
TYPE_MAP       = _cfg['TYPE_MAP']
DEF_ARGS       = _cfg['DEF_ARGS']
ENV_STAND      = _cfg['ENV_STAND']
EXTRA_COLS_PRE = _cfg['EXTRA_COLS_PRE']
EXTRA_COLS_SUF = _cfg['EXTRA_COLS_SUF']
MANDATORY_PRE  = _cfg['MANDATORY_PRE']
MANDATORY_SUF  = _cfg['MANDATORY_SUF']
MODE           = _cfg['MODE']
LIMITS         = _cfg['LIMITS']
BUCKET         = _cfg['BUCKET']
TFS_MAP        = _cfg['TFS_MAP']
S3_CONN        = _cfg['S3_CONN']
VAR_NAME       = _cfg['VAR_NAME']
POOL_NAME      = _cfg['POOL_NAME']

ON_DELIVERY    = 'er_export.er_export.on_delivery'

# ── SQL Query Templates ───────────────────────────────────────────────────────

_REG_WITH_TEMPLATE = """WITH aggr AS (
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
        argMinIf(sanitize_list, prio, lower(sanitize_list) <> 'default')           as sanitize_list,
        argMinIf(increment, prio, prio=1)                                          as increment,
        argMinIf(overlap, prio, prio=1)                                            as overlap,
        argMinIf(time_field, prio, prio=1)                                         as time_field
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
    "auto_confirm_delta",
    "concat('\\'', toString(toDateTimeOrDefault(lower_bound)), '\\'') as lower_bound",
    "toString(selfrun_timeout) as selfrun_timeout",
    "compression_type",
    "compression_ext",
    "max_file_size",
    "If(pg_array_format = 1, 'True', 'False') as pg_array_format",
    "csv_format_params as format_params",
    "If(xstream_sanitize = 1, 'True', 'False') as xstream_sanitize",
    "If(sanitize_array = 1, 'True', 'False') as sanitize_array",
    "sanitize_list",
    "toString(increment) as increment",
    "toString(overlap) as overlap",
    "concat('\\'', time_field, '\\'') as time_field",
]

# ── SQL Builders ──────────────────────────────────────────────────────────────

def build_sql(sql_meta: str | dict, indent: str = "    ") -> str:
    """Assembles a SQL query from a metadata dictionary."""
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


def sql_reg_delta(tbl: str) -> str:
    """SQL to fetch baseline registry settings for a delta-enabled table."""
    return build_sql({
        "with":     _REG_WITH_TEMPLATE.format(tbl=tbl, extra_aggr=''),
        "fields":   _REG_FIELDS_BASE,
        "from":     "aggr",
        "where":    f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1",
    })


def sql_reg_recent(tbl: str) -> str:
    """SQL to fetch registry settings for a 'recent' interval export."""
    extra_aggr = ", argMinIf(recent_interval, prio, prio = 1) as recent_interval"
    return build_sql({
        "with":   _REG_WITH_TEMPLATE.format(tbl=tbl, extra_aggr=extra_aggr),
        "fields": _REG_FIELDS_BASE + [
            "now() as cur_time",
            "concat('\\'', toString(cur_time), '\\'') as extract_time",
            "'null' as extract_count", "'null' as loaded", "'null' as sent", "'null' as confirmed",
            "concat('\\'', toString(cur_time - recent_interval), '\\'') as time_from",
            "concat('\\'', toString(cur_time), '\\'') as time_to",
            "concat('\\'', toString(cur_time - recent_interval), '\\' < ', time_field, ' and ', time_field, ' <= \\'', toString(cur_time), '\\'') as condition",
            "'True' as is_current",
            "toString(recent_interval) as recent_interval",
            "toString(0) as num_state",
        ],
        "from":     "aggr",
        "where":    f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1",
    })


def sql_cur_delta(tbl: str) -> str:
    """SQL to fetch the current delta state for processing."""
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
    """Formats a value as a SQL string literal or 'null'."""
    return 'null' if v is None else f"'{v}'"


def _format_cur_state(cur: dict) -> dict:
    """Prepares state dictionary for runtime SQL injection."""
    tf = str(cur['time_field']).strip("'")
    ec = cur['extract_count']
    fmt_dt = lambda x: _fmt_val(x)
    return {
        'num_state':       str(cur['num_state']),
        'extract_time':    fmt_dt(cur['extract_time']),
        'extract_count':   'null' if ec is None else str(ec),
        'loaded':          fmt_dt(cur['loaded']) if ec is not None else 'null',
        'sent':            fmt_dt(cur['sent']) if ec is not None else 'null',
        'confirmed':       fmt_dt(cur['confirmed']) if ec is not None else 'null',
        'increment':       str(cur['increment']),
        'overlap':         str(cur['overlap']),
        'time_field':      f"'{tf}'",
        'time_from':       fmt_dt(cur['time_from']),
        'time_to':         fmt_dt(cur['time_to']),
        'condition':       f"{fmt_dt(cur['time_from'])} < {tf} and {tf} <= {fmt_dt(cur['time_to'])}",
        'is_current':      'True' if cur.get('current_time') == cur.get('extract_time') else 'False',
        'recent_interval': str(cur.get('recent_interval', 0)),
    }


def parse_ch_type(ch_type: str, mapping: dict) -> tuple[str, bool]:
    """Resolves ClickHouse type to target type and nullability."""
    notnull = True
    if ch_type.startswith("LowCardinality("): ch_type = ch_type[15:-1]
    if ch_type.startswith("Nullable("): 
        ch_type = ch_type[9:-1]
        notnull = False
    base = ch_type.split("(")[0]
    return mapping.get(base, "STRING"), notnull


def produce_msg(scenario_id: str, file_name: str, throttle_delay: int = 1):
    """Generates XML notification for TFS system."""
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
    """Kafka delivery callback."""
    if err: raise AirflowFailException(f"Kafka delivery failed: {err}")
    logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())

# ── Tasks ───────────────────────────────────────────────────────────────────

@task(task_id='init', pool=POOL_NAME)
def _er_init(cfg, **context):
    """Initializes export state, handles bootstrap for new tables."""
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    
    S3Hook(aws_conn_id=S3_CONN).create_bucket(bucket_name=BUCKET)
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
    
    reg = get_dict(hook, cfg['sql_get_registry'])
    if not reg: raise AirflowFailException(f"Registry entry missing for {cfg['tbl']}")
    reg = reg[0]
    
    if reg.get('auto_confirm_delta'): hook.execute(cfg['sql_auto_confirm'])
    
    if cfg['sql_get_current']:
        cur_res = get_dict(hook, cfg['sql_get_current'])
        if not cur_res:
            logger.warning("First execution for %s. Bootstrapping from registry.", cfg['tbl'])
            lb = reg['lower_bound'].strip("'")
            state = {
                'num_state': 0, 'extract_time': lb, 'extract_count': None,
                'loaded': None, 'sent': None, 'confirmed': None,
                'increment': int(reg.get('increment', 60)),
                'overlap': int(reg.get('overlap', 0)),
                'time_field': reg.get('time_field', "'extract_time'"),
                'time_from': lb, 'time_to': lb, 'current_time': lb,
            }
            result = {**reg, **_format_cur_state(state)}
        else:
            cur = cur_res[0]
            result = {**reg, **(cur if 'condition' in cur else _format_cur_state(cur))}
    else:
        result = reg

    # Parameter overrides
    p = context['params']
    key_map = {
        'extract_time': lambda v: f"'{v}'",
        'condition': str, 'is_current': lambda v: 'True' if v else 'False',
        'increment': str, 'selfrun_timeout': str, 'strategy': str,
        'auto_confirm': lambda v: 1 if v else 0, 'max_file_size': str
    }
    for key, transform in key_map.items():
        if p.get(key) is not None: result[key] = transform(p[key])
    
    add_note({k: result.get(k) for k in key_map}, level='Task,DAG', title='Delta State')
    return result


@task(task_id='build_meta', pool=POOL_NAME)
def _er_build_meta(cfg, **context):
    """Builds .meta JSON describing the exported data structure."""
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    dp = context['ti'].xcom_pull(task_ids="init")
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
    rows, _ = hook.execute(f"DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}", with_column_types=True)
    
    ch_cols = {}
    for row in rows:
        stype, notnull = parse_ch_type(row[1], TYPE_MAP)
        ch_cols[row[0]] = {
            "column_name": row[0], "source_type": stype, "notnull": notnull,
            "description": row[4] if len(row) > 4 else None,
        }
    
    fields = cfg.get('fields', ['*'])
    if not fields or fields in (['*'], '*'):
        data_cols = [ch_cols[r[0]] for r in rows]
    else:
        data_cols = []
        for f in fields:
            name = f.split()[-1] if ' as ' in f.lower() else f
            data_cols.append(ch_cols.get(name, {
                "column_name": name, "source_type": "STRING", "notnull": False,
                "description": f"Calculated: {f}"
            }))

    meta = {
        "schema_name": cfg['schema_name'], "table_name": cfg['tbl'],
        "strategy": dp.get('strategy', cfg['strategy']),
        "PK": cfg['PK'], "UK": cfg['UK'],
        "params": {"separation": "\t", "escapesymbol": "\""},
        "columns": EXTRA_COLS_PRE + data_cols + EXTRA_COLS_SUF,
    }
    context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))


@task(task_id='pack_zip', pool=POOL_NAME)
def _er_pack_zip(cfg, **context):
    """Streams data from S3, wraps into ZIP with metadata, and uploads back to S3."""
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
        csv_n  = f"{cfg['schema_name']}__{cfg['tbl']}__{ts_s(0)}__{i+1}_{total}_{rows}.csv"
        meta_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts_s(0)}__{i+1}_{total}_{rows}.meta"
        tkt_n  = f"{cfg['replica']}__{ts_s(1)}.tkt"
        zip_n  = f"{cfg['replica']}__{ts_s(2)}__{cfg['tbl']}__{i+1}_{total}_{rows}.csv.zip"
        
        s3_body = hook.get_key(key=key, bucket_name=BUCKET).get()["Body"]
        members = [
            (tkt_n,  None, S_IFREG | 0o600, ZIP_32, [f"{csv_n};{rows}".encode()]),
            (meta_n, None, S_IFREG | 0o600, ZIP_32, [meta_s.encode()]),
            (csv_n,  None, S_IFREG | 0o600, ZIP_32, s3_body.iter_chunks(chunk_size=8*1024*1024)),
        ]
        hook.load_file_obj(_Reader(stream_zip(members)), key=f"{cfg['s3_prefix']}/{zip_n}", bucket_name=BUCKET, replace=True)
        hook.delete_objects(bucket=BUCKET, keys=[key])
        uploaded.append(zip_n)

    summary_tkt = f"{cfg['replica']}__{ts_s(3)}.tkt"
    hook.load_bytes("\n".join(uploaded).encode(), key=f"{cfg['s3_prefix']}/{summary_tkt}", bucket_name=BUCKET, replace=True)
    
    ti.xcom_push(key="zip_name_list", value=uploaded)
    ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
    ti.xcom_push(key="total_row_count",  value=sum(int(r) for r in counts))


@task(task_id='save_status', trigger_rule='all_done', pool=POOL_NAME)
def _er_save_status(cfg, **context):
    """Records export results in history table."""
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


@task(task_id='schedule_next', pool=POOL_NAME)
def _er_schedule_next(cfg, **context):
    """Triggers subsequent run if delta is not yet current."""
    from airflow.models import DagBag
    from airflow.utils.types import DagRunType
    from airflow.utils.state import DagRunState
    dp = context['ti'].xcom_pull(task_ids="init")
    if str(dp.get('is_current')).lower() in ('true', 't', '1'): return

    DagBag().get_dag(cfg['dag_id']).create_dagrun(
        run_type=DagRunType.MANUAL, execution_date=pendulum.now('UTC').add(minutes=int(dp['selfrun_timeout'])),
        state=DagRunState.QUEUED, external_trigger=True,
    )

# ── DAG Factory ───────────────────────────────────────────────────────────────

def create_export_dag(table_key: str, params: dict) -> tuple[str, DAG]:
    """Generates a dynamic Airflow DAG from metadata."""
    from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator # type: ignore
    from hrp_operators import HrpClickNativeToS3ListOperator # type: ignore

    db, tbl = table_key.split(".", maxsplit=1)
    replica, schema = params['replica'], params['schema']
    if params.get('format', 'TSVWithNames') != 'TSVWithNames':
        raise AirflowFailException("Only TSVWithNames format is supported.")

    scen, prefix = TFS_MAP[replica]
    fields = params.get("fields", [])
    if not fields or fields in (['*'], '*'): fields = ['*']

    def _prep_sql(key):
        m = params.get(key)
        if isinstance(m, dict) and "fields" not in m:
            m = {**m, "fields": MANDATORY_PRE + fields + MANDATORY_SUF}
        return build_sql(m)

    sql_delta, sql_recent = _prep_sql('sql_stmt_export_delta'), _prep_sql('sql_stmt_export_recent')
    if not (sql_delta or sql_recent) or (sql_delta and sql_recent):
        raise AirflowFailException("Must specify exactly one of delta or recent SQL statements.")

    sql_exp = sql_delta or sql_recent
    if LIMITS.get(ENV_STAND, 0) > 0:
        sql_exp = f"SELECT * FROM ({sql_exp}) LIMIT {LIMITS[ENV_STAND]}"

    cfg = {
        'db': db, 'tbl': tbl, 'dag_id': f"export_er__{schema}__{tbl}",
        'schema_name': schema, 'replica': replica, 'scenario': scen, 's3_prefix': f"{prefix}/{replica}",
        'sql_get_registry': sql_reg_delta(tbl) if sql_delta else sql_reg_recent(tbl),
        'sql_get_current': sql_cur_delta(tbl) if sql_delta else None,
        'sql_auto_confirm': f"INSERT INTO export.extract_history SELECT extract_name, extract_time, extract_count, loaded, sent, now(), increment, overlap, recent_interval, time_field, time_from, time_to, exported_files FROM export.extract_history_vw WHERE extract_name = '{tbl}' AND sent IS NOT NULL AND confirmed IS NULL",
        'auto_confirm': params.get('auto_confirm', 1), 'confirm_timeout': params.get('confirm_timeout', 3600),
        'strategy': params.get('strategy', 'FULL_UK'), 'fields': fields, 'PK': params.get('PK', []), 'UK': params.get('UK', []),
        'topic': DEF_ARGS['topic'], 'kafka_in_conn': DEF_ARGS['kafka_in_conn'], 'kafka_in_topic': DEF_ARGS['kafka_in_topic'],
    }

    dag = DAG(
        dag_id=cfg['dag_id'], description=params.get('description', f"ER: {table_key}"),
        doc_md=f"```json\n{json.dumps(params, indent=2, default=str)}\n```",
        default_args=DEF_ARGS, start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval='55 0 * * *', max_active_tasks=1, max_active_runs=1, catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'ER', replica, schema.replace(' ', '_').lower()],
        render_template_as_native_obj=True, is_paused_upon_creation=True,
        params={
            'extract_time': Param(None, type=['string', 'null'], title='Extract time'),
            'condition': Param(None, type=['string', 'null'], title='Condition'),
            'is_current': Param(None, type=['boolean', 'null'], title='Is current'),
            'increment': Param(params.get('increment'), type=['integer', 'null'], title='Increment (сек)'),
            'selfrun_timeout': Param(params.get('selfrun_timeout'), type=['integer', 'null'], title='Selfrun timeout (мин)'),
            'strategy': Param(params.get('strategy', 'FULL_UK'), type='string', title='Strategy'),
            'auto_confirm': Param(bool(params.get('auto_confirm', 1)), type='boolean', title='Auto confirm'),
            'confirm_timeout': Param(params.get('confirm_timeout', 3600), type='integer', title='Confirm timeout (сек)'),
            'max_file_size': Param(None, type=['integer', 'null'], title='Max file size'),
            'pool': Param(POOL_NAME, type='string', title='Pool'),
            'topic': Param(cfg['topic'], type='string', title='Topic'),
        },
    )

    with dag:
        def _pre_exp(ctx):
            dp = ctx['ti'].xcom_pull(task_ids="init")
            op = ctx['task']
            op.sql = op.sql.format(export_time=dp['extract_time'], condition=dp['condition'])
            try: op.max_size = int(dp.get('max_file_size'))
            except: op.max_size = None
            op.pg_array_format, op.xstream_sanitize, op.sanitize_array = dp['pg_array_format'] == 'True', dp.get('xstream_sanitize', 'False') == 'True', dp.get('sanitize_array', 'False') == 'True'
            op.sanitize_list = dp.get('sanitize_list') or ''
            try: op.format_params = ast.literal_eval(dp['format_params'])
            except: op.format_params = {}

        t_init, t_meta = _er_init(cfg=cfg), _er_build_meta(cfg=cfg)
        t_exp = HrpClickNativeToS3ListOperator(
            task_id='export_to_s3', s3_bucket=BUCKET, s3_key=f"{cfg['s3_prefix']}/{{{{ ts_nodash }}}}.csv",
            sql=sql_exp, compression=None, replace=True, post_file_check=False, pre_execute=_pre_exp, pool=POOL_NAME,
        )
        t_zip = _er_pack_zip(cfg=cfg)
        t_msg = ProduceToTopicOperator(
            task_id='notify_tfs', topic="{{ params.topic }}", producer_function=produce_msg, producer_function_args=[cfg['scenario'], ''],
            delivery_callback=on_delivery, pool=POOL_NAME, pre_execute=_pre_kafka(cfg['scenario']),
        )
        t_wait = EmptyOperator(task_id='wait_confirm', trigger_rule='none_failed', pool=POOL_NAME) if cfg.get('auto_confirm') else \
                 AwaitMessageSensor(task_id='wait_confirm', kafka_config_id=cfg['kafka_in_conn'], topics=[cfg['kafka_in_topic']], apply_function=lambda m: True, poke_interval=60, timeout=cfg.get('confirm_timeout', 3600), mode='reschedule', trigger_rule='none_failed', pool=POOL_NAME)
        
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
        raise e
