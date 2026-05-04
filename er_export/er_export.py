"""
DAG Factory for ER exports.
Dynamically generates Airflow DAGs based on metadata loaded from ClickHouse.
"""
from __future__ import annotations

import json
import pendulum
from airflow import DAG
from airflow.models import Param
from airflow.exceptions import AirflowSkipException, AirflowFailException

from er_export.er_config import get_config

_cfg = get_config()
CH_ID         = _cfg['CH_ID']
TYPE_MAP      = _cfg['TYPE_MAP']
DEF_ARGS      = _cfg['DEF_ARGS']
ENV_STAND     = _cfg['ENV_STAND']
EXTRA_COLS    = _cfg['EXTRA_COLS']
MANDATORY_PRE = _cfg['MANDATORY_PRE']
MANDATORY_SUF = _cfg['MANDATORY_SUF']
MODE          = _cfg['MODE']
LIMITS        = _cfg['LIMITS']
BUCKET        = _cfg['BUCKET']
TFS_MAP       = _cfg['TFS_MAP']
S3_CONN       = _cfg['S3_CONN']
VAR_NAME      = _cfg['VAR_NAME']
from er_export.er_core import (
    export_tg,
)

from plugins.ctl_utils import ctl_obj_load

from  logging import getLogger
logger = getLogger("airflow.task")


# Load metadata
wfs = ctl_obj_load(VAR_NAME)

# ── SQL Builders ──────────────────────────────────────────────────────────────

def build_sql(sql_meta: str | dict, indent: str = "    ") -> str:
    """Динамически собирает SQL-запрос из словаря с метаданными."""
    if not sql_meta:
        return ""
    if isinstance(sql_meta, str):
        return sql_meta
    
    sql = ""
    if sql_meta.get("with"):
        sql += f"{sql_meta['with']}\n"

    fields = sql_meta.get("fields", [])
    if isinstance(fields, list):
        fields_str = f",\n{indent}".join(fields)
    else:
        fields_str = fields

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

def sql_reg_delta(tbl: str) -> str:
    return build_sql({
        "with": _REG_WITH.format(tbl=tbl, extra_aggr=''),
        "fields": [
            "auto_confirm_delta                       as auto_confirm_delta",
            "concat('\\'', toString(toDateTimeOrDefault(lower_bound)), '\\'') as lower_bound",
            "toString(selfrun_timeout)                as selfrun_timeout",
            "compression_type                         as compression_type",
            "compression_ext                          as compression_ext",
            "max_file_size                            as max_file_size",
            "If(pg_array_format = 1, 'True', 'False')    as pg_array_format",
            "csv_format_params                           as format_params",
            "If(xstream_sanitize = 1, 'True', 'False')   as xstream_sanitize",
            "If(sanitize_array = 1, 'True', 'False')     as sanitize_array",
            "sanitize_list                                as sanitize_list"
        ],
        "from": "aggr",
        "where": f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1"
    })

def sql_reg_recent(tbl: str) -> str:
    extra_aggr = """,
        argMinIf(increment, prio, prio = 1)       as increment,
        argMinIf(overlap, prio, prio = 1)         as overlap,
        argMinIf(time_field, prio, prio = 1)      as time_field,
        argMinIf(recent_interval, prio, prio = 1) as recent_interval"""
    return build_sql({
        "with": _REG_WITH.format(tbl=tbl, extra_aggr=extra_aggr),
        "fields": [
            "auto_confirm_delta                       as auto_confirm_delta",
            "concat('\\'', toString(toDateTimeOrDefault(lower_bound)), '\\'') as lower_bound",
            "toString(selfrun_timeout)                as selfrun_timeout",
            "compression_type                         as compression_type",
            "compression_ext                          as compression_ext",
            "max_file_size                            as max_file_size",
            "If(pg_array_format = 1, 'True', 'False')    as pg_array_format",
            "csv_format_params                           as format_params",
            "If(xstream_sanitize = 1, 'True', 'False')   as xstream_sanitize",
            "If(sanitize_array = 1, 'True', 'False')     as sanitize_array",
            "sanitize_list                                as sanitize_list",
            "now()                                      as cur_time",
            "concat('\\'', toString(cur_time), '\\'')   as extract_time",
            "'null'                                     as extract_count",
            "'null'                                     as loaded",
            "'null'                                     as sent",
            "'null'                                     as confirmed",
            "toString(increment)                      as increment",
            "toString(overlap)                        as overlap",
            "concat('\\'', time_field, '\\'')         as time_field",
            "concat('\\'', toString(cur_time - recent_interval), '\\'') as time_from",
            "concat('\\'', toString(cur_time), '\\'')   as time_to",
            "concat('\\'', toString(cur_time - recent_interval), '\\' < ', time_field, ' and ', time_field, ' <= \\'', toString(cur_time), '\\'') as condition",
            "'True'                                     as is_current",
            "toString(recent_interval)                as recent_interval",
            "toString(0)                                as num_state"
        ],
        "from": "aggr",
        "where": f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1"
    })

# ── DAG factory ──────────────────────────────────────────────────────────────

def create_export_dag(table_key: str, params: dict) -> tuple[str, DAG]:
    db, tbl = table_key.split(".", maxsplit=1)
    dag_id  = f"export_er__{params['replica']}__{tbl}"
    replica = params['replica']
    schema  = params['schema']
    fmt     = params.get('format', 'TSVWithNames')

    if fmt != 'TSVWithNames':
        raise AirflowFailException(f"Unsupported format: {fmt!r}. Only TSVWithNames is supported.")

    scen, prefix, pool = TFS_MAP[replica]
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
        sql_reg = sql_reg_delta(tbl)
        sql_cur = build_sql({
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
            "from": f"(SELECT * FROM export.extract_current_vw WHERE extract_name = '{tbl}') as a"
        })
    else:
        sql_reg = sql_reg_recent(tbl)
        sql_cur = None

    cfg = {
        'db':          db,
        'tbl':         tbl,
        'dag_id':      dag_id,
        'schema_name': schema,
        'replica':     replica,
        'scenario':    scen,
        's3_prefix':   s3_prefix,
        'bucket':      BUCKET,
        'topic':              DEF_ARGS['topic'],
        'kafka_in_conn': DEF_ARGS['kafka_in_conn'],
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
        'extra_columns': EXTRA_COLS,
        'auto_confirm': params.get('auto_confirm', 1),
        'strategy':      params.get('strategy', 'FULL_UK'),
        'PK':            params.get('PK', []),
        'UK':            params.get('UK', []),
    }

    dag = DAG(
        dag_id=dag_id,
        description=params.get('description') or f"ER-выгрузка {table_key} → S3 ZIP → TFS Kafka",
        doc_md='```\n'+json.dumps(params, ensure_ascii=False, indent=2, default=str)+'\n```',
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
            'extract_time':    Param(None, type=['string', 'null'],  title='Extract time',       description='Техническое время выгрузки (правая граница дельты). Формат: 2024-01-01 00:00:00'),
            'condition':       Param(None, type=['string', 'null'],  title='Condition',           description='Перезаписать значение плейсхолдера {condition}'),
            'is_current':      Param(None, type=['boolean', 'null'], title='Is current',       description='True = данные актуальны, остановить авто-перезапуск (self-run)'),
            'increment':       Param(params.get('increment'), type=['integer', 'null'], title='Increment (сек)', description='Размер временного окна (шаг) одной выгрузки в секундах'),
            'selfrun_timeout': Param(params.get('selfrun_timeout'), type=['integer', 'null'], title='Selfrun timeout (мин)', description='Пауза перед автоматическим запуском следующей итерации (мин)'),
            'strategy':        Param(params.get('strategy', 'FULL_UK'), type='string', title='Strategy', description='Стратегия слияния данных (FULL_UK, DELTA_UK и др.)'),
            'auto_confirm':    Param(bool(params.get('auto_confirm', 1)), type='boolean', title='Auto confirm', description='True = не ждать подтверждения из Kafka'),
            'max_file_size':   Param(None, type=['integer', 'null'], title='Max file size', description='Максимальный размер одного CSV файла (байт). По умолчанию из реестра.'),
            'pool':            Param(pool, type='string', title='Kafka Pool', description='Пул Airflow для задач Kafka'),
            'topic':           Param(cfg['topic'], type='string', title='Kafka Topic', description='Топик для отправки уведомлений'),
        },
    )

    with dag:
        export_tg(
            gid='er_export',
            cfg=cfg,
            sql=sql_export,
            cid=CH_ID,
            s3_conn=S3_CONN,
            bucket=BUCKET,
            type_map=TYPE_MAP,
            mode=MODE,
            pool=pool
        )

    globals()[dag_id] = dag
    return dag_id, dag

for table_key, params in wfs.items():
    try:
        dag_id, dag = create_export_dag(table_key, params)
        globals()[dag_id] = dag
    except Exception as e:
        logger.error(f"Failed to generate DAG for {table_key}: {e}")
