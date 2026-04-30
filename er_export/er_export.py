from __future__ import annotations

import json
import os
import logging
import pendulum
from airflow import DAG
from airflow.models import Variable

from er_export.er_config import (
    CH_ID,
    CH_TYPE_MAP,
    DEFAULT_ARGS,
    ENV_STAND,
    ER_EXTRA_COLUMNS,
    MODE,
    ROW_COUNT_LIMIT_MAP,
    TFS_OUT_BUCKET,
    TFS_OUT_CONFIG_MAP,
    TFS_OUT_CONN_ID,
)
from er_export.er_core import (
    make_er_export_task_group,
    build_registry_sql_delta,
    build_registry_sql_recent,
    build_dynamic_select
)

# Пытаемся импортировать ctl_obj_load из общих утилит
from plugins.ctl_utils import ctl_obj_load

logger = logging.getLogger(__name__)

# Load metadata
VARIABLE_NAME = "datalab_er_wfs"
wfs = ctl_obj_load(VARIABLE_NAME, s3_id='s3', bucket='datalab-er')

# ── DAG factory ──────────────────────────────────────────────────────────────

for table_key, params in wfs.items():
    db, tbl = table_key.split(".", maxsplit=1)
    dag_id      = f"export_er__{params['replica']}__{tbl}"
    replica     = params['replica']
    schema_name = params['schema']
    export_fmt  = params.get('format', 'TSVWithNames')

    if export_fmt != 'TSVWithNames':
        raise ValueError(f"Unsupported format: {export_fmt!r}. Only TSVWithNames is supported.")

    scenario, tfs_prefix, tfs_out_pool = TFS_OUT_CONFIG_MAP[replica]
    s3_prefix = f"{tfs_prefix}/{replica}"

    def _prepare_sql(sql_key):
        sql_meta = params.get(sql_key)
        if isinstance(sql_meta, dict) and "fields" not in sql_meta:
            sql_meta["fields"] = params.get("fields", [])
        return build_dynamic_select(sql_meta)

    sql_delta  = _prepare_sql('sql_stmt_export_delta')
    sql_recent = _prepare_sql('sql_stmt_export_recent')

    if not (sql_delta or sql_recent):
        raise RuntimeError("One of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' must be specified!")
    if sql_delta and sql_recent:
        raise RuntimeError("Only one of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' can be specified!")

    sql_export = sql_delta or sql_recent
    row_limit  = ROW_COUNT_LIMIT_MAP.get(ENV_STAND, 0)
    if row_limit > 0:
        sql_export = f"select * from ({sql_export}) limit {row_limit}"

    if sql_delta:
        sql_get_registry = build_registry_sql_delta(tbl)
        sql_get_current  = build_dynamic_select({
            "with": f"""WITH data AS (
                SELECT
                    num_state       as num_state_v,
                    extract_time    as extract_time_v,
                    extract_count   as extract_count_v,
                    loaded          as loaded_v,
                    sent            as sent_v,
                    confirmed       as confirmed_v,
                    increment       as increment_v,
                    overlap         as overlap_v,
                    time_field      as time_field_v,
                    time_from       as time_from_v,
                    time_to         as time_to_v
                FROM export.extract_current_vw
                WHERE extract_name = '{tbl}'
            )""",
            "fields": [
                "toString(num_state_v)                                                               as num_state",
                "concat('\\'', toString(extract_time_v), '\\'')                                      as extract_time",
                "ifNull(toString(extract_count_v), 'null')                                           as extract_count",
                "if(extract_count_v is null, 'null', concat('\\'', toString(loaded_v), '\\''))       as loaded",
                "if(extract_count_v is null, 'null', concat('\\'', toString(sent_v), '\\''))         as sent",
                "if(extract_count_v is null, 'null', concat('\\'', toString(confirmed_v), '\\''))    as confirmed",
                "toString(increment_v)                                                               as increment",
                "toString(overlap_v)                                                                 as overlap",
                "concat('\\'', time_field_v, '\\'')                                                  as time_field",
                "concat('\\'', toString(time_from_v), '\\'')                                         as time_from",
                "concat('\\'', toString(time_to_v), '\\'')                                           as time_to",
                "concat('\\'', toString(time_from_v), '\\' < ', time_field_v, ' and ', time_field_v, ' <= \\'', toString(time_to_v), '\\'') as condition",
                "'False'                                                                             as is_current",
                "toString(0)                                                                         as recent_interval"
            ],
            "from": "data"
        })
    else:
        sql_get_registry = build_registry_sql_recent(tbl)
        sql_get_current  = None

    task_cfg = {
        'db':          db,
        'tbl':         tbl,
        'dag_id':      dag_id,
        'schema_name': schema_name,
        'replica':     replica,
        'scenario':    scenario,
        's3_prefix':   s3_prefix,
        'bucket':      TFS_OUT_BUCKET,
        'topic':       DEFAULT_ARGS.get('topic', 'TFS.HRPLT.IN'),
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
        'sql_get_registry': sql_get_registry,
        'sql_get_current':  sql_get_current,
        'extra_columns': ER_EXTRA_COLUMNS,
        'strategy':      params.get('strategy', 'FULL_UK'),
        'PK':            params.get('PK', []),
        'UK':            params.get('UK', []),
    }

    dag = DAG(
        dag_id=dag_id,
        description=f"ER-выгрузка {table_key} → S3 ZIP → TFS Kafka",
        default_args=DEFAULT_ARGS,
        start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval='55 0 * * *',
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'xStream', 'ER'],
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
    )

    with dag:
        make_er_export_task_group(
            group_id='er_export',
            task_cfg=task_cfg,
            sql_export=sql_export,
            ch_id=CH_ID,
            tfs_out_conn_id=TFS_OUT_CONN_ID,
            tfs_out_bucket=TFS_OUT_BUCKET,
            ch_type_map=CH_TYPE_MAP,
            mode=MODE,
            tfs_out_pool=tfs_out_pool
        )

    globals()[dag_id] = dag
