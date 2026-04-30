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
from er_export.er_common import (
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
tables = ctl_obj_load(VARIABLE_NAME)

if not tables:
    # Если из Variable/S3 ничего не пришло, пробуем локальный файл (для тестов)
    META_FILE = os.path.join(os.path.dirname(__file__), "er_meta.json")
    if os.path.exists(META_FILE):
        with open(META_FILE, 'r') as f:
            tables = json.load(f)
        logger.info(f"Loaded metadata from local fallback: {META_FILE}")

# ── DAG factory ──────────────────────────────────────────────────────────────

for table_key, params in tables.items():
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
            "fields": [
                "toString(num_state)                                                                                                      as num_state",
                "'''' || toString(toDateTimeOrDefault(extract_time)) || ''''                                                             as extract_time",
                "ifNull(toString(extract_count), 'null')                                                                                 as extract_count",
                "ifNull('''' || toString(loaded)    || '''', 'null')                                                                     as loaded",
                "ifNull('''' || toString(sent)      || '''', 'null')                                                                     as sent",
                "ifNull('''' || toString(confirmed) || '''', 'null')                                                                     as confirmed",
                "toString(increment)                                                                                                     as increment",
                "toString(overlap)                                                                                                       as overlap",
                "'''' || time_field || ''''                                                                                              as time_field",
                "'''' || toString(time_from) || ''''                                                                                    as time_from",
                "'''' || toString(time_to)   || ''''                                                                                    as time_to",
                "'''' || toString(time_from) || ''' < ' || time_field || ' and ' || time_field || ' <= ''' || toString(time_to) || '''' as condition,
                "if(current_time = extract_time, 'True', 'False')                                                                       as is_current",
                "toString(0)                                                                                                             as recent_interval"
            ],
            "from": "export.extract_current_vw",
            "where": f"extract_name = '{tbl}'"
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
