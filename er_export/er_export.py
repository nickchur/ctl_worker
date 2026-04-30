"""
DAG Factory for ER exports.
Dynamically generates Airflow DAGs based on metadata loaded from ClickHouse.
"""
from __future__ import annotations

import json
import logging
import pendulum
from airflow import DAG
from airflow.models import Param

from er_export.er_config import (
    CH_ID,
    TYPE_MAP,
    DEF_ARGS,
    ENV_STAND,
    EXTRA_COLS,
    MANDATORY_PRE,
    MANDATORY_SUF,
    MODE,
    LIMITS,
    BUCKET,
    TFS_MAP,
    S3_CONN,
)
from er_export.er_core import (
    export_tg,
    sql_reg_delta,
    sql_reg_recent,
    build_sql
)

from plugins.ctl_utils import ctl_obj_load

logger = logging.getLogger(__name__)

# Load metadata
VAR_NAME = "datalab_er_wfs"
wfs = ctl_obj_load(VAR_NAME, s3_id='s3', bucket='datalab-er')

# ── DAG factory ──────────────────────────────────────────────────────────────

for table_key, params in wfs.items():
    db, tbl = table_key.split(".", maxsplit=1)
    dag_id  = f"export_er__{params['replica']}__{tbl}"
    replica = params['replica']
    schema  = params['schema']
    fmt     = params.get('format', 'TSVWithNames')

    if fmt != 'TSVWithNames':
        raise ValueError(f"Unsupported format: {fmt!r}. Only TSVWithNames is supported.")

    scen, prefix, pool = TFS_MAP[replica]
    s3_prefix = f"{prefix}/{replica}"

    def _prepare_sql(sql_key):
        meta = params.get(sql_key)
        if isinstance(meta, dict) and "fields" not in meta:
            meta["fields"] = MANDATORY_PRE + params.get("fields", []) + MANDATORY_SUF
        return build_sql(meta)

    sql_delta  = _prepare_sql('sql_stmt_export_delta')
    sql_recent = _prepare_sql('sql_stmt_export_recent')

    if not (sql_delta or sql_recent):
        raise RuntimeError("One of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' must be specified!")
    if sql_delta and sql_recent:
        raise RuntimeError("Only one of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' can be specified!")

    sql_export = sql_delta or sql_recent
    row_limit  = LIMITS.get(ENV_STAND, 0)
    if row_limit > 0:
        sql_export = f"select * from ({sql_export}) limit {row_limit}"

    if sql_delta:
        sql_reg = sql_reg_delta(tbl)
        sql_cur = build_sql({
            "with": f"""WITH data AS (
                SELECT
                    num_state       as num_state,
                    extract_time    as extract_time,
                    current_time    as current_time,
                    extract_count   as extract_count,
                    loaded          as loaded,
                    sent            as sent,
                    confirmed       as confirmed,
                    increment       as increment,
                    overlap         as overlap,
                    time_field      as time_field,
                    time_from       as time_from,
                    time_to         as time_to
                FROM export.extract_current_vw
                WHERE extract_name = '{tbl}'
            )""",
            "fields": [
                "toString(num_state)                                                               as num_state",
                "concat('\\'', toString(extract_time), '\\'')                                      as extract_time",
                "ifNull(toString(extract_count), 'null')                                           as extract_count",
                "if(extract_count is null, 'null', concat('\\'', toString(loaded), '\\''))       as loaded",
                "if(extract_count is null, 'null', concat('\\'', toString(sent), '\\''))         as sent",
                "if(extract_count is null, 'null', concat('\\'', toString(confirmed), '\\''))    as confirmed",
                "toString(increment)                                                               as increment",
                "toString(overlap)                                                                 as overlap",
                "concat('\\'', time_field, '\\'')                                                  as time_field",
                "concat('\\'', toString(time_from), '\\'')                                         as time_from",
                "concat('\\'', toString(time_to), '\\'')                                           as time_to",
                "concat('\\'', toString(time_from), '\\' < ', time_field, ' and ', time_field, ' <= \\'', toString(time_to), '\\'') as condition",
                "if(current_time = extract_time, 'True', 'False')                               as is_current",
                "toString(0)                                                                         as recent_interval"
            ],
            "from": "data"
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
        doc_md=json.dumps(params, ensure_ascii=False, indent=2, default=str),
        default_args=DEF_ARGS,
        start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval='55 0 * * *',
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'ER', replica, schema],
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
