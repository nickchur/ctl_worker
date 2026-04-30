from __future__ import annotations

import json
import logging
import time
import uuid
import zipfile
import ast
from io import BytesIO

import pendulum
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from hrp_operators import HrpClickNativeToS3ListOperator

logger = logging.getLogger(__name__)

TFS_KAFKA_CALLBACK = 'er_export.er_common.tfs_message_delivery_callback'

def select_dic(ch_hook, sql):
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    else:
        return []

def build_dynamic_select(sql_meta: str | dict, indent: str = "    ") -> str:
    """
    Assembles a SELECT SQL string from either a raw string or a structured dictionary.
    Supports with, fields, from, joins, where, settings.
    """
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

def parse_ch_type(ch_type: str, ch_type_map: dict) -> tuple[str, bool]:
    notnull = True
    if ch_type.startswith("LowCardinality(") and ch_type.endswith(")"):
        ch_type = ch_type[15:-1]
    if ch_type.startswith("Nullable(") and ch_type.endswith(")"):
        ch_type = ch_type[9:-1]
        notnull = False
    base = ch_type.split("(")[0]
    return ch_type_map.get(base, "STRING"), notnull

def produce_tfs_kafka_notification(scenario_id: str, file_name: str, throttle_delay: int = 1):
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

def tfs_message_delivery_callback(err, msg) -> None:
    if err is not None:
        raise RuntimeError(f"Failed to deliver message: {err}")
    logger.info(
        "Produced record to topic %s partition [%s] @ offset %s\n%s",
        msg.topic(), msg.partition(), msg.offset(), msg.value(),
    )

def _make_pre_execute_kafka(scenario: str, mode: str):
    def _pre_execute(context):
        if mode != 'prod':
            raise AirflowSkipException("Kafka notification skipped in test mode")
        summary_tkt = context['ti'].xcom_pull(task_ids=f"{context['task'].task_group.group_id}.pack_zip", key='summary_tkt_name')
        context['task'].producer_function_args = [scenario, summary_tkt]
    return _pre_execute

_SQL_REGISTRY_WITH = """
    with
        '{tbl}' as extr_name,
        src as (
            select 1 as prio, auto_confirm_delta, lower_bound, selfrun_timeout,
                   compression_type, compression_ext, max_file_size, xstream_sanitize,
                   sanitize_array, sanitize_list, pg_array_format, csv_format_params{extra_cols}
            from export.extract_registry_vw where extract_name = extr_name
            union all
            select 2 as prio, auto_confirm_delta, lower_bound, selfrun_timeout,
                   compression_type, compression_ext, max_file_size, xstream_sanitize,
                   sanitize_array, sanitize_list, pg_array_format, csv_format_params{extra_cols}
            from export.extract_registry_vw where extract_name = 'default'
        ),
        aggr as (
            select
                argMinIf(extr_name, prio, prio = 1)                                        as extract_name,
                argMinIf(auto_confirm_delta, prio, prio = 1)                               as auto_confirm_delta,
                argMinIf(lower_bound, prio, prio = 1)                                      as lower_bound,
                argMinIf(selfrun_timeout, prio, prio = 1)                                  as selfrun_timeout,
                argMinIf(compression_type, prio, lower(compression_type) <> 'default')    as compression_type,
                argMinIf(compression_ext, prio, lower(compression_ext) <> 'default')      as compression_ext,
                argMinIf(max_file_size, prio, lower(max_file_size) <> 'default')          as max_file_size,
                argMinIf(xstream_sanitize, prio, prio = 1)                                as xstream_sanitize,
                argMinIf(sanitize_array, prio, prio = 1)                                  as sanitize_array,
                argMinIf(sanitize_list, prio, lower(sanitize_list) <> 'default')          as sanitize_list,
                argMinIf(pg_array_format, prio, prio = 1)                                 as pg_array_format,
                argMinIf(csv_format_params, prio, lower(csv_format_params) <> 'default')  as csv_format_params{extra_aggr}
            from src
        )
"""

_SQL_REGISTRY_SELECT = """
    select
        auto_confirm_delta,
        '''' || toString(toDateTimeOrDefault(lower_bound)) || '''' as lower_bound,
        toString(selfrun_timeout)                                   as selfrun_timeout,
        compression_type,
        compression_ext,
        max_file_size,
        If(xstream_sanitize = 1, 'True', 'False')                  as xstream_sanitize,
        If(sanitize_array = 1, 'True', 'False')                    as sanitize_array,
        sanitize_list,
        If(pg_array_format = 1, 'True', 'False')                   as pg_array_format,
        csv_format_params                                           as format_params
"""

def build_registry_sql_delta(tbl: str) -> str:
    return build_dynamic_select({
        "with": _SQL_REGISTRY_WITH.format(tbl=tbl, extra_cols='', extra_aggr=''),
        "fields": [
            "auto_confirm_delta",
            "'''' || toString(toDateTimeOrDefault(lower_bound)) || '''' as lower_bound",
            "toString(selfrun_timeout)                                   as selfrun_timeout",
            "compression_type",
            "compression_ext",
            "max_file_size",
            "If(xstream_sanitize = 1, 'True', 'False')                  as xstream_sanitize",
            "If(sanitize_array = 1, 'True', 'False')                    as sanitize_array",
            "sanitize_list",
            "If(pg_array_format = 1, 'True', 'False')                   as pg_array_format",
            "csv_format_params                                           as format_params"
        ],
        "from": "aggr",
        "where": f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1"
    })

def build_registry_sql_recent(tbl: str) -> str:
    extra_cols = ', increment, overlap, time_field, recent_interval'
    extra_aggr = (
        ',\n                argMinIf(increment, prio, prio = 1)       as increment'
        ',\n                argMinIf(overlap, prio, prio = 1)         as overlap'
        ',\n                argMinIf(time_field, prio, prio = 1)      as time_field_v'
        ',\n                argMinIf(recent_interval, prio, prio = 1) as recent_interval_v'
    )
    return build_dynamic_select({
        "with": _SQL_REGISTRY_WITH.format(tbl=tbl, extra_cols=extra_cols, extra_aggr=extra_aggr),
        "fields": [
            "auto_confirm_delta",
            "'''' || toString(toDateTimeOrDefault(lower_bound)) || '''' as lower_bound",
            "toString(selfrun_timeout)                                   as selfrun_timeout",
            "compression_type",
            "compression_ext",
            "max_file_size",
            "If(xstream_sanitize = 1, 'True', 'False')                  as xstream_sanitize",
            "If(sanitize_array = 1, 'True', 'False')                    as sanitize_array",
            "sanitize_list",
            "If(pg_array_format = 1, 'True', 'False')                   as pg_array_format",
            "csv_format_params                                           as format_params",
            "now()                                                                                                                                                as cur_time",
            "'''' || toString(cur_time) || ''''                                                                                                                   as extract_time",
            "'null'                                                                                                                                               as extract_count",
            "'null'                                                                                                                                               as loaded",
            "'null'                                                                                                                                               as sent",
            "'null'                                                                                                                                               as confirmed",
            "toString(increment)                                                                                                                                   as increment",
            "toString(overlap)                                                                                                                                     as overlap",
            "'''' || time_field_v || ''''                                                                                                                         as time_field",
            "'''' || toString(cur_time - recent_interval_v) || ''''                                                                                              as time_from",
            "'''' || toString(cur_time) || ''''                                                                                                                   as time_to",
            "'''' || toString(cur_time - recent_interval_v) || ''' < ' || time_field_v || ' and ' || time_field_v || ' <= ''' || toString(cur_time) || ''''     as condition",
            "'True'                                                                                                                                               as is_current",
            "toString(recent_interval_v)                                                                                                                          as recent_interval",
            "toString(0)                                                                                                                                          as num_state"
        ],
        "from": "aggr",
        "where": f"extract_name = '{tbl}'",
        "settings": "enable_global_with_statement = 1"
    })

def make_er_export_task_group(
    group_id: str,
    task_cfg: dict,
    sql_export: str,
    ch_id: str,
    tfs_out_conn_id: str,
    tfs_out_bucket: str,
    ch_type_map: dict,
    mode: str,
    tfs_out_pool: str
) -> TaskGroup:
    
    with TaskGroup(group_id=group_id) as tg:

        @task(task_id='init')
        def init(cfg):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            S3Hook(aws_conn_id=tfs_out_conn_id).create_bucket(bucket_name=tfs_out_bucket)
            hook = ClickHouseHook(clickhouse_conn_id=ch_id)
            reg_res = select_dic(hook, cfg['sql_get_registry'])
            if not reg_res:
                raise ValueError(f"No registry entry found for {cfg['tbl']}")
            reg = reg_res[0]
            if reg['auto_confirm_delta']:
                hook.execute(cfg['sql_auto_confirm'])
            if cfg['sql_get_current']:
                cur_res = select_dic(hook, cfg['sql_get_current'])
                if cur_res:
                    return {**reg, **cur_res[0]}
            return reg

        t_init = init(cfg=task_cfg)

        @task(task_id='build_meta')
        def build_meta(cfg, **context):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            hook = ClickHouseHook(clickhouse_conn_id=ch_id)
            rows, _ = hook.execute(f"DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}", with_column_types=True)
            columns = []
            for row in rows:
                source_type, notnull = parse_ch_type(row[1], ch_type_map)
                columns.append({
                    "column_name": row[0],
                    "source_type": source_type,
                    "length":      None,
                    "notnull":     notnull,
                    "precision":   None,
                    "scale":       None,
                })
            columns.extend(cfg['extra_columns'])
            meta = {
                "schema_name": cfg['schema_name'],
                "table_name":  cfg['tbl'],
                "strategy":    cfg['strategy'],
                "PK":          cfg['PK'],
                "UK":          cfg['UK'],
                "params":      {"separation": "\t", "escapesymbol": "\""},
                "columns":     columns,
            }
            context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))

        t_build_meta = build_meta(cfg=task_cfg)

        def _pre_execute_copy(context):
            ti = context['ti']
            dp = ti.xcom_pull(task_ids=f"{group_id}.init")
            op = context['task']
            op.sql              = op.sql.format(export_time=dp['extract_time'], condition=dp['condition'])
            op.max_size         = dp['max_file_size']
            op.xstream_sanitize = dp['xstream_sanitize'] == 'True'
            op.sanitize_array   = dp['sanitize_array'] == 'True'
            op.sanitize_list    = dp['sanitize_list']
            op.pg_array_format  = dp['pg_array_format'] == 'True'
            try:
                op.format_params = ast.literal_eval(dp['format_params']) if dp['format_params'] else {}
            except (ValueError, TypeError):
                logger.warning("Unparseable format_params: %r", dp['format_params'])
                op.format_params = {}

        export_to_s3 = HrpClickNativeToS3ListOperator(
            task_id='export_to_s3',
            s3_bucket=tfs_out_bucket,
            s3_key=f"{task_cfg['s3_prefix']}/{{{{ ts_nodash }}}}.csv",
            sql=sql_export,
            compression=None,
            replace=True,
            post_file_check=False,
            pre_execute=_pre_execute_copy,
        )

        @task(task_id='pack_zip')
        def pack_zip(cfg, **context):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            ti = context["ti"]

            s3_key_list    = ti.xcom_pull(task_ids=f"{group_id}.export_to_s3", key='s3_key_list')
            row_count_list = ti.xcom_pull(task_ids=f"{group_id}.export_to_s3", key='row_count_list')
            meta_json_str  = ti.xcom_pull(task_ids=f"{group_id}.build_meta", key='meta_json')

            if not s3_key_list:
                logger.warning("No CSV parts exported — skipping ZIP packaging")
                ti.xcom_push(key="zip_name_list",    value=[])
                ti.xcom_push(key="summary_tkt_name", value="")
                ti.xcom_push(key="total_row_count",  value=0)
                return

            meta_bytes = meta_json_str.encode()
            hook    = S3Hook(aws_conn_id=tfs_out_conn_id)
            total   = len(s3_key_list)
            base_ts = pendulum.now("UTC")
            uploaded_zips = []

            for i, (s3_key, row_count) in enumerate(zip(s3_key_list, row_count_list)):
                rows  = int(row_count)
                part  = i + 1
                inner_ts = base_ts.add(seconds=i * 2    ).format("YYYYMMDDHHmmss")
                tkt_ts   = base_ts.add(seconds=i * 2 + 1).format("YYYYMMDDHHmmss")
                outer_ts = base_ts.add(seconds=i * 2 + 2).format("YYYYMMDDHHmmss")

                csv_name  = f"{cfg['schema_name']}__{cfg['tbl']}__{inner_ts}__{part}_{total}_{rows}.csv"
                meta_name = f"{cfg['schema_name']}__{cfg['tbl']}__{inner_ts}__{part}_{total}_{rows}.meta"
                tkt_name  = f"{cfg['replica']}__{tkt_ts}.tkt"
                zip_name  = f"{cfg['replica']}__{outer_ts}__{cfg['tbl']}__{part}_{total}_{rows}.csv.zip"

                csv_bytes = hook.get_key(key=s3_key, bucket_name=tfs_out_bucket).get()["Body"].read()

                buf = BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(meta_name, meta_bytes)
                    zf.writestr(tkt_name,  f"{csv_name};{rows}".encode())
                    zf.writestr(csv_name,  csv_bytes)
                hook.load_bytes(
                    buf.getvalue(),
                    key=f"{cfg['s3_prefix']}/{zip_name}",
                    bucket_name=tfs_out_bucket,
                    replace=True,
                )
                hook.delete_objects(bucket=tfs_out_bucket, keys=[s3_key])
                uploaded_zips.append(zip_name)
                logger.info("Packaged %d/%d: %s", part, total, zip_name)

            summary_ts  = base_ts.add(seconds=(total - 1) * 2 + 3).format("YYYYMMDDHHmmss")
            summary_tkt = f"{cfg['replica']}__{summary_ts}.tkt"
            hook.load_bytes(
                "\n".join(uploaded_zips).encode(),
                key=f"{cfg['s3_prefix']}/{summary_tkt}",
                bucket_name=tfs_out_bucket,
                replace=True,
            )
            logger.info("Created summary tkt: %s", summary_tkt)

            ti.xcom_push(key="zip_name_list",    value=uploaded_zips)
            ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
            ti.xcom_push(key="total_row_count",  value=sum(int(r) for r in row_count_list))

        t_pack = pack_zip(cfg=task_cfg)

        notify_tfs = ProduceToTopicOperator(
            task_id='notify_tfs',
            producer_function=produce_tfs_kafka_notification,
            producer_function_args=[task_cfg['scenario'], ''],
            delivery_callback=TFS_KAFKA_CALLBACK,
            pool=tfs_out_pool,
            pre_execute=_make_pre_execute_kafka(task_cfg['scenario'], mode),
        )

        @task(task_id='save_status', trigger_rule='none_failed')
        def save_status(cfg, **context):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            ti         = context['ti']
            dp         = ti.xcom_pull(task_ids=f"{group_id}.init")
            total_rows = ti.xcom_pull(task_ids=f"{group_id}.pack_zip", key='total_row_count')
            zip_names  = ti.xcom_pull(task_ids=f"{group_id}.pack_zip", key='zip_name_list')
            hook = ClickHouseHook(clickhouse_conn_id=ch_id)
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
                    {zip_names!r}
            """)

        t_save = save_status(cfg=task_cfg)

        @task(task_id='schedule_next')
        def schedule_next(cfg, **context):
            from airflow.api.common.trigger_dag import trigger_dag
            ti = context['ti']
            dp = ti.xcom_pull(task_ids=f"{group_id}.init")
            if str(dp['is_current']).lower() in ('true', 't', '1'):
                return
            run_date = pendulum.now('UTC').add(minutes=int(dp['selfrun_timeout']))
            trigger_dag(dag_id=cfg['dag_id'], execution_date=run_date, replace_microseconds=False)

        t_schedule = schedule_next(cfg=task_cfg)

        t_init >> [t_build_meta, export_to_s3]
        [t_build_meta, export_to_s3] >> t_pack
        t_pack >> notify_tfs >> t_save >> t_schedule

    return tg
