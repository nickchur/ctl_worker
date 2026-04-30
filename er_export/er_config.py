from __future__ import annotations

import json
import logging
import os
import time
import uuid
import zipfile
from datetime import timedelta
from io import BytesIO

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.task_group import TaskGroup
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseBranchSQLOperator

from hrp_operators import HrpClickNativeToS3ListOperator

logger = logging.getLogger(__name__)

TFS_OUT_CONN_ID    = 's3-tfs-hrplt'
TFS_OUT_BUCKET     = 'tfshrplt'
TFS_OUT_TOPIC      = 'TFS.HRPLT.IN'
TFS_KAFKA_CALLBACK = 'er_export.er_config.tfs_message_delivery_callback'

ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

TFS_OUT_CONFIG_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er", "tfs_HRPLATFORM-2100"),
}

DEFAULT_ARGS = {
    "owner": "DataLab (CI02420667)",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "aws_conn_id": TFS_OUT_CONN_ID,
    "clickhouse_conn_id": "dlab-click",
    "conn_id": "dlab-click",
    "kafka_config_id": "tfs-kafka-out",
    "topic": TFS_OUT_TOPIC,
}

ROW_COUNT_LIMIT_MAP = {
    "prom": 0,
    "uat":  100,
    "qa":   100,
    "ift":  100,
    "dev":  100,
}

CH_TYPE_MAP: dict[str, str] = {
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


def parse_ch_type(ch_type: str) -> tuple[str, bool]:
    """Strips Nullable/LowCardinality wrappers, maps ClickHouse type to ER source_type.
    Returns (source_type, notnull)."""
    notnull = True

    if ch_type.startswith("LowCardinality(") and ch_type.endswith(")"):
        ch_type = ch_type[15:-1]

    if ch_type.startswith("Nullable(") and ch_type.endswith(")"):
        ch_type = ch_type[9:-1]
        notnull = False

    # strip precision args: DateTime64(3) → DateTime64, FixedString(10) → FixedString
    base = ch_type.split("(")[0]
    return CH_TYPE_MAP.get(base, "STRING"), notnull


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


def get_export_time_placeholder(table_name: str) -> str:
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.get_delta_params')[0][1] }}}}"


def get_delta_condition_placeholder(table_name: str) -> str:
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.get_delta_params')[0][11] }}}}"


def make_er_export_task_group(
    dag: DAG,
    database_name: str,
    table_name: str,
    params: dict,
    bucket: str = TFS_OUT_BUCKET,
) -> TaskGroup:
    """
    Создаёт TaskGroup для выгрузки одной таблицы из ClickHouse в S3 (ZIP-пакеты) и отправки
    уведомления в Kafka TFS.

    params dict:
        replica (str)          — ключ маршрутизации TFS (→ scenario, s3_prefix, pool)
        schema  (str)          — имя целевой схемы в .meta
        format  (str)          — "TSVWithNames" (единственный поддерживаемый формат)
        strategy (str)         — стратегия мерджа для .meta (default: "FULL_UK")
        PK (list)              — первичный ключ для .meta
        UK (list)              — уникальный ключ для .meta
        extra_columns (list)   — вычисляемые колонки, отсутствующие в DESCRIBE TABLE
        sql_stmt_export_delta  — SQL для выгрузки инкремента (или sql_stmt_export_recent)
        sql_stmt_export_recent — SQL для выгрузки актуального среза
        settings (list[str])   — ClickHouse SETTINGS, опционально
    """
    replica_name  = params['replica']
    schema_name   = params['schema']
    export_format = params.get('format', 'TSVWithNames')

    if export_format != 'TSVWithNames':
        raise ValueError(f"Unsupported format: {export_format!r}. Only TSVWithNames is supported.")

    scenario, tfs_prefix, tfs_out_pool = TFS_OUT_CONFIG_MAP[replica_name]
    s3_prefix = f"{tfs_prefix}/{replica_name}"

    sql_stmt_export_delta  = params.get('sql_stmt_export_delta')
    sql_stmt_export_recent = params.get('sql_stmt_export_recent')

    if not (sql_stmt_export_delta or sql_stmt_export_recent):
        raise RuntimeError("One of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' must be specified!")
    if sql_stmt_export_delta and sql_stmt_export_recent:
        raise RuntimeError("Only one of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' can be specified!")

    if sql_stmt_export_delta:
        sql_stmt_export = sql_stmt_export_delta
        base_table_name = table_name
    else:
        sql_stmt_export = sql_stmt_export_recent
        base_table_name = table_name.removesuffix('_recent')

    row_count_limit = ROW_COUNT_LIMIT_MAP.get(ENV_STAND, 0)
    if row_count_limit > 0:
        sql_stmt_export = f"select * from ({sql_stmt_export}) limit {row_count_limit}"

    with TaskGroup(dag=dag, group_id=table_name, tooltip=f"Выгрузка {table_name} → ZIP → TFS") as tg:

        # ── check auto-confirm ──────────────────────────────────────────────
        check_need_auto_confirm_delta = ClickHouseBranchSQLOperator(
            dag=dag,
            task_id='check_need_auto_confirm_delta',
            sql=f"select auto_confirm_delta from export.extract_registry_vw where extract_name = '{table_name}'",
            follow_task_ids_if_true=[f"{tg.group_id}.auto_confirm_delta"],
            follow_task_ids_if_false=[f"{tg.group_id}.not_need_auto_confirm"],
            do_xcom_push=True,
        )

        auto_confirm_delta = ClickHouseOperator(
            dag=dag,
            task_id='auto_confirm_delta',
            sql=f"""
                insert into export.extract_history (
                    extract_name, extract_time, extract_count, loaded, sent, confirmed,
                    increment, overlap, recent_interval, time_field, time_from, time_to, exported_files
                )
                select
                    extract_name, extract_time, extract_count, loaded, sent, now(),
                    increment, overlap, recent_interval, time_field, time_from, time_to, exported_files
                from export.extract_history_vw
                where extract_name = '{table_name}'
                      and sent is not null and confirmed is null
            """,
        )

        not_need_auto_confirm = DummyOperator(dag=dag, task_id='not_need_auto_confirm')

        # ── delta params ────────────────────────────────────────────────────
        if sql_stmt_export_delta:
            sql_get_delta_params = f"""
                select
                    /* 0  */ toString(num_state),
                    /* 1  */ '''' || toString(toDateTimeOrDefault(extract_time)) || '''',
                    /* 2  */ ifNull(toString(extract_count), 'null'),
                    /* 3  */ ifNull('''' || toString(loaded)    || '''', 'null'),
                    /* 4  */ ifNull('''' || toString(sent)      || '''', 'null'),
                    /* 5  */ ifNull('''' || toString(confirmed) || '''', 'null'),
                    /* 6  */ toString(increment),
                    /* 7  */ toString(overlap),
                    /* 8  */ '''' || time_field || '''',
                    /* 9  */ '''' || toString(time_from) || '''',
                    /* 10 */ '''' || toString(time_to)   || '''',
                    /* 11 */ '''' || toString(time_from) || ''' < ' || time_field || ' and ' || time_field || ' <= ''' || toString(time_to) || '''',
                    /* 12 */ if(current_time = extract_time, 'True', 'False'),
                    /* 13 */ toString(0)
                from export.extract_current_vw
                where extract_name = '{table_name}'
            """
        else:
            sql_get_delta_params = f"""
                select
                    /* 0  */ now() as current_time,
                    /* 1  */ '''' || toString(current_time) || '''',
                    /* 2  */ 'null', /* 3 */ 'null', /* 4 */ 'null', /* 5 */ 'null',
                    /* 6  */ toString(increment),
                    /* 7  */ toString(overlap),
                    /* 8  */ '''' || time_field || '''',
                    /* 9  */ '''' || toString(current_time - recent_interval) || '''',
                    /* 10 */ '''' || toString(current_time) || '''',
                    /* 11 */ '''' || toString(current_time - recent_interval) || ''' < ' || time_field || ' and ' || time_field || ' <= ''' || toString(current_time) || '''',
                    /* 12 */ 'True',
                    /* 13 */ toString(recent_interval)
                from export.extract_registry_vw
                where extract_name = '{table_name}'
            """

        get_delta_params = ClickHouseOperator(
            dag=dag,
            task_id='get_delta_params',
            sql=sql_get_delta_params,
            trigger_rule='one_success',
            do_xcom_push=True,
        )

        # ── extract params (compression, max_size, sanitize settings) ──────
        select_extract_params = ClickHouseOperator(
            dag=dag,
            task_id='select_extract_params',
            sql=f"""
                with
                    '{table_name}' AS extr_name,
                    compare_params as (
                        select 1 as priority, lower_bound, selfrun_timeout, compression_type,
                               compression_ext, max_file_size, xstream_sanitize, sanitize_array,
                               sanitize_list, pg_array_format, csv_format_params
                        from export.extract_registry_vw where extract_name = extr_name
                        union all
                        select 2 as priority, lower_bound, selfrun_timeout, compression_type,
                               compression_ext, max_file_size, xstream_sanitize, sanitize_array,
                               sanitize_list, pg_array_format, csv_format_params
                        from export.extract_registry_vw where extract_name = 'default'
                    ),
                    aggr_params as (
                        select
                            argMinIf(extr_name, priority, priority = 1)                                  as extract_name,
                            argMinIf(lower_bound, priority, priority = 1)                                as lower_bound,
                            argMinIf(selfrun_timeout, priority, priority = 1)                            as selfrun_timeout,
                            argMinIf(xstream_sanitize, priority, priority = 1)                           as xstream_sanitize,
                            argMinIf(sanitize_array, priority, priority = 1)                             as sanitize_array,
                            argMinIf(pg_array_format, priority, priority = 1)                            as pg_array_format,
                            argMinIf(compression_type, priority, lower(compression_type) <> 'default')  as compression_type,
                            argMinIf(compression_ext, priority, lower(compression_ext) <> 'default')    as compression_ext,
                            argMinIf(max_file_size, priority, lower(max_file_size) <> 'default')        as max_file_size,
                            argMinIf(sanitize_list, priority, lower(sanitize_list) <> 'default')        as sanitize_list,
                            argMinIf(csv_format_params, priority, lower(csv_format_params) <> 'default') as csv_format_params
                        from compare_params
                    )
                select
                    /* 0 */ '''' || toString(toDateTimeOrDefault(lower_bound)) || '''',
                    /* 1 */ toString(selfrun_timeout),
                    /* 2 */ compression_type,
                    /* 3 */ compression_ext,
                    /* 4 */ max_file_size,
                    /* 5 */ If(xstream_sanitize=1, 'True', 'False'),
                    /* 6 */ If(sanitize_array=1, 'True', 'False'),
                    /* 7 */ sanitize_list,
                    /* 8 */ If(pg_array_format=1, 'True', 'False'),
                    /* 9 */ csv_format_params
                from aggr_params
                where extract_name = '{table_name}'
                settings enable_global_with_statement = 1
            """,
            do_xcom_push=True,
        )

        # ── get metadata via DESCRIBE TABLE ─────────────────────────────────
        def _get_metadata(db_name, tbl_name, tbl_params, clickhouse_conn_id, **context):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            hook = ClickHouseHook(clickhouse_conn_id=clickhouse_conn_id)
            rows = hook.get_records(f"DESCRIBE TABLE {db_name}.{tbl_name}")
            columns = []
            for row in rows:
                source_type, notnull = parse_ch_type(row[1])
                columns.append({
                    "column_name": row[0],
                    "source_type": source_type,
                    "length":      None,
                    "notnull":     notnull,
                    "precision":   None,
                    "scale":       None,
                })
            columns.extend(tbl_params.get("extra_columns", []))
            meta = {
                "schema_name": tbl_params["schema"],
                "table_name":  tbl_name,
                "strategy":    tbl_params.get("strategy", "FULL_UK"),
                "PK":          tbl_params.get("PK", []),
                "UK":          tbl_params.get("UK", []),
                "params":      {"separation": "\t", "escapesymbol": "\""},
                "columns":     columns,
            }
            context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))

        get_metadata = PythonOperator(
            dag=dag,
            task_id='get_metadata',
            python_callable=_get_metadata,
            op_kwargs={
                "db_name":            database_name,
                "tbl_name":           table_name,
                "tbl_params":         params,
                "clickhouse_conn_id": "dlab-click",
            },
        )

        # ── TFS task group: export → zip → kafka ────────────────────────────
        tg_tfs_id = "prepare_and_send_files_via_tfs_route"
        with TaskGroup(dag=dag, group_id=tg_tfs_id) as tg_tfs:

            xp = f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0]"

            copy_clickhouse_query = HrpClickNativeToS3ListOperator(
                dag=dag,
                task_id='copy_clickhouse_query',
                s3_bucket=bucket,
                s3_key=f"{s3_prefix}/{{{{ ts_nodash }}}}.csv",
                sql=sql_stmt_export,
                compression=None,       # ZIP упаковываем сами
                max_size=f"{xp}[4] }}}}",
                replace=True,
                post_file_check=False,  # баг в hash-check при compression=None
                xstream_sanitize=f"{xp}[5] }}}}",
                sanitize_array=f"{xp}[6] }}}}",
                sanitize_list=f"{xp}[7] }}}}",
                pg_array_format=f"{xp}[8] }}}}",
                format_params=f"{xp}[9] }}}}",
            )

            def _package_zip_parts(
                outer_tg_id, inner_tg_id, tbl_name, schema, prefix,
                s3_prefix_path, s3_bucket, aws_conn_id, **context
            ):
                from airflow.providers.amazon.aws.hooks.s3 import S3Hook
                ti = context["ti"]

                s3_key_list    = ti.xcom_pull(task_ids=f"{outer_tg_id}.{inner_tg_id}.copy_clickhouse_query", key="s3_key_list")
                row_count_list = ti.xcom_pull(task_ids=f"{outer_tg_id}.{inner_tg_id}.copy_clickhouse_query", key="row_count_list")
                meta_json_str  = ti.xcom_pull(task_ids=f"{outer_tg_id}.get_metadata", key="meta_json")

                if not s3_key_list:
                    logger.warning("No CSV parts exported — skipping ZIP packaging")
                    ti.xcom_push(key="zip_name_list",    value=[])
                    ti.xcom_push(key="summary_tkt_name", value="")
                    ti.xcom_push(key="total_row_count",  value=0)
                    return

                meta_bytes = meta_json_str.encode()
                hook  = S3Hook(aws_conn_id=aws_conn_id)
                total = len(s3_key_list)
                base_ts = pendulum.now("UTC")
                uploaded_zips = []

                for i, (s3_key, row_count) in enumerate(zip(s3_key_list, row_count_list)):
                    rows  = int(row_count)
                    part  = i + 1
                    inner_ts = base_ts.add(seconds=i * 2    ).format("YYYYMMDDHHmmss")
                    tkt_ts   = base_ts.add(seconds=i * 2 + 1).format("YYYYMMDDHHmmss")
                    outer_ts = base_ts.add(seconds=i * 2 + 2).format("YYYYMMDDHHmmss")

                    csv_name  = f"{schema}__{tbl_name}__{inner_ts}__{part}_{total}_{rows}.csv"
                    meta_name = f"{schema}__{tbl_name}__{inner_ts}__{part}_{total}_{rows}.meta"
                    tkt_name  = f"{prefix}__{tkt_ts}.tkt"
                    zip_name  = f"{prefix}__{outer_ts}__{tbl_name}__{part}_{total}_{rows}.csv.zip"

                    csv_bytes = hook.get_key(key=s3_key, bucket_name=s3_bucket).get()["Body"].read()

                    buf = BytesIO()
                    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                        zf.writestr(meta_name, meta_bytes)
                        zf.writestr(tkt_name,  f"{csv_name};{rows}".encode())
                        zf.writestr(csv_name,  csv_bytes)
                    buf.seek(0)

                    hook.load_bytes(buf.getvalue(), key=f"{s3_prefix_path}/{zip_name}", bucket_name=s3_bucket, replace=True)
                    hook.delete_objects(bucket=s3_bucket, keys=[s3_key])
                    uploaded_zips.append(zip_name)
                    logger.info("Packaged %d/%d: %s", part, total, zip_name)

                summary_ts  = base_ts.add(seconds=(total - 1) * 2 + 3).format("YYYYMMDDHHmmss")
                summary_tkt = f"{prefix}__{summary_ts}.tkt"
                hook.load_bytes(
                    "\n".join(uploaded_zips).encode(),
                    key=f"{s3_prefix_path}/{summary_tkt}",
                    bucket_name=s3_bucket,
                    replace=True,
                )
                logger.info("Created summary tkt: %s", summary_tkt)

                ti.xcom_push(key="zip_name_list",    value=uploaded_zips)
                ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
                ti.xcom_push(key="total_row_count",  value=sum(int(r) for r in row_count_list))

            package_zip_parts = PythonOperator(
                dag=dag,
                task_id='package_zip_parts',
                python_callable=_package_zip_parts,
                op_kwargs={
                    "outer_tg_id":    tg.group_id,
                    "inner_tg_id":    tg_tfs_id,
                    "tbl_name":       table_name,
                    "schema":         schema_name,
                    "prefix":         replica_name,
                    "s3_prefix_path": s3_prefix,
                    "s3_bucket":      bucket,
                    "aws_conn_id":    TFS_OUT_CONN_ID,
                },
            )

            tkt_xcom = f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.{tg_tfs_id}.package_zip_parts', key='summary_tkt_name') }}}}"

            notify_tfs_kafka = ProduceToTopicOperator(
                dag=dag,
                task_id='notify_tfs_kafka',
                producer_function=produce_tfs_kafka_notification,
                producer_function_args=[scenario, tkt_xcom],
                delivery_callback=TFS_KAFKA_CALLBACK,
                pool=tfs_out_pool,
            )

            copy_clickhouse_query >> package_zip_parts >> notify_tfs_kafka

        # ── update send status ───────────────────────────────────────────────
        dp = f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0]"

        update_send_status = ClickHouseOperator(
            dag=dag,
            task_id='update_send_status',
            sql=f"""
                insert into export.extract_history (
                    extract_name, extract_time, extract_count,
                    loaded, sent, confirmed,
                    increment, overlap, recent_interval,
                    time_field, time_from, time_to, exported_files
                )
                select
                    '{table_name}',
                    {dp}[1] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.{tg_tfs_id}.package_zip_parts', key='total_row_count') }}}},
                    now(), now(), null,
                    {dp}[6] }}}},
                    {dp}[7] }}}},
                    {dp}[13] }}}},
                    {dp}[8] }}}},
                    {dp}[9] }}}},
                    {dp}[10] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.{tg_tfs_id}.package_zip_parts', key='zip_name_list') }}}}
            """,
        )

        # ── loop / self-trigger ──────────────────────────────────────────────
        def _get_task_next_run(end_loop, **context):
            if isinstance(end_loop, str):
                end_loop = end_loop.lower() in ('true', 't', '1')
            group_id = context["ti"].task_id.rsplit(".", maxsplit=1)[0]
            return f"{group_id}.fin" if end_loop else f"{group_id}.set_next_run_date"

        check_need_next_run = BranchPythonOperator(
            dag=dag,
            task_id='check_need_next_run',
            python_callable=_get_task_next_run,
            op_args=[f"{dp}[12] }}}}"],
        )

        def _get_next_run_date(timeout_str: str, **context):
            run_date = pendulum.now('UTC').add(minutes=int(timeout_str))
            context['ti'].xcom_push(key='next_run_date', value=run_date)

        set_next_run_date = PythonOperator(
            dag=dag,
            task_id='set_next_run_date',
            python_callable=_get_next_run_date,
            op_args=[f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][1] }}}}"],
            provide_context=True,
        )

        trigger_self = TriggerDagRunOperator(
            dag=dag,
            task_id='trigger_self',
            trigger_dag_id=dag.dag_id,
            logical_date=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.set_next_run_date', key='next_run_date') }}}}",
            reset_dag_run=False,
        )

        fin = DummyOperator(dag=dag, task_id='fin')

        # ── task dependencies ────────────────────────────────────────────────
        check_need_auto_confirm_delta >> [auto_confirm_delta, not_need_auto_confirm] >> get_delta_params
        get_delta_params >> [select_extract_params, get_metadata]
        [select_extract_params, get_metadata] >> tg_tfs
        tg_tfs >> update_send_status >> check_need_next_run
        check_need_next_run >> set_next_run_date >> trigger_self >> fin.as_teardown(setups=set_next_run_date)
        check_need_next_run >> fin

    return tg
