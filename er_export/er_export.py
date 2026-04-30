from __future__ import annotations

import json
import logging
import time
import uuid
import zipfile
from io import BytesIO

import pendulum
from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator  # type: ignore
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseBranchSQLOperator
from hrp_operators import HrpClickNativeToS3ListOperator  # type: ignore

from er_export.er_config import (
    CH_ID,
    CH_TYPE_MAP,
    DEFAULT_ARGS,
    ENV_STAND,
    ROW_COUNT_LIMIT_MAP,
    TFS_OUT_BUCKET,
    TFS_OUT_CONFIG_MAP,
    TFS_OUT_CONN_ID,
    tables,
)

logger = logging.getLogger(__name__)

TFS_KAFKA_CALLBACK = 'er_export.er_export.tfs_message_delivery_callback'


def parse_ch_type(ch_type: str) -> tuple[str, bool]:
    """Снимает обёртки Nullable/LowCardinality и приводит тип ClickHouse к source_type формата ER.
    Возвращает (source_type, notnull): notnull=False если тип был Nullable."""
    notnull = True
    if ch_type.startswith("LowCardinality(") and ch_type.endswith(")"):
        ch_type = ch_type[15:-1]
    if ch_type.startswith("Nullable(") and ch_type.endswith(")"):
        ch_type = ch_type[9:-1]
        notnull = False
    base = ch_type.split("(")[0]
    return CH_TYPE_MAP.get(base, "STRING"), notnull


def produce_tfs_kafka_notification(scenario_id: str, file_name: str, throttle_delay: int = 1):
    """Генератор для ProduceToTopicOperator: формирует XML-сообщение TransferFileCephRq
    и отправляет его в Kafka TFS для инициации передачи файла по сценарию scenario_id."""
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
    """Callback подтверждения доставки Kafka-сообщения. При ошибке бросает RuntimeError."""
    if err is not None:
        raise RuntimeError(f"Failed to deliver message: {err}")
    logger.info(
        "Produced record to topic %s partition [%s] @ offset %s\n%s",
        msg.topic(), msg.partition(), msg.offset(), msg.value(),
    )


def _make_pre_execute_kafka(scenario: str):
    """Фабрика замыкания pre_execute для notify_tfs_kafka: захватывает scenario по значению,
    чтобы избежать переопределения переменной при итерации цикла фабрики DAG-ов."""
    def _pre_execute(context):
        summary_tkt = context['ti'].xcom_pull(task_ids='package_zip_parts', key='summary_tkt_name')
        context['task'].producer_function_args = [scenario, summary_tkt]
    return _pre_execute


# ── DAG factory ──────────────────────────────────────────────────────────────
# Каждая запись в tables порождает отдельный DAG (аналогично ctl_worker).
# Конфиг таблицы передаётся в @task-функции явным аргументом cfg,
# а не через замыкание, чтобы исключить захват переменных цикла.

for _table_key, _params in tables.items():
    _db, _tbl = _table_key.split(".", maxsplit=1)
    _dag_id      = f"export_er__{_params['replica']}__{_tbl}"
    _replica     = _params['replica']
    _schema_name = _params['schema']
    _export_fmt  = _params.get('format', 'TSVWithNames')

    if _export_fmt != 'TSVWithNames':
        raise ValueError(f"Unsupported format: {_export_fmt!r}. Only TSVWithNames is supported.")

    _scenario, _tfs_prefix, _tfs_out_pool = TFS_OUT_CONFIG_MAP[_replica]
    _s3_prefix = f"{_tfs_prefix}/{_replica}"

    _sql_delta  = _params.get('sql_stmt_export_delta')
    _sql_recent = _params.get('sql_stmt_export_recent')

    if not (_sql_delta or _sql_recent):
        raise RuntimeError("One of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' must be specified!")
    if _sql_delta and _sql_recent:
        raise RuntimeError("Only one of 'sql_stmt_export_delta' or 'sql_stmt_export_recent' can be specified!")

    _sql_export = _sql_delta or _sql_recent
    _row_limit  = ROW_COUNT_LIMIT_MAP.get(ENV_STAND, 0)
    if _row_limit > 0:
        _sql_export = f"select * from ({_sql_export}) limit {_row_limit}"

    if _sql_delta:
        _sql_get_delta_params = f"""
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
            where extract_name = '{_tbl}'
        """
    else:
        _sql_get_delta_params = f"""
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
            where extract_name = '{_tbl}'
        """

    _sql_select_extract_params = f"""
        with
            '{_tbl}' AS extr_name,
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
                    argMinIf(extr_name, priority, priority = 1)                                   as extract_name,
                    argMinIf(lower_bound, priority, priority = 1)                                 as lower_bound,
                    argMinIf(selfrun_timeout, priority, priority = 1)                             as selfrun_timeout,
                    argMinIf(xstream_sanitize, priority, priority = 1)                            as xstream_sanitize,
                    argMinIf(sanitize_array, priority, priority = 1)                              as sanitize_array,
                    argMinIf(pg_array_format, priority, priority = 1)                             as pg_array_format,
                    argMinIf(compression_type, priority, lower(compression_type) <> 'default')   as compression_type,
                    argMinIf(compression_ext, priority, lower(compression_ext) <> 'default')     as compression_ext,
                    argMinIf(max_file_size, priority, lower(max_file_size) <> 'default')         as max_file_size,
                    argMinIf(sanitize_list, priority, lower(sanitize_list) <> 'default')         as sanitize_list,
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
        where extract_name = '{_tbl}'
        settings enable_global_with_statement = 1
    """

    # конфиг таблицы, передаётся явным аргументом в @task-функции
    _task_cfg = {
        'db':          _db,
        'tbl':         _tbl,
        'schema_name': _schema_name,
        'replica':     _replica,
        'scenario':    _scenario,
        's3_prefix':   _s3_prefix,
        'bucket':      TFS_OUT_BUCKET,
        'params':      _params,
    }

    with DAG(
        dag_id=_dag_id,
        description=f"ER-выгрузка {_table_key} → S3 ZIP → TFS Kafka",
        default_args=DEFAULT_ARGS,
        start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval='55 0 * * *',
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'xStream', 'ER'],
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
    ) as dag:

        create_bucket = S3CreateBucketOperator(
            task_id='create_bucket',
            bucket_name=TFS_OUT_BUCKET,
        )

        check_need_auto_confirm_delta = ClickHouseBranchSQLOperator(
            task_id='check_need_auto_confirm_delta',
            sql=f"select auto_confirm_delta from export.extract_registry_vw where extract_name = '{_tbl}'",
            follow_task_ids_if_true=['auto_confirm_delta'],
            follow_task_ids_if_false=['not_need_auto_confirm'],
            do_xcom_push=True,
        )

        auto_confirm_delta = ClickHouseOperator(
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
                where extract_name = '{_tbl}'
                      and sent is not null and confirmed is null
            """,
        )

        not_need_auto_confirm = DummyOperator(task_id='not_need_auto_confirm')

        get_delta_params = ClickHouseOperator(
            task_id='get_delta_params',
            sql=_sql_get_delta_params,
            trigger_rule='one_success',
            do_xcom_push=True,
        )

        select_extract_params = ClickHouseOperator(
            task_id='select_extract_params',
            sql=_sql_select_extract_params,
            do_xcom_push=True,
        )

        @task(task_id='get_metadata')
        def get_metadata(cfg, **context):
            """Получает схему таблицы через DESCRIBE TABLE, строит .meta JSON
            (колонки из CH + extra_columns из конфига) и кладёт его в XCom под ключом meta_json."""
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
            rows, _ = hook.execute(f"DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}", with_column_types=True)
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
            columns.extend(cfg['params'].get("extra_columns", []))
            meta = {
                "schema_name": cfg['schema_name'],
                "table_name":  cfg['tbl'],
                "strategy":    cfg['params'].get("strategy", "FULL_UK"),
                "PK":          cfg['params'].get("PK", []),
                "UK":          cfg['params'].get("UK", []),
                "params":      {"separation": "\t", "escapesymbol": "\""},
                "columns":     columns,
            }
            context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))

        t_get_metadata = get_metadata(cfg=_task_cfg)

        def _pre_execute_copy(context):
            """Инжектирует export_time и condition из get_delta_params в SQL выгрузки,
            а также параметры файла (max_size, sanitize и др.) из select_extract_params."""
            ti = context['ti']
            dp = ti.xcom_pull(task_ids='get_delta_params')[0]
            ep = ti.xcom_pull(task_ids='select_extract_params')[0]
            op = context['task']
            op.sql              = op.sql.format(export_time=dp[1], condition=dp[11])
            op.max_size         = ep[4]
            op.xstream_sanitize = ep[5] == 'True'
            op.sanitize_array   = ep[6] == 'True'
            op.sanitize_list    = ep[7]
            op.pg_array_format  = ep[8] == 'True'
            try:
                op.format_params = json.loads(ep[9]) if ep[9] else {}
            except (json.JSONDecodeError, TypeError):
                logger.warning("Unparseable format_params: %r", ep[9])
                op.format_params = {}

        copy_clickhouse_query = HrpClickNativeToS3ListOperator(
            task_id='copy_clickhouse_query',
            s3_bucket=TFS_OUT_BUCKET,
            s3_key=f"{_s3_prefix}/{{{{ ts_nodash }}}}.csv",  # ts_nodash — стандартный Airflow-макрос
            sql=_sql_export,
            compression=None,       # ZIP упаковываем сами
            replace=True,
            post_file_check=False,  # баг в hash-check при compression=None
            pre_execute=_pre_execute_copy,
        )

        @task(task_id='package_zip_parts')
        def package_zip_parts(cfg, **context):
            """Скачивает промежуточные CSV из S3, упаковывает каждый в ZIP вместе с .meta и .tkt,
            удаляет исходные CSV. Создаёт итоговый summary.tkt со списком ZIP-файлов.
            Результаты (zip_name_list, summary_tkt_name, total_row_count) кладёт в XCom."""
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            ti = context["ti"]

            s3_key_list    = ti.xcom_pull(task_ids='copy_clickhouse_query', key='s3_key_list')
            row_count_list = ti.xcom_pull(task_ids='copy_clickhouse_query', key='row_count_list')
            meta_json_str  = ti.xcom_pull(task_ids='get_metadata', key='meta_json')

            if not s3_key_list:
                logger.warning("No CSV parts exported — skipping ZIP packaging")
                ti.xcom_push(key="zip_name_list",    value=[])
                ti.xcom_push(key="summary_tkt_name", value="")
                ti.xcom_push(key="total_row_count",  value=0)
                return

            meta_bytes = meta_json_str.encode()
            hook    = S3Hook(aws_conn_id=TFS_OUT_CONN_ID)
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

                csv_bytes = hook.get_key(key=s3_key, bucket_name=cfg['bucket']).get()["Body"].read()

                buf = BytesIO()
                with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
                    zf.writestr(meta_name, meta_bytes)
                    zf.writestr(tkt_name,  f"{csv_name};{rows}".encode())
                    zf.writestr(csv_name,  csv_bytes)
                buf.seek(0)

                hook.load_bytes(
                    buf.getvalue(),
                    key=f"{cfg['s3_prefix']}/{zip_name}",
                    bucket_name=cfg['bucket'],
                    replace=True,
                )
                hook.delete_objects(bucket=cfg['bucket'], keys=[s3_key])
                uploaded_zips.append(zip_name)
                logger.info("Packaged %d/%d: %s", part, total, zip_name)

            summary_ts  = base_ts.add(seconds=(total - 1) * 2 + 3).format("YYYYMMDDHHmmss")
            summary_tkt = f"{cfg['replica']}__{summary_ts}.tkt"
            hook.load_bytes(
                "\n".join(uploaded_zips).encode(),
                key=f"{cfg['s3_prefix']}/{summary_tkt}",
                bucket_name=cfg['bucket'],
                replace=True,
            )
            logger.info("Created summary tkt: %s", summary_tkt)

            ti.xcom_push(key="zip_name_list",    value=uploaded_zips)
            ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
            ti.xcom_push(key="total_row_count",  value=sum(int(r) for r in row_count_list))

        t_package = package_zip_parts(cfg=_task_cfg)

        notify_tfs_kafka = ProduceToTopicOperator(
            task_id='notify_tfs_kafka',
            producer_function=produce_tfs_kafka_notification,
            producer_function_args=[_scenario, ''],  # заполняется в pre_execute
            delivery_callback=TFS_KAFKA_CALLBACK,
            pool=_tfs_out_pool,
            pre_execute=_make_pre_execute_kafka(_scenario),
        )

        @task(task_id='update_send_status')
        def update_send_status(cfg, **context):
            """Фиксирует результат выгрузки в export.extract_history через ClickHouseHook,
            используя значения из XCom задач get_delta_params и package_zip_parts."""
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            ti         = context['ti']
            dp         = ti.xcom_pull(task_ids='get_delta_params')[0]
            total_rows = ti.xcom_pull(task_ids='package_zip_parts', key='total_row_count')
            zip_names  = ti.xcom_pull(task_ids='package_zip_parts', key='zip_name_list')
            hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
            hook.run(f"""
                insert into export.extract_history (
                    extract_name, extract_time, extract_count,
                    loaded, sent, confirmed,
                    increment, overlap, recent_interval,
                    time_field, time_from, time_to, exported_files
                ) select
                    '{cfg['tbl']}',
                    {dp[1]},
                    {total_rows},
                    now(), now(), null,
                    {dp[6]}, {dp[7]}, {dp[13]},
                    {dp[8]}, {dp[9]}, {dp[10]},
                    {zip_names!r}
            """)

        t_update = update_send_status(cfg=_task_cfg)

        @task.branch(task_id='check_need_next_run')
        def check_need_next_run(**context):
            """Определяет следующую ветку: если end_loop=True — завершает цикл (→ fin),
            иначе планирует повторный запуск (→ set_next_run_date)."""
            dp = context['ti'].xcom_pull(task_ids='get_delta_params')[0]
            end_loop = str(dp[12]).lower() in ('true', 't', '1')
            return 'fin' if end_loop else 'set_next_run_date'

        t_check_next = check_need_next_run()

        @task(task_id='set_next_run_date')
        def set_next_run_date(**context):
            """Вычисляет дату следующего запуска DAG как now() + selfrun_timeout минут
            и кладёт её в XCom под ключом next_run_date."""
            ti = context['ti']
            ep = ti.xcom_pull(task_ids='select_extract_params')[0]
            run_date = pendulum.now('UTC').add(minutes=int(ep[1]))
            ti.xcom_push(key='next_run_date', value=run_date)

        t_set_next = set_next_run_date()

        def _pre_execute_trigger(context):
            """Инжектирует дату следующего запуска из XCom в TriggerDagRunOperator."""
            next_run = context['ti'].xcom_pull(task_ids='set_next_run_date', key='next_run_date')
            context['task'].logical_date = next_run

        trigger_self = TriggerDagRunOperator(
            task_id='trigger_self',
            trigger_dag_id=_dag_id,
            logical_date=None,  # заполняется в pre_execute
            reset_dag_run=False,
            pre_execute=_pre_execute_trigger,
        )

        fin = DummyOperator(task_id='fin')

        # ── task dependencies ────────────────────────────────────────────────
        create_bucket >> check_need_auto_confirm_delta
        check_need_auto_confirm_delta >> [auto_confirm_delta, not_need_auto_confirm] >> get_delta_params
        get_delta_params >> [select_extract_params, t_get_metadata]
        [select_extract_params, t_get_metadata] >> copy_clickhouse_query
        copy_clickhouse_query >> t_package >> notify_tfs_kafka >> t_update >> t_check_next
        t_check_next >> t_set_next >> trigger_self >> fin.as_teardown(setups=t_set_next)
        t_check_next >> fin

    globals()[_dag_id] = dag
