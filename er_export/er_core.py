"""
Core logic and utilities for the ER export process.
Contains task group definitions, SQL builders, and helper functions for packaging and notification.
"""
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
from airflow.utils.task_group import TaskGroup # type: ignore
from airflow.exceptions import AirflowSkipException, AirflowFailException
from plugins.utils import add_note

logger = logging.getLogger(__name__)

ON_DELIVERY = 'er_export.er_core.on_delivery'

def _fmt_dt(v):
    """Безопасное форматирование даты и времени для SQL-запросов. Возвращает 'null', если значение None."""
    return 'null' if v is None else f"'{v}'"

def _format_cur(cur: dict) -> dict:
    """
    Преобразует сырую строку из представления extract_current_vw
    в формат словаря со значениями, готовыми для подстановки в SQL-запрос.
    """
    tf = str(cur['time_field']).strip("'")
    ec = cur['extract_count']
    return {
        'num_state':      str(cur['num_state']),
        'extract_time':   _fmt_dt(cur['extract_time']),
        'extract_count':  'null' if ec is None else str(ec),
        'loaded':         'null' if ec is None else _fmt_dt(cur['loaded']),
        'sent':           'null' if ec is None else _fmt_dt(cur['sent']),
        'confirmed':      'null' if ec is None else _fmt_dt(cur['confirmed']),
        'increment':      str(cur['increment']),
        'overlap':        str(cur['overlap']),
        'time_field':     f"'{tf}'",
        'time_from':      _fmt_dt(cur['time_from']),
        'time_to':        _fmt_dt(cur['time_to']),
        'condition':      f"{_fmt_dt(cur['time_from'])} < {tf} and {tf} <= {_fmt_dt(cur['time_to'])}",
        'is_current':     'True' if cur['current_time'] == cur['extract_time'] else 'False',
        'recent_interval': '0',
    }

def get_dict(ch_hook, sql: str) -> list[dict]:
    """
    Выполняет SQL-запрос в ClickHouse с помощью переданного хука
    и возвращает результат в виде списка словарей.
    """
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    else:
        return []

def parse_type(ch_type: str, type_map: dict) -> tuple[str, bool]:
    """
    Анализирует тип данных ClickHouse, извлекает базовый тип (игнорируя LowCardinality и Nullable)
    и сопоставляет его с целевым типом. Возвращает кортеж (тип, not_null).
    """
    notnull = True
    if ch_type.startswith("LowCardinality(") and ch_type.endswith(")"):
        ch_type = ch_type[15:-1]
    if ch_type.startswith("Nullable(") and ch_type.endswith(")"):
        ch_type = ch_type[9:-1]
        notnull = False
    base = ch_type.split("(")[0]
    return type_map.get(base, "STRING"), notnull

def produce_msg(scenario_id: str, file_name: str, throttle_delay: int = 1):
    """
    Генератор сообщений для Kafka.
    Формирует тело сообщения в формате XML для уведомления системы о готовности файла.
    """
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
    """
    Callback-функция библиотеки Kafka, вызываемая после попытки доставки сообщения.
    Логирует успех или выбрасывает исключение при ошибке.
    """
    if err is not None:
        raise AirflowFailException(f"Failed to deliver message: {err}")
    logger.info(
        "Produced record to topic %s partition [%s] @ offset %s\n%s",
        msg.topic(), msg.partition(), msg.offset(), msg.value(),
    )

def _pre_kafka(scenario: str, mode: str):
    """
    Возвращает функцию pre_execute для задачи Airflow.
    Пропускает отправку уведомления в Kafka, если включен тестовый режим или нет данных.
    """
    def pre_execute(context):
        if mode != 'prod':
            raise AirflowSkipException("Kafka notification skipped in test mode")
        gid = context['task'].task_group.group_id
        summary_tkt = context['ti'].xcom_pull(task_ids=f"{gid}.pack_zip", key='summary_tkt_name')
        if not summary_tkt:
            raise AirflowSkipException("No data exported, skipping notification")
        context['task'].producer_function_args = [scenario, summary_tkt]
    return pre_execute


def export_tg(
    gid: str,
    cfg: dict,
    sql: str,
    cid: str,
    s3_conn: str,
    bucket: str,
    type_map: dict,
    mode: str,
    pool: str
) -> TaskGroup:
    """
    Основная фабрика группы задач (TaskGroup) для Airflow.
    Инкапсулирует весь процесс: получение состояния, выгрузка CSV в S3, 
    упаковка в ZIP, отправка сообщения в Kafka и обновление состояния.
    """
    
    from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator  # type: ignore
    from hrp_operators import HrpClickNativeToS3ListOperator  # type: ignore

    with TaskGroup(group_id=gid) as tg:

        @task(task_id='init')
        def init(cfg, **context):
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            S3Hook(aws_conn_id=s3_conn).create_bucket(bucket_name=bucket)
            hook = ClickHouseHook(clickhouse_conn_id=cid)
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
            hook = ClickHouseHook(clickhouse_conn_id=cid)
            rows, _ = hook.execute(f"DESCRIBE TABLE {cfg['db']}.{cfg['tbl']}", with_column_types=True)
            columns = []
            for row in rows:
                source_type, notnull = parse_type(row[1], type_map)
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
            
            op.sql              = op.sql.format(export_time=dp['extract_time'], condition=dp['condition'])
            
            raw_max_size = dp.get('max_file_size')
            if str(raw_max_size).lower() in ('default', 'none', 'null', ''):
                op.max_size = None
            else:
                try:
                    op.max_size = int(raw_max_size)
                except (ValueError, TypeError):
                    logger.warning("Invalid max_file_size: %r, using None", raw_max_size)
                    op.max_size = None

            op.pg_array_format   = dp['pg_array_format'] == 'True'
            op.xstream_sanitize  = dp.get('xstream_sanitize', 'False') == 'True'
            op.sanitize_array    = dp.get('sanitize_array', 'False') == 'True'
            op.sanitize_list     = dp.get('sanitize_list') or ''
            try:
                op.format_params = ast.literal_eval(dp['format_params']) if dp['format_params'] else {}
            except (ValueError, TypeError):
                logger.warning("Unparseable format_params: %r", dp['format_params'])
                op.format_params = {}

        export_to_s3 = HrpClickNativeToS3ListOperator(
            task_id='export_to_s3',
            s3_bucket=bucket,
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
            from hrp_operators import HrpS3ArchiveOperator  # type: ignore
            ti = context["ti"]

            s3_key_list    = ti.xcom_pull(task_ids=f"{gid}.export_to_s3", key='s3_key_list')
            row_count_list = ti.xcom_pull(task_ids=f"{gid}.export_to_s3", key='row_count_list')
            meta_json_str  = ti.xcom_pull(task_ids=f"{gid}.build_meta", key='meta_json')

            if not s3_key_list or not row_count_list:
                logger.warning("No CSV parts exported — skipping ZIP packaging")
                ti.xcom_push(key="zip_name_list",    value=[])
                ti.xcom_push(key="summary_tkt_name", value="")
                ti.xcom_push(key="total_row_count",  value=0)
                return

            meta_bytes = meta_json_str.encode()
            hook    = S3Hook(aws_conn_id=s3_conn)
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

                meta_s3_key = f"{cfg['s3_prefix']}/{meta_name}"
                tkt_s3_key  = f"{cfg['s3_prefix']}/{tkt_name}"
                csv_s3_key  = f"{cfg['s3_prefix']}/{csv_name}"
                zip_s3_key  = f"{cfg['s3_prefix']}/{zip_name}"

                hook.load_bytes(
                    bytes_data=meta_bytes,
                    key=meta_s3_key,
                    bucket_name=bucket,
                    replace=True
                )
                hook.load_bytes(
                    bytes_data=f"{csv_name};{rows}".encode(),
                    key=tkt_s3_key,
                    bucket_name=bucket,
                    replace=True
                )
                hook.copy_object(
                    source_bucket_key=s3_key,
                    dest_bucket_key=csv_s3_key,
                    source_bucket_name=bucket,
                    dest_bucket_name=bucket,
                )

                archive_op = HrpS3ArchiveOperator(
                    task_id=f'archive_files_{part}',
                    s3_keys_source=[tkt_s3_key, csv_s3_key, meta_s3_key],
                    s3_bucket_source=bucket,
                    aws_conn_id_source=s3_conn,
                    aws_conn_id=s3_conn,
                    compression='zip',
                    s3_bucket=bucket,
                    s3_key=zip_s3_key,
                    replace=True,
                )
                archive_op.execute(context)

                hook.delete_objects(bucket=bucket, keys=[s3_key, tkt_s3_key, csv_s3_key, meta_s3_key])
                uploaded_zips.append(zip_name)
                logger.info("Packaged %d/%d: %s using HrpS3ArchiveOperator", part, total, zip_name)

            summary_ts  = base_ts.add(seconds=(total - 1) * 2 + 3).format("YYYYMMDDHHmmss")
            summary_tkt = f"{cfg['replica']}__{summary_ts}.tkt"
            hook.load_bytes(
                "\n".join(uploaded_zips).encode(),
                key=f"{cfg['s3_prefix']}/{summary_tkt}",
                bucket_name=bucket,
                replace=True,
            )
            logger.info("Created summary tkt: %s", summary_tkt)

            total_rows = sum(int(r) for r in row_count_list)
            add_note(
                {'rows': total_rows, 'parts': total, 'files': uploaded_zips},
                level='Task,DAG', title='Exported',
            )
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
            pool=pool,
            pre_execute=_pre_kafka(cfg['scenario'], mode),
        )

        if not cfg.get('auto_confirm', 1):
            from airflow.providers.apache.kafka.sensors.await_message import AwaitMessageSensor  # type: ignore

            def _handle_confirm(message):
                logger.info("Received confirmation from Kafka: %s", message.value())
                return True

            t_wait_confirm = AwaitMessageSensor(
                task_id='wait_for_confirm',
                kafka_config_id=cfg['kafka_in_conn'],
                topics=[cfg['kafka_in_topic']],
                apply_function=_handle_confirm,
                poke_interval=60,
                timeout=3600,
                mode='reschedule',
            )
        else:
            from airflow.operators.empty import EmptyOperator # type: ignore
            t_wait_confirm = EmptyOperator(task_id='wait_for_confirm')

        @task(task_id='save_status', trigger_rule='none_failed_min_one_success')
        def save_status(cfg, **context):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
            ti         = context['ti']
            dp         = ti.xcom_pull(task_ids=f"{gid}.init")
            total_rows = ti.xcom_pull(task_ids=f"{gid}.pack_zip", key='total_row_count')
            zip_names  = ti.xcom_pull(task_ids=f"{gid}.pack_zip", key='zip_name_list')
            hook = ClickHouseHook(clickhouse_conn_id=cid)
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
