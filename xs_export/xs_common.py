from __future__ import annotations
import logging
import os
from pathlib import Path
import uuid
import time

import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator, S3DeleteObjectsOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.task_group import TaskGroup
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow_clickhouse_plugin.operators.clickhouse_dbapi import ClickHouseBranchSQLOperator

from CI06932748.analytics.datalab.default_default_args import DEFAULT_DEFAULT_ARGS
from hrp_operators import HrpClickNativeToS3Operator, HrpClickNativeToS3ListOperator

logger = logging.getLogger(__name__)

# переменные, относящиеся к конфигурации ТФС, объявлены на странице в конфлюенсе
# https://confluence.delta.sbrf.ru/pages/viewpage.action?pageId=14980745901
TFS_OUT_CONN_ID = 's3-tfs-hrplt'
TFS_OUT_BUCKET = 'tfshrplt'
TFS_OUT_TOPIC = 'TFS.HRPLT.IN'
# TFS_OUT_SCENARIO = 'HRPLATFORM-2100'
# TFS_OUT_PREFIX = 'from/DFDC/hrplt_to_kap802'
TFS_KAFKA_CALLBACK = 'CI06932748.analytics.datalab.export_xs.xs_common.tfs_message_delivery_callback'

# определяем стенд
ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

# определяем параметры конфигурации ТФС, зависящие от потока xStream (TFS_OUT_SCENARIO, TFS_OUT_PREFIX, TFS_OUT_POOL)
if ENV_STAND == "prom":
    # для ПРОМ-стенда
    TFS_OUT_CONFIG_MAP = {
        "hrpl_an": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_as": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_bd": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_lm": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_pd": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_pr": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_py": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_re": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrplatform": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
    }
else:
    # для непром. стендов
    TFS_OUT_CONFIG_MAP = {
        "hrplatform": ("HRPLATFORM-2100", "from/DFDC/hrplt_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_lm": ("HRPLATFORM-2800", "from/DFDC/hrpl_lm_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_re": ("HRPLATFORM-3000", "from/DFDC/hrpl_re_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_an": ("HRPLATFORM-3100", "from/DFDC/hrpl_an_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_pr": ("HRPLATFORM-3200", "from/DFDC/hrpl_pr_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_as": ("HRPLATFORM-3300", "from/DFDC/hrpl_as_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_pd": ("HRPLATFORM-3500", "from/DFDC/hrpl_mc_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_py": ("HRPLATFORM-3400", "from/DFDC/hrpl_py_to_kap802", "tfs_HRPLATFORM-2100"),
        "hrpl_bd": ("HRPLATFORM-3800", "from/DFDC/hrpl_bd_to_kap802", "tfs_HRPLATFORM-2100"),
    }

DEFAULT_ARGS = DEFAULT_DEFAULT_ARGS | dict(
    # настройки S3:
    aws_conn_id=TFS_OUT_CONN_ID,
    # настройки клика:
    clickhouse_conn_id="dlab-click",
    conn_id="dlab-click",
    # настройки кафки:
    kafka_config_id="tfs-kafka-out",
    topic=TFS_OUT_TOPIC,
)

# настройка ограничений на количество выгружаемых записей для стендов (0 - без ограничений)
ROW_COUNT_LIMIT_MAP = {
    "prom": 0,
    "uat": 100,
    "qa": 100,
    "ift": 100,
    "dev": 100,
}

def produce_tfs_kafka_notification(scenario_id: str, file_name: str, throttle_delay: int = 1) -> str:
    """
    Создает управляющее сообщение ТФС по формату определенному здесь:
    https://confluence.delta.sbrf.ru/pages/viewpage.action?pageId=130829910
    :param scenario_id: идентификатор сценария ТФС
    :param file_name: название файла, указывается ключ в S3 без префикса
    :param throttle_delay: задержка в секундах, чтобы не перегружать кафку ТФС (по умолчанию 1 сек)
    :return:
    """
    # выдерживаем паузу, чтобы не перегружать кафку ТФС
    time.sleep(throttle_delay)

    # генерируем UUID
    rq_uuid = str(uuid.uuid4()).replace('-', '')

    # создаем сообщение
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

    # отправляем пару (key, message)
    yield None, message


def tfs_message_delivery_callback(err: object, msg: object) -> None:
    """
    Коллбек, который нужен, чтобы рейзить ошибку, дефолтный просто логгирует
    Нам нужно чтобы таск падал и ретраился
    :param err: ошибка из кафки или None
    :type err: confluent_kafka.KafkaError
    :param msg: сообщение
    :type msg: confluent_kafka.Message
    :return:
    """
    if err is not None:
        raise RuntimeError(f"Failed to deliver message: {err}")
    else:
        logger.info(
            "Produced record to topic %s, partition [%s] @ offset %s, text;\n%s",
            msg.topic(),
            msg.partition(),
            msg.offset(),
            msg.value(),
        )


def get_upstream_task_id(upstream_task_name: str, context):
    """
    Получение идентификатора апстрим таски, с учетом таск группы

    :param upstream_task_name:
    :param context:
    :return:
    """
    if "." in (self_task_id := context["ti"].task_id):
        # так мы проверяем на то, что таск запущен внутри таск группы
        # завязываемся на точку в названии: {task_group_id}.{task_id}
        upstream_task_name = f"{self_task_id.rsplit('.', maxsplit=1)[0]}.{upstream_task_name}"

    return upstream_task_name


def get_uploaded_s3_key(upstream_task_name: str, key_name: str = "s3_key"):
    def _uploaded_s3_key(context, jinja_env):
        """
        Вытаскивает запушенный таском copy_clickhouse_query путь до файла
        Все функции которые используются для вычисления параметров операторов обязаны принимать аргументы context и jinja_env
        https://airflow.apache.org/docs/apache-airflow/2.10.3/core-concepts/operators.html#jinja-templating

        :param context:
        :param jinja_env:

        :return: s3_key
        """
        s3_key = context['ti'].xcom_pull(
            # context["ti"].task_id.split(".")[0] - получаем название TaskGroup
            task_ids=get_upstream_task_id(upstream_task_name, context),
            key=key_name,
        )
        logger.info('Uploaded S3 key: %s', s3_key)
        return s3_key

    return _uploaded_s3_key


def get_s3_key_with_row_count(upstream_task_name: str, xcom_key: str = "row_count", s3_list: bool = False):
    """
    Получаем счетчик строк, вычитаем из него единицу, если надо, за хедер строку

    :param upstream_task_name: имя апстрим таска
    :param decrease_header: нужно ли вычитать единицу из каунта
    :param xcom_key: ключ в XCOM
    :param s3_list: сообщаем что в xcom лежит список S3 ключей

    :return: новый путь в S3
    """
    def _s3_key_with_row_count(context, jinja_env) -> str | list[str]:
        s3_key: str = get_uploaded_s3_key(upstream_task_name)(context, jinja_env)

        row_count = context['ti'].xcom_pull(
            task_ids=get_upstream_task_id(upstream_task_name, context),
            key=xcom_key,
        )
        if not s3_list:
            row_count = int(row_count)
            base_part, extension = s3_key.split(".", maxsplit=1)

            new_key = f"{base_part}_{row_count}.{extension}"
            logger.info("Generated S3 key: %s", new_key)
            return new_key

        else:
            # второй раз не получаем s3_key, приходит пустым
            result = []
            for row in row_count:
                result.append(row)
            return result

    return _s3_key_with_row_count


def get_lower_bound_placeholder(table_name: str):
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.select_exp_params')[0][0] }}}}"


def get_export_time_placeholder(table_name: str):
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.get_delta_params')[0][1] }}}}"


def get_delta_condition_placeholder(table_name: str):
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.get_delta_params')[0][11] }}}}"


def make_xs_export_task_group(
    dag: DAG,
    database_name,
    table_name,
    params,
    bucket: str = TFS_OUT_BUCKET,
    s3_list: bool = True,  # Качать с clickhouse в список S3 файлов
):

    now_dt = pendulum.now('Europe/Moscow').strftime('%Y%m%d_%H%M%S')

    # формат выгрузки в файл
    # export_format = params.get("format", "TSVWithNames")

    # достаем параметры конфигурации маршрута ТФС, зависящие от стенда и потока xStream
    xstream_data_flow = params['name_file'].split("__")[2]
    scenario, tfs_prefix, tfs_out_pool = TFS_OUT_CONFIG_MAP[xstream_data_flow]

    # определяем SQL-запрос для загрузки инкремента в delta-таблицу
    sql_stmt_update_exp = params.get("sql_stmt_update_exp", None)

    # определяем SQL-запрос для загрузки инкремента в delta-таблицу
    sql_stmt_load_delta = params.get("sql_stmt_load_delta", None)
    if not sql_stmt_load_delta:
        raise RuntimeError("Value of parameter sql_stmt_load_delta is not specified!")

    # определяем SQL-запрос для выгрузки инкремента из delta-таблицы в S3
    sql_stmt_export_delta = params.get("sql_stmt_export_delta", None)
    if not sql_stmt_export_delta:
        # sql_stmt = f"{{% include 'export_{table_name}.sql' %}}"
        raise RuntimeError("Value of parameter sql_stmt_export_delta is not specified!")
    else:
        # определяем ограничение на количество выгружаемых строк, в зависимости от стенда
        row_count_limit = ROW_COUNT_LIMIT_MAP.get(
            os.getenv("ENV_STAND", "").lower(),
            0
        )
        # добавляем ограничение на количество выгружаемых строк
        if row_count_limit > 0:
            sql_stmt_export_delta = f"select * from ({sql_stmt_export_delta}) limit {row_count_limit}"

    parameters = ''
    if query_settings := params.get("settings"):
        parameters = f"SETTINGS {', '.join(query_settings)}"

    with TaskGroup(dag=dag, group_id=f"{table_name}", tooltip=f"Выгрузка таблицы {table_name}") as tg:

        # если выгрузка отключена - ничего не делаем
        check_extract_is_active = ClickHouseBranchSQLOperator(
            dag=dag,
            task_id='check_extract_is_active',
            sql=f"""
                select is_active from export.extract_registry_vw where extract_name = '{table_name}'
            """,
            follow_task_ids_if_true=[f"{tg.group_id}.select_exp_params"],
            follow_task_ids_if_false=[],
            do_xcom_push=True,
        )

        # считываем параметры выгрузки
        select_exp_params = ClickHouseOperator(
            dag=dag,
            task_id='select_exp_params',
            sql=f"""
                select
                    /* 0 - lower_bound */
                    '''' || toString(toDateTimeOrDefault(lower_bound)) || ''''
                from export.extract_registry_vw where extract_name = '{table_name}'
                """,
            do_xcom_push=True,
        )

        # если определена строка sql_stmt_update_exp, то обновляем exp-таблицу
        if sql_stmt_update_exp:
            update_exp_table = ClickHouseOperator(
                dag=dag,
                task_id='update_exp_table',
                sql=sql_stmt_update_exp,
                settings={
                    'max_threads': 1,
                    'max_memory_usage': '40G'
                }
            )

        # проверяем, нужна ли оптимизация exp-таблицы
        check_need_optimize_exp_table = ClickHouseBranchSQLOperator(
            dag=dag,
            task_id='check_need_optimize_exp_table',
            sql=f"""
                select optimize_exp_table from export.extract_registry_vw where extract_name = '{table_name}'
            """,
            follow_task_ids_if_true=[f"{tg.group_id}.optimize_exp_table"],
            follow_task_ids_if_false=[f"{tg.group_id}.not_need_optimize"],
            do_xcom_push=True,
        )

        # выполняем оптимизацию exp-таблицы
        optimize_exp_table = ClickHouseOperator(
            dag=dag,
            task_id='optimize_exp_table',
            sql=f"""
                optimize table {database_name}_export.{table_name}_exp final
            """,
            settings={
                "alter_sync": 2,
                "replication_wait_for_inactive_replica_timeout": 600,
            },
        )

        # пропускаем оптимизацию exp-таблицы
        not_need_optimize = DummyOperator(
            dag=dag,
            task_id='not_need_optimize',
        )

        # проверяем, нужна ли оптимизация exp-таблицы
        check_need_auto_confirm_delta = ClickHouseBranchSQLOperator(
            dag=dag,
            task_id='check_need_auto_confirm_delta',
            sql=f"""
                select auto_confirm_delta from export.extract_registry_vw where extract_name = '{table_name}'
            """,
            follow_task_ids_if_true=[f"{tg.group_id}.auto_confirm_delta"],
            follow_task_ids_if_false=[f"{tg.group_id}.not_need_auto_confirm"],
            trigger_rule='one_success',
            do_xcom_push=True,
        )

        # автоматически подтверждаем предыдущий инкремент
        auto_confirm_delta = ClickHouseOperator(
            dag=dag,
            task_id='auto_confirm_delta',
            sql=f"""
                insert into export.extract_history (
                    extract_name,
                    extract_time,
                    extract_count,
                    loaded,
                    sent,
                    confirmed,
                    increment,
                    overlap,
                    time_field,
                    time_from,
                    time_to,
                    exported_files
                )
                select
                    extract_name,
                    extract_time,
                    extract_count,
                    loaded,
                    sent,
                    now(),
                    increment,
                    overlap,
                    time_field,
                    time_from,
                    time_to,
                    exported_files
                from export.extract_history_vw
                where extract_name = '{table_name}'
                      and sent is not null and confirmed is null
            """,
        )

        # пропускаем автоматическое подтверждение предыдущего инкремента
        not_need_auto_confirm = DummyOperator(
            dag=dag,
            task_id='not_need_auto_confirm',
        )

        # считываем текущие параметры дельты, формируем условие для выгрузки инкремента
        get_delta_params = ClickHouseOperator(
            dag=dag,
            task_id='get_delta_params',
            sql=f"""
                select
                    /* 0 - num_state */
                    toString(num_state),
                    /* 1 - extract_time */
                    '''' || toString(toDateTimeOrDefault(extract_time)) || '''',
                    /* 2 - extract_count */
                    ifNull(toString(extract_count), 'null'),
                    /* 3 - loaded */
                    ifNull('''' || toString(loaded) || '''', 'null'),
                    /* 4 - sent */
                    ifNull('''' || toString(sent) || '''', 'null'),
                    /* 5 - confirmed */
                    ifNull('''' || toString(confirmed) || '''', 'null'),
                    /* 6 - increment */
                    toString(increment),
                    /* 7 - overlap */
                    toString(overlap),
                    /* 8 - time_field */
                    '''' || time_field || '''',
                    /* 9 - time_from */
                    '''' || toString(time_from) || '''',
                    /* 10 - time_to */
                    '''' || toString(time_to) || '''',
                    /* 11 - delta_condition */
                    '''' || toString(time_from) || ''' < ' || time_field || ' and ' || time_field || ' <= ''' || toString(time_to) || '''',
                    /* 12 - end_loop */
                    if(current_time = extract_time, 'True', 'False')
                from export.extract_current_vw
                where extract_name = '{table_name}'
            """,
            trigger_rule='one_success',
            do_xcom_push=True,
        )

        # очищаем дельта-таблицу
        truncate_delta_table = ClickHouseOperator(
            dag=dag,
            task_id='truncate_delta_table',
            sql=f"""
                truncate table {database_name}_export.{table_name}_delta on cluster datalab
            """,
        )

        # загружаем инкремент в дельта-таблицу
        insert_delta_table = ClickHouseOperator(
            dag=dag,
            task_id='insert_delta_table',
            sql=sql_stmt_load_delta,
            settings={
                'max_threads': 1,
                'max_memory_usage': '40G'
            }
        )

        # определяем количество записей в текущем инкременте
        select_delta_count = ClickHouseOperator(
            dag=dag,
            task_id='select_delta_count',
            sql=f"""
                select
                    /* 0 - extract_count */
                    count(*),
                    /* 1 - loaded */
                    '''' || now() || ''''
                from {database_name}_export.{table_name}_delta
            """,
            do_xcom_push=True,
        )

        # записываем параметры последнего инкремента в таблицу
        update_extract_history = ClickHouseOperator(
            dag=dag,
            task_id='update_extract_history',
            sql=f"""
                insert into export.extract_history (
                    extract_name,
                    extract_time,
                    extract_count,
                    loaded,
                    sent,
                    confirmed,
                    increment,
                    overlap,
                    time_field,
                    time_from,
                    time_to
                )
                select
                    '{table_name}',
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][1] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_delta_count')[0][0] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_delta_count')[0][1] }}}},
                    null,
                    null,
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][6] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][7] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][8] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][9] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][10] }}}}
            """,
        )

        # считываем параметры выгрузки
        select_extract_params = ClickHouseOperator(
            dag=dag,
            task_id='select_extract_params',
            sql=f"""
                    with
                        '{table_name}' AS extr_name,
                        compare_params as (
                            select
                                1 as priority,
                                lower_bound,
                                selfrun_timeout,
                                compression_type,
                                compression_ext,
                                max_file_size,
                                xstream_sanitize,
                                sanitize_array,
                                sanitize_list,
                                pg_array_format,
                                csv_format_params
                            from export.extract_registry_vw
                            where extract_name = extr_name
                            union all
                            select
                                2 as priority,
                                lower_bound,
                                selfrun_timeout,
                                compression_type,
                                compression_ext,
                                max_file_size,
                                xstream_sanitize,
                                sanitize_array,
                                sanitize_list,
                                pg_array_format,
                                csv_format_params
                            from export.extract_registry_vw
                            where extract_name = 'default'
                        ),
                        aggr_params as (
                            select
                                /* Эти значения индивидуальные у каждой поставки */
                                argMinIf(extr_name, priority, priority = 1) as extract_name,
                                argMinIf(lower_bound, priority, priority = 1) as lower_bound,
                                argMinIf(selfrun_timeout, priority, priority = 1) as selfrun_timeout,
                                argMinIf(xstream_sanitize, priority, priority = 1) as xstream_sanitize,
                                argMinIf(sanitize_array, priority, priority = 1) as sanitize_array,
                                argMinIf(pg_array_format, priority, priority = 1) as pg_array_format,
    
                                /* В этих параметрах можно использовать константу default - в таких случаях берем значение из extract_name = 'default' */
                                argMinIf(compression_type, priority, lower(compression_type) <> 'default') as compression_type,
                                argMinIf(compression_ext, priority, lower(compression_ext) <> 'default') as compression_ext,
                                argMinIf(max_file_size, priority, lower(max_file_size) <> 'default') as max_file_size,
                                argMinIf(sanitize_list, priority, lower(sanitize_list) <> 'default') as sanitize_list,
                                argMinIf(csv_format_params, priority, lower(csv_format_params) <> 'default') as csv_format_params
                            from compare_params
                        )
                    select
                        /* 0 - lower_bound */
                        '''' || toString(toDateTimeOrDefault(lower_bound)) || '''',
                        /* 1 - selfrun_timeout */
                        toString(selfrun_timeout),
                        /* 2 - compression_type */
                        compression_type,
                        /* 3 - compression_ext */
                        compression_ext,
                        /* 4 - max_file_size */
                        max_file_size,
                        /* 5 - xstream_sanitize */
                        If(xstream_sanitize=1, 'True', 'False'),
                        /* 6 - sanitize_array */
                        If(sanitize_array=1, 'True', 'False'),
                        /* 7 - sanitize_list */
                        sanitize_list,
                        /* 8 - pg_array_format */
                        If(pg_array_format=1, 'True', 'False'),
                        /* 9 - csv_format_params */
                        csv_format_params
                    from aggr_params
                    where extract_name = '{table_name}'
                    settings enable_global_with_statement = 1
                """,
            do_xcom_push=True,
        )

        # Подготовка и передача файлов по маршруту TFS
        tg_tfs_id = f"prepare_and_send_files_via_tfs_route"
        with TaskGroup(dag=dag, group_id=tg_tfs_id) as tg_tfs:
            # выгружаем данные из ClickHouse в S3
            click_to_s3_operator = HrpClickNativeToS3ListOperator if s3_list else HrpClickNativeToS3Operator
            # параметры выгрузки можно менять через даг change_extract_registry
            copy_clickhouse_query = click_to_s3_operator(
                dag=dag,
                task_id='copy_clickhouse_query',
                s3_bucket=bucket,
                s3_key=tfs_prefix + '/' + f"{params['name_file']}_{now_dt}.csv"
                    + '.' + f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][3] }}}}",
                compression=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][2] }}}}",

                # None - выгружать все данные одним файлом, либо int + единицы измерения (B, KB, MB, GB). Например: 100 MB - выгружать по 100 MB
                max_size=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][4] }}}}",

                # True - применять параметры очистки и замены символов для текстовых полей
                xstream_sanitize=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][5] }}}}",

                # True - применять параметры очистки и замены символов для текстовых полей внутри массивов
                sanitize_array=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][6] }}}}",

                # sanitize_list - список замен, применяется последовательно по порядку в списке
                sanitize_list=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][7] }}}}",

                # True - выгружать массивы в формате Postgres, False - в формате ClickHouse
                pg_array_format=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][8] }}}}",

                # Параметры выгрузки в csv
                format_params=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][9] }}}}",
                # Параметры выгрузки в csv определяем в виде словаря следующего вида:
                # {"delimiter": "\t", "lineterminator": "\n", "quotechar": "\"", "quoting": 0, "escapechar": None, "doublequote": True} - основной вариант для xStream
                # {"delimiter": "\t", "lineterminator": "\n", "quoting": 3, "escapechar": "\\", "doublequote": False} - по умолчанию в операторе
                # Для вставки через даг change_extract_registry нужно удвоить \
                # Значения quoting подставляем как int в соответствии с библиотекой csv:
                # QUOTE_ALL = 1
                # QUOTE_MINIMAL = 0
                # QUOTE_NONE = 3
                # QUOTE_NONNUMERIC = 2

                replace=True,
                sql=sql_stmt_export_delta,
                params={"sets": parameters},
                post_file_check=False, # сейчас есть ошибка в проверке файла при архивации ZIP, временно отключаем
            )

            def get_s3_keys_expand(**kwargs) -> list[tuple[str, str]]:
                """Получаем список ключей S3 и количество строк из XCom. Подготавливаем expand для задач copy_s3_object
                Возвращаем список с кортежами вида (s3_key, s3_key_with_row_count)"""

                s3_key = "s3_key_list" if s3_list else "s3_key"
                row_count = "row_count_list" if s3_list else "row_count"
                xcom_s3_key = get_uploaded_s3_key('copy_clickhouse_query', key_name=s3_key)(kwargs, None)

                xcom_row_count = get_s3_key_with_row_count(
                    'copy_clickhouse_query', xcom_key=row_count, s3_list=s3_list
                )(kwargs, None)

                s3_keys = xcom_s3_key if s3_list else [xcom_s3_key]

                if s3_list:
                    row_counts = []
                    part_count = len(xcom_row_count)
                    for num, row_count in enumerate(xcom_row_count):
                        row_count = int(row_count)
                        filename = s3_keys[num]
                        ext = "".join(Path(filename).suffixes)
                        new_key = f"{filename.replace(ext, '')}_{part_count}_{row_count}{ext}"
                        logger.info("Generated S3 key: %s", new_key)
                        row_counts.append(new_key)
                else:
                    row_counts = [xcom_row_count]
                return [
                    (s3_key, s3_key_row_count) for s3_key, s3_key_row_count in zip(s3_keys, row_counts)
                ]

            # Создаем базовый объект для expand с s3_key и s3_key_with_row_count
            s3_keys_expand = PythonOperator(
                dag=dag,
                task_id='copy_s3_object_expand',
                python_callable=get_s3_keys_expand,
                do_xcom_push=True,
            )

            def create_copy_s3_object_expand(row):
                """Принимает кортеж (s3_key, s3_key_with_row_count) и возвращает словарь с ключами для задачи copy_s3_object
                """
                s3_key, key_row_count = row
                return {"source_bucket_key": s3_key, "dest_bucket_key": key_row_count}

            copy_s3_object_kwargs = s3_keys_expand.output.map(create_copy_s3_object_expand)

            # создаем копию файла/ов в S3 - чтобы добавить в имя файла количество строк
            copy_s3_object = S3CopyObjectOperator.partial(
                dag=dag,
                task_id='copy_s3_object',
                source_bucket_name=bucket,
                dest_bucket_name=bucket,
            ).expand_kwargs(
                copy_s3_object_kwargs
            )

            delete_s3_object_expand = s3_keys_expand.output.map(lambda row: row[0])
            # удаляем ключ/и в S3
            delete_s3_object = S3DeleteObjectsOperator.partial(
                dag=dag,
                task_id="delete_s3_object",
                bucket=bucket,
                # keys=delete_s3_object_expand,
            ).expand(keys=delete_s3_object_expand)  # Нехорошо т.к. можно удалить список ключей одной таской

            notify_tfs_kafka_expand = s3_keys_expand.output.map(lambda row: (scenario, os.path.basename(row[1])))
            # передаем в кафку ТФС сообщение о выгруженном файле
            notify_tfs_kafka = ProduceToTopicOperator.partial(
                dag=dag,
                task_id='notify_tfs_kafka',
                producer_function=produce_tfs_kafka_notification,
                delivery_callback=TFS_KAFKA_CALLBACK,
                pool=tfs_out_pool,
            ).expand(
                producer_function_args=notify_tfs_kafka_expand,
            )

            copy_clickhouse_query >> s3_keys_expand >> copy_s3_object >> [notify_tfs_kafka, delete_s3_object]

        # записываем данные отправки
        update_send_status = ClickHouseOperator(
            dag=dag,
            task_id='update_send_status',
            sql=f"""
                insert into export.extract_history (
                    extract_name,
                    extract_time,
                    extract_count,
                    loaded,
                    sent,
                    confirmed,
                    increment,
                    overlap,
                    time_field,
                    time_from,
                    time_to,
                    exported_files
                )
                with file_names_xcom as (select {{{{ ti.xcom_pull(task_ids='{tg.group_id}.{tg_tfs_id}.copy_s3_object_expand') }}}} as arr_file_names)
                select
                    '{table_name}',
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][1] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_delta_count')[0][0] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_delta_count')[0][1] }}}},
                    now() as sent,
                    null,
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][6] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][7] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][8] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][9] }}}},
                    {{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][10] }}}},
                    arrayFlatten(arrayMap(x -> x[2], arr_file_names))
                from file_names_xcom
            """,
        )

        # Если параметр True, значит конец цикла и остальные задачи пробрасываем
        def get_task_next_run(end_loop: bool):
            logger.info(f"Значение параметра end_loop: {end_loop}, type: {type(end_loop)}")

            if isinstance(end_loop, str):
                end_loop = end_loop.lower() in ('true', 't', '1')

            if end_loop:
                return f"{tg.group_id}.fin"
            return f"{tg.group_id}.set_next_run_date"

        # если еще не достигли текущей даты, то надо повторить даг
        check_need_next_run = BranchPythonOperator(
            dag=dag,
            task_id='check_need_next_run',
            python_callable=get_task_next_run,
            op_args=[f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.get_delta_params')[0][12] }}}}"],
        )

        def get_next_run_date(timeout_str: str, **context):
            logger.info(f"Значение параметра timeout_str: {timeout_str}, type: {type(timeout_str)}")
            timeout = int(timeout_str)

            run_date = pendulum.now('UTC').add(minutes=timeout)
            context['ti'].xcom_push(
                key='next_run_date',
                value=run_date,
            )

        set_next_run_date = PythonOperator(
            task_id='set_next_run_date',
            python_callable=get_next_run_date,
            op_args=[f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.select_extract_params')[0][1] }}}}"],
            provide_context=True,
        )

        # Задача, запускающая сам DAG заново
        trigger_self = TriggerDagRunOperator(
            task_id='trigger_self',
            trigger_dag_id=dag.dag_id,
            logical_date=f"{{{{ ti.xcom_pull(task_ids='{tg.group_id}.set_next_run_date', key='next_run_date') }}}}",
            reset_dag_run=False,
        )

        # завершение дага
        fin = DummyOperator(
            dag=dag,
            task_id='fin',
        )

        if sql_stmt_update_exp:
            check_extract_is_active >> select_exp_params >> \
            update_exp_table >> \
            check_need_optimize_exp_table >> [optimize_exp_table, not_need_optimize] >> \
            check_need_auto_confirm_delta >> [auto_confirm_delta, not_need_auto_confirm] >> \
            get_delta_params >> truncate_delta_table >> \
            insert_delta_table >> select_delta_count >> update_extract_history >> \
            select_extract_params >> tg_tfs >> update_send_status >> \
            check_need_next_run >> set_next_run_date >> trigger_self >> \
            fin.as_teardown(setups=set_next_run_date)
        else:
            check_extract_is_active >> select_exp_params >> \
            check_need_optimize_exp_table >> [optimize_exp_table, not_need_optimize] >> \
            check_need_auto_confirm_delta >> [auto_confirm_delta, not_need_auto_confirm] >> \
            get_delta_params >> truncate_delta_table >> \
            insert_delta_table >> select_delta_count >> update_extract_history >> \
            select_extract_params >> tg_tfs >> update_send_status >> \
            check_need_next_run >> set_next_run_date >> trigger_self >> \
            fin.as_teardown(setups=set_next_run_date)

    return tg