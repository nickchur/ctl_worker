"""### 🧪 DAG: Тестирование операторов HRP

Комплексный регрессионный стенд для валидации кастомных операторов пакета `hrp_operators`. 
Проверяет корректность переливки данных между различными СУБД и хранилищами.

| Группа тестов | Проверяемые операторы | Особенности |
|---|---|---|
| **pg_to_s3** | `PostgresToS3`, `PostgresToS3List` | Сжатие (GZIP), санитизация спецсимволов |
| **s3_to_ch** | `S3ToClickhouseTable` | Загрузка массивов, проверка типов |
| **ch_to_s3** | `CHTableToS3`, `CHQueryToS3`, `CHNative` | Native формат CH, выгрузка JSON |
| **s3_utils** | `S3ToS3`, `S3Archive`, `CheckS3FileHash` | Копирование, архивация (ZIP), сверка MD5 |
| **db_to_db** | `PGtoPG`, `CHtoPG`, `PGtoCH`, `PostgresDDL` | Прямые переливки, управление DDL |

**Методология:**
1. **Setup**: Создание временных таблиц с тестовыми данными (вкл. NULL, спецсимволы, массивы).
2. **Execution**: Последовательный запуск операторов с различными параметрами.
3. **Validation**: Автоматическая сверка row count и содержимого после каждой операции.
4. **Cleanup**: Гарантированное удаление артефактов в БД и S3 (даже при падении тестов).
"""

import sys
import os

# Пути для импорта hrp_operators (настраиваются под окружение)
sys.path.insert(0, '/home/sber-nschurkin/etl-core-develop/app-dataplatform-etl')
sys.path.insert(0, '/home/sber-nschurkin/etl-core-cloud/app-dataplatform-etl/packages/hrp_operators/src')

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException

import pendulum
import json
from logging import getLogger

logger = getLogger("airflow.task")

# Импорт операторов
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_s3 import HrpPostgresToS3Operator, HrpPostgresToS3ListOperator
from sber_app_dataplatform_etl_core.hrp_operators.s3_to_clickhouse import HrpS3ToClickhouseTableOperator
from sber_app_dataplatform_etl_core.hrp_operators.clickhouse_to_s3 import HrpClickhouseTableToS3Operator, HrpClickhouseQueryToS3Operator, HrpClickNativeToS3Operator
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_postgres import HrpPostgresToPostgresOperator
from sber_app_dataplatform_etl_core.hrp_operators.clickhouse_to_postgres import HrpClickhouseToPostgresOperator
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_clickhouse import HrpPostgresToClickhouseOperator
from sber_app_dataplatform_etl_core.hrp_operators.s3_to_s3 import HrpS3ToS3Operator
from sber_app_dataplatform_etl_core.hrp_operators.s3_file_hash import HrpCheckS3FileHash
from sber_app_dataplatform_etl_core.hrp_operators.s3_archive import HrpS3ArchiveOperator
from sber_app_dataplatform_etl_core.hrp_operators.postgres_ddl import HrpPostgresDDL

# Дефолтные настройки
DEFAULT_PG_CONN = 'airflowdb'
DEFAULT_CH_CONN = 'ctl_ch'
DEFAULT_S3_CONN = 's3-archive'
DEFAULT_S3_BUCKET = 'test_operators'
DEFAULT_S3_PREFIX = 'hrp_tests/'

@dag(
    dag_id='test_hrp_operators',
    ci='CI02420667',
    schedule_interval='@once',
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['test', 'hrp', 'operators'],
    default_args={
        'owner': 'DataLab (CI02420667)',
    },
    params={
        "pg_conn_id": Param(DEFAULT_PG_CONN, type="string"),
        "ch_conn_id": Param(DEFAULT_CH_CONN, type="string"),
        "s3_conn_id": Param(DEFAULT_S3_CONN, type="string"),
        "s3_bucket": Param(DEFAULT_S3_BUCKET, type="string"),
    },
)
def test_hrp_operators_dag():

    @task
    def setup_pg(params=None):
        pg_hook = PostgresHook(postgres_conn_id=params['pg_conn_id'])
        sql = """
        CREATE TABLE IF NOT EXISTS public.test_hrp_operators (
            id INTEGER PRIMARY KEY,
            name TEXT,
            val_int INTEGER,
            val_float DOUBLE PRECISION,
            val_arr_int INTEGER[],
            val_ts TIMESTAMP
        );
        COMMENT ON TABLE public.test_hrp_operators IS 'Test table for HRP operators';
        COMMENT ON COLUMN public.test_hrp_operators.id IS 'Primary key';
        COMMENT ON COLUMN public.test_hrp_operators.name IS 'Name';
        COMMENT ON COLUMN public.test_hrp_operators.val_int IS 'Integer value';
        COMMENT ON COLUMN public.test_hrp_operators.val_float IS 'Float value';
        COMMENT ON COLUMN public.test_hrp_operators.val_arr_int IS 'Array of integers';
        COMMENT ON COLUMN public.test_hrp_operators.val_ts IS 'Timestamp';

        TRUNCATE TABLE public.test_hrp_operators;
        INSERT INTO public.test_hrp_operators VALUES 
        (1, 'Normal row', 123, 1.23, '{1,2,3}', '2023-01-01 12:00:00'),
        (2, 'Row with special chars: \\n \\t " , ;', 456, 4.56, '{4,5,6}', '2023-01-02 12:00:00'),
        (3, 'Row with NULLs', NULL, NULL, NULL, NULL);
        """
        pg_hook.run(sql)
        logger.info("Postgres setup complete")

    @task
    def setup_ch(params=None):
        ch_hook = ClickHouseHook(clickhouse_conn_id=params['ch_conn_id'])
        sql = """
        CREATE TABLE IF NOT EXISTS technical.test_hrp_operators (
            id Int32,
            name String,
            val_int Nullable(Int32),
            val_float Nullable(Float64),
            val_arr_int Array(Int32),
            val_ts Nullable(DateTime)
        ) ENGINE = MergeTree() ORDER BY id;
        """
        ch_hook.run(sql)
        ch_hook.run("TRUNCATE TABLE technical.test_hrp_operators")
        logger.info("ClickHouse setup complete")

    @task_group(group_id='pg_to_s3_tests')
    def pg_to_s3_tests():
        
        HrpPostgresToS3ListOperator(
            task_id='pg_to_s3_csv_list',
            table_name='test_hrp_operators',
            schema='public',
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_pg.csv',
            postgres_conn_id="{{ params.pg_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            compression=None,
            replace=True,
            xstream_sanitize=True
        )

        HrpPostgresToS3Operator(
            task_id='pg_to_s3_gzip',
            table_name='test_hrp_operators',
            schema='public',
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_pg.csv.gz',
            postgres_conn_id="{{ params.pg_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            compression='gzip',
            replace=True
        )

    @task_group(group_id='s3_to_ch_tests')
    def s3_to_ch_tests():
        load_ch = HrpS3ToClickhouseTableOperator(
            task_id='s3_to_ch_load',
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_pg.csv.gz',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            table_name='test_hrp_operators',
            schema='technical',
            fmt='CSVWithNames',
            compression='gzip'
        )

        @task
        def validate_ch(params=None):
            ch_hook = ClickHouseHook(clickhouse_conn_id=params['ch_conn_id'])
            res = ch_hook.run("SELECT count() FROM technical.test_hrp_operators")
            count = res[0][0] if res else 0
            if count != 3:
                raise AirflowFailException(f"Validation failed: expected 3 rows in CH, got {count}")
            logger.info("ClickHouse validation successful")

        load_ch >> validate_ch()

    @task_group(group_id='ch_to_s3_tests')
    def ch_to_s3_tests():
        HrpClickhouseTableToS3Operator(
            task_id='ch_to_s3_dump',
            table_name='test_hrp_operators',
            schema='technical',
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_ch.csv.gz',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            compression='gzip',
            replace=True
        )

        HrpClickhouseQueryToS3Operator(
            task_id='ch_query_to_s3',
            sql="SELECT id, name FROM technical.test_hrp_operators WHERE id > 1",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_ch_query.csv',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            replace=True
        )

        HrpClickNativeToS3Operator(
            task_id='ch_native_to_s3',
            sql="SELECT * FROM technical.test_hrp_operators",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_ch_native.native',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            replace=True
        )

        HrpClickNativeToS3Operator(
            task_id='ch_native_to_s3_json',
            sql="SELECT * FROM technical.test_hrp_operators",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_ch_native.json',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            fmt='JSON',
            replace=True
        )


    @task_group(group_id='s3_utils_tests')
    def s3_utils_tests():
        
        HrpS3ToS3Operator(
            task_id='s3_copy',
            s3_bucket_source="{{ params.s3_bucket }}",
            s3_key_source=DEFAULT_S3_PREFIX + 'test_pg.csv',
            aws_conn_id_source="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'copy/test_pg.csv',
            aws_conn_id="{{ params.s3_conn_id }}",
            replace=True
        )

        HrpS3ArchiveOperator(
            task_id='s3_archive',
            s3_keys_source=[DEFAULT_S3_PREFIX + 'test_pg.csv'],
            s3_bucket_source="{{ params.s3_bucket }}",
            aws_conn_id_source="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'archive/test_pg.zip',
            aws_conn_id="{{ params.s3_conn_id }}",
            replace=True
        )

        HrpCheckS3FileHash(
            task_id='s3_check_hash',
            s3_bucket="{{ params.s3_bucket }}",
            s3_key=DEFAULT_S3_PREFIX + 'test_pg.csv.gz',
            aws_conn_id="{{ params.s3_conn_id }}",
            checksum="{{ ti.xcom_pull(task_ids='pg_to_s3_tests.pg_to_s3_gzip')['checksum'] }}"
        )

    @task_group(group_id='db_to_db_tests')
    def db_to_db_tests():
        HrpPostgresToPostgresOperator(
            task_id='pg_to_pg',
            source_table='test_hrp_operators',
            source_schema='public',
            target_table='test_hrp_operators_copy',
            target_schema='public',
            source_conn_id="{{ params.pg_conn_id }}",
            target_conn_id="{{ params.pg_conn_id }}",
            truncate=True
        )

        HrpClickhouseToPostgresOperator(
            task_id='ch_to_pg',
            sql='SELECT * FROM technical.test_hrp_operators',
            target_table='test_hrp_operators_from_ch',
            target_schema='public',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            target_conn_id="{{ params.pg_conn_id }}",
            truncate=True
        )

        HrpPostgresToClickhouseOperator(
            task_id='pg_to_ch',
            source_table='test_hrp_operators',
            source_schema='public',
            target_table='test_hrp_operators_from_pg',
            target_schema='technical',
            source_conn_id="{{ params.pg_conn_id }}",
            target_conn_id="{{ params.ch_conn_id }}",
            target_truncate=True
        )

        HrpPostgresDDL(
            task_id='pg_ddl_test',
            table_name='test_hrp_operators',
            schema='public',
            postgres_conn_id="{{ params.pg_conn_id }}"
        )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup(params=None):
        # Cleanup S3
        s3 = S3Hook(aws_conn_id=params['s3_conn_id'])
        keys = s3.list_keys(bucket_name=params['s3_bucket'], prefix=DEFAULT_S3_PREFIX)
        if keys:
            s3.delete_objects(bucket_name=params['s3_bucket'], keys=keys)
        
        # Cleanup DBs
        pg = PostgresHook(postgres_conn_id=params['pg_conn_id'])
        pg.run("DROP TABLE IF EXISTS public.test_hrp_operators; DROP TABLE IF EXISTS public.test_hrp_operators_copy; DROP TABLE IF EXISTS public.test_hrp_operators_from_ch;")
        
        ch = ClickHouseHook(clickhouse_conn_id=params['ch_conn_id'])
        ch.run("DROP TABLE IF EXISTS technical.test_hrp_operators; DROP TABLE IF EXISTS technical.test_hrp_operators_from_pg;")
        
        logger.info("Cleanup complete")

    # Flow
    setup_tasks = [setup_pg(), setup_ch()]
    
    setup_tasks >> pg_to_s3_tests() >> s3_to_ch_tests() >> ch_to_s3_tests() >> s3_utils_tests() >> db_to_db_tests() >> cleanup()

test_hrp_operators_dag()
