"""
### 🧪 DAG для тестирования кастомных операторов (HRP Operators)

Этот DAG проверяет работоспособность всех кастомных операторов, находящихся в пакете `hrp_operators`.
Каждая группа тестов проверяет конкретные цепочки (источник -> приемник) с различными параметрами.
"""

import os
import sys
import pendulum
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task, task_group
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
# Предполагаем наличие ClickHouseOperator, если нет - используем Python
try:
    from airflow_clickhouse_plugin.operators.clickhouse_operator import ClickHouseOperator
except ImportError:
    ClickHouseOperator = None

# Добавляем путь к hrp_operators в sys.path
HRP_PATH = os.path.expanduser('~/etl-core-develop/app-dataplatform-etl')
if HRP_PATH not in sys.path:
    sys.path.append(HRP_PATH)

from sber_app_dataplatform_etl_core.hrp_operators import (
    HrpCheckS3FileHash,
    HrpClickHouseClusterOperator,
    HrpClickhouseQueryToS3Operator,
    HrpClickhouseTableToS3Operator,
    HrpClickhouseToPostgresIncarnationOperator,
    HrpClickhouseToPostgresOperator,
    HrpClickNativeToS3ListOperator,
    HrpClickNativeToS3Operator,
    HrpPostgresDDL,
    HrpPostgresIncarnationInsertOperator,
    HrpPostgresToClickhouseOperator,
    HrpPostgresToPostgresOperator,
    HrpPostgresToS3Operator,
    HrpS3ArchiveOperator,
    HrpS3BucketViewerOperator,
    HrpS3FileReadOperator,
    HrpS3ListKeysOperator,
    HrpS3ToClickhouseTableOperator,
    HrpS3ToClickhouseTransformedOperator,
    HrpS3ToS3Operator,
)
# Импорт операторов, не вошедших в __init__.py
from sber_app_dataplatform_etl_core.hrp_operators.postgres_to_s3 import HrpPostgresToS3ListOperator
from sber_app_dataplatform_etl_core.hrp_operators.clickhouse_to_datacatalog import ClickHouseDQExportOperator

default_args = {
    'owner': 'DataLab (CI02420667)',
    'retries': 0,
}

with DAG(
    dag_id='test_hrp_operators',
    doc_md=__doc__,
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=['test', 'hrp_operators'],
    params={
        "postgres_conn_id": Param("postgres_default", type="string", title="Postgres Connection ID"),
        "clickhouse_conn_id": Param("clickhouse_default", type="string", title="ClickHouse Connection ID"),
        "s3_conn_id": Param("s3_default", type="string", title="S3 Connection ID"),
        "s3_bucket": Param("test-bucket", type="string", title="S3 Bucket"),
        "s3_prefix": Param("test_hrp/{{ run_id }}", type="string", title="S3 Prefix"),
        
        "test_pg_to_s3": Param(True, type="boolean", title="Test PG -> S3"),
        "test_s3_to_ch": Param(True, type="boolean", title="Test S3 -> CH"),
        "test_ch_to_s3": Param(True, type="boolean", title="Test CH -> S3"),
        "test_db_to_db": Param(True, type="boolean", title="Test DB <-> DB"),
        "test_s3_utils": Param(True, type="boolean", title="Test S3 Utils"),
        "test_db_utils": Param(True, type="boolean", title="Test DB Utils"),
    }
) as dag:

    # ---------------------------------------------------------------------------
    # 1. SETUP: Создание таблиц и данных
    # ---------------------------------------------------------------------------

    setup_pg = PostgresOperator(
        task_id='setup_pg',
        postgres_conn_id="{{ params.postgres_conn_id }}",
        sql="""
        CREATE TABLE IF NOT EXISTS test_hrp_source_pg (id INT, name TEXT, dt DATE);
        CREATE TABLE IF NOT EXISTS test_hrp_target_pg (id INT, name TEXT, dt DATE);
        CREATE TABLE IF NOT EXISTS test_hrp_inc_pg (id INT, name TEXT, dt DATE, incarnation_id INT);
        
        TRUNCATE TABLE test_hrp_source_pg;
        INSERT INTO test_hrp_source_pg VALUES (1, 'Alice', '2026-05-21'), (2, 'Bob', '2026-05-22');
        
        TRUNCATE TABLE test_hrp_target_pg;
        TRUNCATE TABLE test_hrp_inc_pg;
        """
    )

    if ClickHouseOperator:
        setup_ch = ClickHouseOperator(
            task_id='setup_ch',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            sql="""
            CREATE TABLE IF NOT EXISTS test_hrp_source_ch (id Int32, name String, dt Date) ENGINE = MergeTree() ORDER BY id;
            CREATE TABLE IF NOT EXISTS test_hrp_target_ch (id Int32, name String, dt Date) ENGINE = MergeTree() ORDER BY id;
            CREATE TABLE IF NOT EXISTS test_hrp_inc_ch (id Int32, name String, dt Date, incarnation_id Int32) ENGINE = MergeTree() ORDER BY id;
            
            TRUNCATE TABLE test_hrp_source_ch;
            INSERT INTO test_hrp_source_ch VALUES (10, 'CH-Alice', '2026-05-21'), (20, 'CH-Bob', '2026-05-22');
            
            TRUNCATE TABLE test_hrp_target_ch;
            TRUNCATE TABLE test_hrp_inc_ch;
            """
        )
    else:
        @task
        def setup_ch_python(**context):
            # Заглушка, если нет ClickHouseOperator
            pass
        setup_ch = setup_ch_python()

    # ---------------------------------------------------------------------------
    # 2. TASK GROUPS
    # ---------------------------------------------------------------------------

    @task_group(group_id='pg_to_s3')
    def pg_to_s3_group():
        check = ShortCircuitOperator(
            task_id='check_flag',
            python_callable=lambda p: p['test_pg_to_s3'],
            op_args=[dag.params]
        )

        test_pg_s3_csv = HrpPostgresToS3Operator(
            task_id='test_pg_s3_csv',
            postgres_conn_id="{{ params.postgres_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key="{{ params.s3_prefix }}/pg_source.csv",
            sql="SELECT * FROM test_hrp_source_pg",
            file_format='csv'
        )

        test_pg_s3_list = HrpPostgresToS3ListOperator(
            task_id='test_pg_s3_list',
            postgres_conn_id="{{ params.postgres_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_prefix="{{ params.s3_prefix }}/list/",
            tables=['test_hrp_source_pg']
        )
        
        check >> [test_pg_s3_csv, test_pg_s3_list]

    @task_group(group_id='s3_to_ch')
    def s3_to_ch_group():
        check = ShortCircuitOperator(
            task_id='check_flag',
            python_callable=lambda p: p['test_s3_to_ch'],
            op_args=[dag.params]
        )

        test_s3_ch = HrpS3ToClickhouseTableOperator(
            task_id='test_s3_ch',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key="{{ params.s3_prefix }}/pg_source.csv",
            table="test_hrp_target_ch"
        )
        
        # Зависимость от выгрузки в S3 (для примера цепочки)
        check >> test_s3_ch

    @task_group(group_id='ch_to_s3')
    def ch_to_s3_group():
        check = ShortCircuitOperator(
            task_id='check_flag',
            python_callable=lambda p: p['test_ch_to_s3'],
            op_args=[dag.params]
        )

        test_ch_s3_table = HrpClickhouseTableToS3Operator(
            task_id='test_ch_s3_table',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key="{{ params.s3_prefix }}/ch_table.csv",
            table="test_hrp_source_ch"
        )

        test_ch_s3_native = HrpClickNativeToS3Operator(
            task_id='test_ch_s3_native',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            aws_conn_id="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key="{{ params.s3_prefix }}/ch_native.csv",
            sql="SELECT * FROM test_hrp_source_ch"
        )
        
        check >> [test_ch_s3_table, test_ch_s3_native]

    @task_group(group_id='db_to_db')
    def db_to_db_group():
        check = ShortCircuitOperator(
            task_id='check_flag',
            python_callable=lambda p: p['test_db_to_db'],
            op_args=[dag.params]
        )

        test_pg_pg = HrpPostgresToPostgresOperator(
            task_id='test_pg_pg',
            source_postgres_conn_id="{{ params.postgres_conn_id }}",
            target_postgres_conn_id="{{ params.postgres_conn_id }}",
            sql="SELECT * FROM test_hrp_source_pg",
            target_table="test_hrp_target_pg",
            pre_sql="TRUNCATE TABLE test_hrp_target_pg"
        )

        test_ch_pg = HrpClickhouseToPostgresOperator(
            task_id='test_ch_pg',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            postgres_conn_id="{{ params.postgres_conn_id }}",
            sql="SELECT * FROM test_hrp_source_ch",
            target_table="test_hrp_target_pg"
        )
        
        test_pg_ch = HrpPostgresToClickhouseOperator(
            task_id='test_pg_ch',
            postgres_conn_id="{{ params.postgres_conn_id }}",
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            sql="SELECT * FROM test_hrp_source_pg",
            target_table="test_hrp_target_ch"
        )

        test_ch_pg_inc = HrpClickhouseToPostgresIncarnationOperator(
            task_id='test_ch_pg_inc',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            postgres_conn_id="{{ params.postgres_conn_id }}",
            sql="SELECT id, name, dt, 1 as incarnation_id FROM test_hrp_source_ch",
            target_table="test_hrp_inc_pg"
        )

        test_pg_inc = HrpPostgresIncarnationInsertOperator(
            task_id='test_pg_inc',
            postgres_conn_id="{{ params.postgres_conn_id }}",
            sql="SELECT id, name, dt, 2 as incarnation_id FROM test_hrp_source_pg",
            target_table="test_hrp_inc_pg"
        )

        check >> [test_pg_pg, test_ch_pg, test_pg_ch, test_ch_pg_inc, test_pg_inc]

    @task_group(group_id='s3_utils')
    def s3_utils_group():
        check = ShortCircuitOperator(
            task_id='check_flag',
            python_callable=lambda p: p['test_s3_utils'],
            op_args=[dag.params]
        )

        test_s3_to_s3 = HrpS3ToS3Operator(
            task_id='test_s3_to_s3',
            aws_conn_id="{{ params.s3_conn_id }}",
            source_bucket="{{ params.s3_bucket }}",
            source_key="{{ params.s3_prefix }}/ch_table.csv",
            dest_bucket="{{ params.s3_bucket }}",
            dest_key="{{ params.s3_prefix }}/ch_table_copy.csv"
        )

        test_s3_archive = HrpS3ArchiveOperator(
            task_id='test_s3_archive',
            aws_conn_id="{{ params.s3_conn_id }}",
            bucket="{{ params.s3_bucket }}",
            key="{{ params.s3_prefix }}/ch_native.csv",
            archive_bucket="{{ params.s3_bucket }}",
            archive_key="{{ params.s3_prefix }}/archive/ch_native.csv"
        )

        test_s3_hash = HrpCheckS3FileHash(
            task_id='test_s3_hash',
            aws_conn_id="{{ params.s3_conn_id }}",
            bucket="{{ params.s3_bucket }}",
            key="{{ params.s3_prefix }}/pg_source.csv"
        )

        test_s3_list = HrpS3ListKeysOperator(
            task_id='test_s3_list',
            aws_conn_id="{{ params.s3_conn_id }}",
            bucket="{{ params.s3_bucket }}",
            prefix="{{ params.s3_prefix }}/"
        )

        test_s3_read = HrpS3FileReadOperator(
            task_id='test_s3_read',
            aws_conn_id="{{ params.s3_conn_id }}",
            s3_bucket="{{ params.s3_bucket }}",
            s3_key="{{ params.s3_prefix }}/pg_source.csv"
        )

        test_s3_view = HrpS3BucketViewerOperator(
            task_id='test_s3_view',
            aws_conn_id="{{ params.s3_conn_id }}",
            bucket="{{ params.s3_bucket }}"
        )

        check >> [test_s3_to_s3, test_s3_archive, test_s3_hash, test_s3_list, test_s3_read, test_s3_view]

    @task_group(group_id='db_utils')
    def db_utils_group():
        check = ShortCircuitOperator(
            task_id='check_flag',
            python_callable=lambda p: p['test_db_utils'],
            op_args=[dag.params]
        )

        test_ch_cluster = HrpClickHouseClusterOperator(
            task_id='test_ch_cluster',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            sql="SELECT 1",
            cluster="default_cluster"
        )

        test_pg_ddl = HrpPostgresDDL(
            task_id='test_pg_ddl',
            postgres_conn_id="{{ params.postgres_conn_id }}",
            sql="CREATE TEMPORARY TABLE temp_test (id INT)"
        )

        test_ch_dq = ClickHouseDQExportOperator(
            task_id='test_ch_dq',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            sql="SELECT count(*) FROM test_hrp_source_ch",
            target_table="test_hrp_dq_metrics"
        )

        check >> [test_ch_cluster, test_pg_ddl, test_ch_dq]

    # ---------------------------------------------------------------------------
    # 3. CLEANUP
    # ---------------------------------------------------------------------------

    cleanup_pg = PostgresOperator(
        task_id='cleanup_pg',
        postgres_conn_id="{{ params.postgres_conn_id }}",
        sql="""
        DROP TABLE IF EXISTS test_hrp_source_pg;
        DROP TABLE IF EXISTS test_hrp_target_pg;
        DROP TABLE IF EXISTS test_hrp_inc_pg;
        """,
        trigger_rule='all_done'
    )

    if ClickHouseOperator:
        cleanup_ch = ClickHouseOperator(
            task_id='cleanup_ch',
            clickhouse_conn_id="{{ params.clickhouse_conn_id }}",
            sql="""
            DROP TABLE IF EXISTS test_hrp_source_ch;
            DROP TABLE IF EXISTS test_hrp_target_ch;
            DROP TABLE IF EXISTS test_hrp_inc_ch;
            DROP TABLE IF EXISTS test_hrp_dq_metrics;
            """,
            trigger_rule='all_done'
        )
    else:
        @task(trigger_rule='all_done')
        def cleanup_ch_python(): pass
        cleanup_ch = cleanup_ch_python()

    # ---------------------------------------------------------------------------
    # Зависимости
    # ---------------------------------------------------------------------------

    [setup_pg, setup_ch] >> pg_to_s3_group() >> s3_to_ch_group() >> ch_to_s3_group() >> db_to_db_group() >> s3_utils_group() >> db_utils_group() >> [cleanup_pg, cleanup_ch]
