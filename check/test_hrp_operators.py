"""
### 🧪 DAG для тестирования кастомных операторов (HRP Operators)

Этот DAG проверяет работоспособность всех кастомных операторов, находящихся в пакете `hrp_operators`.
Каждая группа тестов проверяет конкретные цепочки (источник -> приемник) с различными параметрами.
"""

import os
import sys
import pendulum
import re
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

def parse_s3_url(url):
    """Парсит URL формата conn_id://bucket/prefix."""
    match = re.match(r"([^:]+)://([^/]+)/?(.*)", url)
    if not match:
        return "s3_default", "test-bucket", ""
    conn_id, bucket, prefix = match.groups()
    return conn_id, bucket, prefix

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
        "pg_conn_id": Param("", type=["string", "null"], title="Postgres Connection ID"),
        "ch_conn_id": Param("", type=["string", "null"], title="ClickHouse Connection ID"),
        "s3_url": Param("s3://test-bucket", type=["string", "null"], title="S3 URL (conn_id://bucket/prefix)"),
        
        "test_pg_to_s3": Param(True, type="boolean", title="Test PG -> S3"),
        "test_s3_to_ch": Param(True, type="boolean", title="Test S3 -> CH"),
        "test_ch_to_s3": Param(True, type="boolean", title="Test CH -> S3"),
        "test_db_to_db": Param(True, type="boolean", title="Test DB <-> DB"),
        "test_s3_utils": Param(True, type="boolean", title="Test S3 Utils"),
        "test_db_utils": Param(True, type="boolean", title="Test DB Utils"),
    }
) as dag:

    # Извлекаем компоненты S3 URL для использования в операторах
    # В Airflow операторах мы можем использовать макросы для парсинга
    s3_conn_macro = "{{ params.s3_url.split('://')[0] if '://' in params.s3_url else '' }}"
    s3_bucket_macro = "{{ params.s3_url.split('://')[1].split('/')[0] if '://' in params.s3_url else '' }}"
    # Префикс может содержать слеши, поэтому объединяем остаток
    s3_prefix_macro = "{{ '/'.join(params.s3_url.split('://')[1].split('/')[1:]) if '://' in params.s3_url else '' }}"

    # ---------------------------------------------------------------------------
    # 0. PRE-EXECUTE CHECKS (Conditional Skipping)
    # ---------------------------------------------------------------------------

    @task
    def has_pg_conn(pg_id):
        return bool(pg_id)

    @task
    def has_ch_conn(ch_id):
        return bool(ch_id)

    # ---------------------------------------------------------------------------
    # 1. SETUP: Создание таблиц и данных
    # ---------------------------------------------------------------------------

    setup_pg = PostgresOperator(
        task_id='setup_pg',
        postgres_conn_id="{{ params.pg_conn_id }}",
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
            clickhouse_conn_id="{{ params.ch_conn_id }}",
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
        def setup_ch_python(**context): pass
        setup_ch = setup_ch_python()

    # ---------------------------------------------------------------------------
    # 2. TASK GROUPS
    # ---------------------------------------------------------------------------

    @task_group(group_id='pg_to_s3')
    def pg_to_s3_group():
        check = ShortCircuitOperator(
            task_id='pre_execute',
            python_callable=lambda p: p['test_pg_to_s3'] and p['pg_conn_id'] and p['s3_url'],
            op_args=[dag.params]
        )

        test_pg_s3_csv = HrpPostgresToS3Operator(
            task_id='test_pg_s3_csv',
            postgres_conn_id="{{ params.pg_conn_id }}",
            aws_conn_id=s3_conn_macro,
            s3_bucket=s3_bucket_macro,
            s3_key=s3_prefix_macro + ("/pg_source.csv" if s3_prefix_macro else "pg_source.csv"),
            sql="SELECT * FROM test_hrp_source_pg",
            file_format='csv'
        )

        test_pg_s3_list = HrpPostgresToS3ListOperator(
            task_id='test_pg_s3_list',
            postgres_conn_id="{{ params.pg_conn_id }}",
            aws_conn_id=s3_conn_macro,
            s3_bucket=s3_bucket_macro,
            s3_prefix=s3_prefix_macro + ("/list/" if s3_prefix_macro else "list/"),
            tables=['test_hrp_source_pg']
        )
        
        check >> [test_pg_s3_csv, test_pg_s3_list]

    @task_group(group_id='s3_to_ch')
    def s3_to_ch_group():
        check = ShortCircuitOperator(
            task_id='pre_execute',
            python_callable=lambda p: p['test_s3_to_ch'] and p['ch_conn_id'] and p['s3_url'],
            op_args=[dag.params]
        )

        test_s3_ch = HrpS3ToClickhouseTableOperator(
            task_id='test_s3_ch',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id=s3_conn_macro,
            s3_bucket=s3_bucket_macro,
            s3_key=s3_prefix_macro + ("/pg_source.csv" if s3_prefix_macro else "pg_source.csv"),
            table="test_hrp_target_ch"
        )
        
        check >> test_s3_ch

    @task_group(group_id='ch_to_s3')
    def ch_to_s3_group():
        check = ShortCircuitOperator(
            task_id='pre_execute',
            python_callable=lambda p: p['test_ch_to_s3'] and p['ch_conn_id'] and p['s3_url'],
            op_args=[dag.params]
        )

        test_ch_s3_table = HrpClickhouseTableToS3Operator(
            task_id='test_ch_s3_table',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id=s3_conn_macro,
            s3_bucket=s3_bucket_macro,
            s3_key=s3_prefix_macro + ("/ch_table.csv" if s3_prefix_macro else "ch_table.csv"),
            table="test_hrp_source_ch"
        )

        test_ch_s3_native = HrpClickNativeToS3Operator(
            task_id='test_ch_s3_native',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id=s3_conn_macro,
            s3_bucket=s3_bucket_macro,
            s3_key=s3_prefix_macro + ("/ch_native.csv" if s3_prefix_macro else "ch_native.csv"),
            sql="SELECT * FROM test_hrp_source_ch"
        )
        
        check >> [test_ch_s3_table, test_ch_s3_native]

    @task_group(group_id='db_to_db')
    def db_to_db_group():
        check = ShortCircuitOperator(
            task_id='pre_execute',
            python_callable=lambda p: p['test_db_to_db'] and p['pg_conn_id'] and p['ch_conn_id'],
            op_args=[dag.params]
        )

        test_pg_pg = HrpPostgresToPostgresOperator(
            task_id='test_pg_pg',
            source_postgres_conn_id="{{ params.pg_conn_id }}",
            target_postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT * FROM test_hrp_source_pg",
            target_table="test_hrp_target_pg",
            pre_sql="TRUNCATE TABLE test_hrp_target_pg"
        )

        test_ch_pg = HrpClickhouseToPostgresOperator(
            task_id='test_ch_pg',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT * FROM test_hrp_source_ch",
            target_table="test_hrp_target_pg"
        )
        
        test_pg_ch = HrpPostgresToClickhouseOperator(
            task_id='test_pg_ch',
            postgres_conn_id="{{ params.pg_conn_id }}",
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            sql="SELECT * FROM test_hrp_source_pg",
            target_table="test_hrp_target_ch"
        )

        test_ch_pg_inc = HrpClickhouseToPostgresIncarnationOperator(
            task_id='test_ch_pg_inc',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT id, name, dt, 1 as incarnation_id FROM test_hrp_source_ch",
            target_table="test_hrp_inc_pg"
        )

        test_pg_inc = HrpPostgresIncarnationInsertOperator(
            task_id='test_pg_inc',
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT id, name, dt, 2 as incarnation_id FROM test_hrp_source_pg",
            target_table="test_hrp_inc_pg"
        )

        check >> [test_pg_pg, test_ch_pg, test_pg_ch, test_ch_pg_inc, test_pg_inc]

    @task_group(group_id='s3_utils')
    def s3_utils_group():
        check = ShortCircuitOperator(
            task_id='pre_execute',
            python_callable=lambda p: p['test_s3_utils'] and p['s3_url'],
            op_args=[dag.params]
        )

        test_s3_to_s3 = HrpS3ToS3Operator(
            task_id='test_s3_to_s3',
            aws_conn_id=s3_conn_macro,
            source_bucket=s3_bucket_macro,
            source_key=s3_prefix_macro + ("/ch_table.csv" if s3_prefix_macro else "ch_table.csv"),
            dest_bucket=s3_bucket_macro,
            dest_key=s3_prefix_macro + ("/ch_table_copy.csv" if s3_prefix_macro else "ch_table_copy.csv")
        )

        test_s3_archive = HrpS3ArchiveOperator(
            task_id='test_s3_archive',
            aws_conn_id=s3_conn_macro,
            bucket=s3_bucket_macro,
            key=s3_prefix_macro + ("/ch_native.csv" if s3_prefix_macro else "ch_native.csv"),
            archive_bucket=s3_bucket_macro,
            archive_key=s3_prefix_macro + ("/archive/ch_native.csv" if s3_prefix_macro else "archive/ch_native.csv")
        )

        test_s3_hash = HrpCheckS3FileHash(
            task_id='test_s3_hash',
            aws_conn_id=s3_conn_macro,
            bucket=s3_bucket_macro,
            key=s3_prefix_macro + ("/pg_source.csv" if s3_prefix_macro else "pg_source.csv")
        )

        test_s3_list = HrpS3ListKeysOperator(
            task_id='test_s3_list',
            aws_conn_id=s3_conn_macro,
            bucket=s3_bucket_macro,
            prefix=s3_prefix_macro + ("/" if s3_prefix_macro else "")
        )

        test_s3_read = HrpS3FileReadOperator(
            task_id='test_s3_read',
            aws_conn_id=s3_conn_macro,
            s3_bucket=s3_bucket_macro,
            s3_key=s3_prefix_macro + ("/pg_source.csv" if s3_prefix_macro else "pg_source.csv")
        )

        test_s3_view = HrpS3BucketViewerOperator(
            task_id='test_s3_view',
            aws_conn_id=s3_conn_macro,
            bucket=s3_bucket_macro
        )

        check >> [test_s3_to_s3, test_s3_archive, test_s3_hash, test_s3_list, test_s3_read, test_s3_view]

    @task_group(group_id='db_utils')
    def db_utils_group():
        check = ShortCircuitOperator(
            task_id='pre_execute',
            python_callable=lambda p: p['test_db_utils'] and p['pg_conn_id'] and p['ch_conn_id'],
            op_args=[dag.params]
        )

        test_ch_cluster = HrpClickHouseClusterOperator(
            task_id='test_ch_cluster',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            sql="SELECT 1",
            cluster="default_cluster"
        )

        test_pg_ddl = HrpPostgresDDL(
            task_id='test_pg_ddl',
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="CREATE TEMPORARY TABLE temp_test (id INT)"
        )

        test_ch_dq = ClickHouseDQExportOperator(
            task_id='test_ch_dq',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            sql="SELECT count(*) FROM test_hrp_source_ch",
            target_table="test_hrp_dq_metrics"
        )

        check >> [test_ch_cluster, test_pg_ddl, test_ch_dq]

    # ---------------------------------------------------------------------------
    # 3. CLEANUP
    # ---------------------------------------------------------------------------

    cleanup_pg = PostgresOperator(
        task_id='cleanup_pg',
        postgres_conn_id="{{ params.pg_conn_id }}",
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
            clickhouse_conn_id="{{ params.ch_conn_id }}",
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

    can_pg = has_pg_conn("{{ params.pg_conn_id }}")
    can_ch = has_ch_conn("{{ params.ch_conn_id }}")

    can_pg >> setup_pg >> pg_to_s3_group()
    can_ch >> setup_ch >> ch_to_s3_group()
    
    [pg_to_s3_group(), ch_to_s3_group()] >> s3_to_ch_group() >> db_to_db_group() >> s3_utils_group() >> db_utils_group() >> [cleanup_pg, cleanup_ch]
    
    # Убеждаемся, что cleanup_pg зависит от can_pg, а cleanup_ch от can_ch
    can_pg >> cleanup_pg
    can_ch >> cleanup_ch
