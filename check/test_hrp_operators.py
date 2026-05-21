"""
### 🧪 DAG для тестирования кастомных операторов (HRP Operators)

Этот DAG проверяет работоспособность всех кастомных операторов, находящихся в пакете `hrp_operators`.
Параметры и проверки реализованы через `pre_execute` функции.
"""

import os
import sys
import pendulum
import re
from datetime import timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowSkipException
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

# ---------------------------------------------------------------------------
# PRE-EXECUTE FUNCTIONS
# ---------------------------------------------------------------------------

def _parse_s3_url(url):
    if not url or '://' not in url:
        return None, None, None
    conn_id, rest = url.split('://', 1)
    if '/' in rest:
        bucket, prefix = rest.split('/', 1)
    else:
        bucket, prefix = rest, ""
    return conn_id, bucket, prefix

def s3_pre_execute(context):
    """Общий pre_execute для S3-операторов."""
    params = context['params']
    ti = context['task_instance']
    task_obj = ti.task
    
    # 1. Проверка флага группы
    group_id = ti.task_id.split('.')[0] if '.' in ti.task_id else ""
    flag_name = f"test_{group_id}"
    if not params.get(flag_name, True):
        raise AirflowSkipException(f"Group {group_id} is disabled via {flag_name}")

    # 2. Проверка s3_url
    s3_url = params.get('s3_url')
    if not s3_url:
        raise AirflowSkipException("s3_url is empty")
    
    conn, bucket, prefix = _parse_s3_url(s3_url)
    
    # 3. Динамическая подстановка параметров в объект оператора
    # Разные операторы имеют разные имена атрибутов для S3
    if hasattr(task_obj, 'aws_conn_id'): task_obj.aws_conn_id = conn
    if hasattr(task_obj, 'aws_conn_id_source'): task_obj.aws_conn_id_source = conn
    
    if hasattr(task_obj, 's3_bucket'): task_obj.s3_bucket = bucket
    if hasattr(task_obj, 's3_bucket_source'): task_obj.s3_bucket_source = bucket
    if hasattr(task_obj, 'bucket'): task_obj.bucket = bucket # для некоторых утилит
    
    # Формирование ключей с учетом префикса
    def join_prefix(key):
        if not prefix: return key
        return f"{prefix.rstrip('/')}/{key.lstrip('/')}"

    if hasattr(task_obj, 's3_key'): task_obj.s3_key = join_prefix(task_obj.s3_key)
    if hasattr(task_obj, 's3_key_source'): task_obj.s3_key_source = join_prefix(task_obj.s3_key_source)
    if hasattr(task_obj, 's3_prefix'): task_obj.s3_prefix = join_prefix(task_obj.s3_prefix)
    if hasattr(task_obj, 'prefix'): task_obj.prefix = join_prefix(task_obj.prefix)
    if hasattr(task_obj, 'key'): task_obj.key = join_prefix(task_obj.key)
    if hasattr(task_obj, 'archive_key'): task_obj.archive_key = join_prefix(task_obj.archive_key)
    if hasattr(task_obj, 'dest_key'): task_obj.dest_key = join_prefix(task_obj.dest_key)
    if hasattr(task_obj, 'source_key'): task_obj.source_key = join_prefix(task_obj.source_key)

def pg_ch_pre_execute(context):
    """Pre-execute для БД операторов (проверка коннектов и флагов)."""
    params = context['params']
    ti = context['task_instance']
    
    # 1. Проверка флага группы
    group_id = ti.task_id.split('.')[0] if '.' in ti.task_id else ""
    flag_name = f"test_{group_id}"
    if not params.get(flag_name, True):
        raise AirflowSkipException(f"Group {group_id} is disabled")

    # 2. Проверка PG/CH коннектов
    pg_id = params.get('pg_conn_id')
    ch_id = params.get('ch_conn_id')
    
    # Определяем потребности таска по ID или типу
    needs_pg = any(x in ti.task_id for x in ['pg', 'setup_pg', 'cleanup_pg'])
    needs_ch = any(x in ti.task_id for x in ['ch', 'setup_ch', 'cleanup_ch'])
    
    if needs_pg and not pg_id:
        raise AirflowSkipException("pg_conn_id is empty")
    if needs_ch and not ch_id:
        raise AirflowSkipException("ch_conn_id is empty")

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

    # ---------------------------------------------------------------------------
    # 1. SETUP
    # ---------------------------------------------------------------------------

    setup_pg = PostgresOperator(
        task_id='setup_pg',
        postgres_conn_id="{{ params.pg_conn_id }}",
        pre_execute=pg_ch_pre_execute,
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
            pre_execute=pg_ch_pre_execute,
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
        @task(pre_execute=pg_ch_pre_execute)
        def setup_ch_python(**context): pass
        setup_ch = setup_ch_python()

    # ---------------------------------------------------------------------------
    # 2. TASK GROUPS
    # ---------------------------------------------------------------------------

    @task_group(group_id='pg_to_s3')
    def pg_to_s3_group():
        # Смешанная группа (PG и S3), используем s3_pre_execute, 
        # так как она покроет и флаг группы, и s3_url, и pg_conn_id (если добавим)
        
        test_pg_s3_csv = HrpPostgresToS3Operator(
            task_id='test_pg_s3_csv',
            pre_execute=s3_pre_execute,
            postgres_conn_id="{{ params.pg_conn_id }}",
            aws_conn_id="", # заполнится в pre_execute
            s3_bucket="",   # заполнится в pre_execute
            s3_key="pg_source.csv", # дополнится префиксом
            sql="SELECT * FROM test_hrp_source_pg",
            file_format='csv'
        )

        test_pg_s3_list = HrpPostgresToS3ListOperator(
            task_id='test_pg_s3_list',
            pre_execute=s3_pre_execute,
            postgres_conn_id="{{ params.pg_conn_id }}",
            aws_conn_id="",
            s3_bucket="",
            s3_prefix="list/",
            tables=['test_hrp_source_pg']
        )

    @task_group(group_id='s3_to_ch')
    def s3_to_ch_group():
        test_s3_ch = HrpS3ToClickhouseTableOperator(
            task_id='test_s3_ch',
            pre_execute=s3_pre_execute,
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="",
            s3_bucket="",
            s3_key="pg_source.csv",
            table="test_hrp_target_ch"
        )

    @task_group(group_id='ch_to_s3')
    def ch_to_s3_group():
        test_ch_s3_table = HrpClickhouseTableToS3Operator(
            task_id='test_ch_s3_table',
            pre_execute=s3_pre_execute,
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="",
            s3_bucket="",
            s3_key="ch_table.csv",
            table="test_hrp_source_ch"
        )

        test_ch_s3_native = HrpClickNativeToS3Operator(
            task_id='test_ch_s3_native',
            pre_execute=s3_pre_execute,
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            aws_conn_id="",
            s3_bucket="",
            s3_key="ch_native.csv",
            sql="SELECT * FROM test_hrp_source_ch"
        )

    @task_group(group_id='db_to_db')
    def db_to_db_group():
        common_args = {'pre_execute': pg_ch_pre_execute}
        
        test_pg_pg = HrpPostgresToPostgresOperator(
            task_id='test_pg_pg',
            source_postgres_conn_id="{{ params.pg_conn_id }}",
            target_postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT * FROM test_hrp_source_pg",
            target_table="test_hrp_target_pg",
            pre_sql="TRUNCATE TABLE test_hrp_target_pg",
            **common_args
        )

        test_ch_pg = HrpClickhouseToPostgresOperator(
            task_id='test_ch_pg',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT * FROM test_hrp_source_ch",
            target_table="test_hrp_target_pg",
            **common_args
        )
        
        test_pg_ch = HrpPostgresToClickhouseOperator(
            task_id='test_pg_ch',
            postgres_conn_id="{{ params.pg_conn_id }}",
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            sql="SELECT * FROM test_hrp_source_pg",
            target_table="test_hrp_target_ch",
            **common_args
        )

        test_ch_pg_inc = HrpClickhouseToPostgresIncarnationOperator(
            task_id='test_ch_pg_inc',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT id, name, dt, 1 as incarnation_id FROM test_hrp_source_ch",
            target_table="test_hrp_inc_pg",
            **common_args
        )

        test_pg_inc = HrpPostgresIncarnationInsertOperator(
            task_id='test_pg_inc',
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="SELECT id, name, dt, 2 as incarnation_id FROM test_hrp_source_pg",
            target_table="test_hrp_inc_pg",
            **common_args
        )

    @task_group(group_id='s3_utils')
    def s3_utils_group():
        common_args = {'pre_execute': s3_pre_execute, 'aws_conn_id': ""}

        test_s3_to_s3 = HrpS3ToS3Operator(
            task_id='test_s3_to_s3',
            aws_conn_id_source="",
            source_bucket="",
            source_key="ch_table.csv",
            dest_bucket="",
            dest_key="ch_table_copy.csv",
            **common_args
        )

        test_s3_archive = HrpS3ArchiveOperator(
            task_id='test_s3_archive',
            bucket="",
            key="ch_native.csv",
            archive_bucket="",
            archive_key="archive/ch_native.csv",
            **common_args
        )

        test_s3_hash = HrpCheckS3FileHash(
            task_id='test_s3_hash',
            bucket="",
            key="pg_source.csv",
            **common_args
        )

        test_s3_list = HrpS3ListKeysOperator(
            task_id='test_s3_list',
            bucket="",
            prefix="",
            **common_args
        )

        test_s3_read = HrpS3FileReadOperator(
            task_id='test_s3_read',
            s3_bucket="",
            s3_key="pg_source.csv",
            **common_args
        )

        test_s3_view = HrpS3BucketViewerOperator(
            task_id='test_s3_view',
            bucket="",
            **common_args
        )

    @task_group(group_id='db_utils')
    def db_utils_group():
        common_args = {'pre_execute': pg_ch_pre_execute}

        test_ch_cluster = HrpClickHouseClusterOperator(
            task_id='test_ch_cluster',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            sql="SELECT 1",
            cluster="default_cluster",
            **common_args
        )

        test_pg_ddl = HrpPostgresDDL(
            task_id='test_pg_ddl',
            postgres_conn_id="{{ params.pg_conn_id }}",
            sql="CREATE TEMPORARY TABLE temp_test (id INT)",
            **common_args
        )

        test_ch_dq = ClickHouseDQExportOperator(
            task_id='test_ch_dq',
            clickhouse_conn_id="{{ params.ch_conn_id }}",
            sql="SELECT count(*) FROM test_hrp_source_ch",
            target_table="test_hrp_dq_metrics",
            **common_args
        )

    # ---------------------------------------------------------------------------
    # 3. CLEANUP
    # ---------------------------------------------------------------------------

    cleanup_pg = PostgresOperator(
        task_id='cleanup_pg',
        postgres_conn_id="{{ params.pg_conn_id }}",
        pre_execute=pg_ch_pre_execute,
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
            pre_execute=pg_ch_pre_execute,
            sql="""
            DROP TABLE IF EXISTS test_hrp_source_ch;
            DROP TABLE IF EXISTS test_hrp_target_ch;
            DROP TABLE IF EXISTS test_hrp_inc_ch;
            DROP TABLE IF EXISTS test_hrp_dq_metrics;
            """,
            trigger_rule='all_done'
        )
    else:
        @task(trigger_rule='all_done', pre_execute=pg_ch_pre_execute)
        def cleanup_ch_python(): pass
        cleanup_ch = cleanup_ch_python()

    # ---------------------------------------------------------------------------
    # Зависимости
    # ---------------------------------------------------------------------------

    [setup_pg, setup_ch] >> pg_to_s3_group() >> s3_to_ch_group() >> ch_to_s3_group() >> db_to_db_group() >> s3_utils_group() >> db_utils_group() >> [cleanup_pg, cleanup_ch]
