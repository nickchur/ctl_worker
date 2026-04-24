"""### ⏱️ DAG: Управление TTL правилами S3

Просматривает, устанавливает или удаляет правила жизненного цикла (TTL) объектов в S3-бакете.

| Параметр | Описание |
|---|---|
| `conn` | `conn_id/bucket` (из доступных S3-соединений) |
| `prefix` | Префикс объектов для правила TTL |
| `days` | Срок жизни объектов в днях (default: `30`) |
| `drop` | Удалить все правила TTL *(default: `False`)* |
"""

import pendulum
from airflow.models import Param
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException

from plugins.utils import get_conns_by_type, add_note, on_callback
from plugins.s3_utils import s3_get_ttl, s3_set_ttl, s3_del_ttl, s3_get_buckets

from logging import getLogger
logger = getLogger("airflow.task")


s3_list = [
    f"{conn}/{bucket}"
    for conn in get_conns_by_type(conn_type='aws')
    for bucket in s3_get_buckets(conn)
]


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
        'retry_delay': pendulum.duration(seconds=30),
    },
    start_date=pendulum.datetime(2026, 1, 22, tz=pendulum.UTC),
    schedule_interval=None,
    tags=['EDP_ETL', 'tools', 's3'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
    params={
        'conn': Param('s3', type='string', enum=s3_list, title='conn_id/bucket'),
        'prefix': Param('', type=['string', 'null']),
        'days': Param(30, type='integer', minimum=0, maximum=1000, title='TTL days'),
        'drop': Param(False, type='boolean', title='Drop all rules'),
    },
)
def tools_s3_set_ttl():

    @task
    def set_ttl(**context):
        p = context['params']

        drop = p.get('drop') or False
        conn_id, bucket = (p.get('conn') or 's3/').strip().split('/', 1)
        prefix = (p.get('prefix') or '').strip()
        days = int(p.get('days') or 30)

        try:
            hook = S3Hook(aws_conn_id=conn_id)
        except Exception as e:
            msg = f"S3 connection `{conn_id}` not found: {e}"
            add_note(msg, context, level='TASK,DAG', add=False)
            raise AirflowFailException(msg)

        if not bucket:
            msg = "Bucket not specified"
            add_note(msg, context, level='TASK,DAG', add=False)
            raise AirflowFailException(msg)
        if not hook.check_for_bucket(bucket):
            msg = f"Bucket `{bucket}` not found or access denied on `{conn_id}`"
            add_note(msg, context, level='TASK,DAG', add=False)
            raise AirflowFailException(msg)

        old_ttl = s3_get_ttl(conn_id, bucket)
        add_note(old_ttl, context, level='TASK,DAG', title=f'Old TTL {conn_id}/{bucket}')

        if drop:
            response = s3_del_ttl(conn_id, bucket)
        else:
            response = s3_set_ttl(conn_id, bucket, days=days, prefix=prefix)
        add_note(response, context, level='TASK,DAG')

        new_ttl = s3_get_ttl(conn_id, bucket)
        add_note(new_ttl, context, level='TASK,DAG', title=f'New TTL {conn_id}/{bucket}')

    set_ttl()


tools_s3_set_ttl()
