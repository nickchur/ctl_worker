"""### DAG: Список бакетов S3

Перечисляет все бакеты по всем S3-подключениям с размером, количеством объектов и TTL.
"""

import pendulum
from airflow.decorators import task, dag

from plugins.utils import get_conns_by_type, add_note, on_callback, readable_size
from plugins.s3_utils import s3_bucket_size, s3_get_ttl

from logging import getLogger
logger = getLogger("airflow.task")


@dag(
    doc_md=__doc__,
    owner_links={
        'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab',
        'Korchagin Viacheslav': 'mailto:VYurKorchagin@sberbank.ru',
    },
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
        'retry_delay': pendulum.duration(seconds=30),
        'on_failure_callback': on_callback,
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
)
def tools_s3_bucket_list():

    @task
    def chk_s3_conn(**context):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from pprint import pformat
        import json

        conn_ids = get_conns_by_type(conn_type='aws')
        connections_map = {}
        total_size = total_objs = total_buckets = 0

        for cid in conn_ids:
            try:
                hook = S3Hook(aws_conn_id=cid, verify=False)
                response = hook.get_conn().list_buckets()
            except Exception as e:
                logger.warning(f"Cannot list buckets for '{cid}': {e}")
                connections_map[cid] = {'error': str(e)}
                continue

            buckets = {}
            conn_size = conn_objs = 0
            for bucket in response.get('Buckets', []):
                bkt = {'created': str(bucket['CreationDate'])[:10]}
                try:
                    s = s3_bucket_size(cid, bucket['Name'])
                    conn_size += s['size']
                    conn_objs += s['objs']
                    bkt['size'] = readable_size(s['size'])
                    bkt['objs'] = readable_size(s['objs'], 1000)
                    bkt['min_date'] = s['min_date']
                    bkt['max_date'] = s['max_date']
                except Exception as e:
                    logger.warning(f"Cannot get size for '{cid}/{bucket['Name']}': {e}")
                try:
                    bkt['ttl'] = s3_get_ttl(cid, bucket['Name'])
                except Exception as e:
                    logger.warning(f"Cannot get TTL for '{cid}/{bucket['Name']}': {e}")

                buckets[bucket['Name']] = bkt

            total_buckets += len(buckets)
            total_size += conn_size
            total_objs += conn_objs
            buckets['_total'] = {'size': readable_size(conn_size), 'objs': readable_size(conn_objs, 1000)}
            connections_map[cid] = buckets
            logger.info(f"'{cid}': {len(buckets) - 1} buckets, {readable_size(conn_size)}")

        logger.info(f"Final map:\n{pformat(connections_map, indent=4)}")

        add_note(
            f"{len(conn_ids)} connections  |  {total_buckets} buckets  |  "
            f"{readable_size(total_objs, 1000)} objects  |  {readable_size(total_size)}",
            context,
            level='TASK,DAG',
            title='S3 Summary',
        )
        return json.dumps(connections_map, default=str, indent=4)

    chk_s3_conn()


tools_s3_bucket_list()
