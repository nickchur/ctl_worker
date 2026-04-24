"""### 📦 DAG: Копирование между S3-бакетами

Копирует объект из одного S3-бакета в другой с опциональным сжатием.

| Параметр | Описание |
|---|---|
| `src_conn_id` | ID подключения источника |
| `src_bucket` / `src_key` | Исходный бакет и ключ |
| `dst_conn_id` | ID подключения приёмника |
| `dst_bucket` / `dst_key` | Целевой бакет и ключ |
| `dst_compression` | Сжатие: `` \| `gz` \| `zip` \| `tar.gz` |
| `replace` | Перезаписать если существует *(default: `False`)* |

> Если `dst_key` заканчивается на `/` или пуст — имя файла берётся из `src_key`.
> Если расширение `src_key` совпадает с `dst_compression` — повторное сжатие не применяется.
"""

import pendulum
from airflow.models import Param, Connection
from airflow.configuration import get_custom_secret_backend
from airflow.decorators import dag

from hrp_operators import HrpS3ToS3Operator  # type: ignore
from plugins.utils import add_note

from logging import getLogger
logger = getLogger("airflow.task")


def get_conns_by_type(conn_type='aws'):
    try:
        backend = get_custom_secret_backend()
        if not hasattr(backend, '_local_connections'):
            return []
        local_conn: dict[str, Connection] = backend._local_connections
        if conn_type:
            return [id for id, conn in local_conn.items() if conn.conn_type == conn_type.lower()]
        return [id for id, _ in local_conn.items()]
    except Exception as e:
        logger.warning(f"Cannot access connections: {e}")
        return []


s3_conns = get_conns_by_type(conn_type='aws')


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 2,
        'retry_delay': pendulum.duration(seconds=30),
    },
    start_date=pendulum.datetime(2026, 1, 22, tz=pendulum.UTC),
    schedule_interval=None,
    tags=['tools', 's3'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    render_template_as_native_obj=True,
    params={
        'src_conn_id': Param('s3', type='string', examples=s3_conns),
        'src_bucket': Param('', type='string'),
        'src_key': Param('', type='string'),
        'replace': Param(False, type='boolean'),
        'dst_conn_id': Param('s3', type='string', examples=s3_conns),
        'dst_bucket': Param('', type='string'),
        'dst_key': Param('', type=['string', 'null']),
        'dst_compression': Param(
            '',
            type=['string', 'null'],
            enum=['', 'gz', 'zip', 'tar.gz', None],
            description="Формат сжатия: '' — без сжатия, gz — gzip, zip — ZIP, tar.gz — TAR+GZIP",
        ),
    },
)
def tools_s3_to_s3():

    def s3_params(context):
        from pathlib import Path
        p = context.get('params', {})
        obj = context['task_instance'].task

        valid = ['gz', 'zip', 'tar.gz']
        src = p.get('src_compression') if p.get('src_compression') in valid else None
        src = src or (p['src_key'].split('.')[-1] if '.' in p['src_key'] else None)
        src = src if src in valid else None

        dst = p.get('dst_compression') if p.get('dst_compression') in valid else None
        if src == dst:
            src = dst = None

        dst_key = p.get('dst_key', '').strip().lstrip('/') or ''
        dst_key = dst_key + (Path(p['src_key']).name if dst_key.endswith('/') or dst_key == '' else '')
        dst_key += ('.' + dst) if dst else ''

        add_note(dict({'dst_key': dst_key, 'src_compression': src, 'dst_compression': dst}), context)

        obj.compression_source = 'gzip' if src == 'gz' else src
        obj.replace = p.get('replace', False)
        obj.s3_key = dst_key
        obj.compression = 'gzip' if dst == 'gz' else dst
        obj.s3_key_source = obj.s3_key_source.strip().lstrip('/')
        obj.s3_bucket_source = obj.s3_bucket_source.strip().strip('/')
        obj.s3_bucket = obj.s3_bucket.strip().strip('/')

    HrpS3ToS3Operator(
        task_id='s3_to_s3',
        post_file_check=True,
        pre_execute=s3_params,
        aws_conn_id_source='{{ params.src_conn_id }}',
        s3_bucket_source='{{ params.src_bucket }}',
        s3_key_source='{{ params.src_key }}',
        aws_conn_id='{{ params.dst_conn_id }}',
        s3_bucket='{{ params.dst_bucket }}',
    )


tools_s3_to_s3()
