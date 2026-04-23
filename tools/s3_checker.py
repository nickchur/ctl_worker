"""### DAG: Просмотр файлов S3

Получает список объектов по маске, сортирует и читает содержимое файлов (txt, gz, zip).

| Параметр | Описание |
|---|---|
| `aws_conn_id` | ID подключения к S3 |
| `prefix` | `bucket/prefix/mask` (поддерживает `*`, `?`) |
| `order_by` | Сортировка: `None` \| `Key` \| `Date` \| `Size` \| `Name` \| `Ext` |
| `reverse` | Обратный порядок сортировки *(default: `True`)* |
| `items` | Количество файлов для чтения (default: `10`) |
| `rows` | Количество строк превью на файл (default: `300`) |
| `page_size` | Размер страницы пагинации (default: `1000`) |
| `max_items` | Макс. кол-во объектов при сканировании (default: `10000`) |
"""

import pendulum
from airflow.models import Param
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException

from plugins.utils import get_conns_by_type, readable_size, add_note, on_callback

from logging import getLogger
logger = getLogger("airflow.task")


def _split_mask(full_path):
    """Разбивает путь на статический Prefix и Mask по первому спецсимволу (* или ?)."""
    clean = full_path.replace('s3://', '').strip('/')
    star, qmark = clean.find('*'), clean.find('?')
    wild = min([p for p in [star, qmark] if p != -1] or [len(clean)])
    if wild == len(clean):
        return clean, '*'
    last_slash = clean.rfind('/', 0, wild)
    if last_slash == -1:
        return '', clean
    return clean[:last_slash + 1], clean[last_slash + 1:]


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
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
    render_template_as_native_obj=True,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
    params={
        'aws_conn_id': Param('s3', type='string', enum=get_conns_by_type(conn_type='aws'), title='ID подключения'),
        'prefix': Param('dataplatform-monitoring/dataplatform-etl/*.log.gz', type=['string', 'null'], title='bucket/prefix/mask'),
        'order_by': Param('Date', type='string', enum=['None', 'Key', 'Date', 'Size', 'Name', 'Ext']),
        'reverse': Param(True, type='boolean'),
        'items': Param(10, type='integer', minimum=1, maximum=100),
        'rows': Param(300, type='integer', minimum=1, maximum=1000),
        'page_size': Param(1000, type='integer', minimum=1, maximum=1000),
        'max_items': Param(10000, type='integer', minimum=1, maximum=100000),
    },
)
def tools_s3_check_logs():

    @task
    def list_s3_keys(**context):
        from fnmatch import fnmatch

        p = context['params']
        conn_id = p.get('aws_conn_id', 's3')
        prefix = (p.get('prefix') or '').strip()
        max_items = int(p.get('max_items', 10000))
        page_size = int(p.get('page_size', 1000))
        order_by = p.get('order_by', 'None')
        reverse = p.get('reverse', True)

        prefix, mask = _split_mask(prefix)

        # если бакет не задан явно — берём первый сегмент из prefix
        parts = prefix.split('/')
        bucket = parts[0].strip()
        prefix = '/'.join(parts[1:]) if len(parts) > 1 else ''

        logger.info(f"Bucket: {bucket}, Prefix: {prefix}, Mask: {mask}")

        try:
            hook = S3Hook(aws_conn_id=conn_id)
            context['ti'].xcom_push(key='conn_id', value=conn_id)
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

        logger.info('::group:: list_objects_v2')
        paginator = hook.get_conn().get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={'MaxItems': max_items, 'PageSize': page_size},
        )
        files_list = []
        for page in pages:
            for obj in page.get('Contents', []):
                if obj['Key'].endswith('/'):
                    continue
                if fnmatch(obj['Key'].lower(), mask):
                    files_list.append({
                        'modified': obj['LastModified'].isoformat()[:19].replace('T', ' '),
                        'bucket': bucket,
                        'key': obj['Key'],
                        'size_bytes': obj['Size'],
                        'size_human': readable_size(obj['Size']),
                    })
                if len(files_list) >= max_items:
                    logger.warning(f"Reached scan limit of {max_items} files")
                    break
            if len(files_list) >= max_items:
                break
        logger.info('::endgroup::')

        if not files_list:
            msg = f"No files found in s3://{bucket}/{prefix} (mask: {mask})"
            add_note(msg, context, level='TASK,DAG')
            return []

        sort_key = {
            'Date': lambda x: x['modified'],
            'Size': lambda x: x['size_bytes'],
            'Key': lambda x: x['key'],
            'Name': lambda x: x['key'].split('/')[-1],
            'Ext': lambda x: x['key'].split('.')[-1],
        }.get(order_by)
        if sort_key:
            files_list.sort(key=sort_key, reverse=reverse)

        limit = int(p.get('items', 10))
        top_files = files_list[:limit]

        logger.info('::group:: Keys')
        for f in files_list[:100]:
            logger.info(f"  {f['modified']}  {f['size_human']:>10}  s3://{bucket}/{f['key']}")
        logger.info('::endgroup::')

        note = f"Found **{len(files_list)}** files — showing top {len(top_files)}\n\n"
        note += "| # | Key | Size | Modified |\n| ---: | :--- | ---: | :--- |\n"
        for k, f in enumerate(top_files):
            note += f"| {k} | s3://{bucket}/{f['key']} | {f['size_human']} | {f['modified']} |\n"
        add_note(note, context, level='TASK,DAG')

        return [f"s3://{bucket}/{f['key']}" for f in top_files]

    @task(trigger_rule='one_failed')
    def chk_s3_conn(**context):
        from pprint import pformat
        import json

        p = context['params']
        conn_id = context['ti'].xcom_pull(key='conn_id', task_ids='list_s3_keys') or p.get('aws_conn_id')
        conn_ids = get_conns_by_type(conn_type='aws')

        if conn_id not in conn_ids:
            msg = f"S3 connection `{conn_id}` not found. Available: {conn_ids}"
            add_note(msg, context, level='TASK,DAG', add=False)
            return conn_ids

        connections_map = {}
        for cid in [conn_id]:
            try:
                hook = S3Hook(aws_conn_id=cid, verify=False)
                buckets = [b['Name'] for b in hook.get_conn().list_buckets().get('Buckets', [])]
                connections_map[cid] = buckets
                logger.info(f"Connection '{cid}': {len(buckets)} buckets")
            except Exception as e:
                logger.warning(f"Could not list buckets for '{cid}': {e}")
                connections_map[cid] = []

        msg = pformat(connections_map, indent=4)
        add_note(f"```\n{msg}\n```", context, level='TASK')
        return json.dumps(connections_map)

    @task(max_active_tis_per_dag=5, map_index_template="{{ path }}")
    def chk_s3_keys(path: str, conn_id: str, rows: int, **context):
        import gzip
        import io

        path_clean = path.replace('s3://', '')
        bucket = path_clean.split('/')[0]
        key = '/'.join(path_clean.split('/')[1:])

        hook = S3Hook(aws_conn_id=conn_id, verify=False)
        if not hook.check_for_key(key, bucket):
            msg = f"File not found: {path}"
            add_note(msg, context, level='TASK')
            raise AirflowFailException(msg)

        s3_obj = hook.get_key(key, bucket)
        logger.info(f"Reading {path} ({readable_size(s3_obj.content_length)})")

        preview = []
        logger.info('::group:: read')
        try:
            if key.lower().endswith('.zip'):
                from stream_unzip import stream_unzip  # type: ignore
                body = s3_obj.get()['Body']
                items, total_size, archive = 0, 0, []
                for file_name, file_size, chunks in stream_unzip(body):
                    total_size += file_size
                    items += 1
                    try:
                        name = file_name.decode('cp866')
                    except Exception:
                        name = file_name.decode('utf-8', errors='replace')
                    archive.append(f"{readable_size(file_size):>10} | {name}")
                    for _ in chunks:
                        pass
                preview = [
                    f"Archive: {key}  Modified: {str(s3_obj.last_modified)[:19]}  Items: {items}  Size: {readable_size(total_size)}",
                    '-' * 50,
                ] + archive[:int(rows)]
                if items > int(rows):
                    preview.append(f"... and {items - int(rows)} more items")
            else:
                body = s3_obj.get()['Body']
                f = gzip.GzipFile(fileobj=body, mode='rb') if key.lower().endswith('.gz') else body
                for _ in range(int(rows)):
                    line = f.readline()
                    if not line:
                        break
                    preview.append(line.decode('utf-8', errors='replace').replace('\x00', '').strip('\r\n'))
        except Exception as e:
            logger.error(f"Failed to read file: {e}", exc_info=True)
        logger.info('::endgroup::')

        logger.info(f'::group:: {path}')
        for k, line in enumerate(preview):
            logger.info(f"  {k:04d}  {line}")
        logger.info('::endgroup::')

        note = '\n'.join(preview).replace('\x00', '') if preview else "File is empty."
        add_note('```\n' + note, context, level='TASK')

    @task(trigger_rule='one_success')
    def end(**context):
        add_note("Done", context, level='TASK')

    list_keys = list_s3_keys()
    chk_conn = chk_s3_conn()
    dag_end = end()

    list_keys >> chk_conn >> dag_end

    chk_s3_keys.partial(
        conn_id='{{ params.aws_conn_id }}',
        rows='{{ params.rows }}',
    ).expand(path=list_keys) >> dag_end


tools_s3_check_logs()
