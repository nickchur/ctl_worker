"""### 🔍 DAG: Копирование/перемещение файлов S3 → S3

Находит файлы по маске и копирует (или перемещает) их в целевой бакет.

| Параметр | Описание |
|---|---|
| `src_path` | `conn_id://bucket/prefix/mask` |
| `dst_path` | `conn_id://bucket/prefix/` |
| `compress` | Сжать при копировании |
| `unzip` | Распаковать ZIP перед копированием |
| `done` | Создать `.done`-файл в dst после копирования |
| `timestamp` | Добавить метку времени к имени файла |
| `copy` | Копировать файл *(default: `True`)* |
| `delete` | Удалить источник после копирования |
| `max_items` | Макс. количество файлов (default: `25`) |
| `reverse` | Сортировка от новых к старым |
"""

import pendulum
from airflow.models import Param
from airflow.decorators import task, dag
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.base import PokeReturnValue
from airflow.datasets import DatasetAlias, Dataset

from plugins.utils import readable_size, add_note, on_callback, get_conns_by_type
from plugins.s3_utils import s3_to_s3, s3_move_s3, s3_delete, s3_path_parse, s3_from_zip, s3_keys, s3_get_buckets

from logging import getLogger
logger = getLogger("airflow.task")

MAX_ITEMS = 25

s3_list = []
for conn in get_conns_by_type(conn_type='aws'):
    try:
        for bucket in s3_get_buckets(conn):
            s3_list.append(f"{conn}://{bucket}/")
    except Exception:
        s3_list.append(f"{conn}://")


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 2,
        'retry_delay': pendulum.duration(minutes=1),
        'on_failure_callback': on_callback,
        'priority_weight': 999,
    },
    start_date=pendulum.datetime(2026, 1, 22, tz=pendulum.UTC),
    schedule_interval=None,
    tags=['EDP_ETL', 'tools', 's3'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    on_failure_callback=on_callback,
    params={
        'src_path': Param('', type='string', examples=[s + ('*.*' if not s.endswith('//') else '') for s in s3_list]),
        'dst_path': Param('', type='string', examples=s3_list),
        'compress': Param(False, type='boolean', description='Сжать при копировании'),
        'unzip': Param(False, type='boolean', description='Распаковать ZIP перед копированием'),
        'done': Param(False, type='boolean', description='Создать .done-файл после копирования'),
        'timestamp': Param(False, type='boolean', description='Добавить метку времени к имени файла'),
        'copy': Param(True, type='boolean', description='Копировать файл'),
        'delete': Param(False, type='boolean', description='Удалить источник после копирования'),
        'max_items': Param(MAX_ITEMS, type='integer', minimum=1, maximum=1000),
        'reverse': Param(True, type='boolean', description='Сортировка от новых к старым'),
    },
)
def tools_s3_to_s3_test():

    @task
    def s3_find(**context):
        p = context['params']
        path = p.get('src_path', '')
        reverse = p.get('reverse', True)
        max_items = int(p.get('max_items', MAX_ITEMS))

        s3_prm = s3_path_parse(path)
        objects = s3_keys(path)

        if not objects:
            msg = f"No files found in {path}"
            add_note(msg, context, level='TASK,DAG')
            raise AirflowSkipException(msg)

        objects = dict(sorted(objects.items(), key=lambda item: item[1], reverse=reverse))
        add_note(objects, context, level='TASK,DAG', title=f'Files ({len(objects)})')
        context['ti'].xcom_push(key='s3_src', value=s3_prm)

        return {
            f"{s3_prm['conn_id']}://{s3_prm['bucket']}/{key}": value
            for i, (key, value) in enumerate(objects.items())
            if i < max_items
        }

    @task(max_active_tis_per_dag=5, map_index_template="{{ path[0] }}")
    def s3_copy(path: str, **context):
        p = context['params']
        ti = context['ti']
        src_path, src_info = path[0], path[1]
        src = ti.xcom_pull(key='s3_src', task_ids='s3_find')
        prefix = src['prefix']

        compress = p.get('compress', False)
        dst_done = p.get('done', False)
        unzip = p.get('unzip', False)
        timestamp = p.get('timestamp', False)
        copy = p.get('copy', True)
        delete = p.get('delete', False)

        is_done_trigger = src_path.lower().endswith('.done')
        src_path = src_path[:-5] if is_done_trigger else src_path

        src = {**src, 'path': src_path, 'search': prefix + src['file'], 'info': src_info, **s3_path_parse(src_path)}
        ti.xcom_push(key='tfs_src', value=src)
        add_note(src, context, level='TASK', title='Source')

        src_hook = S3Hook(aws_conn_id=src['conn_id'], verify=False)
        src_obj = src_hook.get_key(src['key'], src['bucket'])
        sdt = pendulum.instance(src_obj.last_modified).format('YYYY-MM-DD HH:mm:ss')

        dst_path = p.get('dst_path', '')
        dst_path += '/' if dst_path and not dst_path.endswith('/') else ''
        dst = s3_path_parse(dst_path)
        dst['key'] = src['key'][:-len(src['ext'])].replace(prefix, dst['prefix'], 1)
        if timestamp:
            dst['key'] += ' ' + sdt
        dst['key'] += src['ext'] if src['ext'] else ''
        dst_path = f"{dst['conn_id']}://{dst['bucket']}/{dst['key']}"
        dst['path'] = dst_path
        ti.xcom_push(key='tfs_dst', value=dst)

        size = src_obj.content_length
        count = 1

        try:
            if copy:
                try:
                    if unzip and src['ext'].lower() == '.zip':
                        size, count, stats = s3_from_zip(src_path, dst_path, compress=compress)
                        add_note(stats, context, level='TASK')
                        dst_done = dst_path if dst_done else False
                        dst_path = ''
                except Exception as e:
                    add_note(f"Failed to process file: {e}", context, level='TASK,DAG')
                    dst_path = ''
                    count, size = 0, 0

                if dst_path:
                    if delete:
                        s3_move_s3(src_path, dst_path, compress=compress, done=dst_done)
                    else:
                        s3_to_s3(src_path, dst_path, compress=compress, done=dst_done)
                    add_note(f"{src_path} → {dst_path}", context, level='TASK,DAG')

                if is_done_trigger:
                    src_hook.delete_objects(keys=src['key'] + '.done', bucket=src['bucket'])
                    add_note(f".done deleted: {src_path}", context, level='TASK')

            elif delete:
                s3_delete(src_path)
                add_note(f"Deleted: {src_path}", context, level='TASK')

        except Exception as e:
            add_note(f"Transfer failed: {e}", context, level='TASK,DAG')
            raise AirflowFailException(f"Failed: {src_path}")

        msg = (
            f"Processed {src_path} — {count} files, {readable_size(size)}"
            if count else f"Failed to process {src_path}"
        )
        add_note(msg, context, level='TASK,DAG')
        return msg

    s3_copy.partial().expand(path=s3_find())


tools_s3_to_s3_test()
