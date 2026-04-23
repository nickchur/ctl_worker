"""### DAG: Просмотрщик S3

Получает список ключей из S3-бакета и читает содержимое файлов.

| Параметр | Описание |
|---|---|
| `aws_conn_id` | ID подключения к S3 (default: `s3`) |
| `bucket` | Имя бакета |
| `prefix` | Префикс объектов |
| `page_size` | Размер страницы (1–500, default: `100`) |
| `max_items` | Макс. количество объектов (1–500, default: `200`) |
| `rows` | Количество строк для вывода (1–1000, default: `300`) |
"""

import pendulum
from airflow.models import Param
from airflow.decorators import task, dag

from hrp_operators import HrpS3ListKeysOperator, HrpS3FileReadOperator  # type: ignore
from plugins.utils import add_note, on_callback

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
        'on_success_callback': on_callback,
        'on_retry_callback': on_callback,
    },
    start_date=pendulum.datetime(2026, 1, 21, tz=pendulum.UTC),
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
        'aws_conn_id': Param('s3', type='string', description="ID подключения к S3"),
        'bucket': Param('', type='string', description="Имя S3-бакета"),
        'prefix': Param('', type=['string', 'null'], description="Префикс S3-объекта"),
        'page_size': Param(100, type='integer', description="Размер страницы", minimum=1, maximum=500),
        'max_items': Param(200, type='integer', description="Макс. количество объектов", minimum=1, maximum=500),
        'rows': Param(300, type='integer', description="Количество строк для вывода", minimum=1, maximum=1000),
    },
)
def tools_s3_viewer():

    s3_list_keys = HrpS3ListKeysOperator(
        task_id='s3_list_keys',
        aws_conn_id='{{ params.aws_conn_id }}',
        bucket='{{ params.bucket if params.bucket else "" }}',
        prefix='{{ params.prefix if params.prefix else "" }}',
        max_items='{{ params.max_items }}',
        do_xcom_push=True,
    )

    @task
    def prepare_keys(list_from_s3, **context):
        if not list_from_s3 or not isinstance(list_from_s3, list):
            add_note("Список объектов пуст", context)
            return []
        keys = [obj['Key'] for obj in list_from_s3 if isinstance(obj, dict) and 'Key' in obj]
        result = [k for k in keys if not k.endswith('/')][:10]
        add_note(f"Найдено {len(result)} объектов", context)
        return result

    prepared_keys = prepare_keys(s3_list_keys.output)

    s3_file_read = HrpS3FileReadOperator.partial(
        task_id='s3_file_read',
        aws_conn_id='{{ params.aws_conn_id }}',
        s3_bucket='{{ params.bucket if params.bucket else "" }}',
        rows='{{ params.rows }}',
    ).expand(s3_key=prepared_keys)

    s3_file_read.map_index_template = "{{ task.s3_key }}"


tools_s3_viewer()
