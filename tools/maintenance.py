"""### Обслуживание S3-бакета мониторинга

Ежедневно создаёт бакет (если не существует), удаляет старые объекты и логирует объём.

| Параметр | Описание |
|---|---|
| `months` | Возраст объектов для удаления (месяцы, default: `1`) |
| `days` | Возраст объектов для удаления (дни, default: `0`) |
"""

from datetime import datetime, timezone

import pendulum
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task, dag
from dateutil.relativedelta import relativedelta

from plugins.utils import add_note

BUCKET_NAME = 'edpetl-monitoring'
AWS_CONN_ID = 's3-archive'


def _format_size(size_bytes):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"


def _get_paginator(bucket_name=BUCKET_NAME, page_size=1_000):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
    paginator = s3_hook.get_bucket(bucket_name).meta.client.get_paginator("list_objects_v2")
    return s3_hook, paginator.paginate(Bucket=bucket_name, PaginationConfig={'PageSize': page_size})


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 2,
        'retry_delay': pendulum.duration(seconds=30),
    },
    start_date=pendulum.datetime(2026, 1, 22, tz=pendulum.UTC),
    schedule_interval='17 5 * * *',
    tags=['EDP_ETL', 'tools'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        'months': Param(1, type='integer', description='Возраст объектов для удаления (месяцы)'),
        'days': Param(0, type='integer', description='Возраст объектов для удаления (дни)'),
    },
)
def maintenance():

    @task
    def create_bucket(**context):
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        if not s3_hook.check_for_bucket(BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME)
            add_note(f"Создан бакет `{BUCKET_NAME}`", context)
        else:
            add_note(f"Бакет `{BUCKET_NAME}` существует", context)

    @task
    def clean_logs(**context):
        params = context['params']
        cutoff = datetime.now(timezone.utc) - relativedelta(months=params['months'], days=params['days'])
        s3_hook, paginator = _get_paginator()
        total = 0
        for page in paginator:
            if contents := page.get("Contents"):
                keys = s3_hook._list_key_object_filter(keys=contents, to_datetime=cutoff)
                if keys:
                    total += len(keys)
                    s3_hook.delete_objects(bucket=BUCKET_NAME, keys=keys)
        msg = f"Удалено {total} объектов старше {params['months']}м {params['days']}д"
        add_note(msg, context)
        return msg

    @task
    def show_bucket_size(**context):
        _, paginator = _get_paginator()
        total_size = total_objs = 0
        for page in paginator:
            if contents := page.get("Contents"):
                for obj in contents:
                    total_size += obj.get("Size", 0)
                    total_objs += 1
        msg = f"Объём бакета: {_format_size(total_size)} ({total_objs} объектов)"
        add_note(msg, context)
        return msg

    create_bucket() >> clean_logs() >> show_bucket_size()


maintenance()
