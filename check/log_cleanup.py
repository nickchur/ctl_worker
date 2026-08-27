"""###🛠️ Обслуживание бакета логов задач
*2026-08-27 10:35 MSK · v1.3 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Ежедневно создаёт бакет (если не существует), удаляет старые объекты и логирует объём.
Бакет и префикс берутся из `[logging] remote_base_log_folder`, то есть чистятся ровно
логи задач, а не весь бакет.

| Параметр | Описание |
|---|---|
| `days` | Возраст объектов для удаления (дни, default: `30`) |
"""

from datetime import datetime, timedelta, timezone

from airflow.configuration import conf
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task, dag

try:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size,
    )
except ImportError:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size  # type: ignore

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Берём из настроек логирования, а не прописываем именем: бакет и соединение
# зависят от контура, а conf читается из файла и переменных окружения — в базу
# на уровне модуля этот вызов не ходит
AWS_CONN_ID = conf.get("logging", "REMOTE_LOG_CONN_ID")
# s3://dataplatform-monitoring/dataplatform-etl
_LOG_BASE = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1]
BUCKET_NAME = _LOG_BASE.split("/")[0]
# Всё, что после имени бакета. Без префикса удаление шло бы по всему бакету, а он
# общий: кроме логов задач там лежит чужое, и чистить его этот DAG не должен
PREFIX = _LOG_BASE[len(BUCKET_NAME):].strip("/")
PREFIX = f"{PREFIX}/" if PREFIX else ""


def _get_paginator(bucket_name=BUCKET_NAME, page_size=1_000, prefix=PREFIX):
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
    paginator = s3_hook.get_bucket(bucket_name).meta.client.get_paginator("list_objects_v2")
    return s3_hook, paginator.paginate(
        Bucket=bucket_name, Prefix=prefix, PaginationConfig={'PageSize': page_size}
    )


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 2,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 1, 22, tzinfo=timezone.utc),
    schedule='17 5 * * *',
    tags=['DataLab', 'tools', 'clean'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    on_failure_callback=on_callback,
    params={
        'days': Param(30, type='integer', minimum=1, description='Возраст объектов для удаления (дни)'),
    },
)
def tools_log_cleanup():

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
        cutoff = datetime.now(timezone.utc) - timedelta(days=params['days'])
        s3_hook, paginator = _get_paginator()
        total = 0
        for page in paginator:
            if contents := page.get("Contents"):
                # Было s3_hook._list_key_object_filter(keys=contents, to_datetime=cutoff) —
                # приватный метод провайдера, который следующий мажор унесёт молча.
                # Граница включающая, как в оригинале: он отбрасывал только LastModified > cutoff
                keys = [o["Key"] for o in contents if o["LastModified"] <= cutoff]
                if keys:
                    total += len(keys)
                    s3_hook.delete_objects(bucket=BUCKET_NAME, keys=keys)
        msg = f"Удалено {total} объектов старше {params['days']}д из `{BUCKET_NAME}/{PREFIX}`"
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
        msg = f"Объём `{BUCKET_NAME}/{PREFIX}`: {readable_size(total_size)} ({total_objs} объектов)"
        add_note(msg, context)
        return msg

    create_bucket() >> clean_logs() >> show_bucket_size()


tools_log_cleanup()
