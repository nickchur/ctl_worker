"""
## 🛠️ Загрузка контента в S3

Загружает текстовый контент в S3 из параметров запуска DAG.

| Параметр        | Описание                                                   |
|-----------------|------------------------------------------------------------|
| 📡 `s3_conn_id`  | ID подключения к S3 (тип `aws`)                            |
| 📋 `bucket_name` | Имя бакета                                                 |
| 🔑 `s3_key`      | Путь / ключ объекта в S3                                   |
| 📝 `content`     | Список строк или base64-текст; `{{empty}}` → пустая строка |
| 🗜️ `compress`    | Сжатие: `none` \| `gz` \| `zip`                            |
| 🔄 `replace`     | Перезаписать если существует *(default: `False`)*          |
| ✅ `done_file`   | Создать пустой `<s3_key>.done` после загрузки *(default: `False`)* |

### Режим `compress=zip`

Имя архива и внутреннего файла выводятся из `s3_key` автоматически:

| `s3_key`                        | Архив в S3                  | Файл внутри архива |
|---------------------------------|-----------------------------|--------------------|
| `path/archive.zip/data.csv`     | `path/archive.zip`          | `data.csv`         |
| `path/archive.zip/` или `path/archive.zip` | `path/archive.zip` | `archive`          |
| `path/data.csv.zip`             | `path/data.csv.zip`         | `data.csv`         |
| `path/data.csv`                 | `path/data.csv.zip`         | `data.csv`         |
"""
from airflow import DAG
from airflow.models import Param, Connection
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task, dag
from airflow.configuration import get_custom_secret_backend

import os
import base64
import pendulum

from logging import getLogger
logger = getLogger("airflow.task")


# Только если НЕ 'prom' — создаём DAG
def get_conns_by_type(conn_type='aws'):
    try:
        backend = get_custom_secret_backend()

        if not hasattr(backend, '_local_connections'):
            return []
        local_conn: dict[str, Connection] = backend._local_connections

        if conn_type:
            return [id for id,conn in local_conn.items() if conn.conn_type == conn_type.lower()]
        else:
            return [id for id,_ in local_conn.items()]
    except Exception as e:
        logger.warning(f"Cannot access connections: {e}")
        return []

s3_conns=get_conns_by_type(conn_type='aws')


@dag(
    # dag_id='tools_s3_from_content',
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args = {
        'owner': 'DataLab (CI02420667)',
        'retries': 2,
        'retry_delay': pendulum.duration(seconds=30),
    },
    start_date=pendulum.datetime(2025, 8, 7, tz=pendulum.UTC),
    tags=['DataLab', 'tools', 's3'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    schedule_interval=None,
    params={
        "s3_conn_id": Param('s3', type="string", examples=s3_conns),
        "bucket_name": Param('', type="string",),
        "s3_key": Param('', type="string",),
        "content": Param([],
            title="content",
            type=["array", "null"],
            description='Напишите "{{empty}}" если нужна пустая строка'
        ),
        "compress": Param(
            'none',
            type="string",
            enum=['none', 'gz', 'zip'],
            description="Архиватор: none — без сжатия, gz — gzip, zip — ZIP (s3_key: path/archive.zip/filename.ext)",
        ),
        "replace": Param(False, type="boolean", description="Перезаписать файл если существует"),
        "done_file": Param(False, type="boolean", description="Создать пустой <s3_key>.done после загрузки"),
    },
)
def tools_s3_from_content():

    @task
    def s3_from_content(**context):
        from airflow.models import Connection
        from airflow.exceptions import AirflowNotFoundException, AirflowSkipException

        params = context['params']
        s3_conn_id = params['s3_conn_id']
        bucket_name = params['bucket_name']
        s3_key = params['s3_key']
        content = params['content']
        compress = params.get('compress', 'none')
        replace = params.get('replace', False)
        done_file = params.get('done_file', False)

        if not s3_conn_id or not s3_conn_id.strip():
            raise AirflowSkipException("Параметр 's3_conn_id' не задан")
        try:
            Connection.get_connection_from_secrets(s3_conn_id)
        except AirflowNotFoundException:
            raise AirflowSkipException(f"Подключение '{s3_conn_id}' не найдено в Airflow")

        if not bucket_name or not bucket_name.strip():
            raise AirflowSkipException("Параметр 'bucket_name' не задан")

        logger.info(
            f"upload_string_to_s3 s3_conn_id={s3_conn_id}, bucket_name={bucket_name}, s3_key={s3_key}, content={content}"
        )

        hook = S3Hook(aws_conn_id=s3_conn_id)

        if not hook.check_for_bucket(bucket_name):
            raise AirflowSkipException(f"Бакет '{bucket_name}' не существует или недоступен для '{s3_conn_id}'")

        content = '\n'.join(content)
        content = content.replace('{{empty}}', '')
        try:
            content = base64.b64decode(content).decode('utf-8')
        except Exception:
            pass

        import io, gzip, zipfile

        raw = content.encode('utf-8')

        if compress == 'gz':
            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode='wb') as f:
                f.write(raw)
            buf.seek(0)
            hook.load_file_obj(file_obj=buf, bucket_name=bucket_name, key=s3_key, replace=replace)

        elif compress == 'zip':
            zip_marker = '.zip/'
            pos = s3_key.lower().find(zip_marker)
            if pos != -1:
                zip_s3_key = s3_key[:pos + len('.zip')]
                internal_name = s3_key[pos + len(zip_marker):]
            elif s3_key.lower().endswith('.zip'):
                zip_s3_key = s3_key
                internal_name = ''
            else:
                zip_s3_key = s3_key + '.zip'
                internal_name = os.path.basename(s3_key)
            if not internal_name:
                zip_basename = os.path.basename(zip_s3_key)
                internal_name = zip_basename[:-4]
            buf = io.BytesIO()
            with zipfile.ZipFile(buf, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(internal_name, raw)
            buf.seek(0)
            hook.load_file_obj(file_obj=buf, bucket_name=bucket_name, key=zip_s3_key, replace=replace)
            s3_key = f"{zip_s3_key} (internal: {internal_name})"

        else:
            hook.load_bytes(raw, bucket_name=bucket_name, key=s3_key, replace=replace)

        logger.info(f"Successfully uploaded to s3://{bucket_name}/{s3_key}")

        if done_file:
            done_key = zip_s3_key + '.done' if compress == 'zip' else s3_key + '.done'
            hook.load_bytes(b'', bucket_name=bucket_name, key=done_key, replace=replace)
            logger.info(f"Created done file s3://{bucket_name}/{done_key}")

    s3_from_content()


ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

if ENV_STAND == "prom":
    msg = "DAG upload_content_to_s3 is disabled on 'prom' stand. Skipping DAG registration."
    logger.warning(msg)
else:
    tools_s3_from_content()
