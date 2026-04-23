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
    },
)
def tools_s3_from_content():
    """
    Загружает текстовый контент в S3 из параметров запуска DAG.

    Параметры:
        s3_conn_id   — ID подключения к S3 (тип aws)
        bucket_name  — имя бакета
        s3_key       — путь/ключ объекта в S3
        content      — список строк (или base64-encoded текст);
                       напишите "{{empty}}" для пустой строки
        compress     — сжатие: none | gz | zip
                       для zip формат s3_key: path/archive.zip/filename.ext
        replace      — перезаписать объект если уже существует (по умолчанию False)
    """

    @task
    def s3_from_content(**context):
        """
        Uploads a string content to S3 bucket

        :param s3_conn_id: S3 connection ID configured in Airflow
        :param bucket_name: S3 bucket name
        :param s3_key: S3 key (path) where to put the file
        :param content: Text content to upload
        """
        from airflow.models import Connection
        from airflow.exceptions import AirflowNotFoundException

        params = context['params']
        s3_conn_id = params['s3_conn_id']
        bucket_name = params['bucket_name']
        s3_key = params['s3_key']
        content = params['content']
        compress = params.get('compress', 'none')
        replace = params.get('replace', False)

        if not s3_conn_id or not s3_conn_id.strip():
            raise ValueError("Параметр 's3_conn_id' не задан")
        try:
            Connection.get_connection_from_secrets(s3_conn_id)
        except AirflowNotFoundException:
            raise ValueError(f"Подключение '{s3_conn_id}' не найдено в Airflow")

        if not bucket_name or not bucket_name.strip():
            raise ValueError("Параметр 'bucket_name' не задан")

        logger.info(
            f"upload_string_to_s3 s3_conn_id={s3_conn_id}, bucket_name={bucket_name}, s3_key={s3_key}, content={content}"
        )

        hook = S3Hook(aws_conn_id=s3_conn_id)

        if not hook.check_for_bucket(bucket_name):
            raise ValueError(f"Бакет '{bucket_name}' не существует или недоступен для '{s3_conn_id}'")

        content = '\n'.join(content)
        content = content.replace('{{empty}}', '')
        try:
            content = base64.b64decode(content).decode('utf-8')
        except Exception:
            pass

        import io

        if compress == 'gz':
            import gzip

            buf = io.BytesIO()
            with gzip.GzipFile(fileobj=buf, mode='wb') as f:
                f.write(content.encode('utf-8'))
            buf.seek(0)
            hook.load_file_obj(
                file_obj=buf,
                bucket_name=bucket_name,
                key=s3_key,
                replace=replace,
                extra_args={'ContentEncoding': 'gzip', 'ContentType': 'text/plain'},
            )

        elif compress == 'zip':
            import zipfile

            # s3_key формат: path/archive.zip/filename.ext
            zip_marker = '.zip/'
            zip_marker_pos = s3_key.lower().find(zip_marker)
            if zip_marker_pos == -1:
                raise ValueError(
                    f"Для compress='zip' s3_key должен иметь формат 'path/archive.zip/filename.ext', получено: '{s3_key}'"
                )
            zip_s3_key = s3_key[:zip_marker_pos + len('.zip')]
            internal_name = s3_key[zip_marker_pos + len(zip_marker):]
            if not internal_name:
                raise ValueError(
                    f"Не задано имя файла внутри архива в s3_key: '{s3_key}'"
                )

            buf = io.BytesIO()
            with zipfile.ZipFile(buf, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(internal_name, content.encode('utf-8'))
            buf.seek(0)
            hook.load_file_obj(
                file_obj=buf,
                bucket_name=bucket_name,
                key=zip_s3_key,
                replace=replace,
                extra_args={'ContentType': 'application/zip'},
            )
            logger.info(f"Successfully uploaded ZIP to s3://{bucket_name}/{zip_s3_key} (internal: {internal_name})")
            return

        else:
            hook.load_bytes(
                content.encode('utf-8'),
                bucket_name=bucket_name,
                key=s3_key,
                replace=replace,
            )
        logger.info(f"Successfully uploaded content to s3://{bucket_name}/{s3_key}")

    s3_from_content()


ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

if ENV_STAND == "prom":
    msg = "DAG upload_content_to_s3 is disabled on 'prom' stand. Skipping DAG registration."
    logger.warning(msg)
else:
    tools_s3_from_content()
