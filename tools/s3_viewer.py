"""### 🗂️ DAG: Просмотрщик S3
*2026-09-02 09:44 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Получает список ключей из S3-бакета и читает содержимое файлов.

| Параметр | Описание |
|---|---|
| `aws_conn_id` | ID подключения к S3 (default: `s3`) |
| `bucket` | Имя бакета |
| `prefix` | Префикс объектов |
| `page_size` | Размер страницы (1–500, default: `100`) |
| `max_items` | Макс. количество объектов (1–500, default: `200`) |
| `rows` | Количество строк для вывода (1–1000, default: `300`) |

⚠️ `max_items` и `rows` до операторов доезжают в обход конструктора: ядро до etl-core
PR #15 применяет к ним потолок прямо в `__init__`, где вместо числа лежит нерендеренный
шаблон, и весь даг падает «Broken DAG». Подробности — у `_late_template` и `prepare_keys`.
"""

from datetime import datetime, timedelta, timezone
from airflow.models import Param
from airflow.decorators import task, dag

from hrp_operators.s3_viewer_operator import HrpS3ListKeysOperator, HrpS3FileReadOperator  # type: ignore
from plugins.utils import add_note, on_callback  # type: ignore

from logging import getLogger
logger = getLogger("airflow.task")


def _late_template(op, **fields):
    """Кладёт шаблон в поле ОПЕРАТОРА после конструктора, а не передаёт аргументом.

    Ядро до etl-core PR #15 считает потолки прямо в __init__:
    `min(max_items, MAX_ITEMS) + 1` у списка ключей и `rows + 1` у читателя. Поля при
    этом шаблонные, то есть на разборе в них лежит строка '{{ … }}' — и файл падает
    TypeError'ом, унося весь даг («Broken DAG»), а не одну задачу.

    Присваивание после конструктора безопасно: оба поля перечислены в template_fields,
    поэтому Airflow отрендерит их перед запуском независимо от того, что лежало там на
    разборе. На ядре с фиксом потолок применяется в execute — присваивание ничего не
    меняет и просто становится лишним; убрать его можно будет, когда версия с фиксом
    раскатится везде.

    ⚠️ На СТАРОМ ядре вместе с потолком теряется и «+1 for has_next»: просмотрщик может
    не отметить, что за последней строкой есть продолжение. На новом всё точно.
    """
    for name, value in fields.items():
        setattr(op, name, value)
    return op


@dag(
    doc_md=__doc__,
    owner_links={
        'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab',
        'Korchagin Viacheslav': 'mailto:VYurKorchagin@sberbank.ru',
    },
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
        'retry_delay': timedelta(seconds=30),
        'on_failure_callback': on_callback,
        'on_success_callback': on_callback,
        'on_retry_callback': on_callback,
    },
    start_date=datetime(2026, 1, 21, tzinfo=timezone.utc),
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

    s3_list_keys = _late_template(
        HrpS3ListKeysOperator(
            task_id='s3_list_keys',
            aws_conn_id='{{ params.aws_conn_id }}',
            bucket='{{ params.bucket if params.bucket else "" }}',
            prefix='{{ params.prefix if params.prefix else "" }}',
            do_xcom_push=True,
        ),
        max_items='{{ params.max_items }}',
    )

    @task
    def prepare_keys(list_from_s3, **context):
        """Ключи для чтения — вместе с числом строк.

        rows возвращается отсюда числом, а не шаблоном у оператора: у mapped-задачи
        Airflow вызывает __init__ ВНУТРИ render_template_fields (unmap), то есть до
        подстановки, и старое ядро ловит на `rows + 1` строку '{{ params.rows }}'.
        Значение из expand_kwargs приезжает уже готовым — работает на любом ядре.
        """
        if not list_from_s3 or not isinstance(list_from_s3, list):
            add_note("Список объектов пуст", context)
            return []
        keys = [obj['Key'] for obj in list_from_s3 if isinstance(obj, dict) and 'Key' in obj]
        result = [k for k in keys if not k.endswith('/')][:10]
        rows = int(context['params'].get('rows') or 100)
        add_note(f"Найдено {len(result)} объектов, читаем по {rows} строк", context)
        return [{'s3_key': k, 'rows': rows} for k in result]

    prepared_keys = prepare_keys(s3_list_keys.output)

    s3_file_read = HrpS3FileReadOperator.partial(
        task_id='s3_file_read',
        aws_conn_id='{{ params.aws_conn_id }}',
        s3_bucket='{{ params.bucket if params.bucket else "" }}',
    ).expand_kwargs(prepared_keys)

    s3_file_read.map_index_template = "{{ task.s3_key }}"


tools_s3_viewer()
