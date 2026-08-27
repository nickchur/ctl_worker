"""###🛠️ Обслуживание бакета логов задач
*2026-08-27 12:00 MSK · v1.6 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Ежедневно создаёт бакет (если не существует), удаляет старые объекты и логирует объём.
Бакет и префикс берутся из `[logging] remote_base_log_folder`, то есть чистятся ровно
логи задач, а не весь бакет.

| Параметр | Описание |
|---|---|
| 📅 `days`        | Возраст объектов для удаления (дни, default: `30`) |
| ⏰ `schedule`    | Расписание DAG-а: cron или пресет `@daily`, пусто — только вручную *(default: `17 5 * * *`)* |
| 💾 `save_params` | `True` — сохранить параметры этого запуска как значения по умолчанию, `False` *(default)* |

Значения по умолчанию берутся из переменной `tools_log_cleanup_params`, если она задана,
иначе из кода. Записывается переменная только запуском с `save_params=True` — то есть
разовый эксперимент в UI ночное окно не двигает, а осознанная правка двигает, без выкладки.
Новое расписание подхватывается со следующего парсинга DAG-а; негодное значение таск
`params` не записывает (падает), а уже записанное битым — игнорируется в пользу кода.

**Таски:**
- **params** — сохранение параметров запуска в переменную (пропускается при `save_params=False`)
- **create_bucket** — создание бакета, если его нет
- **clean_logs** — удаление объектов старше `days`
- **show_bucket_size** — объём и число объектов после чистки
"""

from datetime import datetime, timedelta, timezone
import logging

from airflow.configuration import conf
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task, dag
from airflow.utils.trigger_rule import TriggerRule

try:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size, saved_params, store_params, valid_schedule,
    )
except ImportError:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, readable_size, saved_params, store_params, valid_schedule,
    )

logger = logging.getLogger("airflow.task")

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


# Значения по умолчанию для формы запуска: код задаёт запасной вариант, переменная —
# рабочий. Пишет переменную только запуск с save_params=True, см. таск params.
# Механика повторяет db_cleanup.py; общее на два DAG-а — чтение переменной
# (utils.saved_params) и проверка расписания (utils.valid_schedule).
PARAMS_VAR = 'tools_log_cleanup_params'
DEFAULT_SCHEDULE = '17 5 * * *'
SAVED = saved_params(PARAMS_VAR)


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


def _schedule():
    """Расписание DAG-а: из переменной, если оно осмысленное, иначе из кода."""
    value = SAVED.get('schedule', DEFAULT_SCHEDULE)
    if not valid_schedule(value):
        logger.warning(f"⚠️ {PARAMS_VAR}: расписание '{value}' не разобрано — беру {DEFAULT_SCHEDULE}")
        return DEFAULT_SCHEDULE
    return None if value in (None, '', 'None') else str(value).strip()


params = {
    'days': _param(
        'days', 30,
        type='integer',
        minimum=1,
        description='Возраст объектов для удаления (дни)',
    ),
    'schedule': _param(
        'schedule', DEFAULT_SCHEDULE,
        type='string',
        description='Расписание: cron или пресет @daily; пусто — только вручную. Применяется со следующего парсинга',
    ),
    # Разовое действие, а не настройка: в переменную не сохраняется и берётся всегда из кода
    'save_params': Param(
        False,
        type='boolean',
        description='True — сохранить параметры этого запуска как значения по умолчанию',
    ),
}


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
    schedule=_schedule(),
    tags=['DataLab', 'tools', 'clean'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    on_failure_callback=on_callback,
    params=params,
)
def tools_log_cleanup():

    @task(task_id='params')
    def save_params(**context):
        """💾 Сохраняет параметры запуска в переменную как значения по умолчанию."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        status, msg = store_params(PARAMS_VAR, SAVED, context)
        if status == 'skip':
            raise AirflowSkipException(msg)
        if status == 'fail':
            raise AirflowFailException(msg)
        return msg

    # NONE_FAILED, а не дефолтный ALL_SUCCESS: params штатно пропускает себя при
    # save_params=False, а пропуск апстрима по ALL_SUCCESS утягивает в skip всю цепочку
    @task(trigger_rule=TriggerRule.NONE_FAILED)
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

    save_params() >> create_bucket() >> clean_logs() >> show_bucket_size()


tools_log_cleanup()
