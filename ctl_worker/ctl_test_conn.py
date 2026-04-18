"""### 🔧 DAG: Проверка критически важных соединений CTL

Этот DAG предназначен для **регулярного мониторинга доступности ключевых систем**, используемых в инфраструктуре CTL. Он обеспечивает раннее обнаружение проблем с подключениями и предотвращает сбои в ETL-процессах.

---

#### 🎯 Назначение

- Проверка соединения с **PostgreSQL** (Airflow DB).
- Проверка соединения с **Greenplum** (основное хранилище данных).
- Проверка соединения с **CTL API** (система управления загрузками).
- Проверка соединения с **S3** (хранилище метаданных и файлов).

---

#### ⚙️ Особенности реализации

- **Частота запуска:** каждую минуту (`schedule_interval: minutes=1`)
- **Режим опроса:** `reschedule` — освобождает слоты при ожидании.
- **Поведение при ошибке:** 
  - Бесконечные попытки (`retries: 1000`)
  - Экспоненциальная задержка между попытками
  - Максимальная задержка = интервал проверки
- **SLA:** 5 минут — уведомление при превышении.
- **Высокий приоритет:** `priority_weight: 1000`
- **Таймаут DAG:** 70 минут

---

#### 📌 Логика работы

1. Для каждого соединения из `get_config()['conns']`:
   - Формируется задача-сенсор.
   - Поддерживаются типы: `Postgres`, `S3`, `KerberosHttp`.
2. Каждая задача:
   - Проверяет доступность ресурса.
   - При успехе — завершается с `is_done=False` (продолжает опрос).
   - При ошибке — выбрасывает исключение, вызывая retry.
3. Используется `task_group` для группировки и читаемости в интерфейсе Airflow.

---

#### 🏷️ Метаданные

- **ID DAG:** `CTL.<profile>.test_conn`
- **Теги:** `['CTL', 'CTL_agent', 'tools', 'test']`
- **Max active runs:** `1`
- **Catchup:** `False`
- **Пул:** `pg_pool`
- **Режим выполнения:** sensor + reschedule

---

> 💡 *Примечание:* DAG не завершается успешно — он работает непрерывно, пока все соединения доступны.  
> Сбой одной из задач приводит к активации механизма retry с экспоненциальной задержкой.
"""

from datetime import timedelta 
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.sensors.base import PokeReturnValue # type: ignore

from plugins.utils import  on_callback, default_args, str2timedelta # type: ignore
from plugins.ctl_utils import get_config, add_note # type: ignore 
from plugins.ctl_core import chk_any_conn # type: ignore

import logging
logger = logging.getLogger("airflow.task")

profile =  get_config()['profile']
conns = get_config()['conns']

test_interval = str2timedelta(get_config().get('test_interval','minutes=1'))
timeout=timedelta(minutes=60)


with DAG(
    dag_id=f'CTL.{get_config()["profile"]}.test_conn',
    description="Проверка критически важных соединений",
    default_args={ **default_args,
        'pool': 'pg_pool',
        'max_active_runs': 1, 
        'priority_weight':1000,
        # 'sla': timedelta(minutes=5),
        # 'execution_timeout': timedelta(seconds=30), 
        'retries': 1000,
        'retry_delay': timedelta(seconds=5),
        'retry_exponential_backoff': True,  
        'max_retry_delay': test_interval,
        
        # 'on_failure_callback': on_callback,
        # 'on_success_callback': on_callback,
        # 'on_retry_callback': on_callback,
        # 'on_execute_callback': None,
    },
    start_date=days_ago(1),
    schedule_interval=test_interval,
    # schedule_interval=None,
    catchup=False,
    tags=['CTL', 'CTL_agent', 'tools', 'test'],
    on_failure_callback=on_callback,
    # on_success_callback=on_callback,
    # sla_miss_callback = on_callback,
    dagrun_timeout= timeout + timedelta(minutes=10),
    max_active_runs=1,
    doc_md=__doc__,
) as dag:

    def chk_any(id, data=None, **context):
        try:
            ret = chk_any_conn(id, data, **context)
            return PokeReturnValue(is_done=False, xcom_value=ret)
        except Exception as e:
            # return PokeReturnValue(is_done=True, xcom_value=str(e))
            raise e
    
    # @task_group(tooltip="Проверка доступности соединений",)
    # def chk_conn():
    #     """Проверка соединений"""

    for id, data in conns.items():
        if data.get('type') not in ['Postgres', 'S3', 'KerberosHttp']:
            continue
        
        args = dict(
            task_id=f'chk_{id}', 
            doc_md=f'chk_{id} {data}',
            mode='reschedule', 
            soft_fail=True,
            poke_interval=test_interval,
            timeout=timeout,
        )
        chk_task = task.sensor(**args)(chk_any)(id=id, data=data)
    
    
    # @task(trigger_rule = 'none_failed')
    # def chk_end():
    #     pass
    
    
    # chk_conn() #>> chk_end()
