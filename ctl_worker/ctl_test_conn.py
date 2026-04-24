"""### 🔌 DAG: Проверка подключений CTL

Непрерывный сенсор (каждую минуту, `reschedule`) — проверяет доступность всех соединений из `get_config()['conns']`.
Поддерживает типы: `Postgres`, `S3`, `KerberosHttp`. При сбое — экспоненциальный retry (до 1000 попыток).
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
