"""### 🧪 DAG: Тестирование CTL (Change Tracking & Loading)

Этот DAG предназначен для **ручного тестирования и симуляции** различных сценариев работы системы **CTL (Change Tracking & Loading)**.  
Он позволяет безопасно проверять логику запуска workflow'ов, параметры, обработку событий и интеграцию с Greenplum без влияния на продакшн.

---

#### 🔧 Назначение

- ✅ Симуляция **событий загрузки** (как если бы они пришли извне)
- ✅ Проверка **триггеров и параметров** workflow'ов
- ✅ Тестирование **логики retry, расписаний, категорий**
- ✅ Отладка **интеграций**: CTL → Airflow, GP, API
- ✅ Валидация поведения при ошибках и перезапусках

---

#### ⚙️ Режим работы

- **Запуск:** вручную или по расписанию (`minutes=5` по умолчанию)
- **Профиль:** зависит от `get_config()['profile']`
- **Пул:** `ctl_pool` — ограниченный пул для API-запросов
- **Catchup:** `False` — не накапливает пропущенные запуски
- **Max active runs:** `1` — исключает параллельные запуски
- **Таймаут DAG'а:** 10 минут
- **Режим тестирования:** активен только при `get_config()['test_mode'] = True`

---

#### 📌 Ключевые задачи

| Задача | Описание |
|-------|----------|
| `test_events` | Случайным образом выбирает workflow'ы и запускает их в тестовом режиме с уникальным `run_id`. |

---

#### 🏷️ Метаданные

- **ID DAG:** `CTL.<profile>.test_simulator`
- **Теги:** `['CTL', '<profile>', 'CTL_agent', 'tools']`
- **Расписание:** каждые 5 минут (настраивается через `events_interval`)
- **Автор:** `EDP.ETL`
- **Уведомления:** отключены (`email_on_failure = False`)
- **Retry:** отключены
- **Коллбэки:** `on_callback` — для логирования статуса 

---

#### 🔄 Как использовать?

1. Убедитесь, что в конфиге включён тестовый режим:
   ```python
   "test_mode": true
"""

from airflow import DAG, Dataset
from airflow.datasets import DatasetAlias
from airflow.api.common.trigger_dag import trigger_dag           
from airflow.utils.dates import days_ago
from airflow.decorators import task

from airflow.exceptions import AirflowFailException, AirflowSkipException, AirflowRescheduleException
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from plugins.utils import add_note, on_callback, get_current_load, str2timedelta  # type: ignore
from plugins.ctl_utils import get_config, ctl_obj_load, ctl_api # type: ignore 
from plugins.ctl_core import chk_any_conn 

import random
import pendulum
from datetime import timedelta

from logging import getLogger
logger = getLogger("airflow.task")

MAX_XCOM = 500

profile =  get_config()['profile']
enames = {int(k):v for k,v in ctl_obj_load('ctl_enames').items()}


with DAG(f'CTL.{get_config()["profile"]}.test_simulator',
    start_date=days_ago(1),
    schedule_interval=str2timedelta(get_config().get('simulator_interval','minutes=5')),
    default_args={ 
    'owner': 'EDP.ETL',
        'depends_on_past': False,
        'email': ['p1080@sber.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        "on_failure_callback": on_callback,
        "on_success_callback": None,
        "priority_weight": 999,
        'pool': 'ctl_pool',
    },
    catchup=False,
    tags=['CTL', profile, 'CTL_agent', 'tools'],
    max_active_runs=1,
    # dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    is_paused_upon_creation=False,
    on_failure_callback=on_callback,
    # on_success_callback=on_callback,
    doc_md=__doc__,
) as dag:
    
    @task(pool='ctl_pool')
    def test_events(**context): 
        
        chk_any_conn('ctl')
        
        # TEST !!!
        test_mode = get_config().get('test_mode', False)
        test_mode = False if str(test_mode).lower() in ['false', 'none', '', '0'] else test_mode
            
        if not test_mode:
            msg = "🔥 Test mode is off"
            add_note(msg, context, level='Task,DAG')
            raise AirflowSkipException(msg)

        
        cl = get_current_load('gp_pool')
        cnt = cl['pool_slots'] - cl['scheduled']

        if cnt <= 1:
            msg = "🔥 Sysytem is overloaded"
            add_note(msg, context, level='Task,DAG')
            raise AirflowSkipException(msg)
        
        ret = []
        if test_mode == 'event':
            
            all_events = str(get_config().get('all_events', False) ).lower() in  ['true', '1', 'yes']
            
            events = [k for k in ctl_obj_load('ctl_events').keys() if all_events or k.split('/')[0]!=profile]
            add_note(f"⏳ Testing {len(events)} events. test_mode: {test_mode}", context, level='Task,DAG')
            
            for k in range(random.randint(1, cnt)):
                evn = random.choice(events)
                prf, eid, sid = evn.split('/')
                try:
                    ctl_api(f'/v4/api/entity/{eid}/stat/{sid}/profile/{prf}/statval', 'POST', json=["1"])
                except Exception as e:
                    continue
                ret.append(evn)
                
        elif test_mode == 'dataset':
            events = list(ctl_obj_load('ctl_events').keys())
            add_note(f"⏳ Testing {len(events)} events", context, level='Task,DAG')
            
            for k in range(random.randint(1, cnt)):
                evn = random.choice(events)
                ret.append(evn)
                
        else: # trigger_dag
            wfs = list(ctl_obj_load('ctl_workflows').values())
            add_note(f"⏳ Testing {len(wfs)} DAGs", context, level='Task,DAG')
            
            for k in range(random.randint(1, cnt)):
                wf = random.choice(wfs)
                if wf['profile'] != profile: continue
                if wf['scheduled'] and wf['singleLoading']: continue 
                # if wf['category'] == "p1080.ARCHIVE": continue
                if wf['category'] == get_config().get('archive_category'): continue

                wf_name = wf['name']
                ret.append(wf_name)
                
                af_sdt = pendulum.instance(context['task_instance'].start_date).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
                extra={ "af_sdt": af_sdt, }        
                
                run_id=f'test__{af_sdt}'
                run_id = run_id.replace(' ','_')
                logger.info(f"🔍 Triggering {wf_name} with run_id={run_id}")
                
                try:
                    trigger_dag(
                        dag_id=f'CTL.{wf_name}',
                        run_id=run_id,
                        conf=extra,
                    )
                except Exception as e:
                    logger.error(f"🔥 Error triggering {wf_name}: {e}")

            add_note(ret, title=f"🔍 New events {len(ret)} created", context=context, level='Task,DAG')
               
        add_note(ret, context, level='Task,DAG', title=f'Test_mode {test_mode}: {len(ret)}')      
        
        return ret if test_mode == 'dataset' else []
    
    
    @task(pool='pg_pool', outlets=[DatasetAlias(f"CTL/events")],
        max_active_tis_per_dag=15, 
        map_index_template="{{ event }}"
    )
    def set_events(event, **context): 
        
        prf, eid, sid = event.split('/')
        ds = Dataset(f'CTL/{prf}/{eid}/{enames[int(eid)]}')
        extra = { f"0/{event}": pendulum.now().format('YYYY-MM-DD HH:mm:ss') }
        add_note(event, context, level='Task', title=event)
        context['outlet_events'][f"CTL/events"].add(ds,extra=extra) 
    
        return event[1]
    
    
    events = test_events()
    # chk_conn() >> 
    events >> set_events.expand(event = events)

