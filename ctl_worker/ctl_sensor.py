"""### 🔔 DAG: Сенсор событий CTL (Change Tracking & Loading)

Этот DAG работает как **сенсор**, отслеживающий активные загрузки в системе CTL, находящиеся в статусах:
- `RUNNING`
- `TIME-WAIT`
- `EVENT-WAIT`

---

#### 🔧 Основные функции

1. **Периодический опрос API CTL** каждую минуту на наличие новых или ожидающих задач.
2. **Фильтрация задач** по профилю, статусу и другим критериям.
3. Для каждой подходящей задачи:
   - Проверка возможности запуска (по времени, событиям).
   - При необходимости — откладывает выполнение (`TIME-WAIT` / `EVENT-WAIT`).
   - Запускает соответствующий workflow-DAG через `trigger_dag`.
4. **Обновление статуса загрузки** в CTL на `WAIT-AF` после передачи в Airflow.

---

#### ⚙️ Особенности

- **Частота запуска:** каждую минуту (`schedule_interval: minutes=1`)
- **Пул выполнения:** `ctl_pool` — ограничивает параллельные запуски.
- **Логирование:** пропущенные и обработанные задачи логируются через `add_note`.
- **Механизм retry:** поддерживается через XCom и конфигурацию запуска.
- **Интеграция:** взаимодействие с внешней системой CTL через `ctl_api`.

---

#### 📌 Ключевые задачи

| Задача | Описание |
|-------|----------|
| `ctl_add_get` | Получает список активных загрузок из CTL. |
| `ctl_add_chk` | Проверяет условия запуска и инициирует запуск DAG'ов. |
| `ctl_add_end` | Собирает результаты и логирует итоговое состояние. |

---

#### 🏷️ Метаданные

- **ID DAG:** `CTL.<profile>.sensor`
- **Теги:** `['CTL', '<profile>', 'CTL_agent', 'sensor']`
- **Приоритет:** `999` (очень высокий)
- **Max active runs:** `1`
- **Таймаут DAG:** `10 минут`
- **Режим catchup:** `False`

---

#### 📊 Логика работы

1. **Получение загрузок** из CTL со статусами `RUNNING`, `TIME-WAIT`, `EVENT-WAIT`.
2. **Фильтрация** по профилю, активности и наличию ID.
3. **Проверка условий**:
   - Новый запуск или повтор (`ctl_chk_new`).
   - Готовность событий (`ctl_chk_expire`).
   - Отложенный запуск (`ctl_chk_wait`).
4. **Запуск DAG** через `trigger_dag` или Dataset.
5. **Обновление статуса** в CTL на `WAIT-AF`.

---

#### 🔄 Состояния загрузок

| Иконка | Статус | Описание |
|--------|--------|----------|
| 🔵 | `RUNNING` | Выполняется в CTL |
| ⏳ | `TIME-WAIT` | Ожидание времени |
| 🔔 | `EVENT-WAIT` | Ожидание события |
| ⚪ | `WAIT-AF` | Ожидание в Airflow |

> 💡 *Примечание:* DAG использует `task sensor`-подобную логику без блокировки.  
> Поддерживает режим запуска через **Dataset** при `dug_run == 'dataset'`.
"""

from airflow import DAG, Dataset
from airflow.datasets import DatasetAlias
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.api.common.trigger_dag import trigger_dag           
from airflow.exceptions import DagRunAlreadyExists
from airflow.exceptions import AirflowSkipException, AirflowFailException


from plugins.utils import add_note, on_callback, str2timedelta, get_current_load  # type: ignore
from plugins.ctl_utils import get_config, ctl_api, ctl_obj_load, ctl_obj_save # type: ignore 
from plugins.ctl_core import chk_any_conn, ctl_loading_load, ctl_chk_new, ctl_chk_expire, ctl_chk_wait, ctl_set_status, ctl_get_retry # type: ignore

# from datetime import timedelta
from psycopg2 import errors
import pendulum
import json
from datetime import timedelta

from logging import getLogger
logger = getLogger("airflow.task")

MAX_WFS = 50
MAX_XCOM = 500

wfs_dict = ctl_obj_load('ctl_workflows')

profile = ctl_obj_load('ctl_profile')
profile_id = profile['id']
profile = profile['name']

default_args = {
    'owner': 'EDP.ETL',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['p1080@sber.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    # 'retry_delay': timedelta(minutes=5),
    'pool': 'default_pool',
    # 'xcom_push': True,  
    # 'execution_timeout': timedelta(minutes=15),  
    'on_failure_callback': on_callback,
    # 'on_success_callback': on_callback,
    # 'on_retry_callback': on_callback,
    # 'on_execute_callback': None,
}

# with DAG('_test_dag', start_date=days_ago(1), catchup=False, tags=['tools']) as dag:
#     @task
#     def test_task(): 
#         print("Hello")
#     test_task()


with DAG(f'CTL.{get_config()["profile"]}.sensor',
    start_date=days_ago(1),
    schedule_interval=str2timedelta(get_config().get('sensor_interval','minutes=1')),
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
    tags=['CTL', profile, 'CTL_agent', 'sensor'],
    max_active_runs=1,
    # dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    is_paused_upon_creation=False,
    on_failure_callback=on_callback,
    # on_success_callback=on_callback,
    doc_md=__doc__,
) as dag:

    # === Task Group (не вызывается!) ===
    @task_group(tooltip="Проверка доступности соединений",
        # ui_color="#00FF6A",
        # ui_fgcolor='#000000',
        # prefix_group_id=False,
        default_args= {            
            'pool': 'pg_pool',
            'max_active_runs': 1, 
            'priority_weight':1000,
            'execution_timeout': timedelta(seconds=10), 
            'retries': 1000,
            'retry_delay': timedelta(seconds=5),
            'retry_exponential_backoff': True,  
            'max_retry_delay': timedelta(minutes=5),
            
            # 'on_failure_callback': on_callback,
            # 'on_success_callback': on_callback,
            # 'on_retry_callback': on_callback,
            # 'on_execute_callback': None,
            'sla': timedelta(minutes=10),
        },
        # sla_miss_callback = on_callback,
    )
    def chk_conn():
        """Проверка соединений"""
        conns = get_config().get('conns', {})
            
        for id, data in conns.items():
            if data.get('type') not in ['Postgres', 'S3', 'KerberosHttp']:
                continue
            
            args = dict(
                task_id=f'chk_{id}', 
                # on_failure_callback=on_failure, 
                doc_md=f'chk_{id} {data}'
            )
            chk_task = task(**args)(chk_any_conn)(id=id, data=data)
            


    # with TaskGroup(group_id='ctl_add') as ctl_add:
    # @task_group
    # def ctl_add():

    @task(pool='ctl_pool')
    def ctl_add_get(**context):
        """### Сбор активных загрузок из CTL

        Опрашивает API CTL и получает список активных загрузок со статусами:
        - `RUNNING`
        - `TIME-WAIT`
        - `EVENT-WAIT`

        Фильтрует по:
        - Профилю (`profile_id`)
        - Активности (`alive == 'ACTIVE'`)
        - Наличию `loading_id`, `wf_id`
        - Статусу и логике предотвращения дублей

        Сохраняет промежуточные данные в `ctl_working/<lid>` (не публично).

        **Формат выхода:**
        - Возвращает до MAX_XCOM задач в формате `params`.
        - Пушит XCom: `lid_to_chk`, `lid_skiped`.

        **Логирование:**
        - Таблица с `lid` и `wf_name` через `add_note`.
        - Количество пропущенных и обработанных задач.

        **Источник:** `/v4/api/loading` (через `ctl_loading_load`)
        """
        ctl_api()
        
        cl = get_current_load('gp_pool')
        if cl['pool_slots'] - cl['scheduled'] <= 1:
            msg = "🔥 Sysytem is overloaded"
            add_note(msg, context, level='Task,DAG')
            raise AirflowSkipException(msg)
        
        data={
                'alive': '["ACTIVE"]', 
                'engines': '["dummy"]',
                'profile_ids': f'[{profile_id}]', 
                # 'category_ids': str(category_ids),
                'status': '["RUNNING","TIME-WAIT","EVENT-WAIT"]',
        }
        tsk = ctl_loading_load(data, save=False)

        branch = []
        lid_skiped = {}
        lid_to_chk = {}
        wf_adding = []

        for jsn in sorted(tsk, key=lambda x: x['id']):
            # lid = jsn['id']
            # ctl_obj_save(f"ctl_working/{lid}", jsn, var=False)
            if jsn.get('loading_status') is not None: del jsn['loading_status']
            if jsn.get('stats') is not None: del jsn['stats']
            p = jsn.get('params', {})
            # wf_name = p.get('wfp_name') or 'unknown'
            wf_name = jsn.get('workflow',{}).get('name') or jsn.get('wf_name') 
            rep = {
                # 'alive': jsn['alive'], 
                # 'profile': jsn['profile'], 
                # 'auto': jsn.get('auto'),
                # 'type': p.get('wfp_run_type','').lower(),
                # 'start': jsn.get('start_dttm','')[:16],
                'sdt': jsn.get('status_sdt','')[:16],
                'status': jsn.get('status',''),
                'wf': wf_name.split('.')[-1],
                'log': jsn.get('status_log',''), 
            }
            
            # Проверка задания на профиль, статус и старт
            skip_add = False
            # bad response
            if jsn['alive'] != 'ACTIVE':
                rep['msg'] = 'not active'
            elif jsn['profile'] != profile:
                rep['msg'] = 'not profile'
            elif not jsn.get('id'):
                rep['msg'] = 'no id'
            elif not jsn.get('wf_id'):
                rep['msg'] = 'no wf_id'
            # not RUNNING
            elif not p.get('loading_id'):
                rep['msg'] = 'no loading_id'
            #elif jsn.get('wf_id') in wf_adding:
            elif jsn['status'] not in ['RUNNING', 'TIME-WAIT', 'EVENT-WAIT']:
                skip_add = True
                rep['msg'] = 'not add status'
            elif ( jsn['status'] == 'RUNNING' and jsn.get('status_log') ):
                skip_add = True
                rep['msg'] = 'already run'
            # add 
            elif len(branch) < MAX_WFS:
                if jsn['wf_id'] in wf_adding:
                    rep['msg'] = f"WF {wf_name} ({jsn['wf_id']}) already added"
                else:
                    skip_add = False
                    wf_adding.append(jsn['wf_id'])
                    branch.append(jsn)
                    lid_to_chk[jsn['id']] = str(rep).replace("'", "")
                    # ctl_obj_save(f"ctl_working/{lid}", jsn, var=False)
            else:
                rep['msg'] = f'limit {MAX_WFS} exceeded'
            
            if skip_add and len(lid_skiped) < MAX_XCOM:
                lid_skiped[jsn['id']] = str(rep).replace("'", "")

        note = "| lid | wf_name |\n"
        note += "| ---: | :--- | \n"
        note += "\n".join([
            f"| {j.get('id')} | {j.get('wf_name','').split('.')[-1]} |" 
            for j in branch
        ])

        add_note(note, context, level='Task')
        add_note(f"⏩ lid skiped: {len(lid_skiped)}", context, level='Task,DAG')
        add_note(f"❓ lid to chk: {len(lid_to_chk)}", context, level='Task,DAG')
        # add_note(f"⚙️ lid branch: {len(branch)}", context, level='Task,DAG')
        
        ti = context['task_instance']
        ti.xcom_push(key='lid_to_chk', value=lid_to_chk)
        ti.xcom_push(key='lid_skiped', value=lid_skiped)
        
        return branch[:MAX_WFS]
    
    outlets = [DatasetAlias(f"CTL/{profile}/wfs")] if get_config().get('dug_run')=='dataset' else []
    @task(pool='ctl_pool', outlets=outlets,
        max_active_tis_per_dag=10, 
        map_index_template="{{ jsn['id'] }}/{{ jsn['wf_name'] }}",
        on_success_callback=None,
    )
    def ctl_add_chk(jsn, **context):
        """### Проверка условий запуска и инициация workflow

        Для каждой загрузки проверяет:
        1. Необходимость повторного запуска (`ctl_chk_new`).
        2. Готовность событий (`ctl_chk_expire`).
        3. Необходимость отложенного запуска (`wf_wait` → `ctl_chk_wait`).

        При успешной проверке:
        - Обновляет статус в CTL на `WAIT-AF`.
        - Запускает целевой DAG через `trigger_dag`.
        - Или отправляет Dataset при активированном режиме `dug_run`.

        **XCom:**
        - Пушит `result` и `extra` (параметры запуска).
        - Использует `outlet_events` для Dataset-сигналов.

        **Режимы запуска:**
        - `dug_run == 'dataset'`: сигнал через Dataset.
        - Иначе: запуск через `trigger_dag`.

        **Ссылка в логе:** добавляется ссылка на загрузку в интерфейсе CTL.
        """
        ti = context['task_instance']
        # return ctl_chk_loading(jsn, context)

        profile = get_config()['profile']
        
        status = jsn['status']
        log    = jsn.get('status_log') or ''
        # sdt = jsn['status_sdt']
        
        params = jsn['params']
        lid = params['loading_id']
        wid = params['wf_id']
        run_type = params.get('wfp_run_type','UNKNOWN')
        
        wf = wfs_dict[wid]
        wf_name = wf['name']
        
        af_sdt = pendulum.instance(ti.start_date).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
        params['af_sdt'] = af_sdt
        
        ctl_url = f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}"
        msg = f"🔗 [Открыть {lid} в CTL]({ctl_url})"
        add_note(msg, context, level='Task')


        # Проверка на новый запуск и времени повторного запуска 
        # retry, is_new = ctl_chk_new(wf, params, context)
        retry, is_new = ctl_chk_new(lid, wf_name, status, log, context)
        retry = retry if retry else ctl_get_retry(wf=wf, params=params)
        
        # Проверка условия запуска на событие EVENT-WAIT
        ctl_chk_expire(wf, params, context)
        
        # Отложенный запуск (wf_wait)
        if ( is_new and params.get('wf_wait')
            and run_type == 'EVENT-WAIT'
        ):
            ctl_chk_wait(wf, params, context)
            
        # af_sdt = str(ti.start_date)

        # if retry:
        params['wfp_retry'] = str(retry)
        ti.xcom_push(key='extra', value=params)
        ds = f'dataset__CTL/wf/{wid}/{wf_name}'
        run_id=f'sensor__{lid}_{retry.get("try", 1)}_{af_sdt}'.replace(' ','_')
        
        # Запуск DAG
        if get_config().get('dug_run')=='dataset':
            context['outlet_events'][f"CTL/{profile}/wfs"].add(Dataset(ds), extra=params)
        else:
            new_dag = dict(dag_id=f'CTL.{wf_name}', run_id=run_id, conf=params, )
            try:
                trigger_dag(**new_dag)
            except errors.UniqueViolation as e:
                ret = {
                    "action": "❌ already",
                    "id": lid, 
                    "name": wf_name,
                    "msg": run_id
                }
                ti.xcom_push(key='result', value=ret)
                add_note(ret, context, level='Task', title='❌ ALREADY')
                raise AirflowSkipException(e)

        # Установка статуса
        status = 'RUNNING'
        log ='WAIT-AF ' + (ds if get_config().get('dug_run')=='dataset' else run_id)
        ctl_set_status(lid, status, log)
        
        ret = {
            "action": "🚀 start",
            "id": lid, 
            "name": wf_name,
            "msg": f"{status} {log}"
        }
        ti.xcom_push(key='result', value=ret)
        add_note(ret, context, level='Task', title='🚀 START')


    @task(pool='pg_pool', trigger_rule = 'none_failed')
    def ctl_add_end(res, **context):
        """### Сбор и логирование результатов

        Выполняется после завершения всех `ctl_add_chk`.
        Собирает результаты из XCom и формирует итоговый отчёт.

        **Действия:**
        - Собирает `result` из всех экземпляров `ctl_add_chk`.
        - Преобразует в словарь `{lid: {action, name, msg}}`.
        - Пушит в XCom под ключом `add_result`.
        - Логирует полный список через `add_note`.

        **Режим:** `trigger_rule = 'all_done'` — выполняется всегда.

        **Назначение:** аудит, мониторинг, отладка.
        """
        ti = context['task_instance']
        wfs = ti.xcom_pull(key='result', task_ids=f'ctl_add_chk') or []
        wfs = {
            r['id']: {
                'action':r.get('action',''), 
                'name': r.get('name',''), 
                'msg':r.get('msg')
            } 
            for r in wfs if r['id']
        }
        # ti.xcom_push(key='add_result', value={k:str(v) for k,v in wfs.items()})
        ti.xcom_push(key='add_result', value=json.dumps(wfs, default=str))
        # add_note(wfs, context, level='Task', title='')
        
        # info = [ w['action'][:2] + w['name'] for w in wfs.values() if 'wait' not in w['action']]
        info = [ w['action'][:2] + w['name'] for w in wfs.values() ]
        add_note(info, context, level='Task', title='', compact=False)
         
        info = {}
        for w in wfs.values():
            info[w['action']] = info.get(w['action'], 0) + 1
        
        add_note(info, context, level='DAG,Task', title='')
        # return res

       
    add_get = ctl_add_get()
    add_chk = ctl_add_chk.expand(jsn = add_get)
    add_end = ctl_add_end(add_chk)
        
    chk_conn() >> add_get

    