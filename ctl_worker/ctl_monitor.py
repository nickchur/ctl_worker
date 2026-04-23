"""### DAG: Мониторинг CTL

Каждые 15 минут анализирует активные загрузки и выполняет автоматические действия.

| Действие | Описание |
|---|---|
| 🔁 `reRunned` | Повторная попытка при ошибке |
| 🚫 `Aborted` | Остановка при исчерпании попыток |
| ✅ `Completed` | Успешное завершение |
| ⚠️ `reStarted` | Перезапуск зависшей задачи |
| 🚨 `SLA` | Нарушение времени выполнения |
| 🛑 `Stopped` | Остановка вручную |
| ⚪ `Skipped` | Пропуск без изменений |
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.base import PokeReturnValue # type: ignore

from plugins.utils import add_note, on_callback, str2timedelta, get_current_load # type: ignore
from plugins.ctl_utils import get_config, gp_exe, ctl_obj_load, eval_delta, ctl_api # type: ignore
from plugins.ctl_core import chk_any_conn, ctl_loading_load, status_icons, ctl_wf_norm, ctl_events_mon, ctl_set_status  # type: ignore

import ast
import sys
from functools import partial
from datetime import timedelta
import pendulum

from logging import  getLogger
logger = getLogger('airflow.task')

MAX_XCOM = 500
MAX_WFS = 25

action_icons = { 
    'reRunned': '🔁', 
    'Aborted': '🚫', 
    'Completed': '✅', 
    'reStarted': '⚠️', 
    'Stopped': '🛑', 
    
    'notFound': '❓', 
    'New':'⏳' ,
    'Skipped': '⚪', 
    'SLA': '🚨',
}

monitor_interval = str2timedelta(get_config().get('monitor_interval','minutes=15'))
timeout = timedelta(hours=24)

with DAG(f'CTL.{get_config()["profile"]}.monitor',
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'logger'],
    start_date=days_ago(1),
    schedule_interval=monitor_interval,
    catchup=False,
    default_args={
        'owner': 'EDP.ETL',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'email': ['p1080@sber.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1),
        'pool': 'ctl_pool',
        # 'xcom_push': True,  
        # 'execution_timeout': timedelta(minutes=15),  
        'on_failure_callback': on_callback,
        # 'on_success_callback': on_callback,
        # 'on_retry_callback': on_callback,
        # 'on_execute_callback': None,
    },
    max_active_runs=1,
    is_paused_upon_creation=False,
    on_failure_callback=partial(on_callback, level='DAG'),
    on_success_callback=partial(on_callback, level='DAG'),
    # dagrun_timeout=timeout + str2timedelta(config.get('dagrun_timeout','minutes=10')),
    doc_md=__doc__,
) as dag:
    
    
    @task.sensor(pool='ctl_pool',
        mode='reschedule', 
        soft_fail=True,
        poke_interval=monitor_interval,
        timeout=timeout,
    )
    def ctl_monitor(**context):
        """Sensor: опрашивает активные загрузки категории и принимает решения по каждой.

        Для каждой загрузки определяет action: reRunned / reStarted / Aborted / Completed / SLA / Skipped.
        Возвращает PokeReturnValue(is_done=True, xcom_value={lid: r}) при наличии загрузок для обработки,
        иначе is_done=False (продолжает опрос).
        """
        chk_any_conn('ctl', **context)
        ti = context['ti']

        cl = get_current_load('gp_pool')
        if cl['pool_slots'] - cl['scheduled'] <= 1:
            msg = "🔥 Sysytem is overload"
            add_note(msg, context, level='Task,DAG')
            raise AirflowSkipException(msg)
        
        
        now = pendulum.now(get_config()['tz'])
        # wfs = ctl_obj_load('ctl_workflows')
        
        war = {}
        res = {}
        actions = {}
        stats = {}

        for c in ctl_obj_load('ctl_categories').keys():
            
            data={
                    'alive': '["ACTIVE"]', 
                    'engines': '["dummy"]',
                    # 'profile_ids': f'[{profile_id}]', 
                    'category_ids': f'[{c}]'
                    # 'category_ids': str(category_ids),
                    # 'status': '["SUCCESS","ERROR","LOCK","RUNNING","TIME-WAIT","EVENT-WAIT"]',
            }
            tsk = ctl_loading_load(data, save=False)
            
            for ld in sorted(tsk, key=lambda x: x['id']):
                
                if ld['alive'] != 'ACTIVE': continue
                    
                lid = ld['id']
                wid = str(ld['wf_id'])
                prm = ld.get('params', {})
                wfn = ld.get('wf_name','unknown')
                # wf = wfs[wid]
                wf = ctl_api(f'/v4/api/wf/{wid}')
                wf = ctl_wf_norm(wf, None)

                # ctl_obj_save(f"ctl_working/{lid}", jsn, var=False)
                
                sts = ld.get('status','')
                log = ld.get('status_log','')
                sdt = ld.get('status_sdt','')[:19]
                sla = prm.get('wf_interval','')
                
                stats[sts] = stats.get(sts, 0) + 1

                abortOnFailure = wf.get('faultTolerance', {}).get('abortOnFailure', False)
                scheduled = wf.get('scheduled')
                auto = ld.get('auto', False)
                running = prm.get('loading_id') is not None
                
                # t = now - datetime.strptime(sdt, "%Y-%m-%d %H:%M:%S")
                # time = f'{t}'
                t = now - pendulum.parse(sdt, tz=get_config()['tz'])
                time = (f'{t.days} d ' if t.days else '') + f'{t.hours:02}:{t.minutes:02}'
                
                action = None
                # msg = {}
                
                r = {
                    'time': time.split('.')[0],
                    'sdt': sdt[:19],
                    'SLA': sla,
                    'sch': scheduled,
                    'sts': sts, 
                    'log': True if log else False,
                    'act': None,
                    # 'msg': None,
                    'icon': None,
                    'wid': wid, 
                    'wfn': wfn,
                }
                
                # status "INIT", "TIME-WAIT", "EVENT-WAIT", "LOCK-WAIT", "PREREQ", "LOCK", "PARAM", "START", "RUNNING", "SUCCESS", "ERROR", "ERRORCHECK", "ABORTING"
                tst = pendulum.parse(eval_delta(sdt, 'minutes=60'), tz=get_config()['tz'])
                
                if tst <= now and not action:
                    if sts == 'RUNNING' and not log:
                        action = 'Skipped'

                    if sts in ['TIME-WAIT', 'EVENT-WAIT'] and running:
                        action = 'Skipped'

                    elif sts in ["PREREQ", "PARAM", "START"]:
                        action = 'reStarted'

                    if sts == "ERRORCHECK":
                        action = 'reRunned'

                    elif sts == 'ERROR':
                        action = 'Aborted' if abortOnFailure else 'Completed'

                    elif sts == 'SUCCESS':
                        action = 'Completed'

                    elif sts == 'ABORTING':
                        action = log.split(' ').strip()
                        action = action if status_icons.get(action) else 'Aborted'

                    elif sts in ['LOCK-WAIT', 'LOCK']:
                        # if datetime.strptime(eval_delta(sdt, 'hours=5'), "%Y-%m-%d %H:%M:%S") <= now:
                        if pendulum.parse(eval_delta(sdt, 'hours=5'), tz=get_config()['tz']) <= now:
                            action = 'reStarted'
                        else:
                            action = 'Skipped'
                            
                    elif sts == 'RUNNING' and log and not log.startswith('RUN'):
                        action = 'reRunned'

                    elif sts == 'RUNNING' and log and log.startswith('RUN'):
                        # if datetime.strptime(eval_delta(sdt, 'hours=5'), "%Y-%m-%d %H:%M:%S") <= now:
                        if pendulum.parse(eval_delta(sdt, 'hours=6'), tz=get_config()['tz']) <= now:
                            action = 'reRunned'
                        else:
                            action = 'Skipped'

                    elif sts == 'TIME-WAIT' and not running:
                        if log.startswith('Start scheduled on '):
                            # time = datetime.strptime(log[19:], "%Y-%m-%d %H:%M:%S")
                            time = pendulum.parse(log[19:], tz=get_config()['tz'])

                            if time + timedelta(minutes=15) <= now:
                                add_note(log, context, level='Task', title=f'reStarted {lid}')
                                action = 'reStarted'
                            else:
                                action = 'Skipped'
                        else:
                            action = 'Aborted'

                    elif sts == 'EVENT-WAIT' and not running:
                        chk = ctl_events_mon(sdt, wf, now)
                        
                        if not chk['chk']:
                            add_note(chk, context, level='Task', title=f'reStarted {lid}')
                            action = 'reStarted'
                        else:
                            action = 'Skipped'
                else:
                    action = 'New'

                if not action: 
                    action = 'notFound'
                    
                if action in ['Skipped']:
                    wf_interval = gp_exe(None, f"SELECT '{sla}'::interval") if sla else timedelta(days=1)
                    # wf_interval = timedelta(hours=6)
                    tst = pendulum.parse(sdt, tz=get_config()['tz']) + wf_interval 
                    if tst <= now:
                        action = 'SLA'


                actions[action] = actions.get(action, 0) + 1
                
                r = {**r,
                    'act': action,
                    'icon': action_icons[action],
                    # 'msg': f'{msg}',
                }
                
                if action in ['Skipped', 'New']:
                    continue
                elif action in ['notFound', 'SLA']:
                    if len(war) < MAX_XCOM: 
                        war[lid] = r
                    continue
                else:
                    if len(res) < MAX_WFS:
                        res[lid] = r
                    else:
                        continue
            
        actions = { f'{action_icons[a]}  {a}': v for a,v in actions.items()}
        add_note(actions, context, level='Task,DAG', title='Action', add=False)
        
        stats = { f'{status_icons[s]}  {s}': v for s,v in stats.items()}
        add_note(stats, context, level='Task,DAG', title='Status')
        
        # all={k:str(v) for k,v in all.items()}
        ti.xcom_push(key='SLA', value=war)
        
        chk_any_conn('ctl', **context)
        
        if res:
            return PokeReturnValue(is_done=True, xcom_value=res)
        else:
            return PokeReturnValue(is_done=False, xcom_value=stats)
    

    @task(pool='ctl_pool')
    def ctl_action(res, **context):
        """Выполняет действия над загрузками из res (XCom от ctl_monitor).

        res — dict {lid: r}, где r содержит act/wid/sch. Для каждой загрузки:
        - ABORTING статус → aborted/completed через API;
        - reRunned → RUNNING статус;
        - reStarted/Stopped → удаляет расписание, завершает, создаёт новую загрузку;
        - scheduled → ставит в расписание.
        """
        chk_any_conn('ctl', **context)
        
        active = True
        
        if not res:
            raise AirflowSkipException('Notheng to do')
        
        for lid, r in res.items():
            lid = int(lid)
            wid = r['wid']
            action = r['act']
            scheduled = r['sch']

            ctl_url = f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}"
            add_note(r, context, level='Task', title=f"🔗 {lid}({ctl_url})")

            if action in ['notFound', 'Skipped', 'New', 'SLA']:
                continue

            # Status
            # retry = ctl_get_retry(retry=None, logs='', params=ld['loading_status'], wf=wfs[wid])
            # if retry:
            #     r['retry'] = retry
            # if action in ['reStarted',]:
            #     if retry.get('try'):
            #         prm['wfp_retry']  = str(retry)
            
            
            # Status ABORTING
            if action not in ['Completed']:
                # data={'loading_id': lid, 'status': 'ABORTING', 'log': f'{action} {r}'}
                # if active: ctl_api(f'/v4/api/loading/{lid}/status', 'put', json=data)
                if active: ctl_set_status(lid, 'ABORTING', f'{action} {r}')


            # Status RUNNING
            if action in ['reRunned',]:
                # data={'loading_id': lid, 'status': 'RUNNING', 'log': ''}
                # if active: ctl_api(f'/v4/api/loading/{lid}/status', 'put', json=data)
                if active: ctl_set_status(lid, 'RUNNING', '')
                continue
            
            # Schedule delete
            if scheduled:
                if active: ctl_api(f'/v4/api/wf/{wid}/scheduled','delete')

            # Close Completed/Aborted
            if active: ctl_api(f"/v4/api/loading/{lid}/{'completed' if action=='Completed' else 'aborted'}", 'put') 

            # Start and Schedule
            if action in ['reStarted', 'Stopped']:
                prm = { k:str(v) for k,v in r.items() if k.startswith('wf') }
                if active: ctl_api(f"/v4/api/wf/{wid}/loading?scheduleAfterStart={scheduled}", "post", json=prm)
            
            # Schedule start
            elif scheduled:
                if active: ctl_api(f'/v4/api/wf/{wid}/scheduled','put')

    
    ctl_action(res = ctl_monitor())

