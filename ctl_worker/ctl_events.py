"""### DAG: События CTL → Airflow Dataset

Каждые 5 минут получает события из CTL и публикует Dataset'ы для оркестрации DAG'ов.

| Dataset | Описание |
|---|---|
| `CTL/wf/{wid}/{wf_name}` | Событие workflow |
| `CTL/entity/{eid}/{ename}` | Событие сущности |
| `CTL/{profile}/entities` | Алиас всех сущностей профиля |
"""

from airflow import DAG, Dataset
from airflow.datasets import DatasetAlias
from airflow.utils.dates import days_ago
from airflow.decorators import task
# from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.base import PokeReturnValue

from plugins.utils import add_note, on_callback, str2timedelta  # type: ignore
from plugins.ctl_utils import get_config, ctl_api, pg_exe, ctl_obj_load # type: ignore 
from plugins.ctl_core import chk_any_conn, conns  # type: ignore

from datetime import timedelta
import json
import pendulum
import ast
from logging import getLogger
logger = getLogger("airflow.task")

MAX_EVENTS = 25

profile =  get_config()['profile']

enames = {int(k):v for k,v in ctl_obj_load('ctl_enames').items()}

events_interval = str2timedelta(get_config().get('events_interval','minutes=1'))

with DAG(f'CTL.{get_config()["profile"]}.events',
    start_date=days_ago(1),
    schedule_interval=events_interval,
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
    tags=['CTL', 'CTL_agent', 'events'],
    max_active_runs=1,
    # dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    is_paused_upon_creation=False,
    on_failure_callback=on_callback,
    # on_success_callback=on_callback,
    doc_md=__doc__,
) as dag:
    
    @task.sensor(
        mode='reschedule', 
        soft_fail=True,
        poke_interval=events_interval,
        timeout=pendulum.duration(hours=24),
    )
    def get_events(**context): 

        # TEST !!!
        # test_mode = '' if str(config.get('test_mode')).lower() in ['event'] else '--'
        # if config.get('test_mode') == ['event'] :
        ti = context['ti']
        
        all_events = str(get_config().get('all_events', False) ).lower() in  ['true', '1', 'yes']
        if all_events :
            whr = ''
        else:
            whr = f" and b.key not LIKE '{profile}/%'"
        
        sql = """
            select b.key 
                , (string_to_array(b.key, '/'))[1] as prf
                , (string_to_array(b.key, '/'))[2] as eid
                , (string_to_array(b.key, '/'))[3] as sid
                , max(b.value::json #>> '{}')::bool value
                , max(c.updated_at)
            from main.variable a
            CROSS JOIN lateral json_each(a.val::json ) b
            left join main.dataset c on c.uri LIKE 'CTL/'||(string_to_array(b.key, '/'))[1]||'/'||(string_to_array(b.key, '/'))[2]||'/%'
            where a."key" = 'ctl_events'
            """ + whr + """
                and b.key like '%/%/%'
            group by b.key
            -- order by max nulls first
            order by random()
        """
        res = pg_exe(sql)
        ret = {}
        
        add_note(f"⏳ Cheking {len(res)} events", context, level='Task,DAG', add=False)
        
        for r in res:
            prf = r['prf']
            eid = r['eid']
            sid = r['sid']
            # ts = r['ts']
            ename = enames.get(int(eid), '')
            CTL_sched = r['value']

            logger.debug(f"🔍 {json.dumps(r, default=str)}")
            
            if CTL_sched:
                logger.debug(f"🔄 skipped scheduled in CTL")
                continue
            
            try:
                last = ctl_api(f"/v4/api/entity/{eid}/stat/{sid}/statval/last?profile={prf}")
            except:
                continue

            logger.debug(f"⚠️ {json.dumps(last, default=str)}")
            
            if not last or len(last) == 0:
                logger.debug(f"⚠️ skipped no events")
                continue
            
            last = (last[0] or {})
            lid = int(last.get('loading_id', '0'))
            ldt = last.get('published_dttm', '')
            evn = f"{prf}/{eid}/{ename}"
            ind = f"{lid}/{prf}/{eid}"

            if ldt and r['max'] and pendulum.parse(ldt, tz=get_config()['tz']) < r['max'] - timedelta(minutes=1):
                logger.debug(f"⏳ skip {ldt} < {str(r['max'])}")
                continue
            elif lid:
                whr= f"""
                    and b.key like '{lid}/{prf}/{eid}/{sid}'
                """
            else:
                whr = f"""
                    and b.key like '0/{prf}/{eid}/dt'
                    and  (b.value::json #>> '{ "{}" }') = '{ldt}'
                """
            sql= f"""
                select a.timestamp ts
                FROM main.dataset_event a 
                CROSS JOIN lateral json_each(a.extra::json) b
                join main.dataset c on a.dataset_id = c.id
                WHERE c.uri LIKE 'CTL/%' 
                    {whr}
                order by 1 desc
                limit 1
            """
            chk = pg_exe(sql)
            logger.debug(f"⏳ {json.dumps(chk,default=str)}")
            if chk:
                logger.debug(f"⏳ skipped allredy send")
                continue
            
            new = {}
            new[f"{ind}/dt"] = ldt 
            if len(ret) < MAX_EVENTS:
                if lid:
                    new[f"{ind}/url"] = f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}"
                    stats = ctl_api(f"/v4/api/loading/{lid}/statvals")
                    for s in stats:
                        if prf == s['profile'] and int(eid) == s['entity_id']:
                            stat = f"{s['loading_id']}/{s['profile']}/{s['entity_id']}/{s['stat_id']}"
                            new[stat] = s['value']
                    ret[evn] = new
                
                else:
                    stat = f"0/{prf}/{eid}/{sid}"
                    new[stat] = last['value']
                    ret[evn] = new
                
                logger.info(f"✳️ Event {new} ")
            else:
                logger.debug(f"🔒 MAX_EVENTS break")
                break
        if len(ret) == 0:
            return PokeReturnValue(is_done=False, xcom_value=None)
        else:
            # ti.xcom_push(key='events', value=json.dumps(ret, default=str))
            add_note(list(ret.keys()), context, level='Task,DAG', title=f'🔍 New events {len(ret)}')
            return PokeReturnValue(is_done=True, xcom_value=ret)        
    
    
    @task(pool='pg_pool', outlets=[DatasetAlias(f"CTL/events")],
        max_active_tis_per_dag=15, 
        map_index_template="{{ event[0] }}"
    )
    def set_events(event, **context): 

        # ds = Dataset(f'CTL/entity/{event[0]}')
        ds = Dataset(f'CTL/{event[0]}')
        add_note(event[1], context, level='Task', title=event[0])
        context['outlet_events'][f"CTL/events"].add(ds,extra=event[1]) 
    
        return event[1]
    
    
    events = get_events()
    task(task_id=f'chk_ctl')(chk_any_conn)(id='ctl', data=conns['ctl']) >> events >> set_events.expand(event = events)

