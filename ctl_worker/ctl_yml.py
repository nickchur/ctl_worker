"""### 💾 DAG: Экспорт конфигурации CTL в YAML

Ручной запуск. Экспортирует конфигурацию CTL в YAML и сохраняет в S3.

| Файл | Описание |
|---|---|
| `categories.yml` | Категории и сущности |
| `workflows.yml` | Workflow'ы с параметрами и расписанием |
"""

from airflow import DAG
from airflow.operators.python import task
from airflow.utils.dates import days_ago

from plugins.utils import add_note, default_args # type: ignore
from plugins.ctl_utils import get_config, ctl_obj_save, ctl_obj_load, ctl_api # type: ignore

import json
import pendulum

from  logging import getLogger
logger = getLogger("airflow.task")

wfs = [
    'pc1080.',
]

profiles = {
    "DEV_SDP_UE": "CTL_SDPUE_PROFILE",
    "SDP_DEV_UE": "CTL_SDPUE_PROFILE",
    "IFT_SDP_UE": "CTL_SDPUE_PROFILE",
    "atdisdpcap": "CTL_SDPUE_PROFILE",
    "arnsdpue": "CTL_SDPUE_PROFILE",
    "HR_Data": "CTL_PROFILE_NAME"
}



with DAG(f"CTL_{get_config()['profile']}.yml", 
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'tools'],
    description='CTL',
    start_date=days_ago(1), 
    schedule=None, 
    catchup=False, 
    default_args=default_args,
    params={
        'wfs': wfs,
        'profiles': profiles,
        'safe': True,
    },
    doc_md=__doc__
) as dag:


    def req(url, method='get', data={}, log=True, logs=None):
        if log:
            logger.info(dict(url=url, method=method, data=data))
        
        if type(logs) == dict:
            ind = len(logs)
            logs[ind] = str(data)
        elif type(logs) == list:
            logs.append(str(data))
        
        if not url.startswith('/v'):
            url = '/v5/api' + url 
            ret = ctl_api(url, method, json=data)
        elif url.startswith('/v4'):
            ret = ctl_api(url, method, json=data)
        else:
            ret = ctl_api(url, method, json=data)
        
        return ret

    def save_category(cats):
        yaml_data = dict()
        key = f"ctl_yaml/ctl_category_{get_config()['root_category']}"

        yaml_data['cats'] = list()
        for j,v in cats.items():
            ed = dict()
            ed['category'] = {
                'name': cats[j]['name'],
                'deleted':  cats[j]['deleted']
            }

            if cats[j].get('parent_name'):
                ed['category']['parent_name'] = cats[j]['parent_name']
            yaml_data['cats'].append(ed)
        logger.debug(yaml_data)
        ctl_obj_save(key, yaml_data, ext='yml')

    def save_entity(e_ids):
        yaml_data = dict()

        key = f"ctl_yaml/ctl_entity"
        yaml_data['entities'] = list()
        for eid in e_ids:
            ed = dict()
            ed['entity'] = {
                'id': e_ids[eid]['id'],
                'name': e_ids[eid]['name'],
                'storage': e_ids[eid]['storage'],
                'parent-id': e_ids[eid]['parentId']
            }
            if e_ids[eid].get('path'):
                ed['entity']['path'] = e_ids[eid]['path']
            yaml_data['entities'].append(ed)

        logger.debug(yaml_data)
        ctl_obj_save(key, yaml_data, ext='yml')

    def wf_yml(yml, jsn, yaml_data, safe=True):
        sch = False
        wf = jsn['wf']
        ed = dict()
        ed[wf['name']] = {
            'name': wf['name'],
            'category': wf['category'],
            'profile': '{{ '+ profiles[wf['profile']] + ' }}' if profiles.get(wf['profile']) else wf['profile'],
            'singleLoading': wf['singleLoading'],
            'orchestrator': wf['engine'] if wf['engine'] in
                                            ('espd', 'ab_initio', 'oozie', 'informatica_pc', 'dummy', 'sdp_compure'
                                            , 'informatica', 'qlik', 'oracle', 'jenkins', 'ETL') else 'dummy'
        }

        if jsn.get('connectedEntities'):
            ed[wf['name']]['link_entities'] = jsn['connectedEntities']

        if wf.get('wf_event_sched') or wf.get('wf_time_sched'):
            sch = True
            ed[wf['name']]['schedule_params'] = {
                'eventAwaitStrategy': wf['eventAwaitStrategy']
            }

            if wf.get('wf_event_sched'):  #and not safe
                tf = True
                for j in wf.get('wf_event_sched'):
                    if safe and str(j['entity_id'])[:4] != '9410':
                        tf = False

                if tf:
                    # yml.write(f"""        entities:\n""")
                    ed[wf['name']]['schedule_params']['entities'] = list()

                    for j in wf.get('wf_event_sched'):
                        ee = dict()
                        ee['entity'] = {
                            'id': j['entity_id'],
                            'statisticId': j['stat_id'],
                            'profile': '{{ '+profiles[j['profile']]+' }}' if profiles.get(j['profile']) else j['profile'],
                            'active': j['active']
                        }
                        ed[wf['name']]['schedule_params']['entities'].append(ee)

            if wf.get('wf_time_sched'):
                j = wf['wf_time_sched']
                ed[wf['name']]['schedule_params']['cron'] = {
                    'expression': j['sched'],
                    'active': j['active']
                }

        if not sch or safe:
            ed[wf['name']]['schedule'] = {'type': 'none'}
        else:
            ed[wf['name']]['schedule'] = {'type': 'schedule'}

        if wf.get('statusNotifications'):
            ed[wf['name']]['notifications'] = list()
            for j in wf['statusNotifications']:
                ed[wf['name']]['notifications'].append(j)

        safe_param = False
        if wf.get('param'):
            ed[wf['name']]['params'] = list()
            for j in wf['param']:
                ee = dict()
                ee['param'] = {
                    'name': j['param'],
                    'prior_value': j['prior_value'] if j.get('prior_value') else ''
                }
                if j['param'] == 'wf_event_sched': safe_param = True
                ed[wf['name']]['params'].append(ee)

        if wf.get('wf_event_sched') and safe and not safe_param:
            tf = False
            for j in wf.get('wf_event_sched'):
                if safe and not str(j['entity_id'])[:4] == '9410':
                    tf = True
            if tf:
                if not wf.get('param'):
                    ed[wf['name']]['params'] = list()
                n = list()
                for j in wf['wf_event_sched']:
                    n.append(dict(
                        entity_id=j['entity_id'],
                        stat_id=j['stat_id'],
                        profile=j['profile'],
                        active=j['active']))
                ee = dict()
                ee['param'] = {
                    'name': 'wf_event_sched',
                    'prior_value': json.dumps(n)
                }
                ed[wf['name']]['params'].append(ee)
        yaml_data['workflows'].append(ed)

    @task
    def get_yml(**context): 
        
        safe = context['params']['safe']
        
        wfs = context['params']['wfs']
        add_note(wfs, context, level='DAG,Task', title='Workflows')
        
        cats = ctl_obj_load('ctl_categories')
        add_note(f'Categories:{len(cats)}', context, level='DAG,Task')
        save_category(cats)
        
        e_ids = ctl_obj_load('ctl_entities')
        e_ids = {int(k): v for k, v in e_ids.items() if str(k) >= get_config()['root_entity']}
        add_note(f'Entities:{len(e_ids)}', context, level='DAG,Task')
        save_entity(e_ids)
        
        ver = req('/info', 'get')
        add_note(ver, context, level='DAG,Task')  

        # res = req('/wf/extended', 'get', {'category_ids': f'{[*cats]}'})
        ids = ','.join(cats.keys())
        res = ctl_api('/v5/api/wf/extended', 'get', {'category_ids': f'[{ids}]' })

        # used = dict()
        # used_eids = dict()
        find = list()
        for i, data in enumerate(sorted(res, key=lambda x: x['wf']['id'])):
            # print(data['wf']['id'])
            if int(data['wf']['id']) > 999999 or int(data['wf']['id']) in [0, ] or data['wf']['name'] in wfs:
                # for eid in data['connectedEntities']:
                #     if eid != 941010000 and e_ids.get(eid):
                #         if e_ids[eid].get('parentId'):
                #             used_eids[e_ids[eid].get('parentId')] = e_ids[e_ids[eid].get('parentId')]
                #         used_eids[eid] = e_ids[eid]

                # cat = next((c["id"] for k, c in cats.items() if c["name"] == data['wf']['category']), None)
                # used[cat] = cats[cat]
                # if cats[cat].get('parentId'):
                #     used[cats[cat].get('parentId')] = cats[cats[cat].get('parentId')]

                j = dict()
                j['date'] = pendulum.now(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
                j['version'] = ver['version'] + ' ' + ver['ci']
                j['wfExt'] = data
                
                yaml_data = dict()
                yaml_data['workflows'] = list()
                wf_yml(None, data, yaml_data, safe=safe)

                key = f"ctl_yaml/ctl_wf_{data['wf']['id']}_{data['wf']['name']}"
                ctl_obj_save(key, yaml_data, ext='yml')
                logger.debug(yaml_data)
                find.append(data['wf']['name'])
        
        add_note({'Find': find}, context, level='DAG,Task')

        not_found = [w for w in wfs if w not in find]
        if not_found:
            add_note({'Not found': not_found}, context, level='DAG,Task')

    get_yml()
