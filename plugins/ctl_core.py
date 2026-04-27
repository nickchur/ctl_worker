"""### 🛠️ Ядро логики CTL (`plugins/ctl_core.py`)

Центральные функции бизнес-логики, используемые всеми DAG'ами CTL.

| Функция | Описание |
|---|---|
| `ctl_set_status` / `ctl_chk_status` | Обновление и проверка статуса загрузки |
| `ctl_loading_norm` / `ctl_loading_load` | Нормализация и массовая загрузка активных процессов |
| `ctl_get_retry` | Извлечение и восстановление конфигурации retry |
| `ctl_chk_wait` / `ctl_chk_new` | Управление TIME-WAIT и готовностью к запуску |
| `ctl_events_mon` / `ctl_chk_expire` | Мониторинг EVENT-WAIT (OR/AND), таймаут ожидания |
| `ctl_wf_norm` / `ctl_get_eids` | Нормализация workflow, список участвующих сущностей |
| `ctl_send_html` | Потоковая отправка HTML-отчётов в CTL |
| `chk_any_conn` | Проверка доступности Postgres / S3 / KerberosHttp |
"""

from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.decorators import task, task_group
from airflow.operators.python import get_current_context

from datetime import timedelta
import ast
import pendulum
import time
import json

from plugins.utils import query_to_dict, pool_slots, on_callback, add_note # type: ignore
from plugins.ctl_utils import get_config, ctl_api, ctl_obj_load, eval_delta, logging, ctl_obj_save # type: ignore

from logging import getLogger
logger = getLogger("airflow.task")

# Константа: максимальная длина фрагмента HTML
MAX_HTML = 5000

icons ='📡 📝 🔍'

alive_icons = {
    "ACTIVE": "⚠️",    # Процесс активен
    "COMPLETED": "✳️", # Завершено успешно
    "ABORTED": "💀"     # Прервано/Отменено
}

status_icons = {
    "INIT": "⚪",
    "TIME-WAIT": "⏳",
    "EVENT-WAIT": "🔔",
    "LOCK-WAIT": "🔒",
    "PREREQ": "📋",
    "LOCK": "🔐",
    "PARAM": "⚙️",
    "START": "🚀",
    "RUNNING": "🔵",
    "SUCCESS": "✅",
    "ERROR": "❌",
    "ERRORCHECK": "🔄",
    "ABORTING": "🛑",
}


step_icons = {
    # RUNNING
    "WAIT": "⏱️ ",
    "WAIT-AF": "⏳",
    "NEW-AF": "🆕",
    "TFS": "🔍", 
    "TFS-OK": "✅",
    "RUN": "🚀",
    "END": "🏁",
}

def ctl_set_status(lid, status, log=''):
    """Устанавливает статус загрузки lid через PUT /v4/api/loading/{lid}/status."""
    lid = int(lid)
    data = {'loading_id': lid, 'status': status, 'log': str(log)}
    return ctl_api(f'/v4/api/loading/{lid}/status', 'put', json=data)

def ctl_set_completed(lid, action=None, completed=True):
    """Завершает загрузку lid: PUT /v4/api/loading/{lid}/completed (или aborted/другой action)."""
    if not action:
        action = 'completed' if completed else 'aborted'
    return ctl_api(f'v4/api/loading/{lid}/{action.lower()}', 'put') # Completed/Aborted

def ctl_get_status(ld):
    """Разбирает status_log загрузки и возвращает нормализованный dict: lid, alive, status, step, data, msg, run_type."""
    logger.debug(ld)
    log = ld.get('status_log','')
    try:
        data = ast.literal_eval(log)
        step = ''
        msg = ''
    except (ValueError, SyntaxError):
        try: 
            step, data = log.split(' ', 1)
            data = ast.literal_eval(data)
            msg = ''
        except (ValueError, SyntaxError):
            try: 
                step, msg = log.split(' ', 1)
                data = {}
            except (ValueError, SyntaxError):
                step = log
                msg = ''
                data = {}
    return {
        'lid': ld['id'],
        'start_dttm': ld.get('start_dttm'),
        'alive': ld['alive'],
        # 'alive_icon': alive_icons[ld['alive']],
        'status_sdt': ld.get('status_sdt'),
        'status': ld['status'],
        # 'status_icon': status_icons[ld['status']],
        'step': step,
        'data': data,
        'msg': msg,
        # 'status_log': log,
        # 'end_dttm': ld.get('end_dttm', ''),
        'run_dttm': ld.get('ld_status_last', {}).get('START',''),
        'run_type': ld.get('ld_run_type', '')
    }


def ctl_chk_status(lid, wf_name, alive=None, status=None, step=None, log_empty=None, save=True):
    """Проверяет, что загрузка lid находится в ожидаемых alive/status/step/log_empty.

    Нормализует и сохраняет загрузку в S3. При несоответствии — AirflowSkipException.
    Возвращает ld_sts (результат ctl_get_status) при успешной проверке.
    """
    
    lid = int(lid)
    if isinstance(alive, str): alive = [alive]
    if isinstance(status, str): status = [status]
    if isinstance(step, str): step = [step]
    
    ld = ctl_api(f"/v4/api/loading/{lid}")
    ld = ctl_loading_norm(wf_name, ld)
    if save: ctl_obj_save(f"ctl_working/{lid}", ld, var=False)
    ld_sts = ctl_get_status(ld)
    msg = ''
    ld_alive = ld_sts['alive']
    ld_status = ld_sts['status']
    ld_step = ld_sts['step']
    ld_log = ld_sts['msg'] or ld_sts['data']
    
    if alive and ld_alive not in alive: 
        msg = f'Bag state. Not {alive} !!! {alive_icons[ld_alive]} {ld_alive}'
    
    elif status and ld_status not in status:
        msg = f'Bag status. Not {status} !!! {status_icons[ld_status]} {ld_status}'
    
    elif step and ld_sts['step'] not in step:
        msg = f'Bag step. Not {step} !!! {step_icons.get(ld_step, "⚠️")} {ld_step}'
    
    elif log_empty is not None:
        if log_empty and not ld_log: 
            msg = f'⚠️ Bag log. Not Empty !!! {ld_log}'
        elif not log_empty and ld_log: 
            msg = f'⚠️ Bag log. Empty !!! {ld_log}'
            
    if msg:
        # msg += f" {lid}"
        add_note(ld_sts, level='Task,DAG', title=msg)
        raise AirflowSkipException(msg)

    return ld_sts


def ctl_get_retry(params=None, wf=None, logs=None, retry=None):
    """
    Полная логика получения/восстановления конфигурации retry.
    Поддерживает: log → params → wf.faultTolerance.
    """
    params = params or {}
    wf = wf or {}
    logs = logs or [params.get('wfp_status_log')] if params.get('wfp_status_log') else []

    # 1. Явно переданный retry
    if isinstance(retry, dict) and retry.get('try'):
        return retry

    # 2. Из параметров
    wfp_retry = params.get('wfp_retry')
    if wfp_retry:
        try:
            retry = ast.literal_eval(wfp_retry) if isinstance(wfp_retry, str) else wfp_retry
        except (ValueError, SyntaxError):
            logger.warning(f"Invalid wfp_retry: {wfp_retry}")
            pass
    
        if isinstance(retry, dict) and retry.get('try'):
            return retry
    

    # 3. Извлечение из лога
    for log in logs:
        if not isinstance(log, str): continue
        try:
            start, end = log.find('{'), log.rfind('}') + 1
            if start != -1 and end > start:
                extracted = ast.literal_eval(log[start:end])
                retry = extracted.get('retry') if isinstance(extracted, dict) and 'retry' in extracted else extracted
                if isinstance(retry, dict) and retry.get('try'):
                    return retry
        except (ValueError, SyntaxError):
            pass

    # 4. Создание нового retry
    retry = {}
    if params.get('wf_retry_on'):
        retry['on'] = [r.strip().lower() for r in params['wf_retry_on'].split(',') if r.strip()]

    retry_cnt = int(params.get('wf_retry_cnt', 1))
    ft = wf.get('faultTolerance', {})

    if retry_cnt > 1:
        retry.update({
            'try': 1,
            'left': retry_cnt - 1,
            'delay': params.get('wf_retry_delay'),
            'add': params.get('wf_retry_add'),
        })
    elif isinstance(ft, dict) and int(ft.get('numAttempts', 1)) > 1:
        retry.update({
            'try': 1,
            'left': int(ft['numAttempts']) - 1,
            'delay': f"seconds=+{int(ft.get('retryDelayMs', 0) // 1000)}" if ft.get('retryDelayMs') else None
        })

    return {k: v for k, v in retry.items() if v is not None} if retry.get('try') else {}


def ctl_chk_wait(wf, params, context):
    """Отложенный запуск по `wf_wait`."""
    lid = params['loading_id']
    sdt = params['af_sdt'][:19]
    pause = params['wf_wait']
    new_time = eval_delta(sdt, pause)

    # data = {'loading_id': lid, 'status': 'TIME-WAIT', 'log': str({'time': new_time, 'wait': pause})}
    # ctl_api(f'/v4/api/loading/{lid}/status', 'put', json=data)
    # logging(f"{wf['name']} wait: {new_time}", action='wait', obj=lid)
    ctl_set_status(lid, 'TIME-WAIT', {'time': new_time, 'wait': pause})

    ret = {"action": "⌚ wf_wait", "id": lid, "name": wf['name'], "msg": f"TIME-WAIT {new_time}"}
    context['task_instance'].xcom_push(key='result', value=ret)
    add_note(ret, context, level='Task', title='⌚ WF-WAIT')
    raise AirflowSkipException(ret)


def ctl_chk_new(lid, wf_name, status, log, context):
    """Проверяет, можно ли запустить задачу (новая или TIME-WAIT истек)."""
    now = pendulum.now(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
    # lid = params['loading_id']
    # status = params['wfp_status']
    # log = params['wfp_status_log']
    # wf_name = wf['name']

    if status == 'TIME-WAIT':
        try:
            r = ast.literal_eval(log)
        except Exception as err:
            logging(f"Ошибка при парсинге TIME-WAIT: {err}", action='error', obj=lid)
            raise AirflowFailException(err)
        
        sdt = r['time']
        if sdt <= now:
            retry = r.get('retry', {})
            if retry:
                retry['try'] = retry.get('try', 1) + 1
                retry['left'] = max(0, retry.get('left', 0) - 1)
            return retry, False
        else:
            ret = {"action": "⏳ time-wait", "id": lid, "name": wf_name, "msg": f"{sdt} > {now}"}
            context['task_instance'].xcom_push(key='result', value=ret)
            add_note(ret, context, level='Task', title='⏳ TIME-WAIT')
            raise AirflowSkipException(ret)
        

    # retry = ctl_get_retry(wf=wf, params=params)
    return {}, True


def ctl_send_html(html_content, lid, eid):
    """
    Отправляет HTML-содержимое в CTL как статистику (statval), разбивая на фрагменты
    по MAX_HTML символов с учётом целостности HTML-тегов.

    Параметры:
        ret (dict): Словарь с ключом 'html' (строка или список).
        lid (int): ID загрузки.
        eid (int): ID сущности.

    Логика:
    - Если сообщение ≤ MAX_HTML — отправляется целиком.
    - Если > MAX_HTML и ≤ MAX_HTML * 10 — разбивается с сохранением <tr>.
    - Очень длинные (> MAX_HTML * 10) — пропускаются.
    """

    max_len = get_config().get('max_html', MAX_HTML)
    max_total = max_len * 10
    profile = get_config()['profile']
    stat_id = 12

    try:
        # Приводим к списку строк
        if isinstance(html_content, str):
            messages = json.loads(html_content)
        else:
            messages = html_content

        for n, msg in enumerate(messages):
            msg = str(msg)
            length = len(msg)

            # Если короче или равно порогу — отправляем целиком
            if length <= max_len:
                url = f'/v4/api/loading/{lid}/entity/{eid}/stat/{stat_id}/statval?profile={profile}'
                jsn = [f"{n:3}{length:6} {msg}"]
                ctl_api(url, 'post', json=jsn)

            # Если длиннее, но не слишком — разбиваем с учётом HTML
            elif length <= max_total:
                hd, ft = '', ''  # префикс/суффикс для склейки
                pos = 0
                while pos < length:
                    chunk = msg[pos:pos + max_len]
                    if not chunk:
                        break

                    # На последнем фрагменте — просто отправляем остаток
                    if pos + max_len >= length:
                        s = hd + chunk
                        url = f'/v4/api/loading/{lid}/entity/{eid}/stat/{stat_id}/statval?profile={profile}'
                        jsn = [f"{n:3}{(pos - len(hd)):6}{len(s):6} {s}"]
                        ctl_api(url, 'post', json=jsn)
                        break

                    # Ищем последнее закрытие строки для "красивого" разрыва
                    cut_pos = chunk.rfind('</tr>') + 5
                    cut_pos = cut_pos if cut_pos > 5 else max_len
                    s = hd + msg[pos:pos + cut_pos]

                    # Подготавливаем суффикс для следующего фрагмента
                    if cut_pos == max_len:
                        ft = '<tr id="np"><td>'
                    else:
                        ft = ''

                    url = f'/v4/api/loading/{lid}/entity/{eid}/stat/{stat_id}/statval?profile={profile}'
                    jsn = [f"{n:3}{(pos - len(hd)):6}{len(s):6} {s}{ft}"]
                    ctl_api(url, 'post', json=jsn)

                    hd = '</td></tr>' if ft else ''
                    pos += cut_pos

            # Сообщения длиннее max_total — пропускаем
            else:
                continue

    except Exception as err:
        logger.error(f"[ERROR] Failed to send HTML to statval: {err}")
        raise


def ctl_loading_norm(wf_name, data):
    """Нормализует сырой dict загрузки из API: разворачивает params/stats, определяет run_type,
    строит ld_status_last (статус→дата) и loading_status (дата→'статус лог'). Возвращает отсортированный dict.
    """
    j = data.copy()
    j['params'] = {
        p['param']: p.get('value')
        for p in sorted(j.get('params', []), key=lambda x: x['param'])
        if p.get('param').startswith('wf') or 'loading' in p.get('param')
    }
    # j['params']['wfp_name'] = wf_name
    # j['wf_name'] = j['workflow']['name'] if j.get('workflow') else wf_name
    j['wf_name'] = wf_name

    if j.get('stats'):
        j['stats'] = {
            f"{s['loading_id']}/{s['profile']}/{s['entity_id']}/{s['stat_id']}": s.get('value')
            for s in sorted(j.get('stats', []), key=lambda x: (x['stat_id'], x.get('value', '')))
        }

    if not j['params'].get('wfp_run_type'):
        run_type = 'MANUAL' if not j['auto'] else sorted(j['loading_status'], key=lambda x: x['effective_from'])[0]['status']
        j['params']['wfp_run_type'] = run_type if run_type in ("TIME-WAIT", "EVENT-WAIT", "MANUAL") else "NO-WAIT"
    
    if not j.get('ld_run_type'): j['ld_run_type'] = j['params'].get('wfp_run_type')

    try:
        j['status_sdt'] = j['loading_status'][0]['effective_from']
    except IndexError:
        j['status_sdt'] = j['start_dttm']
        
    # j['params']['wfp_status_sdt'] = j['status_sdt']
    # j['params']['wfp_status'] = j['status']
    # j['params']['wfp_status_log'] = j.get('status_log', '')

    j['ld_status_last'] = {
        p['status']: p['effective_from'] #+ ' ' + p.get('log', '')
        for p in sorted(j.get('loading_status', []), key=lambda x: x['effective_from'], reverse=False)
    }

    j['loading_status'] = {
        p['effective_from']: p['status'] + ' ' + p.get('log', '')
        for p in sorted(j.get('loading_status', []), key=lambda x: x['effective_from'], reverse=True)
    }

    return dict(sorted(j.items()))


def ctl_loading_load(prm, save=True):
    """Запрашивает загрузки через /v5/api/loading/extended, нормализует каждую и опционально сохраняет в S3.

    Применяет ctl_limit из конфига. Возвращает список нормализованных dict-загрузок.
    """
    if get_config().get('ctl_limit') > 0:
        prm['limit'] = get_config()['ctl_limit']

    data = ctl_api("/v5/api/loading/extended", data=prm)
    logger.info(f"🔍 Load Loading: {len(data)}")

    wfs_dict = ctl_obj_load('ctl_workflows')
    lids = []
    for j in sorted(data, key=lambda x: x['id']):
        wf_name = wfs_dict.get(str(j['wf_id']), {}).get('name', '')
        j = ctl_loading_norm(wf_name, j)
        lids.append(j)
        if save:
            ctl_obj_save(f"ctl_loadings/{j['profile']}/{j['id']}", j, var=False)
    return lids


def chk_any_conn(id, data=None, **context):
    """Проверяет доступность соединения id (Postgres / S3 / KerberosHttp).

    data — dict с type/conn_id/pool_slots; при отсутствии берётся из get_config()['conns'][id].
    При успехе обновляет pool_slots и пишет заметку в Airflow. При ошибке — обнуляет слоты и пробрасывает исключение.
    """
    
    data = data if data else get_config().get('conns', {}).get(id, {})
    context = context if context else get_current_context()

    ti = context['ti']
    try_number = ti.try_number
    sdt = pendulum.instance(ti.start_date).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss zz')
    ts = time.time()
    try:
    # if True:
        if data['type'] == 'Postgres':
            if data.get('default'):
                from airflow.utils.session import create_session
                from sqlalchemy import text
                with create_session() as session:
                    result = dict(session.execute(text(
                        'SELECT current_user, current_database(), inet_server_addr()'
                    )).fetchone())
            else:
                from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
                hook = PostgresHook(postgres_conn_id=data['conn_id'])
                result = query_to_dict(hook, 'SELECT current_user, current_database(), inet_server_addr()', timeout=10)[0]
            
        elif data['type'] == 'S3':
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook # type: ignore
            hook = S3Hook(aws_conn_id=data['conn_id'])
            result = hook.get_conn().list_buckets()['Buckets']
            
        elif data['type'] == 'KerberosHttp':
            from hrp_operators.utils.kerberos_http import KerberosHttpHook # type: ignore
            hook = KerberosHttpHook(method='GET', http_conn_id=data['conn_id'])
            response = hook.run('/v5/api/info', headers={'Accept': 'application/json'})
            response.raise_for_status()
            result = response.json()
        else:
            result = None
        
        if data.get('pool_slots'):
            pool_slots(f'{id}_pool', slots=data.get('pool_slots'))
        
        logger.info(f"🔍 {result}")       
        msg = f"✅ {time.time()-ts:.2f} sec chk_{id}_conn"
        add_note({'try':try_number, 'sdt':sdt}, context, title=msg)
        
    except Exception as err:
        if data.get('pool_slots') and not data.get('default', False):
            pool_slots(f'{id}_pool', slots=0)
        
        response = getattr(err, 'response', None)
        logger.error(response)      
        msg = f"❌ {time.time()-ts:.2f} sec chk_{id}_conn ERROR Try {try_number} {sdt}"
        add_note(err, context, level='Task,DAG', title=msg)
        # raise AirflowFailException(msg)
        raise err

        
def ctl_wf_norm(wf, connectedEntities=None):
    """Нормализует workflow dict из API: разворачивает param→params, statusNotifications, wf_event_sched.

    Вычисляет wf_entity (набор entity_id) через ctl_get_eids. Возвращает обогащённый wf dict.
    """
    wf['params'] = {p['param']: p.get('prior_value') for p in wf.get('param', [])}
    wf['statusNotifications'] = {p['status']: p.get('emails') for p in wf.get('statusNotifications', [])}
    wf.pop('param', None)
    
    eids = ctl_get_eids(wf['id'], wf['params'], connectedEntities)
    if eids: 
        wf['params']['wf_entity'] = ','.join(map(str, sorted(eids)))
    
    events = wf.get('wf_event_sched', [])
    if not events and wf['params'].get('wf_event_sched'):
        try:
            events = ast.literal_eval(wf['params'].get('wf_event_sched','[]'))
        except:
            pass
    
    if events and type(events) is list:
        # wf['params']['wf_event_type'] = wf.get('eventAwaitStrategy','or')
        wf['wf_event_sched'] = {f"{e.get('profile')}/{e.get('entity_id')}/{e.get('stat_id')}": e.get('active', True) for e in events }
    elif events and type(events) is str:
        wf['wf_event_sched'] = {e.strip(): True for e in events.split(',') if e.strip()}

    return wf


def ctl_chk_expire(wf, params, context):
    """Проверка выполнения всех событий (EVENT-WAIT)"""
    ti = context['task_instance']
    lid = params['loading_id']
    sdt = params['af_sdt'][:19]
    run_type = params['wfp_run_type']

    wf_expire = params.get('wf_expire') or get_config().get('event_expire') or get_config().get('expire')
    
    if not ( wf_expire
        and wf.get('eventAwaitStrategy', 'or') == 'and'
        and run_type in ["EVENT-WAIT", "AF-EVENT"]
        # and len(wf.get('wf_event_sched', [])) > 1
    ):
        logger.info(f"🚀 {run_type}: {wf.get('eventAwaitStrategy')} {wf_expire}")
        return {}

    event = {}
    exp = False
    ready = False
    for s, act in wf.get('wf_event_sched', {}).items():
        prf, eid, sid = s.split('/')
        # eid, sid, prf, act = s['entity_id'], s['stat_id'], s['profile'], s.get('active', True)
        if not act:
            event[eid] = 'Off'
            continue
        
        last = ctl_api(f'/v4/api/entity/{eid}/stat/{sid}/statval/last?profile={prf}')
        last_dt = (last[0] or {}).get('published_dttm', '') if last else ''
            
        if not last_dt or last_dt < eval_delta(sdt, wf_expire):
            event[eid] = '🔔 ' + (last_dt[:19] if last_dt else 'Never')
            exp = True
        else:
            event[eid] = '🚀 ' + last_dt[:19] if last_dt else 'Never'
            ready = True

    event = dict(sorted(event.items()))
    event['and'] = wf_expire

    if exp and ready:
        ctl_set_status(lid, 'EVENT-WAIT', event)
        
        ret = {"action": "🔔 event-wait", "id": lid, "name": wf['name'], "msg": event}
        ti.xcom_push(key='result', value=ret)
        add_note(ret, context, level='Task', title='🔔 EVENT-WAIT')
        raise AirflowSkipException(ret)
    
    elif exp and not ready:
        ctl_set_status(lid, 'ERROR', {"msg":"💀 expired", **event, "res": -99})
        
        ret = {"action": "💀 expired", "id": lid, "name": wf['name'], "msg": event}
        ti.xcom_push(key='result', value=ret)
        add_note(ret , context, level='Task,DAG', title='💀 EXPIRED')

        ctl_api(f'v4/api/loading/{lid}/aborted', 'put') # Completed/Aborted
        raise AirflowSkipException(ret)
    
    else:
        add_note(event, context, level='Task', title='🚀 READY')
        return event
            


                        
def ctl_events_mon(sdt, wf, now):
    """
    Мониторинг событий в CTL.
    is_and = False (OR): Ждем хотя бы одно событие >= sdt.
    is_and = True (AND): Ждем, пока ВСЕ события станут >= sdt.
    """
    # sdt = wf['start_dttm'][:19]
    name = wf['name']
    
    events = wf.get('wf_event_sched', [])
    if not events:
        return {'chk': True}
    is_and = wf.get('eventAwaitStrategy') == 'and'
    
    # sdt должен быть строкой в формате ISO или сопоставимым типом
    sdt = str(sdt)
    chk = { 'and': is_and }

    empty = True
    for event, act in events.items():
        if not act:
            chk[event] = 'Off'
            continue
        else:
            empty = False
        
        # Получаем последнее значение статистики
        prf, eid, sid = event.split('/')
        last_data = ctl_api(f'/v4/api/entity/{eid}/stat/{sid}/statval/last?profile={prf}')
        
        # Безопасно извлекаем дату публикации (API обычно возвращает список)
        last_item = last_data[0] if isinstance(last_data, list) and last_data else {}
        last_dt = last_item.get('published_dttm', '')
        chk[event] = last_dt[:19]
        
        # if last_dt and now - datetime.strptime(last_dt[:19], "%Y-%m-%d %H:%M:%S") < timedelta(minutes=30):
        if last_dt and now - pendulum.parse(last_dt[:19], tz=get_config()['tz']) < timedelta(minutes=30):
            return {**last_item, 'chk': True, 'icon': '🔁'}

        if is_and:
            # Если стратегия AND и хотя бы одно событие ЕЩЕ НЕ наступило
            if not last_dt or last_dt < sdt:
                # logger.info(f"⏳ {name} Event {eid} {prf} {sid} not ready (AND strategy). Last: {last_dt} < {sdt}")
                return {**last_item, 'chk': True, 'icon': '⏳'} # Не готово, продолжаем ждать
        else:
            # Если стратегия OR и хотя бы одно событие наступило
            if last_dt and last_dt >= sdt:
                # logger.info(f"✅ {name} Event {eid} {prf} {sid} found (OR strategy). DT: {last_dt} >= {sdt}")
                return {**last_item, 'chk': False, 'icon': '✅'} # Найдено, выходим из цикла

    if empty:
        return {'chk': True}
    
    # Финальный результат:
    # При AND: если дошли сюда, все события подошли -> возвращаем False (ожидание окончено)
    # При OR: если дошли сюда, ни одно событие не подошло -> возвращаем True (нужно ждать дальше)
    if is_and: 
        # logger.info(f"✅ {name} ready (AND strategy) {sdt} {chk}")
        chk['icon'] ='✅'
    else:
        chk['icon'] ='⏳'

    return {**chk, 'chk': not is_and}


def ctl_get_eids(wid, wf_prm, connected=None):
    """Собирает entity_id для workflow wid: из wf_prm['wf_entity'], либо из /v4/api/wf/{wid}/entity.

    Возвращает set[int]. connected — опциональный список entity_id из connectedEntities.
    """
    wid = int(wid)
    

    # Собираем сущности, участвующие в событиях
    # 1. Безопасно получаем список ID из параметров (убираем пустые строки и пробелы)
    # prm_data = wf_prm.get('wf_entity', ','.join(connected)) or ''
    prm_data = wf_prm.get('wf_entity', ','.join(map(str, connected or []))) or ''
    prm_eids = {int(e.strip()) for e in prm_data.split(',') if e.strip().isdigit()}
    
    if not prm_eids:
        # 2. Получаем данные из API в set
        api_data = ctl_api(f'/v4/api/wf/{wid}/entity') or []
        api_eids = {int(i['id']) for i in api_data if 'id' in i}
        eids = api_eids  | prm_eids
    else:
        eids = prm_eids
    # if eids:
    #     wfs[wid]['entity'] = ','.join(map(str, sorted(eids)))
    return eids

