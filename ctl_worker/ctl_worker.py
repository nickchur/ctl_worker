"""### ⚙️ DAG: `CTL.{wf_name}` — Рабочий процесс
*2026-09-04 14:30 MSK · v1.6 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Динамически генерируемый DAG для выполнения ETL-загрузок CTL.
Поддерживает расписание: `Dataset`, `Cron`, `DatasetOrTimeSchedule`, `startCondition (AND/OR)`.

| Задача | Описание |
|---|---|
| `run_prm` | Инициализация, создание `loading_id` в CTL, определение типа запуска |
| `run_tfs` | (опц.) Загрузка файлов из S3/TFS в Greenplum |
| `run_exe` | Выполнение SQL-процедуры в Greenplum |
| `run_out` | (опц.) Экспорт данных, генерация Dataset-событий |
| `run_end` | Публикация метрик, финализация загрузки |

🧪 Тестовый режим (`test_mode` в `ctl_config`, только контуры DEV и IFT): вместо процедуры
воркфлоу выполняется ожидание, а код результата берётся из профиля — `ok`, `ok-no` или
`ok-no-error`. Параметр воркфлоу из CTL режим не включает.
"""
# Airflow 2.10.1

from airflow import DAG

from airflow.datasets import DatasetAlias, Dataset
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

from airflow.utils.state import State
from airflow.utils.session import create_session #, provide_session
from airflow.models import  Param, TaskInstance

# from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException

# from datetime import timedelta
# from functools import partial
import ast
import time
import json
import pendulum
import random
import re

from datetime import timedelta, datetime, timezone
from functools import reduce
from operator import and_, or_
from pprint import PrettyPrinter


from plugins.utils import add_note, env_stand, on_callback, str2timedelta, update_dag_pause, safe_eval, readable  # type: ignore
from plugins.ctl_utils import get_config, gp_exe, ctl_obj_load, ctl_api, eval_delta, gp_upload_s3_csv  # type: ignore
from plugins.s3_utils import s3_move_s3, s3_keys, s3_delete  # type: ignore
from plugins.ctl_core import (ctl_send_html, ctl_get_retry, ctl_chk_expire, ctl_chk_status, status_icons, raise_status,  # type: ignore
                              ctl_get_eids, chk_any_conn, ctl_set_status, ctl_set_completed, ctl_loading_snapshot,
                              ctl_exe_recover)

from logging import  getLogger
logger = getLogger('airflow.task')

# repr по умолчанию печатает адрес объекта — <...DatasetOrTimeSchedule object at 0x7f40…>,
# и он в каждом процессе разбора свой. Через doc_md адрес попадает в сериализацию, dag_hash
# меняется на каждом парсинге, и serialized_dag переписывается вхолостую раз в
# min_file_process_interval. default=str не помогает: str() у такого объекта — тот же repr.
# (?=>) обязателен: адрес должен закрывать repr, иначе вычистили бы и текст в данных,
# где встретилось " at 0x…"
_ADDR_RE = re.compile(r" at 0x[0-9a-fA-F]+(?=>)")


def pformat(obj) -> str:
    """PrettyPrinter без адресов объектов: тип виден, id() отброшен."""
    return _ADDR_RE.sub("", PrettyPrinter(indent=4, width=200, compact=True).pformat(obj))

# Справочники
profile = get_config()['profile']
archive = get_config().get('archive_category', 'p1080.ARCHIVE')

# cat_dict = ctl_obj_load('ctl_categories')
# wfs_dict = ctl_obj_load('ctl_workflows')
# ent_dict = ctl_obj_load('ctl_entities')
enames = {int(k):v for k,v in ctl_obj_load('ctl_enames').items()}

# 🎛️ Режим оркестрации: кто владеет расписаниями и зависимостями (openspec/project.md).
#   ctl   — CTL; фабрика дагу расписания не строит;
#   mixed — как сложилось: нет расписания в CTL, значит расписание строит Airflow;
#   af    — Airflow; расписание строится всегда.
# Ключа нет — значит mixed: выкладка кода сама по себе поведения не меняет.
ORCHESTRATOR_MODES = ('ctl', 'mixed', 'af')


def _orchestrator() -> str:
    value = str(get_config().get('orchestrator', 'mixed')).strip().lower()
    if value not in ORCHESTRATOR_MODES:
        logger.warning("orchestrator = %s: неизвестный режим, беру mixed. Известные: %s",
                       value, ', '.join(ORCHESTRATOR_MODES))
        return 'mixed'
    return value


ORCHESTRATOR = _orchestrator()


def pause_new_dag(is_scheduled: bool) -> bool:
    """Создавать ли даг запаузенным.

    Раньше это было следствием поля `scheduled`, и в режиме `ctl` то же правило блокировало
    ручной запуск из CTL: запуск паузу не проверяет, ран уходит в очередь и не едет.
    Теперь настройка своя, а `auto` разворачивается по режиму:

      * `ctl`   — не паузим: расписаний у дагов нет, пауза только мешает;
      * `mixed` — паузим тех, у кого расписание строит Airflow (как было);
      * `af`    — паузим всех: даг со своим расписанием не должен стартовать сам, пока его
        не «взвели», иначе первая же выкладка даёт стампед из сотен дагов.
    """
    value = str(get_config().get('pause_new_dags', 'auto')).strip().lower()
    if value in ('yes', 'true', '1'):
        return True
    if value in ('no', 'false', '0'):
        return False
    return {'ctl': False, 'mixed': not is_scheduled, 'af': True}[ORCHESTRATOR]


# 🌍 Тестовый режим: выражение воркфлоу подменяется ожиданием, код результата — своим.
# Список контуров — константа в коде, а не ключ конфигурации: контур это единственный
# признак, которым нельзя управлять ни из ctl_config, ни из параметров воркфлоу в CTL.
# Пустое и незнакомое имя приравниваем к бою — переменная выставлена на всех контурах.
TEST_STANDS = ('DEV', 'IFT')
STAND = env_stand()

# Коды исхода и веса по контракту pr_swf_start_ctl (GP/readme.md): 1 — успех, 0 — нет
# данных, отрицательные — ошибка, причём -7 уводит в циклический повтор и TIME-WAIT.
# Успех тяжелее прочих: иначе на стенде до конца не доходит ничего.
TEST_PROFILES = {
    'ok':           ([1],                 [1]),
    'ok-no':        ([1, 0],              [3, 1]),
    'ok-no-error':  ([1, 0, -1, -2, -7],  [3, 1, 1, 1, 1]),
}
# Прежнее истинное значение ключа означало ровно random.randint(0,2) — успех и «нет
# данных», ошибка недостижима. Это профиль ok-no, только под старым именем.
TEST_LEGACY = ('true', '1', 'yes', 'event', 'dataset', 'trigger')
TEST_OFF = ('', 'off', 'false', 'none', '0')


def test_profile(context=None):
    """Профиль тестового режима: имя из `TEST_PROFILES` или None, если режима нет.

    Источник один — `ctl_config`; параметр воркфлоу из CTL режим не включает. Контур не
    из `TEST_STANDS` гасит режим, и это не должно быть молчаливым: иначе «почему на бою
    ничего не подделывается» станет загадкой на полдня, а фиктивный успех на бою —
    незамеченным.
    """
    mode = str(get_config().get('test_mode', 'off')).strip().lower()
    if mode in TEST_OFF:
        return None

    if mode in TEST_LEGACY:
        logger.warning(f"test_mode = {mode}: устаревшее значение, читаем как ok-no. "
                       f"Новые значения: {', '.join(TEST_PROFILES)}")
        mode = 'ok-no'

    if mode not in TEST_PROFILES:
        msg = f"⚠️ test_mode = {mode}: неизвестный профиль, тестовый режим выключен"
        logger.warning(msg)
        if context: add_note(msg, context, level='task')
        return None

    if STAND not in TEST_STANDS:
        msg = (f"⚠️ Тестовый режим ({mode}) игнорируется: контур {STAND or 'не задан'}, "
               f"режим разрешён на {'/'.join(TEST_STANDS)}. Загрузка выполняется по-настоящему")
        logger.warning(msg)
        if context: add_note(msg, context, level='task')
        return None

    return mode


def test_res(mode):
    """Код результата по профилю — вместо настоящего ответа Greenplum."""
    codes, weights = TEST_PROFILES[mode]
    return random.choices(codes, weights=weights)[0]


def statval(data, log=False, logs=None):
    if log:
        logger.info(dict(url='/statval/m', method='post', data=data))
    if isinstance(logs, dict):
        logs[len(logs)] = str(data)
    elif isinstance(logs, list):
        logs.append(str(data))
    lid = data.get('loading_id')
    eid = data.get('entity_id')
    sid = data.get('stat_id')
    url = f'/v4/api/loading/{lid}/entity/{eid}/stat/{sid}/statval?profile={profile}'
    return ctl_api(url, 'post', json=data['avalue'])

def get_params(context):
    ti = context['task_instance']
    wf_prm = ti.xcom_pull(key='params', task_ids='run_prm')
    if not wf_prm:
        msg = "⚠️Params is empty"
        add_note(msg, context, level='task,DAG')
        raise AirflowSkipException(msg)
    return wf_prm

def parse_condition(cond, enames):
    ctype = cond.get('$type')
    
    # 1. Базовые элементы
    if ctype == 'statVal':
        e_id = cond['entityId']
        return Dataset(f"CTL/{cond['profile']}/{e_id}/{enames.get(e_id, 'unknown')}")
    
    if ctype == 'cronExpression':
        return CronTriggerTimetable(cond['expr'], timezone="Europe/Moscow")
    
    # 2. Рекурсивная сборка (OR / AND)
    if ctype in ('or', 'and'):
        datasets = []
        timetable = None
        
        for c in cond.get('inner', []):
            res = parse_condition(c, enames)
            if isinstance(res, CronTriggerTimetable):
                timetable = res  # Запоминаем крон
            elif res:
                datasets.append(res)
        
        # Объединяем найденные датасеты
        op = or_ if ctype == 'or' else and_
        combined_ds = reduce(op, datasets) if datasets else None
        
        # 3. Финальная сборка узла
        if timetable and combined_ds:
            if ctype == 'or':
                # Обязательно список [combined_ds]
                return DatasetOrTimeSchedule(timetable=timetable, datasets=[combined_ds])
            else:
                logger.warning("parse_condition: AND + timetable is not supported, schedule will be None")
                return None
            #     return DatasetAndTimeSchedule(timetable=timetable, datasets=[combined_ds])

        return combined_ds or timetable

    return None

def get_schedule(w, enames):
    wf_params = w.get('params', {})
    is_dataset_run = get_config().get('dug_run') == 'dataset'
    
    # 1. Начальный schedule
    schedule = Dataset(f"CTL/wf/{w['id']}/{w['name']}", extra={}) if is_dataset_run else None
    
    # 2. Если воркер в архиве — возвращаем как есть
    if w.get('category') == archive:
        return schedule, "ARCHIVE"

    # 3. Определение тега (упрощенный маппинг)
    is_scheduled = w.get('scheduled', False)
    # Кто строит расписание — решает режим, а не одно поле scheduled: в ctl не строим
    # никому, в af строим всем, в mixed — тем, кого не ведёт CTL (как было).
    owns_schedule = ORCHESTRATOR == 'af' or (ORCHESTRATOR == 'mixed' and not is_scheduled)
    prefix = 'AF_' if owns_schedule else 'CTL_'
    
    if w.get('wf_event_sched'):   tag_type = 'event'
    elif w.get('wf_time_sched'):  tag_type = 'time'
    elif w.get('startCondition'): tag_type = 'condition'
    else:                         tag_type = 'manual'
    
    tag = f"{prefix}{tag_type}"
    
    # 4. Обработка логики расписания — только там, где им владеет Airflow
    if not owns_schedule and (tfs_path := wf_params.get('wf_tfs_in')):
        # ТФС и Kafka — зона Airflow при ЛЮБОМ режиме: CTL про файлы не знает и знать не
        # должен. Раньше датасет получали только воркфлоу, которых не ведёт CTL, — то есть
        # у воркфлоу «на расписании в CTL и с источником ТФС» приход файла не поднимал
        # ничего. Это расходилось с границей ответственности: режим делит расписания и
        # зависимости, а не источники событий.
        ds = Dataset(f"TFS/{profile}/{tfs_path.lstrip('/')}")
        schedule = (schedule | ds) if schedule else ds
        tag = 'AF_tfs-in'

    if owns_schedule:
        # Вариант TFS
        if tfs_path := wf_params.get('wf_tfs_in'):
            ds = Dataset(f"TFS/{profile}/{tfs_path.lstrip('/')}")
            schedule = (schedule | ds) if schedule else ds
            tag = 'AF_tfs-in'

        # Вариант Events
        elif event_sched_dict := w.get('wf_event_sched'):
            datasets = [
                Dataset(f"CTL/{pes.split('/')[0]}/{pes.split('/')[1]}/{enames.get(int(pes.split('/')[1]), pes.split('/')[1])}")
                for pes, active in event_sched_dict.items() if active
            ]
            if datasets:
                op = and_ if w.get('eventAwaitStrategy') == 'and' else or_
                event_combined = reduce(op, datasets)
                schedule = (schedule | event_combined) if schedule else event_combined

        # Вариант Time
        elif time_sched := w.get('wf_time_sched'):
            cron = CronTriggerTimetable(time_sched.get("sched", '0 0 1 1 *'), timezone="Europe/Moscow")
            schedule = DatasetOrTimeSchedule(timetable=cron, datasets=[schedule]) if schedule else cron
        
        elif cond_dict := w.get('startCondition'):
            parsed_cond = parse_condition(cond_dict, enames)
            schedule = parsed_cond
            
            # if parsed_cond:
            #     schedule = (schedule | parsed_cond) if schedule else parsed_cond

    return schedule, tag

def set_pause(wf_name, cat):
    
    if get_config().get('is_paused'):
        is_paused = True
    elif get_config().get('un_paused') and cat != archive:
        is_paused = False
    else:
        is_paused = None
    
    if is_paused is not None:
        try:
            # Находим запись дага в БД и ставим is_paused 
            update_dag_pause(dag_id = f'CTL.{wf_name}', paused=is_paused)
        except Exception as e:
            logger.debug(f"Failed to update pause state for {wf_name}: {e}")


def _publish_stats(lid, eid, result):
    res = int(result['res']) if result.get('res') is not None else -99
    logs = {}
    statval({'loading_id': lid, 'entity_id': eid, 'profile_name': profile, 'stat_id': 11,
             'avalue': [pendulum.now(get_config()["tz"]).format('YYYY-MM-DD HH:mm:ss')]}, logs=logs)
    if res > 0:
        statval({'loading_id': lid, 'entity_id': eid, 'profile_name': profile, 'stat_id': 2,
                 'avalue': ['1']}, logs=logs)
        if result.get('cdc'):
            statval({'loading_id': lid, 'entity_id': eid, 'profile_name': profile, 'stat_id': 1,
                     'avalue': [str(result['cdc'])]}, logs=logs)
        if result.get('hub'):
            statval({'loading_id': lid, 'entity_id': eid, 'profile_name': profile, 'stat_id': 56,
                     'avalue': [json.dumps(result['hub'])]}, logs=logs)
            if result['hub'].get('dataBusiness'):
                statval({'loading_id': lid, 'entity_id': eid, 'profile_name': profile, 'stat_id': 55,
                         'avalue': [json.dumps(dict(dataBusiness=result['hub']['dataBusiness']))]}, logs=logs)
    if result.get('stat'):
        for k, v in result['stat'].items():
            statval({'loading_id': lid, 'entity_id': eid, 'profile_name': profile, 'stat_id': int(k),
                     'avalue': [v]}, logs=logs)
    return logs


def _emit_datasets(lid, eids, result, context):
    res = int(result['res']) if result.get('res') is not None else -99
    if result.get('html') and eids:
        ctl_send_html(result['html'], lid, int(eids[0].split('/')[0]))
        result['html'] = ''
    msg = {}
    for eid_str in eids:
        eid = int(eid_str.split('/')[0])
        logs = _publish_stats(lid, eid, result)
        ent_name = enames.get(eid, str(eid))
        ind = f'{lid}/{profile}/{eid}'
        ext = {
            f"{ind}/url": f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}",
            f"{ind}/dt": pendulum.now(get_config()["tz"]).format('YYYY-MM-DD HH:mm:ss'),
        }
        ent_key = f"{ind}/{ent_name}"
        msg.setdefault(ent_key, {})
        for k, l in logs.items():
            v = ast.literal_eval(l)
            msg[ent_key][v['stat_id']] = v['avalue'][0]
            ext[f"{ind}/{v['stat_id']}"] = v['avalue'][0]
        add_note(msg, context, level='task')
        if res > 0:
            ds = Dataset(f'CTL/{profile}/{eid}/{ent_name}')
            add_note(ds, context, level='task')
            outlet_key = f"CTL/{profile}/{eid}"
            if outlet_key in context['outlet_events']:
                context['outlet_events'][outlet_key].add(ds, extra=ext)
            else:
                logger.warning("outlet_events missing key %s — entity not in w_eids, dataset publish skipped", outlet_key)
    logger.info(f"✅ {context['outlet_events']}")
    for outlet, value in context['outlet_events'].items():
        add_note(value.extra, context, level='task', title=outlet)
    return msg


def _finalize_status(lid, wid, result, wf, wf_prm, context):
    sdt = (wf_prm.get('af_sdt') or '')[:19]
    retry = wf_prm.get('wfp_retry', {})
    res = int(result['res']) if result.get('res') is not None else -99

    tmpl = ctl_api(f'/v4/api/wf/{wid}/tmpl') or {}
    if tmpl.get('id') != 2:
        if tmpl.get('id'):
            ctl_api(f'/v4/api/wf/{wid}/tmpl/{tmpl["id"]}', 'delete')
        ctl_api(f'/v4/api/wf/{wid}/tmpl/2', 'put')

    if res > 0:
        res_msg, res_icon = 'ok', '✅'
        retry['ok'] = retry.get('ok', 0) + 1
    elif res == 0:
        res_msg, res_icon = 'no', '⚠️'
        retry['no'] = retry.get('no', 0) + 1
    else:
        res_msg, res_icon = 'error', '❌'
        retry['err'] = retry.get('err', 0) + 1

    retry_on = retry.get('on', ['error'])
    if retry.get('left', 0) > 0 and (res_msg in retry_on or str(res) in retry_on):
        status, action = 'ERRORCHECK', 'retry'
    else:
        if retry.get('try', 0) > 0:
            ctl_set_status(lid, 'RUNNING', f"END {retry}")
        if retry.get('ok', 0) > 0 or retry.get('no', 0) > 0:
            status, action = 'SUCCESS', 'Completed'
        else:
            status = 'ERROR'
            action = 'Aborted' if wf.get('faultTolerance', {}).get('abortOnFailure', False) else 'Completed'

    ctl_set_status(lid, status, result)

    # Ответ финализирующего вызова забираем, а не выбрасываем: в нём CTL отдаёт загрузку
    # уже с итоговым статусом, и это избавляет run_end от лишнего GET ради снимка.
    if action == 'retry':
        retry.setdefault('try', 1)
        now = pendulum.now(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
        new_time = eval_delta(now, retry.get('delay'))
        for _ in range(1, retry.get('try')):
            new_time = eval_delta(new_time, retry.get('add'))
        ld_end = ctl_set_status(lid, 'TIME-WAIT', dict(time=new_time, retry=retry))
    else:
        ld_end = ctl_set_completed(lid, action)

    data = {
        'wf': wf['name'],
        'retry': retry,
        'sdt': sdt,
        'run_type': wf_prm.get('wfp_run_type', 'UNKNOWN'),
        'time': (pendulum.now(get_config()['tz']) - pendulum.parse(sdt, tz=get_config()['tz'])).in_words(locale='ru') if sdt else '',
    }
    add_note({**data, 'action': action, 'obj': lid}, context, level='task,DAG', title='Log')

    return status, action, res_msg, res_icon, ld_end


def build_worker_dag(w):

    cat_full = w['category']
    # wid = w['id']
    wf_name = w['name'] #.split('.')[-1]
    wf_short = w['name'].split('.')[-1]
    
    tags=[w['profile'], 'CTL_wf', cat_full]
    tags.append('CTL_archive' if cat_full == archive else 'CTL')
    
    # Определяем is_paused (вызов перенесён в run_prm, здесь не нужен)

    # Определение расписания и зависмости
    try:
        schedule, tag = get_schedule(w, enames)
    except Exception as e:
        logger.warning("get_schedule failed for %s: %s", wf_name, e)
        schedule, tag = None, 'AF_error'
    tags.append(tag)
    
    # Параметры
    wf_params = w['params'].copy()
    wf_params['wf_exe'] = wf_params.get('wf_exe', wf_params.get('wf_exec')) or f'pr_{wf_short}()'
    wf_params['save_params'] = Param(False, type="boolean", title="Сохранить параметры")
    wf_params['start_wf'] = Param(True, type="boolean", title="Запустить")
    
    schedule_wf = w.get('scheduled', False) and w.get('singleLoading', False)
    wf_params['schedule_wf'] = Param(schedule_wf, type="boolean", title="Поставить на расписание")


    #TFS
    if wf_tfs_in  := wf_params.get('wf_tfs_in'):  tags.append('tfs-in')
    if wf_tfs_out := wf_params.get('wf_tfs_out'): tags.append('tfs-out')

    # Entity
    w_eids = [int(e.strip()) for e in w['params'].get('wf_entity', '').split(',') if e.strip()]

    # Timeouts
    # task_time = str2timedelta(config.get('task_timeout', 'hours=1'))
    # prm_task_to = task_time
    # tfs_task_to = (prm_task_to + task_time) if w.get('wf_tfs_in') else prm_task_to
    
    exe_timeout = wf_params.get('wf_timeout', get_config().get('exe_timeout', 'hours=4'))
    try: exe_timeout = timedelta(minutes = int(exe_timeout))
    except (ValueError, TypeError): exe_timeout = str2timedelta(exe_timeout)
    # exe_timeout += timedelta(minutes = +10)
    
    # exe_task_to = tfs_task_to + exe_timeout + task_time
    # end_task_to = exe_task_to + task_time
    # dagrun_timeout =  end_task_to + task_time
    sla_time = str2timedelta(get_config().get('sla_time', 'hours=2'))

            
    conf = dict(
        schedule=schedule,
        tags=tags,
        max_active_runs= 1 if w.get('singleLoading', True) else 10,
        # dagrun_timeout=dagrun_timeout,
        is_paused_upon_creation=pause_new_dag(w.get('scheduled', False)),
    )

    ctl_url = f"{get_config()['conns']['ctl']['url']}/#/workflow-details/{w['id']}"
    doc_md = (
        f"###🔗 [CTL/{profile}/{wf_name}]({ctl_url})  TimeOut: {exe_timeout}\n\n"
        # f"```json\n{json.dumps(w, indent=4)}\n```"
        f"```\n{pformat(w)}\n```\n"
        f"```\n{pformat(conf)}\n```\n"
        f"```\n{pformat(w_eids)}\n```\n"
    )
    
    with DAG(f'CTL.{wf_name}', 
        default_args={
            # 'owner': 'EDP.ETL',
            'owner': profile,
            'depends_on_past': False,
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 2,
            'retry_delay': timedelta(minutes=1),
            'retry_exponential_backoff': True,  
            'max_retry_delay': timedelta(minutes=10),
            'pool': 'ctl_pool',
            # 'execution_timeout': timedelta(minutes=15),  
            'on_failure_callback': on_callback,
            # 'on_success_callback': on_callback,
            # 'on_retry_callback': on_callback,
            # 'on_execute_callback': None,
        },
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        on_failure_callback=on_callback,
        # on_success_callback=on_callback,
        params=wf_params,
        doc_md=doc_md, 
        **conf, 
    ) as dag:


        @task(pool='ctl_pool', retries=0) #   execution_timeout=prm_task_to,  retries=0: POST /loading неидемпотентен — ретрай создаёт дубль loading_id
        def run_prm(wf, params=None, **context):
            """### Инициализация параметров и создание загрузки

            - Обрабатывает триггер (событие, время, ручной запуск).
            - Формирует `params` с учётом retry, времени и типа запуска.
            - Создаёт новую загрузку в CTL через `/v4/api/wf/{wid}/loading`.
            - Обновляет статус на `RUNNING`.

            **XCom Output:** `params` — полный набор параметров для следующих задач.

            **Особенности:**
            - Поддерживает `scheduleAfterStart` — перевод в режим расписания
              (в режиме `af` не передаётся: расписаниями владеет Airflow).
            - Автоматически удаляет расписание, если `!singleLoading`.
            """
            chk_any_conn('ctl')
            ti = context['task_instance']
            set_pause(wf['name'], wf['category'])

            wid = int(wf['id'])
            schedule_wf = params.get('schedule_wf', False)
            
            if params.get('save_params'):
                res = {}
                new_prm = [   
                    # { "param": k, "prior_value": v if isinstance(v, (bool, int, float, str)) or v else str(v) } 
                    { "param": k, "prior_value": str(v) } 
                    for k,v in params.items() if k.startswith('wf')
                ]
                res['del'] = ctl_api(f"/v4/api/wf/{wid}/params", "delete")
                res['new'] = ctl_api(f"/v4/api/wf/{wid}/params", "post", json=new_prm)
                res['prm'] = new_prm
                
                msg = '⚙️ Параметры были сохранены.'
                add_note(res, context, level='task,DAG', title=msg)
                
            if not params.get('start_wf'):
                msg = '⚠️ Задание не запущено.'
                if schedule_wf and not wf['scheduled']:
                    # Ставить расписание в CTL, когда расписаниями владеет Airflow, — это
                    # своими руками сделать двойной запуск: тот же воркфлоу поедет и по
                    # расписанию дага, и по загрузке из CTL. Снятие расписания при этом
                    # разрешено: оно ведёт ровно в ту сторону, куда идёт режим.
                    if ORCHESTRATOR == 'af':
                        msg += (' ⏸️ Расписание в CTL не поставлено: в режиме af им владеет '
                                'Airflow, и вторая постановка дала бы двойной запуск.')
                    else:
                        ctl_api(f'/v4/api/wf/{wid}/scheduled','put')
                        msg += ' ⏰ Задание поставлено на расписание.'
                elif not schedule_wf and wf['scheduled']: 
                    ctl_api(f'/v4/api/wf/{wid}/scheduled','delete')
                    msg += ' 💀 Задание снято с расписания.'

                add_note('', context, level='task,DAG', title=msg)
                raise AirflowSkipException(msg)
                

            all_events = context.get("triggering_dataset_events", {})
            if all_events:
                # Проходим по всем датасетам, которые триггернули запуск и берем последнее событие
                run_type = 'AF-UNKNOWN'
                ds_url = None
                for dataset_uri, events in all_events.items():
                    ds_url = dataset_uri
                    for event in events:
                        af_sdt = ''
                        af_event = ''
                        run_id = event.source_dag_run.run_id if event.source_dag_run else ''
                        
                        if event.extra.get("af_sdt"):
                            params = {**params, **event.extra}
                        
                        for k,v in event.extra.items():
                            for ek, ev in wf.get('wf_event_sched', {}).items():
                                if ev and k.endswith(ek): 
                                    af_event = k
                            if k.endswith('/dt'): 
                                af_sdt = v
                        
                        params[ds_url] = {
                            af_event: af_sdt,
                            'src_id': run_id,
                        }
                        
                        params['af_sdt'] = max(af_sdt, params.get('af_sdt', ''))
                        run_type = params.get('wfp_run_type', 'AF-EVENT')
                        # break
                        
                    # params['af_ds'] = ds_url
                        
                run_note = f"dataset {ds_url}"
                # add_note("🔍 "+run_note, context)
            else:
                run_note = context['dag_run'].run_id.split('_')[0]
                # context['dag_run'].external_trigger
                # context['dag_run'].run_type.upper()
                run_type = params.get('wfp_run_type', 'AF-' + run_note.upper())
            add_note(f'🔍{run_type} {run_note}', context)
            
            if context['dag_run'].conf.get('loading_id'): # old loading (explicit conf trigger)

                params = context['dag_run'].conf
                lid = int(params['loading_id'])
                # wid = int(params['wf_id'])
                
                # Проверяем статус загрузки
                st, ld_sts = ctl_chk_status(lid, wf['name'], alive='ACTIVE', status='RUNNING', step='WAIT-AF')
                raise_status(st, ld_sts)
                ti.xcom_push(key='current', value=json.dumps(ld_sts, default=str))
                
                retry = ctl_get_retry(params=params, wf=wf)
                params['wfp_retry'] = retry
                logger.info(f"🔍 {lid} {retry}")
                
            else: # new loading

                # wid = int(params.get('wf_id', wf.get('id', 0)))
                # if wf['scheduled'] and wf['singleLoading']: 
                if wf['scheduled']: 
                    ctl_api(f'/v4/api/wf/{wid}/scheduled','delete')
                    
                prm = { k:str(v) for k,v in params.items() if k.startswith('wf') }  
                prm['wfp_run_type'] = run_type
                # ctl_api(f'/v4/api/wf/{wid}/scheduled','delete')

                # scheduleAfterStart вернул бы расписание в CTL после запуска — в режиме af
                # это та же двойная оркестрация, только отложенная.
                sched_after = schedule_wf and ORCHESTRATOR != 'af'
                new_lid = ctl_api(f"/v4/api/wf/{wid}/loading?scheduleAfterStart={sched_after}", "post", json=prm)
                lid = int(new_lid['loadingId'])
                ctl_set_status(lid, 'RUNNING', 'NEW-AF ' + context['dag_run'].run_id)
                
                retry = ctl_get_retry(params=params, wf=wf)
                wfp_name = params.get('wfp_name', wf.get('name', ''))

                if not params.get('af_sdt'):
                    params["af_sdt"] = pendulum.instance(ti.start_date).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')

                params['loading_id'] = lid
                params['wf_id'] = wid
                params['wfp_name'] = wfp_name
                params['wfp_run_type'] = run_type
                # params['wfp_status'] = 'RUNNING'
                # params['wfp_status_log'] = ''
                # params['wfp_status_sdt'] = params['af_sdt']
                params['wfp_retry'] = retry
                
                
                # Проверяем события на expire
                st, res_exp = ctl_chk_expire(wf, params, context)
                raise_status(st, res_exp)
                
            # Статистика по сущностям
            eids = ctl_get_eids(wid, params)
            eids = [f"{e}/{enames.get(e, str(e))}" for e in eids]
            ti.xcom_push(key='eids', value=eids)
            add_note(eids, context, level='task,DAG',title=f"🔍 Entities")
            # params['eids'] = eids
                        
            tfs = params.get('wf_tfs_in')
            
            if tfs:
                tfs_mask = params.get('wf_tfs_mask', '*').strip(' ')
                tfs_table = params.get('wf_tfs_table','')
                tfs_schema = params.get('wf_tfs_schema','dia')
                ctl_set_status(lid, 'RUNNING', f"TFS 🔍 {tfs}/{tfs_mask} -> {tfs_schema}.{tfs_table}")
            else:
                ctl_set_status(lid, 'RUNNING', f"RUN {retry}")

            add_note(params, context, level='task', title='Params')
            ti.xcom_push(key='params', value=params)

            ctl_url = f"{get_config()['conns']['ctl']['url']}/#/loading/{lid}"
            msg = f"🔗 [Loading {lid}_{retry.get('try',1)} {run_type} {run_note}]({ctl_url})"
            add_note(msg, context, level='task,DAG')
            
            # return params

        if wf_tfs_in:
            @task(pool='gp_pool', ) #execution_timeout=tfs_task_to,)
            def run_tfs(wf, **context):
                """### Загрузка файлов из TFS"""
                from pathlib import Path
                
                ti = context['task_instance']
                
                # Параметры
                wf_prm = get_params(context)
                
                prefix  = wf_prm.get('wf_tfs_in','').strip(' ').strip('/')+'/'
                mask    = wf_prm.get('wf_tfs_mask', '*').strip(' ')
                options = wf_prm.get('wf_tfs_options')
                table   = wf_prm.get('wf_tfs_table')
                schema  = wf_prm.get('wf_tfs_schema')
                truncate = str(wf_prm.get('wf_tfs_truncate', False)).lower() in ['true', 'yes']

                wf_timeout = wf_prm.get('wf_timeout', get_config().get('gp_timeout', 'hours=3'))
                try: wf_timeout = timedelta(minutes = int(wf_timeout))
                except (ValueError, TypeError): wf_timeout = str2timedelta(wf_timeout)
                
                if schema:
                    if not schema.startswith('s_grnplm_vd_hr_edp_'):
                        schema = 's_grnplm_vd_hr_edp_' + schema
                
                err_prefix = get_config().get('tfs_err_prefix') or 'tfs-errors'
                arc_prefix = get_config().get('tfs_arc_prefix') or 'tfs-archive'

                s3 = get_config()['conns']['files']
                path = f"{s3['conn_id']}://{s3['bucket']}/{prefix}{mask}"

                lid = wf_prm.get('loading_id', 0)

                if not wf_prm.get('wf_tfs_in', '').strip():
                    msg = f"✳️ TFS files are not required."
                    add_note(msg, context, level='task,DAG')
                    ctl_set_status(lid, 'ERRORCHECK', msg)
                    ctl_set_completed(lid, 'completed') # Completed/Aborted
                    raise AirflowSkipException(msg)
                # Проверяем статус загрузки
                st, ld_sts = ctl_chk_status(lid, wf['name'], alive='ACTIVE', status='RUNNING', step='TFS')
                raise_status(st, ld_sts)
                ti.xcom_push(key='current', value=json.dumps(ld_sts, default=str))
                
                if '/.done' in path:
                    done_path = path.split('/.done')[0] + '/.done'
                    done_mask = path.replace(done_path, '', 1)
                    done_keys = s3_keys(done_path)
                    keys = dict()
                    for done_key in done_keys:
                        key_path = f"{s3['conn_id']}://{s3['bucket']}/" + done_key[:-5] + done_mask
                        logger.info(f"🔍 {key_path}")
                        keys = {**keys, **s3_keys(key_path)}
                else:
                    done_keys = {}
                    keys = s3_keys(path)
                    
                
                if len(keys) == 0:
                    msg = '⚠️ No new TFS files'
                    add_note(msg, context, level='task,DAG')
                    ctl_set_status(lid, 'ERRORCHECK', msg)
                    ctl_set_completed(lid, 'completed') # Completed/Aborted
                    raise AirflowSkipException(msg)

                for key, value in keys.items():
                    new_path = f"{s3['conn_id']}://{s3['bucket']}/{key}"
                    arc_path = f"{s3['conn_id']}://{s3['bucket']}/{arc_prefix}/{key}"
                    err_path = f"{s3['conn_id']}://{s3['bucket']}/{err_prefix}/{key}"
                    try:
                        key_tbl = Path(key).name
                        for s in [" ", "'", '"', ":", ";", "."]:
                            key_tbl = key_tbl.split(s)[0]
                            
                        rows = gp_upload_s3_csv(
                            table=table or key_tbl, 
                            key=key, 
                            options=options, 
                            gp_schema=schema, 
                            truncate=truncate,
                            timeout=int(wf_timeout.total_seconds()), 
                        )
                        
                        msg = f"✅ {value}: {readable(rows, 1000)}"
                        ctl_set_status(lid, 'RUNNING', f'TFS-OK {msg}')
                        add_note(msg, context, level='task,DAG', title=f"✅ {key}")
                        s3_move_s3(new_path, arc_path)
                    except Exception as e:
                        try:
                            s3_move_s3(new_path, err_path)
                        except Exception as move_err:
                            logger.warning("Failed to move %s to err_path: %s", key, move_err)
                        add_note(str(e), context, level='task,DAG', title='❌ Error')
                        ctl_set_status(lid, 'ERROR', str(e))
                        ctl_set_completed(lid, 'completed') # Completed/Aborted
                        raise AirflowFailException(str(e)) from e
                
                if len(done_keys) > 0:
                    for done_key in done_keys:
                        done_path = f"{s3['conn_id']}://{s3['bucket']}/" + done_key
                        s3_delete(done_path)
                        add_note(f"🗑️ .done deleted: {done_path}", context, level='task')

                
                # retry = ctl_get_retry(params=wf_prm, wf=wf)
                retry = ctl_get_retry(params=wf_prm)
                ctl_set_status(lid, 'RUNNING', f"RUN {retry}")
                

        # retries=1 — не для повтора ETL, а чтобы подобрать потерянный ответ: pr_swf_start_ctl
        # по-прежнему неидемпотентна, и повторная попытка её не зовёт (см. ниже). Повторами
        # самой загрузки распоряжается CTL через TIME-WAIT.
        @task(pool='gp_pool', sla=sla_time, retries=1) # execution_timeout=exe_task_to,)
        def run_exe(wf, **context):
            """### Выполнение бизнес-логики в Greenplum

            Повторная попытка ETL не запускает: она спрашивает журнал Greenplum, не
            выполнилась ли загрузка в прошлый раз, и отдаёт найденный ответ. Не нашла —
            падает, оставляя повтор за CTL.

            - Запускает SQL-процедуру через `gp_exe`.
            - Возвращает результат:
            - `res`: код (положительный — успех, ноль — нет данных, отрицательный — ошибка).
            - `msg`: сообщение.
            - `cdc`, `hub`, `stat`, `html` — опциональные метрики.

            **XCom Output:** `result` — словарь с результатом выполнения.

            **Особенности:**
            - Использует `PostgresHook` для подключения к Greenplum.
            - Логирует PID и время начала.
            """     
            chk_any_conn('gp')
            ti = context['task_instance']
            wf_prm = get_params(context)
            
            wf_name = wf.get('name', 'unknown').split('.')[-1]
            cat = wf['category'].split('.')[-1]
            lid = wf_prm.get('loading_id', 0)
            wid = wf_prm.get('wf_id', 0)
            sdt = wf_prm.get('af_sdt', '')
            exe = wf_prm.get('wf_exe', wf_prm.get('wf_exec')) or f'pr_{wf_name}()'
            
            # Потолок времени нужен обеим ветвям: и запуску ETL, и ожиданию его в повторе.
            wf_timeout = wf_prm.get('wf_timeout', get_config().get('gp_timeout', 'hours=3'))
            try: wf_timeout = timedelta(minutes = int(wf_timeout))
            except (ValueError, TypeError): wf_timeout = str2timedelta(wf_timeout)

            # Ответ достоверен потому, что функция в Greenplum атомарна и кладёт его в
            # журнал той же транзакцией (подробно — в docstring gp_loading_result). Сюда
            # попадают из-за обрыва в самом конце: Greenplum отправлял результат, а воркера
            # уже убили по OOM или рестарту.
            if ti.try_number > 1:
                gp_pid = ti.xcom_pull(key='gp_pid', task_ids='run_exe')
                try:
                    st, done = ctl_exe_recover(lid, gp_pid, deadline=int(wf_timeout.total_seconds()))
                except Exception as e:
                    # Неизвестность трактуем как «работа могла быть выполнена»: запустить
                    # ETL заново здесь дороже, чем отдать загрузку на разбор в CTL.
                    raise AirflowFailException(
                        f"⚠️ Повтор {ti.try_number}: Greenplum недоступен "
                        f"({type(e).__name__}: {e}) — ETL заново не запускаем") from e
                if st != 'ok':
                    raise AirflowFailException(f"⚠️ Повтор {ti.try_number}: {done}")
                done = {**done, 'ts': 'подобран из журнала GP'}
                ti.xcom_push(key='result', value=done)
                add_note(done, context, level='task,DAG', title='Result (ответ подобран из журнала GP)')
                return

            # Проверяем статус загрузки
            st, ld_sts = ctl_chk_status(lid, wf['name'], alive='ACTIVE', status='RUNNING', step='RUN')
            raise_status(st, ld_sts)
            ti.xcom_push(key='current', value=json.dumps(ld_sts, default=str))

            # 🧪 Тестовый режим: только из ctl_config и только на разрешённых контурах
            test_mode = test_profile(context)

            if test_mode:
                # Вместо процедуры воркфлоу — ожидание случайной длины. Куб смещает выборку
                # к коротким прогонам: длинные нужны редко, а слот пула держат по-настоящему.
                sleep_to = str2timedelta(get_config().get('test_sleep', 'minutes=45'))
                rand = int((random.random() ** 3) * sleep_to.total_seconds())
                exe = f"'Ok Test work',pg_sleep({rand})"


            retry = wf_prm.get('wfp_retry', {})
            
            val = {
                'wf': wf_name  # имя воркфлоу
                , 'cat': cat   # категория воркфлоу
                , 'exe': exe   # запуск
                , 'lid': lid   # id задачи
                , 'cwf': wid   # id воркфлоу
                , 'sdt': sdt   # дата запуска
                # , 'wfp': wfp   # параметры
                , 'rtr': retry # повторы
            }
            if wf_prm.get('wf_sch') is not None: val['sch'] = wf_prm['wf_sch']   # схема execute
            # if params.get('swf_id') is not None: val['swf'] = params['swf_id']
            if wf_prm.get('wf_zt_sch') is not None: val['zts'] = wf_prm['wf_zt_sch']   # ККД схема
            if wf_prm.get('wf_zt_tbl') is not None: val['ztt'] = wf_prm['wf_zt_tbl']   # ККД таблица
            if wf_prm.get('wf_zt_active') is not None: val['zta'] = wf_prm['wf_zt_active'] # ККД actual
            if wf_prm.get('wf_zt_rollback') is not None: val['ztb'] = wf_prm['wf_zt_rollback'] # ККД rollback
            if wf_prm.get('wf_zt_error') is not None: val['zte'] = wf_prm['wf_zt_error']   # ККД error
            if wf_prm.get('wf_zt_param') is not None: val['ztp'] = wf_prm['wf_zt_param']   # ККД параметры

            ti.xcom_push(key='run_prm', value={**val, 'timeout': str(wf_timeout)} )
            add_note({**val, 'timeout': str(wf_timeout)}, context, level='task', title='Run_prm')


            # gp_hook = PostgresHook(postgres_conn_id=config['gp_conn_id'])
            sql = f"""select pr_swf_start_ctl($jsn${json.dumps(val)}$jsn$::json)"""
            
            # EXECUTE !!!
            ts = time.time()
            res = gp_exe(sql=sql, ti=ti, timeout=int(wf_timeout.total_seconds())) 
            # res['ts'] = pendulum.duration(seconds= int(time.time() - ts)).in_words()
            res['ts'] = str(timedelta(seconds=int(time.time() - ts)))

            # 🧪 Код исхода по профилю. Пишем и сам профиль: без этого тестовая -7 в логах
            # и письмах неотличима от настоящей ошибки Greenplum.
            if test_mode:
                res['res'] = test_res(test_mode)
                res['test'] = test_mode

            ti.xcom_push(key='result', value=res)
            add_note(res, context, level='task,DAG', title='Result')

            # return eids

        if wf_tfs_out:
            # Без пула намеренно: тело пустое, в Greenplum таск не ходит, а слотов в
            # gp_pool меньше, чем разрешено запросов в самом Greenplum — они оставлены под
            # короткие служебные запросы, и занимать их публикацией датасета незачем.
            @task(outlets=[ DatasetAlias(f"TFS/{profile}/{wf_tfs_out}") ],)
            def run_out(wf, **context):
                pass

        outlets = [DatasetAlias(f"CTL/{profile}/{e}") for e in w_eids]
        @task(outlets=outlets,)
        def run_end(wf, **context):
            ti = context['task_instance']
            chk_any_conn('ctl')
            wf_prm = get_params(context)

            result = ti.xcom_pull(key='result', task_ids='run_exe')
            if not result:
                add_note("⚠️ Result is empty", context, level='task,DAG')
                raise AirflowSkipException("⚠️ Result is empty")

            eids = ti.xcom_pull(key='eids', task_ids='run_prm')
            add_note(eids, context, level='task', title='Entities')

            lid = wf_prm.get('loading_id', 0)
            st, ld_sts = ctl_chk_status(lid, wf['name'], alive='ACTIVE', status='RUNNING', step='RUN')
            raise_status(st, ld_sts)
            ti.xcom_push(key='current', value=json.dumps(ld_sts, default=str))

            msg = _emit_datasets(lid, eids, result, context)
            ti.xcom_push(key='eids_result', value=json.dumps(msg, default=str))

            # Номер попытки берём ДО финализации: внутри неё retry.setdefault('try', 1)
            # меняет тот же словарь, что лежит в wf_prm['wfp_retry'] — он пришёл по ссылке,
            # и после вызова первая попытка выглядела бы как первая повторная.
            attempt = int((wf_prm.get('wfp_retry') or {}).get('try', 0) or 0)

            status, action, res_msg, res_icon, ld_end = _finalize_status(
                lid, int(wf['id']), result, wf, wf_prm, context)

            # Итог попытки — отдельным снимком, до веток raise ниже: пропуск на повторе и
            # падение на ошибке иначе не оставили бы в S3 ничего, а разбирать инцидент
            # интереснее всего как раз по ним. Ошибка сохранения исход не меняет: загрузка
            # уже финализирована в CTL, и красный таск после этого противоречил бы статусу.
            try:
                ctl_loading_snapshot(lid, wf['name'], f"ctl_done/{lid}_{attempt}", ld=ld_end)
            except Exception as e:
                msg = f"⚠️ Снимок итога не сохранён: {type(e).__name__}: {e}"
                logger.warning(msg, exc_info=True)
                add_note(msg, context, level='task')

            title = f"{status_icons[status]}{status} {action.upper()}\n\n"
            note = f"{res_icon} {result.get('msg') or res_msg.upper()}\n\n"
            add_note(note, context, level='task,DAG', title=title)

            state = State.FAILED if res_msg == 'error' else (State.SKIPPED if res_msg == 'no' else State.SUCCESS)
            with create_session() as session:
                other_ti = session.query(TaskInstance).filter(
                    TaskInstance.dag_id == ti.dag_id,
                    TaskInstance.run_id == ti.run_id,
                    TaskInstance.task_id == 'run_exe'
                ).first()
                if other_ti:
                    other_ti.set_state(state)

            if action == 'retry':
                raise AirflowSkipException()
            elif status == 'ERROR':
                raise AirflowFailException()
    
                
        task_prm = run_prm(wf = w)
        task_exe = run_exe(wf = w)
        task_end = run_end(wf = w)
        
        if wf_tfs_in: 
            task_prm >> run_tfs(wf = w) >> task_exe
        else:
            task_prm  >> task_exe
        
        if wf_tfs_out:
            task_exe >> run_out(wf = w) >> task_end
        else:
            task_exe >> task_end

    return dag


def _wf_eligible(wfs: dict) -> list:
    """Воркфлоу, из которых вообще строятся даги: наш профиль и не удалённые."""
    return [w for w in wfs.values()
            if w.get('profile') == profile and not w.get('deleted', False)]


def _wf_check_shrink(eligible: list) -> None:
    """Не давать «успешному» разбору с усохшим списком уничтожить уже собранные даги.

    Airflow считает даг пропавшим, если очередной разбор файла его не вернул, и через
    stale_dag_threshold удаляет строку из serialized_dag (dag_processing/manager.py,
    deactivate_stale_dags). То есть короткий список воркфлоу молча убивает даги, а
    ошибка импорта — нет: при ней уже сериализованные даги остаются на месте.

    Откуда берётся короткий список: ctl_obj_load при пустой Variable подставляет копию
    из S3, а она может быть старее. Ожидаемое число пишет ctl_loader в ctl_workflows_stat.
    """
    pct = int(get_config().get('wf_shrink_pct', 10))
    if not eligible:
        raise RuntimeError(
            'ctl_workflows пуст или не содержит ни одного воркфлоу профиля '
            f'{profile}: даги не строим, чтобы не потерять уже собранные. '
            'Проверьте Variable ctl_workflows и прогон ctl_loader'
        )
    stat = ctl_obj_load('ctl_workflows_stat') or {}
    expected = int(stat.get('eligible') or 0)
    if expected and len(eligible) < expected * (100 - pct) / 100:
        raise RuntimeError(
            f'ctl_workflows усох: ожидали около {expected} воркфлоу профиля {profile}, '
            f'а прочитали {len(eligible)} (порог {pct}%). Даги не строим — иначе Airflow '
            'вычистит недостающие из serialized_dag. Прогоните ctl_loader; если воркфлоу '
            'и правда удалили массово, обновите ctl_workflows_stat'
        )


_eligible = _wf_eligible(ctl_obj_load('ctl_workflows'))
_wf_check_shrink(_eligible)

# Режим af означает, что расписаниями владеет Airflow. Если при этом воркфлоу остались на
# расписании и в CTL, их будут запускать обе системы. Разбор из-за этого не роняем: иначе
# переключение режима становится опасным — предупреждаем и строим даги как обычно.
if ORCHESTRATOR == 'af':
    _still_in_ctl = [w['name'] for w in _eligible if w.get('scheduled')]
    if _still_in_ctl:
        logger.warning(
            "orchestrator=af, но в CTL остались на расписании %d воркфлоу — их будут "
            "запускать обе системы. Снимите расписание в CTL: %s%s",
            len(_still_in_ctl), ', '.join(_still_in_ctl[:5]),
            ' …' if len(_still_in_ctl) > 5 else '')

for w in _eligible:
    try:
        dag = build_worker_dag(w)
        globals()[dag.dag_id] = dag
    except Exception as e:
        # Один битый workflow не должен ронять импорт модуля и забирать с собой ВСЕ остальные DAG'и
        logger.exception("Не удалось построить DAG для workflow %s: %s", w.get('name'), e)
        continue
    
    