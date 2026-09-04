"""### 📊 DAG: Мониторинг CTL
*2026-09-04 13:47 MSK · v1.6 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Каждые 15 минут анализирует активные загрузки и выполняет автоматические действия.

| Действие | Описание |
|---|---|
| 🔁 `reRunned` | Повторная попытка при ошибке |
| 🚫 `Aborted` | Остановка при исчерпании попыток |
| ✅ `Completed` | Успешное завершение |
| ⚠️ `reStarted` | Перезапуск зависшей задачи |
| 🚨 `SLA` | Нарушение времени выполнения |
| 🛑 `Stopped` | Остановка вручную |
| ☮️ `Skipped` | Пропуск без изменений |
"""

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param
from airflow.sensors.base import PokeReturnValue # type: ignore

from plugins.utils import add_note, on_callback, str2timedelta, get_current_load # type: ignore
from plugins.ctl_utils import get_config, gp_exe, pg_exe, ctl_obj_load, eval_delta, ctl_api # type: ignore
from plugins.ctl_core import chk_any_conn, ctl_loading_load, status_icons, ctl_wf_norm, ctl_events_mon, ctl_set_status, ctl_set_completed, ctl_wait_until  # type: ignore

import ast
import re
import sys
from functools import partial
from datetime import timedelta, datetime, timezone
import pendulum

from logging import  getLogger
logger = getLogger('airflow.task')

MAX_WFS = 25

action_icons = { 
    'reRunned': '🔁', 
    'Aborted': '🚫', 
    'Completed': '✅', 
    'reStarted': '⚠️', 
    'Stopped': '🛑', 
    
    'notFound': '❓', 
    'New':'⏳' ,
    'Skipped': '☮️', 
    'SLA': '🚨',
}

monitor_interval = str2timedelta(get_config().get('monitor_interval','minutes=15'))
timeout = timedelta(hours=24)

with DAG(f'CTL.{get_config()["profile"]}.monitor',
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'logger'],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule_interval=monitor_interval,
    catchup=False,
    default_args={
        'owner': 'EDP.ETL',
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1, tzinfo=timezone.utc),
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
    params={
        'zombie_dry_run': Param(
            False, type='boolean', title='Санитар: только показать',
            description='Не закрывать зависшие таски и загрузки, а только перечислить их '
                        'в заметке. Полезно первым запуском на новом контуре.',
        ),
    },
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
        
        sla_notes = {}
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
            
            for ld in sorted(tsk, key=lambda x: int(x['id'])):
                
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
                        # Время старта достаём общим разбором: наш словарь, старый текст
                        # CTL 'Start scheduled on …' либо расписание воркфлоу. Раньше здесь
                        # понимался ТОЛЬКО текст CTL, а всё остальное считалось мусором и
                        # уходило в Aborted — то есть повтор, поставленный нами же, монитор
                        # отменял. CTL этот текст больше не пишет, так что ветка else стала
                        # основной.
                        # Сначала только лог: наш словарь либо старый текст CTL.
                        wait_to = ctl_wait_until(log)

                        if not wait_to:
                            # Лог молчит — смотрим условие запуска. Оно приходит ТОЛЬКО
                            # в полной загрузке (в списке /loading/extended его нет),
                            # поэтому дотягиваем её здесь, а не для каждой ожидающей.
                            #
                            # Спрашивать условие ДО расписания обязательно: при заданном
                            # startCondition CTL игнорирует wf_time_sched, и посчитанное
                            # по нему время было бы неправдой.
                            cond = (ctl_api(f'/v4/api/loading/{lid}') or {}).get('startCondition')
                            wait_to = ctl_wait_until(log, wf=wf, sdt=sdt, cond=cond)

                        if not wait_to:
                            # Время неизвестно НАМ, но известно CTL: он и разбудит загрузку.
                            # Пропускаем, а залипшую поймает проверка SLA ниже и покажет
                            # человеку — это лучше, чем отменить молча.
                            action = 'Skipped'
                        elif pendulum.parse(wait_to, tz=get_config()['tz']) + timedelta(minutes=15) <= now:
                            add_note({'log': log, 'старт был назначен на': wait_to},
                                     context, level='Task', title=f'reStarted {lid}')
                            action = 'reStarted'
                        else:
                            action = 'Skipped'

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
                    sla_notes[lid] = f"{r['icon']} {r['wfn']} {r['time']}"
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
        
        if sla_notes:
            add_note(sla_notes, context, level='Task', title='SLA')

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

            try:
                # Status ABORTING
                if action not in ['Completed']:
                    if active: ctl_set_status(lid, 'ABORTING', f'{action} {r}')

                # Status RUNNING
                if action in ['reRunned',]:
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

            except Exception as e:
                logger.error(f"ctl_action failed for lid={lid} action={action}: {e}")

    
    @task(pool='ctl_pool')
    def ctl_zombie(**context):
        """🧟 Закрывает таски, зависшие в RUNNING с мёртвым heartbeat.

        Зачем. Умерший воркер оставляет таск в состоянии RUNNING. Планировщик объявляет
        его зомби и шлёт колбэк «пометить упавшим», но колбэк исполняется в контексте
        файла дага — а даг к тому времени мог исчезнуть (воркфлоу выпал из ctl_workflows,
        и фабрика его больше не строит). Тогда состояние не меняется никогда: те же таски
        объявляются зомби каждые десять секунд годами. На alpha так накопилось 33 штуки,
        самым старым — с апреля; они держали слоты ctl_pool, а каждое объявление дёргало
        наш on_failure_callback (за пять минут — под тысячу вызовов и 33 МБ лога).

        Что делает: помечает такие таски упавшими и закрывает осиротевшие раны. С
        загрузкой CTL за раном (`sensor__<lid>_…`) поступает по-разному, и это главное
        различие:

        * ехать некуда (нет живых задач, даг выпал из сериализации) — загрузка
          переводится в ABORTED: CTL иначе считает её активной, а идти ей действительно
          некуда;
        * ждёт снятия паузы — загрузка НЕ трогается. Снимут паузу — сенсор запустит её
          заново; закрыть её значило бы потерять загрузку из-за плановых работ. Тот же
          принцип, что у сенсора при `DagNotFound`: «вернётся даг — поедет и она».

        Порог берётся из конфига (`zombie_after`, по умолчанию 6 часов) и обязан быть
        БОЛЬШЕ exe_timeout: длинный run_exe не бьётся чаще, чем работает, и попасть под
        нож не должен. Параметр формы `zombie_dry_run` показывает список, ничего не трогая.
        """
        dry = bool(context['params'].get('zombie_dry_run'))
        after = str2timedelta(get_config().get('zombie_after', 'hours=6'))
        secs = int(after.total_seconds())

        # Порог — единственное, что отделяет уборку от бойни: при нуле под нож идёт всё,
        # что не успело добежать. Пять минут — не «разумное значение», а граница
        # абсурда; настоящий порог обязан быть больше exe_timeout.
        if secs < 300:
            raise AirflowFailException(
                f"🔥 zombie_after = {after} — меньше пяти минут; санитар с таким порогом "
                "закрывает живые раны. Поправьте ctl_config.")

        # Сколько ранов закрываем за один заход. Без потолка первый боевой прогон по
        # накопленному бэклогу закрыл бы всё разом и утащил бы за собой поштучный обход
        # CTL — под statement_timeout это отказ посреди работы. Бэклог доедается за
        # несколько кругов, а «покажи» и «сделай» считают одинаково.
        LIMIT = 500

        # Критерий один на выборку и на правку: таск в RUNNING, а его job либо не
        # зарегистрирован, либо не бился дольше порога. Живой таск со свежим heartbeat
        # сюда не попадает ни при каких условиях — это главное свойство запроса.
        where = (
            "ti.state = 'running' AND ("
            "  ti.job_id IS NULL"
            "  OR j.id IS NULL"
            "  OR j.state <> 'running'"
            f"  OR j.latest_heartbeat < now() - interval '{secs} seconds')"
        )

        stuck = pg_exe(
            "SELECT ti.dag_id, ti.task_id, ti.run_id, ti.map_index, ti.start_date,"
            "       j.state AS job_state, j.latest_heartbeat"
            "  FROM task_instance ti"
            "  LEFT JOIN job j ON j.id = ti.job_id"
            f" WHERE {where}"
            " ORDER BY ti.start_date LIMIT 500"
        )

        note = {
            f"{r['dag_id']}.{r['task_id']}": {
                'run_id': r['run_id'],
                'start': str(r['start_date'])[:19],
                'heartbeat': str(r['latest_heartbeat'])[:19] if r['latest_heartbeat'] else 'нет job',
            }
            for r in stuck
        }

        # Раны, которые планировщик разбирает вхолостую на каждом круге. Считаются
        # ОТДЕЛЬНО от тасков: ран висит и после того, как его таски добиты — хоть нами,
        # хоть штатным зомби-килом. Три случая, все старше порога, но исходы у них
        # РАЗНЫЕ, поэтому и запроса два.
        #
        # «Ехать некуда» — ран закрывается, загрузка за ним переводится в ABORTED:
        #
        #   1. RUNNING, в котором не осталось ни одной живой задачи — двигаться ему некуда;
        #   2. RUNNING или QUEUED у дага, которого нет в serialized_dag: воркфлоу выпал
        #      из ctl_workflows, фабрика его больше не строит, и планировщик на каждом
        #      круге пишет «DAG ... not found in serialized_dag». На alpha один такой
        #      призрак давал 60 строк ошибок в секунду.
        #
        # «Поедет, но позже» — ран закрывается, ЗАГРУЗКА НЕ ТРОГАЕТСЯ:
        #
        #   3. QUEUED у запаузенного дага: trigger_dag состояние паузы не смотрит и
        #      создаёт ран сразу в очереди, а планировщик запаузенный даг не разбирает.
        #      Такой ран не поедет, пока паузу не снимут, — и копится в очереди.
        #
        # Разница принципиальная. Пауза — это плановые работы, а не смерть: снимут её —
        # сенсор запустит загрузку заново (он и делает это, пока у неё пустой status_log).
        # Закрывать загрузку из-за ночного окна работ значило бы терять её на ровном
        # месте. Ран же закрыть надо: он всё равно не поедет — сенсор создаст новый.
        #
        # Порядок запросов важен: случай 2 забирает раны дагов, пропавших из сериализации,
        # включая запаузенные, — после него они уже 'failed' и во второй запрос не попадут.
        #
        # Живые состояния перечислены явно, и up_for_reschedule среди них обязателен:
        # сенсоры в режиме reschedule (tfs_wait ждёт файл до суток) между опросами живут
        # именно в нём. Без него санитар убивал бы их на шестом часе ожидания — включая
        # собственный ран монитора.
        LIVE_TASK = "('running','queued','scheduled','up_for_retry','deferred'," \
                    "'up_for_reschedule','restarting')"
        AGE = ("coalesce(dr.start_date, dr.queued_at, dr.execution_date)"
               f" < now() - interval '{secs} seconds'")

        dead_where = (
            f"dr.state IN ('running','queued') AND {AGE}"
            "   AND ("
            "        (dr.state = 'running'"
            "         AND NOT EXISTS (SELECT 1 FROM task_instance ti"
            "                          WHERE ti.dag_id = dr.dag_id AND ti.run_id = dr.run_id"
            f"                            AND ti.state IN {LIVE_TASK}))"
            "     OR NOT EXISTS (SELECT 1 FROM serialized_dag sd WHERE sd.dag_id = dr.dag_id)"
            "       )"
        )
        paused_where = (
            f"dr.state = 'queued' AND {AGE}"
            "   AND EXISTS (SELECT 1 FROM dag d WHERE d.dag_id = dr.dag_id AND d.is_paused)"
        )

        def orphan_sql(action, where):
            """Отбор ранов: один и тот же для «покажи» и «сделай» — порядок и потолок.

            Потолок вешается на подзапрос с id, а не на сам UPDATE: PostgreSQL не знает
            LIMIT в UPDATE, а без него первый прогон по бэклогу закрывает всё разом.
            """
            return (
                f"{action} WHERE dr.id IN ("
                f"  SELECT dr.id FROM dag_run dr WHERE {where}"
                "   ORDER BY coalesce(dr.start_date, dr.queued_at, dr.execution_date)"
                f"  LIMIT {LIMIT})"
            )

        def lids_of(rows):
            """Загрузки CTL: id лежит в имени рана — sensor__<lid>_<попытка>_<дата>."""
            return sorted({int(m.group(1)) for r in rows
                           if (m := re.match(r'sensor__(\d+)_', str(r['run_id'])))})

        if dry:
            # Отбор тот же, что в боевой ветке ниже, иначе «покажи» и «сделай» показывали
            # бы разное. Полное число — отдельным счётом: потолок скрывает размер бэклога,
            # а знать его нужно именно до первого боевого прогона.
            cols = "SELECT dr.dag_id, dr.run_id FROM dag_run dr"
            dead = pg_exe(orphan_sql(cols, dead_where))
            paused = pg_exe(orphan_sql(cols, paused_where))
            total = pg_exe(f"SELECT (SELECT count(*) FROM dag_run dr WHERE {dead_where}) AS dead,"
                           f"       (SELECT count(*) FROM dag_run dr WHERE {paused_where}) AS paused")[0]
            add_note({'🧟 Санитар (только показать)': note or 'зависших тасков нет',
                      f"осиротевшие раны (найдено {total['dead']}, потолок {LIMIT})":
                          [f"{r['dag_id']} / {r['run_id']}" for r in dead] or 'нет',
                      f"⏸️ раны на паузе (найдено {total['paused']}, потолок {LIMIT})":
                          [f"{r['dag_id']} / {r['run_id']}" for r in paused] or 'нет',
                      'загрузки под ABORTED': lids_of(stuck + dead),
                      'загрузки, которые не трогаем (ждут снятия паузы)': lids_of(paused)},
                     context, level='Task,DAG')
            return {'stuck': len(stuck), 'runs': len(dead), 'paused': len(paused),
                    'loadings': lids_of(stuck + dead), 'dry_run': True}

        # RETURNING, а не «обновили и надеемся»: в отчёт и в закрытие загрузок идёт то,
        # что база действительно поменяла, а не то, что показала выборка секунду назад.
        failed = []
        if stuck:
            failed = pg_exe(
                "UPDATE task_instance ti SET state = 'failed', end_date = now()"
                "  FROM job j WHERE j.id = ti.job_id AND " + where +
                " RETURNING ti.dag_id, ti.task_id, ti.run_id"
            )
            # Таски без job вообще (ti.job_id пуст) отдельным запросом: JOIN их не поймает.
            # Порог здесь тот же, что и везде: между «таск встал в running» и «job
            # записался» есть окно, и попадать в него санитару незачем.
            failed += pg_exe(
                "UPDATE task_instance SET state = 'failed', end_date = now()"
                " WHERE state = 'running' AND job_id IS NULL"
                f"   AND start_date < now() - interval '{secs} seconds'"
                " RETURNING dag_id, task_id, run_id"
            )

        upd = "UPDATE dag_run dr SET state = 'failed', end_date = now()"
        ret = " RETURNING dr.dag_id, dr.run_id"
        # Порядок: сперва «ехать некуда» — иначе запаузенный ран пропавшего дага попал бы
        # во второй запрос и загрузка осталась бы висеть активной.
        runs = pg_exe(orphan_sql(upd, dead_where) + ret)
        paused_runs = pg_exe(orphan_sql(upd, paused_where) + ret)

        # Загрузки берутся и с тасков, и с ранов: у рана-призрака живых тасков нет
        # вовсе, а загрузка за ним всё равно числится активной. Раны с паузы сюда НЕ
        # входят — за ними стоят живые загрузки, которые поедут после снятия паузы.
        reasons = {lid: f'Zombie: таск не бился дольше {after}, закрыт санитаром'
                   for lid in lids_of(failed)}
        for lid in lids_of(runs):
            # Своя причина: у осиротевшего рана таск мог не запускаться вовсе, и «таск не
            # бился» увело бы разбор по ложному следу.
            reasons.setdefault(lid, f'Zombie: ран осиротел дольше {after} '
                                    '(нет живых задач или даг выпал из сериализации), '
                                    'закрыт санитаром')

        closed, skipped = [], []
        for lid, reason in reasons.items():
            try:
                ld = ctl_api(f"/v4/api/loading/{lid}")
                if (ld or {}).get('alive') != 'ACTIVE':
                    skipped.append(lid)
                    continue
                ctl_set_status(lid, 'ERROR', reason)
                ctl_set_completed(lid, completed=False)
                closed.append(lid)
            except Exception as e:                       # одна загрузка не должна ронять уборку
                logger.error(f"ctl_zombie: не закрыл загрузку {lid}: {e}")

        if not (failed or runs or paused_runs):
            add_note('🧟 Зависших тасков и осиротевших ранов нет', context, level='Task')
            return {'stuck': 0, 'runs': 0, 'paused': 0}

        add_note({'🧟 Закрыто зависших тасков': len(failed), 'таски': note,
                  'закрыто ранов': len(runs), 'загрузки ABORTED': closed,
                  'уже закрыты': skipped,
                  '⏸️ закрыто ранов на паузе': len(paused_runs),
                  'их загрузки не тронуты': lids_of(paused_runs)},
                 context, level='Task,DAG')
        return {'stuck': len(failed), 'runs': len(runs), 'paused': len(paused_runs),
                'closed': closed, 'skipped': skipped}


    ctl_action(res = ctl_monitor())
    ctl_zombie()

