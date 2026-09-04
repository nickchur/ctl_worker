"""### 🧪 DAG: Симулятор нагрузки CTL
*2026-09-04 12:04 MSK · v1.3 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Генерирует нагрузку: события сущностей, Dataset-сигналы или запуски дагов воркфлоу.
Режим задаётся ключом `simulator` в `ctl_config`, частота — `simulator_interval`.

| `simulator` | Что делает |
|---|---|
| `off` (умолчание) | ничего, таск уходит в пропуск |
| `event` | POST значений событий в CTL API — **только DEV** |
| `dataset` | публикует Dataset-сигналы в Airflow |
| `trigger` | случайно запускает даги воркфлоу через `trigger_dag` — только те, что реально поедут: не на паузе, активные, сериализованные |

⚠️ Запуск дага воркфлоу — это **настоящая загрузка в CTL**: `run_prm` создаёт её через
`POST /v4/api/wf/{wid}/loading`. Поэтому симулятор существует только на DEV, IFT и PSI, а
на боевом и неизвестном контуре не регистрируется вовсе. Притворяться выполнение будет
только там, где разрешён `test_mode` (см. `ctl_worker.py`) — это отдельный ключ.
"""

from airflow import DAG, Dataset
from airflow.datasets import DatasetAlias
from airflow.api.common.trigger_dag import trigger_dag           
from airflow.decorators import task

from airflow.exceptions import AirflowFailException, AirflowSkipException, AirflowRescheduleException
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from plugins.utils import add_note, env_stand, on_callback, get_current_load, str2timedelta  # type: ignore
from plugins.ctl_utils import get_config, ctl_obj_load, ctl_api # type: ignore 
from plugins.ctl_core import chk_any_conn  # type: ignore

# Отбор запускаемых дагов идёт по метабазе: пауза и сериализация живут там, а не в CTL
from airflow.models import DagModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import create_session

import random
import pendulum
from datetime import timedelta, datetime, timezone

from logging import getLogger
logger = getLogger("airflow.task")

MAX_XCOM = 500

# get_config() — ленивый синглтон, один запрос на процесс парсинга, и убрать его нельзя:
# от него зависят dag_id, schedule_interval и tags, а они нужны в момент разбора файла.
# А вот ctl_obj_load кэша не имеет: каждый вызов это Variable.get, при промахе ещё и поход
# в S3. Раньше ctl_enames грузились здесь же, хотя нужны единственному месту — set_events
profile =  get_config()['profile']

# 🌍 Контуры. Списки — константы в коде, а не ключи конфигурации: контур это единственный
# признак, которым нельзя управлять ни из ctl_config, ни из параметров воркфлоу в CTL.
# Пустое и незнакомое имя приравниваем к бою: переменная выставлена на всех контурах,
# включая стенд, поэтому «не знаю, где я» — повод не генерировать ничего.
SIM_STANDS = ('DEV', 'IFT', 'PSI')   # где симулятор вообще существует
EVENT_STANDS = ('DEV',)              # где ему позволено писать события в CTL
STAND = env_stand()
OFF = ('', 'off', 'false', 'none', '0')


def runnable_dags(dag_ids: list) -> set:
    """Из списка дагов оставляет те, что действительно поедут.

    `trigger_dag` состояние паузы не смотрит (`airflow/api/common/trigger_dag.py`): он
    создаёт ран сразу в QUEUED, а планировщик запаузенный даг не разбирает. Такой ран
    висит в очереди навсегда — место в списке занимает, для дага считается активным, и
    очередь по нему перестаёт читаться.

    Симулятор без этой проверки целится ровно в такие даги: фабрика создаёт даг
    запаузенным, если воркфлоу не стоит на расписании (`ctl_worker.py`:
    `is_paused_upon_creation = not w['scheduled']`), а из выборки ниже исключены как раз
    воркфлоу «на расписании и с единственной загрузкой».

    Сериализацию проверяем заодно: без неё `trigger_dag` бросает DagNotFound — этому мы
    научены отдельно, в сенсоре.
    """
    if not dag_ids:
        return set()

    # Два запроса, оба пакетом на весь список, а не по одному на кандидата. sorted —
    # ради детерминизма в логах: на план запроса порядок не влияет.
    with create_session() as session:
        active = {r[0] for r in session.query(DagModel.dag_id).filter(
            DagModel.dag_id.in_(sorted(set(dag_ids))),
            DagModel.is_paused.is_(False),
            DagModel.is_active.is_(True),
        ).all()}
        if not active:
            return set()
        # Второй запрос уже сужен списком active, поэтому его результат и есть ответ.
        return {r[0] for r in session.query(SerializedDagModel.dag_id).filter(
            SerializedDagModel.dag_id.in_(sorted(active))).all()}


# Симулятор существует не везде: даг воркфлоу, запущенный отсюда, создаёт настоящую
# загрузку в CTL (run_prm зовёт POST /v4/api/wf/{wid}/loading). На боевом и неизвестном
# контуре его не должно быть вовсе — включать нечего, как у пишущих дагов tools/.
if STAND not in SIM_STANDS:
    logger.warning(f"DAG CTL.{profile}.test_simulator не регистрируется: "
                   f"контур {STAND or 'не задан'}, симулятор разрешён на {'/'.join(SIM_STANDS)}")
else:
    with DAG(f'CTL.{get_config()["profile"]}.test_simulator',
        start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
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
        
            mode = str(get_config().get('simulator', 'off')).strip().lower()

            if mode in OFF:
                msg = "🔥 Симулятор выключен: simulator = off"
                add_note(msg, context, level='Task,DAG')
                raise AirflowSkipException(msg)

            # Событийный режим пишет statval'ы в CTL, в том числе по сущностям чужих профилей —
            # это дёргает зависимости соседних команд, поэтому только DEV. Проверка здесь, а не
            # при разборе файла: режим задаётся конфигурацией и меняется без выкладки.
            if mode == 'event' and STAND not in EVENT_STANDS:
                msg = (f"🔥 Событийная симуляция доступна только на {'/'.join(EVENT_STANDS)}, "
                       f"контур {STAND or 'не задан'}")
                add_note(msg, context, level='Task,DAG')
                raise AirflowSkipException(msg)


            cl = get_current_load('gp_pool')
            cnt = cl['pool_slots'] - cl['scheduled']

            if cnt <= 1:
                msg = "🔥 Sysytem is overloaded"
                add_note(msg, context, level='Task,DAG')
                raise AirflowSkipException(msg)
        
            ret = []
            if mode == 'event':
            
                all_events = str(get_config().get('all_events', False) ).lower() in  ['true', '1', 'yes']
            
                events = [k for k in ctl_obj_load('ctl_events').keys() if all_events or k.split('/')[0]!=profile]
                add_note(f"⏳ Testing {len(events)} events. simulator: {mode}", context, level='Task,DAG')
            
                for k in range(random.randint(1, cnt)):
                    evn = random.choice(events)
                    prf, eid, sid = evn.split('/')
                    try:
                        ctl_api(f'/v4/api/entity/{eid}/stat/{sid}/profile/{prf}/statval', 'POST', json=["1"])
                    except Exception as e:
                        continue
                    ret.append(evn)
                
            elif mode == 'dataset':
                events = list(ctl_obj_load('ctl_events').keys())
                add_note(f"⏳ Testing {len(events)} events", context, level='Task,DAG')
            
                for k in range(random.randint(1, cnt)):
                    evn = random.choice(events)
                    ret.append(evn)
                
            else: # trigger_dag
                # Кандидаты отбираются заранее, а не по одному в цикле: запускать можно
                # только те даги, что реально поедут, и это два запроса к метабазе на
                # весь список сразу.
                #
                # Ключи читаем через .get с теми же умолчаниями, что у фабрики
                # (ctl_worker.py): ctl_wf_norm их не гарантирует, они приходят как есть
                # из ответа CTL. Раньше о битую запись спотыкался random.choice — редко;
                # предварительный отбор проходит все записи, и одна такая роняла бы
                # каждый прогон. deleted и архив исключаем по тем же правилам, что и
                # фабрика: не строит она их — значит и запускать нечего.
                archive_cat = get_config().get('archive_category', 'p1080.ARCHIVE')
                # Отбор в два шага, чтобы потом было что показать оператору: сначала наш
                # профиль, потом годность. Чужие профили в счёт не идут — их «отсеяно»
                # ничего не объясняет.
                mine = [w for w in ctl_obj_load('ctl_workflows').values()
                        if w.get('name') and w.get('profile') == profile]
                wfs = [w for w in mine
                       if not w.get('deleted', False)
                       and not (w.get('scheduled', False) and w.get('singleLoading', True))
                       and w.get('category') != archive_cat]

                if not wfs:
                    raise AirflowSkipException(
                        f"🔥 Запускать нечего: в ctl_workflows нет воркфлоу профиля {profile}, "
                        "годных для симуляции")

                # Словарём, а не тремя f-строками подряд: заодно схлопываются одноимённые
                # воркфлоу — иначе два элемента с одним dag_id дали бы DagRunAlreadyExists
                # на втором триггере, ровно тот шум, от которого избавляемся.
                by_dag_id = {f"CTL.{w['name']}": w for w in wfs}
                runnable = runnable_dags(list(by_dag_id))
                ready = [by_dag_id[d] for d in sorted(runnable)]

                # Два счётчика, а не один: воркфлоу отсеиваются на двух разных этапах, и
                # «отсеяно 676» без разделения читается как «676 дагов на паузе». Свойства
                # воркфлоу (deleted, архив, своё расписание) — это «нам такое не нужно»;
                # метабаза — «нужно, но не поедет». Ответы на «почему запусков мало» разные.
                by_props = len(mine) - len(wfs)
                by_meta = len(by_dag_id) - len(ready)

                msg = f"⏳ Кандидатов к запуску: {len(ready)}"
                if by_meta:
                    msg += f" · не поедут {by_meta} (на паузе, неактивны или не сериализованы)"
                if by_props:
                    msg += f" · не подошли по свойствам {by_props}"
                add_note(msg, context, level='Task,DAG')

                if not ready:
                    raise AirflowSkipException(
                        f"🔥 Запускать нечего: из {len(by_dag_id)} кандидатов ни один даг не поедет")

                failed = []
                # Выбор без повторов внутри одной попытки: run_id собирается из
                # start_date таска и в её пределах одинаков, поэтому повторный выбор того
                # же дага упирался бы в DagRunAlreadyExists. Пока кандидатов были сотни,
                # это случалось редко; после отбора список короткий, и повтор стал бы
                # нормой. На ретрае start_date другой, run_id тоже — там дубль возможен,
                # и это осознанно: ретрай симулятора и должен создать новую нагрузку.
                for wf in random.sample(ready, k=random.randint(1, min(cnt, len(ready)))):
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
                        # В заметку, а не только в лог: иначе «запустил 3 из 5» видно, а
                        # почему двух не хватает — только чтением логов таска.
                        logger.error(f"🔥 Error triggering {wf_name}: {e}")
                        failed.append(f"{wf_name}: {type(e).__name__}: {e}")
                        ret.remove(wf_name)

                add_note(ret, title=f"🔍 New events {len(ret)} created", context=context, level='Task,DAG')
                if failed:
                    add_note(failed, title=f"🔥 Не запустились: {len(failed)}",
                             context=context, level='Task,DAG')
               
            add_note(ret, context, level='Task,DAG', title=f'Simulator {mode}: {len(ret)}')      
        
            return ret if mode == 'dataset' else []
    
    
        @task(pool='pg_pool', outlets=[DatasetAlias(f"CTL/events")],
            max_active_tis_per_dag=15, 
            map_index_template="{{ event }}"
        )
        def set_events(event, **context):

            enames = {int(k):v for k,v in ctl_obj_load('ctl_enames').items()}

            prf, eid, sid = event.split('/')
            ds = Dataset(f'CTL/{prf}/{eid}/{enames[int(eid)]}')
            extra = { f"0/{event}": pendulum.now().format('YYYY-MM-DD HH:mm:ss') }
            add_note(event, context, level='Task', title=event)
            context['outlet_events'][f"CTL/events"].add(ds,extra=extra) 
    
            return event[1]
    
    
        events = test_events()
        # chk_conn() >> 
        events >> set_events.expand(event = events)

