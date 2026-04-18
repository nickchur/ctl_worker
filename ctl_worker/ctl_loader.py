"""### 📦 DAG: Загрузчик метаданных CTL (Change Tracking & Loading)

Этот DAG отвечает за **регулярную выгрузку и сохранение ключевых сущностей** из системы CTL, таких как профили, категории, workflow'ы, сущности и события. Данные сохраняются в хранилище (S3) и Airflow Variables, доступны другим DAG'ам через `ctl_obj_load`.

---

#### 🔧 Основные функции

1. **Загрузка профиля**:
   - Получение информации о текущем профиле из API CTL.
   - Сохранение в переменной `ctl_profile`.

2. **Загрузка категорий**:
   - Выгрузка дерева категорий, начиная с корневой.
   - Фильтрация по `root_category` и `ue_category`.
   - Сохранение в `ctl_categories`.

3. **Загрузка workflow'ов**:
   - Извлечение всех workflows по категориям.
   - Нормализация параметров, событий и уведомлений.
   - Сохранение в `ctl_workflows`.

4. **Загрузка сущностей (entities)**:
   - Построение иерархии сущностей начиная с `root_entity`.
   - Определение связанных сущностей по событиям.
   - Формирование человекочитаемых имён (`ctl_enames`).

5. **Загрузка событий**:
   - `ctl_ue_events`: события из категории `ue_category` за последние N дней.
   - `ctl_events`: события по профилю за последние N дней.

---

#### ⚙️ Особенности

- **Частота запуска:** каждые 15 минут (`schedule_interval: minutes=15`)
- **Пул выполнения:** `ctl_pool` для API запросов.
- **Хранилище:** S3 (бакет `ctl`) и Airflow Variables.
- **Интеграция:** взаимодействие с CTL через `ctl_api`.

---

#### 📌 Ключевые задачи

| Задача | Описание |
|-------|----------|
| `load_profile` | Загружает информацию о профиле. |
| `load_categories` | Выгружает дерево категорий. |
| `load_workflows` | Извлекает все workflows. |
| `load_entities` | Строит иерархию сущностей. |
| `load_events` | Загружает события за последние N дней. |

---

#### 🏷️ Метаданные

- **ID DAG:** `CTL.<profile>.loader`
- **Теги:** `['CTL', '<profile>', 'CTL_agent', 'loader']`
- **Расписание:** каждые 15 минут
- **Catchup:** `False`
- **Max active runs:** `1`
- **Таймаут:** `10 минут`
- **Пул:** `ctl_pool`

---

#### 📊 Сохраняемые объекты

| Объект | Описание | Хранилище |
|--------|----------|-----------|
| `ctl_profile` | Текущий профиль | Variable + S3 |
| `ctl_categories` | Иерархия категорий | Variable + S3 |
| `ctl_workflows` | Все workflows с параметрами | Variable + S3 |
| `ctl_entities` | Дерево сущностей | Variable + S3 |
| `ctl_enames` | Человекочитаемые имена | Variable + S3 |
| `ctl_events` | События по профилю | Variable + S3 |
| `ctl_entity_events` | События по сущностям | Variable + S3 |
| `ctl_ue_events` | События UE | Variable + S3 |

---

#### 🔄 Цикл обновления

1. **Получение профиля** → сохранение в `ctl_profile`.
2. **Загрузка категорий** → построение дерева.
3. **Извлечение workflows** → нормализация параметров.
4. **Построение сущностей** → иерархия + имена.
5. **Загрузка событий** → за последние 5 дней.
6. **Сохранение** → S3 + Airflow Variables.

> 💡 *Примечание:* Используется `pendulum` для корректной работы с часовыми поясами (`get_config()['tz']`).  
> Все данные доступны через `ctl_obj_load()` и используются другими компонентами системы (worker, sensor, checker).
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task, task_group
from airflow.exceptions import AirflowFailException, AirflowSkipException, AirflowRescheduleException


from plugins.utils import add_note, on_callback, readable_size, str2timedelta, md5_hash # type: ignore
from plugins.ctl_utils import get_config,  ctl_obj_save, ctl_obj_load, ctl_api # type: ignore
from plugins.ctl_core import ctl_loading_load, ctl_wf_norm, chk_any_conn # type: ignore


from functools import partial
import pendulum
from datetime import timedelta

from logging import  getLogger
logger = getLogger('airflow.task')


def load_obj_save(obj, data, var=False, skip=False, **context):
    # Сохраняем в S3
    if ctl_obj_save(obj, data, var=var):
        add_note(f"🔍 {obj}: {len(data)} / {readable_size(len(str(data)))}", context, level='DAG,Task')
    else:
        msg = f'⚠️ {obj} не изменились'
        add_note(msg, context, level='DAG,Task')
        if skip:
            raise AirflowSkipException(msg)


with DAG(f'CTL.{get_config()["profile"]}.loader',
    tags=['CTL', 'CTL_agent', 'logger'],
    start_date=days_ago(1),
    schedule_interval=str2timedelta(get_config().get('loader_interval','minutes=5')),
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
    # dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    doc_md=__doc__,
) as dag:
    
    
    @task(pool='ctl_pool')
    def chk_ctl():
        return chk_any_conn('ctl')
   
    
    # @task_group
    # def ctl_load():
    
    @task(pool='ctl_pool')
    def load_profile(**context):
        """### Загрузка профиля CTL

        Получает информацию о текущем профиле из API CTL по имени.
        Сохраняет данные в переменную `ctl_profile` через `ctl_obj_save(var=True)`,
        чтобы другие DAG'и могли получить к ним доступ.

        **Источник:** `/v4/api/profile/name/{profile_name}`  
        **Сохранение:** `ctl_profile` 
        """            
        data = ctl_api(f'/v4/api/profile/name/{get_config()["profile"]}')
        
        # Сохраняем в S3
        load_obj_save('ctl_profile', data, var=True, skip=True)


    def entity_kids(ent, entity_ids):
        entity_ids[ent['entity']['id']] = ent['entity']
        for j in ent['kidz']: entity_kids(j, entity_ids)

    @task(pool='ctl_pool')
    def load_entities(**context):
        # Entities
        # data = ctl_api(f'/v5/api/entity/child/c/{config["root_entity"]}/export')['entityExt']
        # data = ctl_api(f'/v4/api/entity/{config["root_entity"]}/child')
        data = ctl_api(f'/v4/api/entity/tree?search={get_config()["root_entity"]}&offset=0&limit={get_config().get("ctl_limit", 10000)}')[1]
        eids = {}
        entity_kids(data, eids)  
        
        # Сохраняем в S3
        load_obj_save('ctl_entities', eids, var=True, skip=True)
        
        
    @task(pool='ctl_pool')
    def load_categories(**context):
        """### Загрузка дерева категорий

        Выгружает все категории из CTL и строит иерархическое дерево,
        начиная с корневой (`root_category`). Фильтрует только те категории,
        которые принадлежат иерархии корня (включая потомков).

        Особое внимание — категории `ue_category`, которая сохраняется отдельно.

        **Функционал:**
        - Трёхпроходная сборка дерева (на случай неупорядоченных данных).
        - Добавление `parent_name` для удобства.
        - Сохранение в `ctl_categories` (публично).

        **Источник:** `/v4/api/category`  
        **Сохранение:** `ctl_categories`, `ctl_ue_category`
        """            
        # all_cats = ctl_api('/v5/api/category/m')
        all_cats = ctl_api('/v4/api/category')
        categories = {}

        # Три прохода для построения дерева (на случай, что родители идут после детей)
        for _ in range(3):
            for item in all_cats:
                cat_id = item['id']
                parent_id = item.get('parentId')

                if item['name'] == get_config()['root_category'] or (parent_id and parent_id in categories):
                    if cat_id not in categories:
                        categories[cat_id] = {**item}

        # Добавляем parent_name
        for cat in categories.values():
            parent_id = cat.get('parentId')
            if parent_id and parent_id in categories:
                cat['parent_name'] = categories[parent_id]['name']
                
            # if cat['name'] == config['root_category']:
            #     add_note(f"🔍 ctl_root_category: {cat}", context)
            #     load_obj_save("ctl_root_category", cat, var=True)
            
            if cat['name'] == get_config()['ue_category']:
                add_note(f"🔍 ctl_ue_category: {cat}", context)
                # Сохраняем в S3
                load_obj_save("ctl_ue_category", cat, var=True)
                        

        logger.info(f"🔍 ctl_categories: {categories}")
        context['ti'].xcom_push(key=f'categories', value=categories)
        
        # Сохраняем в S3
        load_obj_save('ctl_categories', categories, var=True, skip=True)


    @task(pool='ctl_pool')
    def load_workflows(**context):
        """### Загрузка и нормализация workflow'ов

        Для каждой категории из `ctl_categories` выгружает все workflows.
        Нормализует структуру:
        - Параметры → словарь `param: value`.
        - Уведомления → `status: emails`.
        - События → список `(entity_id, profile, stat_id, active)`.

        Также:
        - Собирает сущности, участвующие в событиях.
        - Строит иерархию имён сущностей (`Parent/child`).
        - Сохраняет `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events`.

        **Источник:** `/v4/api/wf?category_id={id}`  
        **Сохранение:** `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events` (все публично)
        """

        category_ids = ctl_obj_load('ctl_categories') or {}
        if not category_ids:
            msg = '❌ ctl_categories не загружены'
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)

        ids = ','.join(category_ids.keys())
        data = ctl_api('/v5/api/wf/extended', 'get', {'category_ids': f'[{ids}]' })
        md5  = md5_hash(data)
        wfs = {j['wf']['id']: ctl_wf_norm(j['wf'], j.get('connectedEntities', [])) for j in data}

        # Сохраняем в S3
        load_obj_save('ctl_workflows', wfs, var=True, skip=True)


    @task(pool='ctl_pool')
    def load_workflows_old(**context):
        """### Загрузка и нормализация workflow'ов

        Для каждой категории из `ctl_categories` выгружает все workflows.
        Нормализует структуру:
        - Параметры → словарь `param: value`.
        - Уведомления → `status: emails`.
        - События → список `(entity_id, profile, stat_id, active)`.

        Также:
        - Собирает сущности, участвующие в событиях.
        - Строит иерархию имён сущностей (`Parent/child`).
        - Сохраняет `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events`.

        **Источник:** `/v4/api/wf?category_id={id}`  
        **Сохранение:** `ctl_workflows`, `ctl_entities`, `ctl_enames`, `ctl_entity_events` (все публично)
        """
        # category_ids = context['ti'].xcom_pull(key=f'categories', task_ids=f'ctl_load.load_categories')
        category_ids = ctl_obj_load('ctl_categories') or {}
        wfs_old = ctl_obj_load('ctl_workflows') or {}

        
        # Workflows
        wfs ={}
        change = False
        for c in category_ids.keys():
            data = ctl_api(f'/v4/api/wf?category_id={c}', skip=False)
            for j in data:
                if j.get('deleted'): continue
                
                wid = j['id']
                
                wfExt = ctl_api(f'/v4/api/wf/{wid}/export', skip=False) or []
                # con = ctl_api(f'/v4/api/wf/{wid}/entity') or []
                hash = wfs_old.get(str(wid), {}).get('hash')
                if wfExt['hash'] == hash:
                    # logger.debug(f"📋 ctl_workflows no changes: {wid} {hash}")
                    wfs[wid] = wfs_old[str(wid)]
                else:
                    logger.debug(f"🔍 ctl_workflows : {wid} {hash} != {wfExt['hash']}")
                    wfExt['wfExt']['wf']['date'] = wfExt['date']
                    wfExt['wfExt']['wf']['hash'] = wfExt['hash']
                    wfs[wid] = ctl_wf_norm(wfExt['wfExt']['wf'], wfExt['wfExt']['connectedEntities'])
                    change = True

        # Сохраняем в S3
        if change:
            ctl_obj_save('ctl_workflows', wfs, var=True)
            add_note(f"🔍 ctl_workflows: {len(wfs)} / {readable_size(len(str(wfs)))}", context, level='DAG,Task')
        else:
            msg =f"⚠️ ctl_workflows no changes: {len(wfs)}"
            add_note(msg, context, level='DAG,Task')
            raise AirflowSkipException(msg)
        
        
    @task(pool='ctl_pool')
    def load_events(**context):
        
        wfs = ctl_obj_load('ctl_workflows') or {}
        if not wfs:
            msg = '❌ ctl_workflows не загружены'
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)


        # Events
        entity_events = set()
        events = {}
        for wf in wfs.values():
            for e in wf.get('wf_event_sched',[]):
                entity_events.add(int(e.split('/')[1]))
                events[e] = wf.get('scheduled', False) and events.get(e, True)
        
        # Сохраняем в S3
        load_obj_save('ctl_events', events, var=True, skip=True)

        eids = { int(k):v for k,v in (ctl_obj_load('ctl_entities') or {}).items() }
                
        data = ctl_api(f'/v4/api/entity')
        # Сохраняем в S3
        load_obj_save('ctl_entities_all', data, var=False)
            
        data = { d['id']:d for d in data }
        
        eids_parents = [int(e['parentId']) for e in eids.values()]
        
        events_parents = [int(data[e]['parentId']) for e in entity_events if data.get(e)]
        
        all_keys = set(eids.keys()) | entity_events | set(eids_parents) | set(events_parents)
        
        # Извлекаем имя, берем часть после # и оставляем только ASCII
        enames = {
            e: ''.join(c for c in data.get(e,{}).get('name','_Not_found_').split('#')[-1] if ord(c) < 128).strip() or str(e)  
            for e in all_keys if e > 0
        }
        # Сохраняем в S3
        load_obj_save('ctl_enames', enames, var=True)

        
        entity_events = { e: enames.get(int(e), '_Not_found_') for e in sorted(entity_events) }
        # Сохраняем в S3
        load_obj_save('ctl_entity_events', entity_events, var=True)

        # return wfs
    
    @task(pool='ctl_pool')
    def load_ue_events(**context):
        """### Загрузка событий UE-категории

        Выгружает загрузки (loadings) из категории `ue_category` за последние N дней.
        Используется для мониторинга активности внешних систем.

        **Фильтрация:**
        - По `category_ids` (ID `ue_category`).
        - При наличии `ctl_days` — по дате старта.

        **Сохранение:** не сохраняется напрямую, только логируется.

        **Источник:** `/v4/api/loading` (через `ctl_loading_load`)
        """
        ue_cat = ctl_obj_load('ctl_ue_category')
        if not ue_cat:
            msg = "❌ No ue_category found"
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)

        
        prm = {'category_ids': f'[{ue_cat["id"]}]'}

        if get_config().get('ctl_days') > 0:
            prm['start'] =  pendulum.now(get_config()['tz']).subtract(days=get_config()['ctl_days']).start_of('day').int_timestamp * 1000

        data = ctl_loading_load(prm, save=False)
        # Сохраняем в S3
        load_obj_save('ctl_ue_events', data, var=False, skip=True)

    @task(pool='ctl_pool')
    def load_prf_events(**context):
        """### Загрузка событий по профилю

        Выгружает загрузки (loadings), связанные с текущим профилем, за последние N дней.
        Используется для аудита и анализа активности CTL.

        **Фильтрация:**
        - По `profile_ids`.
        - При наличии `ctl_days` — по дате старта.

        **Сохранение:** не сохраняется напрямую, только логируется.

        **Источник:** `/v4/api/loading` (через `ctl_loading_load`)
        """
        profile = ctl_obj_load('ctl_profile')
        if not profile:
            msg = "❌ No profile found"
            add_note(msg, context, level='DAG,Task')
            # raise AirflowFailException(msg)
            raise Exception(msg)
        
        prm = {'profile_ids': f'[{profile["id"]}]'}

        if get_config().get('ctl_days') > 0:
            prm['start'] = pendulum.now(get_config()['tz']).subtract(days=get_config()['ctl_days']).start_of('day').int_timestamp * 1000

        data = ctl_loading_load(prm, save=False)
        # Сохраняем в S3
        load_obj_save('ctl_prf_events', data, var=False, skip=True)

    @task(pool='ctl_pool')
    def load_enames(**context):
        pass
        
    chk_ctl() >> [
        load_profile(),
        load_categories(),
        load_entities(),
        load_workflows(),
        load_events(),
        load_ue_events(),
        load_prf_events(),
    ]
    # [profile, entities , categories, ue_events, ctl_events]
    # load_entities >> load_workflows
    # load_categories >> load_workflows 
    # load_categories >> load_ue_events
    # load_profile >> load_ctl_events
    # load_workflows >> load_enames

    # chk_ctl() >> ctl_load() #>> EmptyOperator(task_id='end') 