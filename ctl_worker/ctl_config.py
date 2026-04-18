"""### ⚙️ DAG: Управление конфигурацией CTL (Change Tracking & Loading)

Этот DAG предназначен для **настройки и хранения глобальных параметров** системы CTL. Он позволяет централизованно управлять конфигурацией, используемой другими компонентами: `sensor`, `loader`, `worker`.

> ⚠️ **Выполняется один раз вручную** при изменении настроек. Не запускается автоматически.

---

#### 🎯 Назначение

- Централизованное хранение конфигурации CTL.
- Сохранение параметров в Airflow Variable `ctl_config`.
- Валидация прав доступа через PIN-код.
- Настройка TTL для S3-хранилища (автоудаление старых объектов).

---

#### 📌 Основная задача

| Задача | Описание |
|-------|----------|
| `config_save` | Сохраняет обновлённые параметры в `Variable` и настраивает S3. |

---

#### ⚙️ Параметры DAG (через UI)

DAG использует `params` для безопасного редактирования конфигурации через интерфейс Airflow.

| Параметр | Описание | Пример / Значение по умолчанию |
|---------|----------|-------------------------------|
| `profile` | Имя профиля CTL | `HR_Data` |
| `root_category` | Корневая категория workflow'ов | `p1080` |
| `root_entity` | Корневая сущность в иерархии | `941010000` |
| `ue_category` | Категория внешних событий (UE) | `p1080.sdpue` |
| `gp_conn_id` | ID подключения к Greenplum | `alpha-adb_dev_comm-read` |
| `gp_schema` | Схема GP для выполнения логики | `s_grnplm_vd_hr_edp_srv_wf` |
| `gp_timeout` | Таймаут выполнения SQL | `hours=4` |
| `gp_task_timeout` | Таймаут задачи GP | `hours=5` |
| `s3_conn_id` | Подключение к S3 | `s3` |
| `ctl_bucket` | Бакет для хранения метаданных | `ctl` |
| `ctl_ttl` | Время жизни объектов в S3 (дни) | `7` |
| `ctl_conn_id` | Подключение к API CTL | `ctl` |
| `ctl_timeout` | Таймаут запросов к API (сек) | `60` |
| `ctl_pool_slots` | Размер пула `ctl_pool` | `20` |
| `ctl_limit` | Лимит загрузки сущностей | `1000` |
| `ctl_days` | Глубина выгрузки событий (дней) | `5` |
| `ctl_url` | URL интерфейса CTL | `https://ctl-dev.dev.df.sbrf.ru:9080` |
| `tz` | Часовой пояс | `Europe/Moscow` |
| `expire` | Время ожидания события по умолчанию | `time=0:00` |
| `CTL_PIN` | Защитный PIN для сохранения | (скрыто) |

> 💡 **PIN-защита:** Чтобы сохранить изменения, значение `CTL_PIN` должно совпадать с переменной окружения `AIRFLOW__CTL_PIN`.

---

#### 🔄 Логика выполнения

1. Пользователь запускает DAG вручную.
2. Редактирует параметры в UI.
3. Вводит `CTL_PIN` для подтверждения.
4. Задача:
   - Проверяет PIN.
   - Сохраняет конфиг в `Variable['ctl_config']`.
   - Обновляет TTL для S3-бакета.

---

#### 🏷️ Метаданные

- **ID DAG:** `CTL.<profile>.config`
- **Теги:** `['CTL', '<profile>', 'CTL_agent', 'config']`
- **Расписание:** `@once` (выполняется один раз)
- **Catchup:** `False`
- **Max active runs:** `1`
- **Таймаут:** `5 минут`
- **Пул:** `default`

---

> 💡 *Примечание:* При первом запуске создает `Variable['ctl_config']`.  
> Все последующие изменения должны подтверждаться PIN-кодом.
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.models import Variable, Param

from plugins.utils import on_callback, add_note, default_args, str2timedelta, get_conns_by_type, get_conn 
from plugins.s3_utils import s3_set_ttl, s3_create_bucket
import os
import pendulum

from logging import  getLogger
logger = getLogger('airflow.task')

conf = Variable.get('ctl_config', default_var={}, deserialize_json=True)

conns = {
    'ctl': {
        'type': 'KerberosHttp',
        'conn_id': 'ctl',
        'pool_slots': [10, 40],
        'timeout': 30, # in seconds
        'url': "https://ctl-dev.dev.df.sbrf.ru:9080",
    },
    'gp': {
        'type': 'Postgres',
        # 'conn_id': 'alpha-adb_dev_comm-read', 
        'conn_id': conf.get('conns', {}).get('gp', {}).get('conn_id') or (
            [ c for c in get_conns_by_type('postgres') 
                  if c.startswith('alpha-') and c.endswith('-read')
            ] or ['alpha-capgp2-read'])[0], 
        'pool_slots': 20,
        'timeout': 300, # in seconds
        'schema': 's_grnplm_vd_hr_edp_srv_wf',
    },
    'pg': {
        'conn_id': 'airflowdb',
        'type': 'Postgres',
        'pool_slots': 20,
        'default': True,
    },
    's3': {
        'type': 'S3',
        'conn_id': 's3',
        'pool_slots': 20,
        "bucket": "edpetl-ctl",
        "ttl": 7, # days
    },
    'files': {
        'type': 'S3',
        'conn_id': 's3-archive',
        'pool_slots': 20,
        "bucket": "edpetl-files",
        "ttl": 30, # days
    },
    'tfs': {
        'type': 'S3',
        'conn_id': 's3',
        'pool_slots': 20,
        "bucket": "edpetl-tfs",
        "ttl": 30, # days
    },
}

config = { 
    'profile': 'HR_Data',
    'root_entity': '941010000',
    'root_category': 'p1080',
    'ue_category': "p1080.sdpue",
    "archive_category": "p1080.ARCHIVE",
    "event_expire": "time=0:00",
    'task_timeout': 'hours=1', 
    'exe_timeout': 'hours=4',
    'sla_time': 'hours=1', 
    'ctl_limit': 1000,  #сколько записей запросить из CTL
    'ctl_days': 5, #сколько дней назад запросить из CTL
    # 'ctl_task_timeout': 'hours=+5',
    'tz': 'Europe/Moscow',
    'conns': conns,
    **conf,
}

if not conf:
    ctl = get_conn('ctl')
    config['conns']['ctl']['url'] = f"{ctl.get('schema', 'https')}://{ctl.get('host')}:{ctl.get('port','9080')}"
    Variable.set('ctl_config', config, serialize_json=True, description=str(pendulum.now(config['tz']))[:19])

with DAG(f'CTL.{config["profile"]}.config',
    tags=['CTL', 'CTL_agent', 'tools'],
    start_date=days_ago(1),
    schedule='@once',
    catchup=False,
    default_args={ **default_args,
        "retries": 0,
        "on_failure_callback": on_callback,
        # "on_success_callback": on_callback,
    },    max_active_runs=1,
    is_paused_upon_creation=False,
    on_failure_callback=on_callback,
    on_success_callback=on_callback,
    dagrun_timeout=str2timedelta(config.get('dagrun_timeout','minutes=10')),
    params={
        **config,
        "CTL_PIN": '',
    },
    doc_md=__doc__,
) as dag:
    
    @task
    def config_save(**context):
        """### Сохранение конфигурации CTL

        Выполняет:
        - Проверку PIN-кода (`CTL_PIN == AIRFLOW__CTL_PIN`).
        - Сохранение параметров в Airflow Variable `ctl_config`.
        - Настройку срока хранения (TTL) для S3-бакета.

        **Логика:**
        - Если PIN не совпадает — сохранение отменяется.
        - После успешного сохранения — обновляется TTL в S3.

        **XCom Output:** полный словарь конфигурации.

        **Использование:**
        - Только для администраторов.
        - Требуется ручной запуск с подтверждением.
        """
        
        config = context["params"]
        pin = config.pop('CTL_PIN')
        if pin == os.getenv("AIRFLOW__CTL_PIN"):
            from plugins.ctl_utils import ctl_obj_save # type: ignore
            # Save config to Variable
            ctl_obj_save('ctl_config', config, var=True)
            
            msg = "✅ Configuration successfully saved to Variable 'ctl_config'"
            
            for e in os.environ:
                if e.startswith('AIRFLOW__'):
                    logger.info("⚠️{}: {}".format(e, os.getenv(e)))
        else:
            msg = "⚠️ Save skipped: 'CTL_PIN' is False}"
        
        add_note(msg, context, 'DAG,Task')
        
        
        s3_id = config.get('conns',{}).get('s3',{}).get('conn_id')
        bucket = config.get('ctl_bucket')
        ttl = config.get('ctl_ttl')
        if s3_id and bucket:
            s3_create_bucket(s3_id, bucket)
            
            if ttl:
                response = s3_set_ttl(s3_id, bucket, days=ttl, prefix='')
                logger.info(response)
        
        
        s3_id = config.get('conns',{}).get('files',{}).get('conn_id')
        bucket = config.get('files_bucket')
        ttl = config.get('files_ttl')
        if s3_id and bucket:
            s3_create_bucket(s3_id, bucket)
            
            if ttl:
                response = s3_set_ttl(s3_id, bucket, days=ttl, prefix='')
                logger.info(response)
        
        # conn = get_conn('ctl')
        # add_note(conn)
        
        return config
    
    config_save()
