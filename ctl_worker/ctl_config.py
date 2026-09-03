"""### 🔐 DAG: Конфигурация CTL
*2026-09-03 10:20 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Сохраняет параметры системы в `Variable['ctl_config']`. Запускается вручную. Требует PIN-код (`CTL_PIN` = `AIRFLOW__CTL_PIN`).

| Параметр | Описание |
|---|---|
| `profile` / `root_category` / `root_entity` / `ue_category` | Профиль и иерархия CTL |
| `gp_conn_id` / `gp_schema` / `gp_timeout` / `gp_task_timeout` | Подключение Greenplum |
| `s3_conn_id` / `ctl_bucket` / `ctl_ttl` | S3-хранилище |
| `ctl_conn_id` / `ctl_url` / `ctl_timeout` | CTL API |
| `ctl_pool_slots` / `ctl_limit` / `ctl_days` | Лимиты CTL |
| `tz` / `expire` | Часовой пояс и таймаут ожидания |
| `simulator` / `test_mode` / `test_sleep` | Отладочные режимы: генератор нагрузки и фиктивное выполнение. Действуют не на всех контурах — см. `ctl_test.py` и `ctl_worker.py` |
| `CTL_PIN` | PIN подтверждения (скрыто) |
"""

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable, Param

from plugins.utils import on_callback, add_note, default_args, str2timedelta, get_conns_by_type, get_conn  # type: ignore
from plugins.s3_utils import s3_set_ttl, s3_create_bucket  # type: ignore
import os
import base64
import json
import pendulum

from logging import  getLogger
from datetime import datetime, timezone
logger = getLogger('airflow.task')

def get_scrt(s: str) -> str:
    """Читает секрет из /vault/secrets/application и декодирует base64 → UTF-8."""
    with open('/vault/secrets/application') as f:
        secrets = json.load(f)
    return base64.b64decode(secrets[s]).decode()


conf = Variable.get('ctl_config', default_var={}, deserialize_json=True)


def enum_default(value, allowed, fallback='off'):
    """Значение по умолчанию для списка в форме.

    В Variable может лежать что угодно — устаревшее значение или опечатка, — а `Param` с
    `enum` роняет запуск, если значение вне списка. Непонятное гасим в `fallback`: тракт
    от этого не меняется (его читают ctl_test.py и ctl_worker.py), а форма открывается.
    """
    v = str(value or '').strip().lower()
    return v if v in allowed else fallback

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

# Режимы отладочных ключей. Контуры, на которых они вообще действуют, задаются в коде
# потребителей (ctl_test.py, ctl_worker.py) и отсюда не управляются — это и есть смысл
# гейта: контур не должен переопределяться настройкой.
SIMULATOR_MODES = ['off', 'event', 'dataset', 'trigger']
TEST_MODES = ['off', 'ok', 'ok-no', 'ok-no-error']

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
    'simulator': 'off',        # генератор нагрузки, ctl_test.py: off/event/dataset/trigger
    'test_mode': 'off',        # фиктивное выполнение, ctl_worker.py: off/ok/ok-no/ok-no-error
    'test_sleep': 'minutes=45',# верхняя граница ожидания вместо процедуры воркфлоу
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
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
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
        # Списком, а не руками: значения разбираются кодом, опечатка молча выключает режим.
        # В сам config кладутся простые строки — он же уходит в Variable, а Param не
        # сериализуется.
        'simulator': Param(enum_default(config.get('simulator'), SIMULATOR_MODES), type='string',
                           enum=SIMULATOR_MODES,
                           title='Симулятор нагрузки (event — только DEV)'),
        'test_mode': Param(enum_default(config.get('test_mode'), TEST_MODES), type='string',
                           enum=TEST_MODES,
                           title='Фиктивное выполнение (только DEV и IFT)'),
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
        if pin == get_scrt("AIRFLOW__CTL_PIN"):
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
        
        
        # Бакеты и сроки хранения. Создание бакета оставлено без перехвата: без него
        # загрузкам некуда писать, и об этом надо знать сразу. А вот lifecycle-правило
        # к моменту вызова уже ничего не решает — конфигурация сохранена выше, и ронять
        # из-за него таск незачем: красный таск после успешного сохранения сбивает с толку.
        for name, bucket_key, ttl_key in (('s3', 'ctl_bucket', 'ctl_ttl'),
                                          ('files', 'files_bucket', 'files_ttl')):
            s3_id = config.get('conns',{}).get(name,{}).get('conn_id')
            bucket = config.get(bucket_key)
            ttl = config.get(ttl_key)
            if not (s3_id and bucket):
                continue

            s3_create_bucket(s3_id, bucket)

            if not ttl:
                continue
            try:
                logger.info(s3_set_ttl(s3_id, bucket, days=ttl, prefix=''))
                add_note(f"⏱️ TTL {ttl}д на {bucket}", context, 'Task')
            except Exception as err:
                logger.warning("⚠️ TTL на %s не выставлен: %s", bucket, err, exc_info=True)
                add_note(f"⚠️ TTL на {bucket} не выставлен: {err}", context, 'Task')
        
        # conn = get_conn('ctl')
        # add_note(conn)
        
        return config
    
    config_save()
