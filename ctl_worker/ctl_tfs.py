"""#### 🔍 DAG: Перемещение файлов из TFS в S3 Archive (Change Tracking & Loading)

Этот DAG предназначен для **автоматического копирования файлов из внешних систем TFS (Transfer File System)** в централизованное хранилище S3 (`edpetl-files`), где они становятся доступны для последующей обработки ETL-процессами.

---

#### 🎯 Назначение

- Мониторинг входящих директорий TFS на наличие новых файлов.
- Поддержка триггеров по `.done`-файлам (указывает на завершение выгрузки).
- Распаковка ZIP-архивов при необходимости.
- Перемещение и архивация файлов в S3 с сохранением структуры и метаданных.
- Публикация событий через **Dataset** для запуска зависимых DAG'ов.

---

#### ⚙️ Особенности реализации

- **Частота опроса:** каждые 5 минут (`schedule_interval: minutes=5`)
- **Режим опроса:** `reschedule` — освобождает слоты Airflow при отсутствии файлов.
- **Пул выполнения:** `files_pool` — выделен для операций с файлами.
- **Поддержка нескольких источников TFS:** конфигурируется через `conns` с типом `tfs-in`.
- **Гибкая маршрутизация:** каждый источник имеет свой `tfs_id`, префикс и поведение.
- **Обработка архивов:** при включённом `unzip` — извлекает содержимое ZIP.
- **Сжатие:** опционально сжимает файлы при переносе (`compress=True`).

---

#### 📌 Ключевые задачи

| Задача | Описание |
|-------|----------|
| `tfs_wait` | Сенсор, ожидающий появления файлов в TFS (или `.done`-триггера). |
| `tfs_copy` | Копирует, распаковывает и архивирует файлы; публикует Dataset. |

---

#### 🏷️ Метаданные

- **ID DAG:** `CTL.<profile>.tfs_sensor`
- **Теги:** `['CTL', '<profile>', 'CTL_agent', 'tfs_sensor']`
- **Max active runs:** `1`
- **Catchup:** `False`
- **Пул:** `files_pool`
- **Режим выполнения:** sensor + reschedule

---

#### 🔄 Логика работы

1. **Ожидание файлов**:
   - Периодически проверяет пути из `tfs_conns`.
   - При ручном запуске — использует параметры из UI.
2. **Обнаружение**:
   - Находит новые файлы или `.done`-файлы.
   - Игнорирует временные/служебные файлы.
3. **Копирование и обработка**:
   - Перемещает файл из TFS в `edpetl-files`.
   - При `unzip=True` — распаковывает ZIP, сохраняя структуру.
   - Архивный файл перемещается в `tfs-archive/`, ошибки — в `tfs-errors/`.
4. **Публикация события**:
   - Отправляет сигнал через `DatasetAlias("TFS/<profile>")`.
   - В `extra` передаёт путь, размер, количество файлов.

---

#### 📥 Параметры (через UI)

| Параметр | Описание | Пример |
|---------|----------|--------|
| `path` | Путь к файлу в формате `conn_id://bucket/prefix/mask` | `tfs-alpha://incoming/data_*.csv` |
| `tfs_id` | Идентификатор источника (для логики) | `sales_data` |
| `compress` | Сжимать ли файл при копировании | `true` |
| `done` | Удалить исходный файл после копирования | `true` |
| `unzip` | Распаковать ZIP-архив | `true` |

> 💡 *Примечание:* DAG поддерживает как автоматический режим, так и ручной запуск с указанием параметров.  
> Все действия логируются через `add_note` и доступны в интерфейсе Airflow.
"""

from airflow import DAG
from airflow.models import Param
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor 
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago
from airflow.sensors.base import PokeReturnValue
from airflow.datasets import DatasetAlias, Dataset

from plugins.utils import readable_size, add_note, on_callback, str2timedelta
from plugins.s3_utils import s3_move_s3, s3_path_parse, s3_from_zip, s3_keys 
from plugins.ctl_utils import get_config   # type: ignore

import pendulum
from fnmatch import fnmatch
from datetime import timedelta
from logging import getLogger

logger = getLogger("airflow.task")

conns =get_config().get('conns', {})
files_conn = conns.get('files')
profile = get_config()["profile"]

# tfs_conns = {k: f"{c['conn_id']}://{c['mask']}" for k, c in conns.items() if c.get('type') == 'tfs-in'}
tfs_conns = {
    f"{c['conn_id']}://{c['mask']}" : { **c , 'tfs_id': k, } 
    for k, c in conns.items() 
    if c.get('type') == 'tfs-in'
}

TFS_IN_DATASET = f'CTL/{profile}/TFS'
MAX_ITEMS = 25


tfs_interval = str2timedelta(get_config().get('tfs_interval','minutes=5'))
# Основная логика DAG
with DAG(f'CTL.{get_config()["profile"]}.tfs_sensor',
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'tfs_sensor'],
    start_date=days_ago(1),
    schedule_interval=tfs_interval,
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
        'pool': 'files_pool',
        'max_active_tis_per_dag': 5,
    },
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
    params={ 
        "path": Param('', type="string", examples=list(tfs_conns.keys())), 
        "tfs_id": "manual",
        "compress": False, 
        "done": False,
        "unzip": False, 
    },
    on_failure_callback=on_callback,
    on_success_callback=None,
    doc_md=__doc__,
) as dag:

    @task.sensor(
        mode='reschedule', 
        soft_fail=True,
        poke_interval=tfs_interval,
        timeout=pendulum.duration(hours=24),
    )
    def tfs_wait(**context):
        """ Ожидание появления файла в TFS """
        ti = context['ti']
        manual = context['run_id'].startswith('manual__')
        params = context['params']
        
        if manual and params.get('path'):
            tfs = { 
                params['path']: dict(
                    tfs_id = params.get('tfs_id', 'manual)'), 
                    compress = params.get('compress', False),
                    done = params.get('done', False),
                    unzip = params.get('unzip', False),
                ) 
            }
        else: 
            tfs = tfs_conns
        
        add_note(tfs, context, level='DAG,Task', title='TFS')
        ti.xcom_push(key='tfs', value=tfs)
        
        for path, tfs_prm,  in tfs.items():

            tfs_prm = { **tfs_prm, **s3_path_parse(path)}
            
            objects = s3_keys(path)
            
            if objects:
                ti.xcom_push(key='tfs_src', value=tfs_prm)
                # Сортируем: самые свежие файлы будут Последними в списке
                objects = dict(sorted(objects.items(), key=lambda item: item[1]))
                add_note(objects, context, level='DAG,Task', title=f'Files ({len(objects)}) ')
                
                # Забираем имена ключей для XCom
                ret_keys = {
                    f"{tfs_prm['conn_id']}://{tfs_prm['bucket']}/{key}" : value
                    for i, (key, value) in enumerate(objects.items())
                    if i < int(MAX_ITEMS)
                }
                
                return PokeReturnValue(is_done=True, xcom_value=ret_keys)
        
        if manual:
            msg = f">❌ No files found in TFS"
            add_note(msg, context, level='DAG,Task')
            raise AirflowFailException(msg)
        else:
            return False
    
    
    @task(map_index_template="{{ path[0] }}", outlets=[DatasetAlias(f"TFS/{profile}")],)
    def tfs_copy(path: str, **context):
        """ Копирование файла из TFS в S3 """
        ti = context['ti']
        src_path = path[0]
        src_info = path[1]
        src = ti.xcom_pull(key='tfs_src', task_ids='tfs_wait')
        
        tfs_id = src['tfs_id']
        prefix = src['prefix']
        compress = src.get('compress', False)
        dst_done = src.get('done', False)
        unzip = src.get('unzip', False)
        
        err_prefix = get_config().get('tfs_err_prefix') or 'tfs-errors'
        arc_prefix = get_config().get('tfs_arc_prefix') or 'tfs-archive'

        # Если на входе done-файл, определяем путь к реальным данным
        is_done_trigger = src_path.lower().endswith('.done')
        src_path = src_path[:-5] if is_done_trigger else src_path        
        
        src = { **src,
            'path': src_path,
            'search': prefix + src['file'],
            'info': src_info,
            **s3_path_parse(src_path),
        }
        ti.xcom_push(key='tfs_src', value=src)
        add_note(src, context, level='Task', title='Source')
        
        src_hook = S3Hook(aws_conn_id=src['conn_id'], verify=False) 
        src_obj = src_hook.get_key(src['key'], src['bucket'])
        sdt = pendulum.instance(src_obj.last_modified).in_timezone(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss')
        
        dst = {}
        dst['conn_id'] = files_conn['conn_id']
        dst['bucket'] = files_conn['bucket']
        # dst['tfs_id'] = src['tfs_id']
        # dst['compress'] = compress
        # dst['done'] = dst_done
        # dst['prefix'] = files_conn.get('prefix','tfs-in').strip('/')
        dst['prefix'] = tfs_id + '/'
        
        # Берем целевой префикс + остаток пути от источника (без старого префикса и расширения)
        # Добавляем метку времени и возвращаем расширение
        dst['key'] = src['key'][:-len(src['ext'])].replace(prefix, dst['prefix'], 1)
        dst['key'] += ' ' + sdt + src['ext'] if src['ext'] else ''
        
        dst_path = f"{dst['conn_id']}://{dst['bucket']}/{dst['key']}"
        dst['path'] = dst_path
        ti.xcom_push(key='tfs_dst', value=dst)
        
        size = src_obj.content_length
        count = 1

        try:
            try:
                if unzip and src['ext'].lower() =='.zip':
                    # 1. Распаковка содержимого
                    size, count, stats = s3_from_zip(src_path, dst_path, compress=compress)
                    add_note(stats, context, level='Task')
                    
                    # 2. Формируем путь для переноса САМОГО архива в archive/
                    # dst_key = '/tfs-archive' + dst['key'][len(dst['prefix']):]
                    # dst_key = dst['key'].replace(dst['prefix'], arc_prefix, 1)
                    dst_done = dst_path if dst_done else False
                    dst_path = f"{dst['conn_id']}://{dst['bucket']}/{arc_prefix}/{dst['key']}"
                    
            except Exception as e:
                add_note(f"❌ Failed to process file: {e}", context, level='Task,DAG')
                # Путь для ошибок: tfs-in/errors/file.zip
                # dst_key = dst['key'].replace(dst['prefix'], err_prefix, 1)
                dst_path = f"{dst['conn_id']}://{dst['bucket']}/{err_prefix}/{dst['key']}"
                count, size = 0, 0
                
            # Выполняем перенос исходного файла (или архива)
            s3_move_s3(src_path, dst_path, compress=compress, done=dst_done)
            add_note(f"🚚 move: {src_path} -> {dst_path}", context, level='Task,DAG')

            # УДАЛЕНИЕ done-файла
            if is_done_trigger:
                src_hook.delete_objects(keys=src['key']+'.done',  bucket=src['bucket'])
                add_note(f"🗑️ .done deleted: {src_path}", context, level='Task')
            # src_hook.delete_objects(keys=src['key'],  bucket=src['bucket'])

        except Exception as e:
            add_note(f"❌ Transfer failed: {e}", context, level='Task,DAG')
            raise AirflowFailException(f"🚨 Failed {src_path}")
        
        if count:
            # key = dst['key'].split(' ')[0].split('_')[0]
            ds = Dataset(f"TFS/{profile}/{tfs_id}")
            add_note(ds, context, level='Task')
            context['outlet_events'][f"TFS/{profile}"].add(ds, extra={**dst, 'count':count, 'size':readable_size(size)})    
            
            msg = f"✅ Successfully processed {src_path} ({count})| Size: {readable_size(size)}"
        else:
            msg = f"❌ Failed to process {src_path}"
        add_note(msg, context, level='Task,DAG')
        return msg


    path_list = tfs_wait()

    # Запускаем параллельно 
    tfs_copy.partial().expand( path = path_list ) 
