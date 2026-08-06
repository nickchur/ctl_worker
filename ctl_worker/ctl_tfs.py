"""### 📁 CTL TFS → S3
*2026-08-06 19:25 MSK · v1.1 · Чуркин Николай · [nschurkin@sberbank.ru](mailto:nschurkin@sberbank.ru)*

Модуль содержит два DAG'а для копирования файлов из TFS (источник S3) в `edpetl-files`.

---

#### `CTL.<profile>.tfs_sensor` — по расписанию

Каждые N минут (параметр `tfs_interval`, по умолчанию 5 мин) опрашивает все `tfs-in`-источники из конфига. При обнаружении файлов копирует их в S3 и публикует `DatasetAlias("TFS/<profile>")`.

| Параметр (UI) | Описание |
|---|---|
| `path` | `conn_id://bucket/prefix/mask` (при ручном запуске) |
| `tfs_id` | Идентификатор источника |
| `compress` | Сжать при копировании |
| `done` | Удалить исходник после копирования |
| `unzip` | Распаковать ZIP-архив |

---

#### `CTL.<profile>.tfs_kafka` — по событию Kafka

Запускается внешним триггером. Читает сообщение `TransferFileCephRq` из Kafka, копирует указанные в XML файлы в S3, затем отправляет квитанцию `TransferFileCephRs` (StatusCode=0 при успехе, 104 при ошибке).

`ScenarioId` из XML используется как `tfs_id`; `RqUID` — в квитанцию.

| Параметр (UI) | Описание |
|---|---|
| `path` | Базовый путь в TFS: `conn_id://bucket/prefix/` |
| `tfs_id` | Fallback-идентификатор (если `ScenarioId` пуст) |
| `compress` | Сжать при копировании |
| `done` | Удалить исходник после копирования |
| `unzip` | Распаковать ZIP-архив |
| `kafka_rcv` | `kafka_config_id` для чтения — по умолчанию `tfs-kafka-out`; список kafka-коннектов берётся из Variable `local_connections` |
| `topic_rcv` | Топик входящих сообщений |
| `timeout` | Таймаут ожидания (мин, по умолчанию 60) |
| `kafka_snd` | `kafka_config_id` для квитанции — по умолчанию `tfs-kafka-in` |
| `topic_snd` | Топик квитанций (необязательно: пустой — квитанция не отправляется) |
"""

from airflow import DAG
from airflow.models import Param
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor 
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sensors.base import PokeReturnValue
from airflow.datasets import DatasetAlias, Dataset

from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.utils.session import create_session
from airflow.models import TaskInstance, Variable
from airflow.utils.state import State

from plugins.utils import readable_size, add_note, on_callback, str2timedelta
from plugins.s3_utils import s3_move_s3, s3_path_parse, s3_from_zip, s3_keys
from plugins.ctl_utils import get_config   # type: ignore

import xml.etree.ElementTree as ET
import pendulum
from fnmatch import fnmatch
from datetime import timedelta, datetime, timezone
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


# IN/OUT в именах — сторона TFS, и у соединения, и у топика: читаем из его выхода,
# квитанцию кладём в его вход
KAFKA_RCV_CONN = 'tfs-kafka-out'
KAFKA_SND_CONN = 'tfs-kafka-in'


def _kafka_conn_ids() -> list[str]:
    """conn_id kafka-соединений из Variable `local_connections` — для выпадающего списка.

    Variable наполняет DAG `tools_show_connections`: {conn_type: [{conn_id, host, ...}]}.
    Читается на парсинге, иначе examples не попадут в форму запуска. Дефолтные коннекты
    держим в списке всегда: без них выпадашка откроется без своего же значения, а при
    недоступной Variable осталась бы пустой.
    """
    conn_ids = {KAFKA_RCV_CONN, KAFKA_SND_CONN}
    try:
        var_data = Variable.get('local_connections', deserialize_json=True, default_var=None) or {}
        conn_ids |= {c['conn_id'] for c in var_data.get('kafka', []) if c.get('conn_id')}
    except Exception as exc:
        logger.warning("Не прочитали local_connections, список conn_id только из дефолтов: %s", exc)
    return sorted(conn_ids)


KAFKA_CONN_IDS = _kafka_conn_ids()


tfs_interval = str2timedelta(get_config().get('tfs_interval','minutes=5'))


def _kafka_wait_any(conn_id: str, topic: str, timeout_min: int) -> str | None:
    """Ждёт любое сообщение из топика опросом на воркере, не дольше timeout_min минут.

    Возвращает текст сообщения или None, если ничего не пришло. Группу берём из коннекта
    и коммитим offset полученного сообщения, чтобы оно не пришло повторно.
    """
    import time

    from airflow.hooks.base import BaseHook
    from confluent_kafka import Consumer

    conn = BaseHook.get_connection(conn_id)
    config = dict(conn.extra_dejson)  # extra_dejson = librdkafka config (контракт Kafka-провайдера)
    config.setdefault("bootstrap.servers", conn.host)
    config["enable.auto.commit"] = False

    consumer = Consumer(config)
    try:
        consumer.subscribe([topic])
        deadline = time.time() + timeout_min * 60
        while time.time() < deadline:
            msg = consumer.poll(timeout=5)
            if msg is None or msg.error():
                continue
            text = msg.value().decode("utf-8", errors="replace")
            consumer.commit(message=msg, asynchronous=False)
            logger.info("Kafka message received from %s [%s]", msg.topic(), msg.partition())
            return text
    finally:
        consumer.close()
    return None


def _produce_receipt(rq_uid: str, status_code: int, status_desc: str):
    """Генератор квитанции TransferFileCephRs для ProduceToTopicOperator."""
    message = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        f'<TransferFileCephRs>'
        f'<RqUID>{rq_uid}</RqUID>'
        f'<RqTm>{pendulum.now().format("YYYY-MM-DDTHH:mm:ss.SSSZ")}</RqTm>'
        f'<StatusCode>{status_code}</StatusCode>'
        f'<StatusDesc>{status_desc}</StatusDesc>'
        f'</TransferFileCephRs>'
    )
    logger.info("Receipt prepared: rq_uid=%s status=%s", rq_uid, status_code)
    yield None, message


def _on_delivery_tfs(err, msg) -> None:
    """Delivery-колбэк: падает при ошибке доставки в Kafka."""
    if err:
        raise AirflowFailException(f"Kafka delivery failed: {err}")
    logger.info("Receipt delivered to %s [%s]", msg.topic(), msg.partition())


@task(map_index_template="{{ path[0] }}", outlets=[DatasetAlias(f"TFS/{profile}")],)
def tfs_copy(path: str, **context):
    """Копирует файл из TFS в S3; при unzip — распаковывает архив; при ошибке перемещает в tfs-errors."""
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
                dst_done = dst_path if dst_done else False
                dst_path = f"{dst['conn_id']}://{dst['bucket']}/{arc_prefix}/{dst['key']}"

        except Exception as e:
            add_note(f"❌ Failed to process file: {e}", context, level='Task,DAG')
            dst_path = f"{dst['conn_id']}://{dst['bucket']}/{err_prefix}/{dst['key']}"
            count, size = 0, 0

        # Выполняем перенос исходного файла (или архива)
        s3_move_s3(src_path, dst_path, compress=compress, done=dst_done)
        add_note(f"🚚 move: {src_path} -> {dst_path}", context, level='Task,DAG')

        # УДАЛЕНИЕ done-файла
        if is_done_trigger:
            src_hook.delete_objects(keys=src['key']+'.done', bucket=src['bucket'])
            add_note(f"🗑️ .done deleted: {src_path}", context, level='Task')

    except Exception as e:
        add_note(f"❌ Transfer failed: {e}", context, level='Task,DAG')
        raise AirflowFailException(f"🚨 Failed {src_path}")

    if count:
        ds = Dataset(f"TFS/{profile}/{tfs_id}")
        add_note(ds, context, level='Task')
        context['outlet_events'][f"TFS/{profile}"].add(ds, extra={**dst, 'count': count, 'size': readable_size(size)})

        msg = f"✅ Successfully processed {src_path} ({count})| Size: {readable_size(size)}"
    else:
        msg = f"❌ Failed to process {src_path}"
    add_note(msg, context, level='Task,DAG')
    return msg


# Основная логика DAG
with DAG(f'CTL.{get_config()["profile"]}.tfs_sensor',
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'tfs_sensor'],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
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
        """Сканирует TFS-источники по маске; при ручном запуске берёт path из params."""
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
    
    
    path_list = tfs_wait()

    # Запускаем параллельно
    tfs_copy.partial().expand(path=path_list)


# ── Kafka-triggered DAG ──────────────────────────────────────────────────────
with DAG(f'CTL.{get_config()["profile"]}.tfs_kafka',
    tags=['CTL', get_config()['profile'], 'CTL_agent', 'tfs_kafka'],
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    default_args={
        'owner': 'EDP.ETL',
        'depends_on_past': False,
        'email': ['p1080@sber.ru'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1),
        'on_failure_callback': on_callback,
        'pool': 'files_pool',
    },
    max_active_runs=3,
    catchup=False,
    render_template_as_native_obj=True,
    params={
        "path":     Param('', type="string", examples=list(tfs_conns.keys())),
        "tfs_id":   "manual",
        "compress": False,
        "done":     False,
        "unzip":    False,
        # направления по стороне TFS: читаем через tfs-kafka-out, пишем через tfs-kafka-in
        "kafka_rcv": Param(KAFKA_RCV_CONN, type="string", description="kafka_config_id для чтения",
                           examples=KAFKA_CONN_IDS),
        "topic_rcv": Param('', type="string", description="Kafka topic входящих сообщений"),
        "timeout":   Param(60, type="integer", description="Таймаут ожидания сообщения (мин)"),
        "kafka_snd": Param(KAFKA_SND_CONN, type="string", description="kafka_config_id для отправки квитанции",
                           examples=KAFKA_CONN_IDS),
        "topic_snd": Param('', type="string", description="Kafka topic для квитанции"),
    },
    on_failure_callback=on_callback,
    doc_md=__doc__,
) as dag_kafka:

    @task(task_id='kafka_wait')
    def kafka_wait(**context):
        """⏳ Ждёт любое сообщение из топика опросом на воркере.

        Раньше здесь стоял AwaitMessageSensor, но он deferrable: без живого Triggerer
        таск уходит в deferred и висит со статусом unknown. Поведение сохранено:
        ждём не дольше params.timeout минут, по истечении — skip (был soft_fail=True).
        """
        p = context['params']
        if not p.get('kafka_rcv') or not p.get('topic_rcv'):
            raise AirflowFailException("Params 'kafka_rcv' and 'topic_rcv' are required")

        text = _kafka_wait_any(p['kafka_rcv'], p['topic_rcv'], int(p.get('timeout', 60)))
        if text is None:
            raise AirflowSkipException(f"Сообщение в {p['topic_rcv']} не пришло за {p.get('timeout', 60)} мин")
        return text

    @task
    def tfs_wait(**context):
        """Парсит XML TransferFileCephRq из Kafka, строит path_list для tfs_copy."""
        ti = context['ti']
        params = context['params']

        msg_value = ti.xcom_pull(task_ids='kafka_wait')
        if not msg_value:
            raise AirflowFailException("No Kafka message received")
        add_note({'kafka_msg': str(msg_value)[:200]}, context, level='Task', title='KafkaMsg')

        root = ET.fromstring(msg_value)
        file_infos = root.findall('.//{*}FileInfo') or root.findall('.//FileInfo')

        rq_uid      = (root.findtext('.//{*}RqUID')      or root.findtext('.//RqUID')      or '').strip()
        scenario_id = (root.findtext('.//{*}ScenarioId') or root.findtext('.//ScenarioId') or '').strip()
        ti.xcom_push(key='rq_uid', value=rq_uid)

        tfs_prm = {**s3_path_parse(params['path']), **{
            'tfs_id':   scenario_id or params.get('tfs_id', 'manual'),
            'compress': params.get('compress', False),
            'done':     params.get('done', False),
            'unzip':    params.get('unzip', False),
        }}
        ti.xcom_push(key='tfs_src', value=tfs_prm)

        ret_keys = {}
        for file_info in file_infos[:MAX_ITEMS]:
            name   = (file_info.findtext('{*}Name') or file_info.findtext('Name') or '').strip()
            folder = (file_info.findtext('{*}FolderSource') or file_info.findtext('FolderSource') or '').strip('/')
            if not name:
                continue
            prefix = tfs_prm['prefix']
            subdir = folder + '/' if folder else ''
            key    = f"{prefix}{subdir}{name}"
            path   = f"{tfs_prm['conn_id']}://{tfs_prm['bucket']}/{key}"
            ret_keys[path] = {}

        if not ret_keys:
            raise AirflowFailException("No files found in Kafka message")
        add_note(ret_keys, context, level='DAG,Task', title='Kafka Files')
        return ret_keys

    def _pre_receipt(context):
        """Проверяет статусы tfs_copy и формирует аргументы квитанции TransferFileCephRs."""
        p = context['params']
        if not p.get('kafka_snd') or not p.get('topic_snd'):
            from airflow.exceptions import AirflowSkipException
            raise AirflowSkipException("kafka_snd/topic_snd not set, skipping receipt")
        context['task'].kafka_config_id = p['kafka_snd']
        context['task'].topic = p['topic_snd']

        ti = context['task_instance']
        rq_uid = ti.xcom_pull(task_ids='tfs_wait', key='rq_uid') or ''

        with create_session() as session:
            copy_tis = session.query(TaskInstance).filter(
                TaskInstance.dag_id == ti.dag_id,
                TaskInstance.run_id == ti.run_id,
                TaskInstance.task_id == 'tfs_copy',
            ).all()
        has_error = any(t.state in (State.FAILED, State.UPSTREAM_FAILED) for t in copy_tis)
        status_code = 104 if has_error else 0
        status_desc = 'Successful' if status_code == 0 else 'Transfer failed'
        context['task'].producer_function_args = [rq_uid, status_code, status_desc]

    send_receipt = ProduceToTopicOperator(
        task_id='send_receipt',
        kafka_config_id='',
        topic='',
        producer_function=_produce_receipt,
        delivery_callback=_on_delivery_tfs,
        pre_execute=_pre_receipt,
        trigger_rule='all_done',
    )

    path_list = tfs_wait()
    kafka_wait() >> path_list
    copies = tfs_copy.partial().expand(path=path_list)
    copies >> send_receipt
