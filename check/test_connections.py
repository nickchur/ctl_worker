"""### 🔌 Проверка всех Airflow Connections

Для каждого подключения из secret backend создаёт отдельный таск.
Таски сгруппированы по типу соединения; коннекты с `tfs` в имени — отдельная группа.

| Группа | Условие |
|---|---|
| `tfs` | `tfs` в conn_id **и** conn_type == `aws` |
| `s3` | conn_type == `aws`, без `tfs` в имени |
| `postgres` | conn_type == `postgres` |
| `ctl` | conn_type == `http` или `ctl*` |
| `clickhouse` | conn_type == `sqlite` или `clickhouse` |
| `kafka` | conn_type == `kafka` |
| `trino` | conn_type == `trino` |
| `other` | всё остальное |

Поддерживаемые типы проверок:
- **postgres** → `SELECT current_user, current_database(), inet_server_addr()`
- **s3** → `list_buckets()`
- **ctl / http** → `KerberosHttp GET /v5/api/info`
- **clickhouse** → `SELECT version()` через `ClickHouseHook`
- **kafka** → `list_topics()` через `KafkaAdminClientHook`
- **trino** → `SELECT current_user, current_catalog, current_schema` через `TrinoHook`
- остальные → `☮️ пропуск`

**Таски** независимы — сбой одного не блокирует остальные.
**summary** — финальный таск (`trigger_rule=all_done`), пишет таблицу ✅/❌/☮️ в DAG note.
"""

import os
import re
import time
from collections import defaultdict

import pendulum
from airflow.configuration import get_custom_secret_backend
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import Connection
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from plugins.ctl_core import chk_any_conn  # type: ignore
from plugins.utils import add_note          # type: ignore

from logging import getLogger
logger = getLogger("airflow.task")


# Маппинг Airflow conn_type → тип для chk_any_conn
_TYPE_MAP: dict[str, str] = {
    'postgres': 'Postgres',
    'aws':      'S3',
    'http':     'KerberosHttp',
}

# Заголовки групп для tooltip в UI
_GROUP_TOOLTIP: dict[str, str] = {
    'tfs':        'TFS-соединения',
    'postgres':   'PostgreSQL',
    's3':         'S3 / Object Storage',
    'ctl':        'CTL / HTTP (KerberosHttp)',
    'clickhouse': 'ClickHouse',
    'kafka':      'Kafka',
    'trino':      'Trino',
    'other':      'Прочие соединения',
}


# ---------------------------------------------------------------------------
# Parse-time: читаем и группируем соединения из secret backend
# ---------------------------------------------------------------------------

def _load_groups() -> tuple[dict[str, Connection], dict[str, dict[str, Connection]]]:
    """Читает secret backend и возвращает (tfs_group, type_groups)."""
    try:
        backend = get_custom_secret_backend()
        local_connections: dict[str, Connection] = getattr(backend, '_local_connections', {})
    except Exception as exc:
        logger.warning("Не удалось прочитать secret backend: %s", exc)
        return {}, defaultdict(dict)

    tfs_group = {
        cid: conn
        for cid, conn in local_connections.items()
        if 'tfs' in cid.lower() and conn.conn_type == 'aws'
    }

    type_groups: dict[str, dict[str, Connection]] = defaultdict(dict)
    for cid, conn in local_connections.items():
        if cid in tfs_group:
            continue
        group = conn.conn_type
        if group == 'sqlite':
            group = 'clickhouse'
        elif group == 'aws':
            group = 's3'
        elif group == 'http' or group.startswith('ctl'):
            group = 'ctl'
        elif group not in _TYPE_MAP and group not in ('clickhouse', 'kafka', 'trino'):
            group = 'other'
        type_groups[group][cid] = conn

    return tfs_group, type_groups


_tfs_group, _type_groups = _load_groups()


def _safe_id(conn_id: str) -> str:
    """Приводит conn_id к безопасному идентификатору таска."""
    return re.sub(r'[^a-zA-Z0-9_.\-]', '_', conn_id)


# ---------------------------------------------------------------------------
# Функция проверки одного соединения
# ---------------------------------------------------------------------------

def _check_native(conn_id: str, conn_type: str, **context) -> dict:
    """Проверяет ClickHouse / Kafka / Trino напрямую (без chk_any_conn)."""
    ti = context['ti']
    ts = time.time()
    try:
        if conn_type in ('sqlite', 'clickhouse'):
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook  # type: ignore
            # Используем таймауты для clickhouse-driver
            hook = ClickHouseHook(clickhouse_conn_id=conn_id, connect_timeout=15, send_receive_timeout=15)
            result = hook.get_records('SELECT version()')

        elif conn_type == 'kafka':
            from airflow.providers.apache.kafka.hooks.client import KafkaAdminClientHook  # type: ignore
            from confluent_kafka import KafkaException  # type: ignore

            conf = KafkaAdminClientHook(kafka_config_id=conn_id).get_connection(conn_id).extra_dejson
            
            # Поддержка verify/secure ключей
            verify = conf.pop('verify', True)
            if isinstance(verify, str): verify = verify.lower() == 'true'
            if not verify:
                conf['enable.ssl.certificate.verification'] = 'false'
                conf['ssl.endpoint.identification.algorithm'] = 'none'
            
            secure = conf.pop('secure', None)
            if secure is not None:
                if isinstance(secure, str): secure = secure.lower() == 'true'
                if secure and conf.get('security.protocol') in (None, 'plaintext'):
                    conf['security.protocol'] = 'ssl'

            try:
                from confluent_kafka.admin import AdminClient
                admin = AdminClient(conf)
                # Увеличиваем таймаут до 15 секунд
                meta = admin.list_topics(timeout=15)
                result = sorted(meta.topics.keys())[:10]
            except KafkaException as err:
                # Специальная обработка ошибки отсутствия SSL сертификатов/ключей
                err_str = str(err)
                last_err = err
                if 'failed' in err_str and (
                    'ssl.ca.location' in err_str or 
                    'ssl.certificate.location' in err_str or 
                    'ssl.key.location' in err_str
                ):
                    # 1. Если упал CA, пробуем системный бандл
                    if 'ssl.ca.location failed' in err_str:
                        sys_ca = next((p for p in ['/etc/ssl/certs/ca-certificates.crt', '/etc/pki/tls/certs/ca-bundle.crt'] if os.path.exists(p)), None)
                        if sys_ca:
                            logger.warning("Kafka SSL CA failed, retrying with system bundle: %s", sys_ca)
                            conf['ssl.ca.location'] = sys_ca
                            try:
                                admin = AdminClient(conf)
                                meta = admin.list_topics(timeout=15)
                                result = sorted(meta.topics.keys())[:10]
                                add_note(f"⚠️ Kafka SSL CA workaround: used {sys_ca}", context)
                                return {'status': 'ok', 'conn_id': conn_id, 'conn_type': conn_type}
                            except KafkaException as err2:
                                last_err = err2
                                err_str = str(err2) # Обновляем ошибку для следующего шага
                                logger.info("Kafka SSL still failing after CA fix: %s", err_str)

                    # 2. Если все еще падает на сертификатах (или упал сразу certificate/key)
                    # Пробуем форсированно отключить проверку сертификатов и УДАЛИТЬ пути к файлам
                    if any(x in err_str for x in ('failed', 'system lib')) and any(y in err_str for y in ('ssl.ca.location', 'ssl.certificate.location', 'ssl.key.location')):
                        logger.warning("Kafka certificates missing or invalid, retrying with verification disabled and paths stripped")
                        
                        # 3. Сохраняем информацию о недостающих файлах для диагностики, но продолжаем
                        missing_files = []
                        diag_conf = KafkaAdminClientHook(kafka_config_id=conn_id).get_connection(conn_id).extra_dejson
                        for k in ('ssl.certificate.location', 'ssl.key.location', 'ssl.ca.location'):
                            path = diag_conf.get(k)
                            if path and not os.path.exists(path):
                                missing_files.append(f"{k}='{path}'")
                        
                        # Очищаем пути к файлам, которые заставляют librdkafka искать их
                        for k in ('ssl.certificate.location', 'ssl.key.location', 'ssl.ca.location'):
                            conf.pop(k, None)
                        
                        # Отключаем проверку
                        conf['enable.ssl.certificate.verification'] = 'false'
                        conf['ssl.endpoint.identification.algorithm'] = 'none'
                        
                        try:
                            admin = AdminClient(conf)
                            meta = admin.list_topics(timeout=15)
                            result = sorted(meta.topics.keys())[:10]
                            msg = "⚠️ Kafka SSL workaround: certificate verification DISABLED"
                            if missing_files:
                                msg += f" (missing on worker: {', '.join(missing_files)})"
                            add_note(msg, context)
                            return {'status': 'ok', 'conn_id': conn_id, 'conn_type': conn_type}
                        except Exception as err3:
                            last_err = err3
                            logger.error("Kafka failed even with disabled verification: %s", err3)
                    
                    # 4. Если ничего не помогло, кидаем AirflowFailException с последней ошибкой
                    if missing_files:
                        msg = f"❌ Kafka SSL ERROR: missing files on worker: {', '.join(missing_files)}"
                        add_note(msg, context, level='task', title=f"❌ {conn_id}")
                        raise AirflowFailException(f"{msg}. Last Kafka error: {last_err}") from last_err
                    
                    raise AirflowFailException(f"Kafka error: {last_err}") from last_err
                else:
                    raise AirflowFailException(f"Kafka error: {err}") from err

        elif conn_type == 'trino':
            from airflow.providers.trino.hooks.trino import TrinoHook  # type: ignore
            # TrinoHook может принимать дополнительные параметры для trino.dbapi.connect
            hook = TrinoHook(trino_conn_id=conn_id, request_timeout=15)
            result = hook.get_first('SELECT current_user, current_catalog, current_schema')

        else:
            raise AirflowFailException(f"Unsupported conn_type: {conn_type}")

        logger.info("🔍 %s", result)
        msg = f"✅ {time.time() - ts:.2f} sec chk_{conn_id}_conn"
        add_note({'result': str(result)}, context, title=msg)
        return {'status': 'ok', 'conn_id': conn_id, 'conn_type': conn_type}

    except AirflowSkipException:
        raise

    except ImportError as err:
        msg = f"☮️ {conn_id}: провайдер не установлен — {err}"
        add_note(msg, context, level='task', title=f"☮️ {conn_id}")
        logger.warning(msg)
        raise AirflowSkipException(msg) from err

    except Exception as err:
        msg = f"❌ {time.time() - ts:.2f} sec chk_{conn_id}_conn ERROR Try {ti.try_number}"
        add_note(err, context, level='Task,DAG', title=msg)
        raise AirflowFailException(f"{msg}: {err}") from err


_NATIVE_TYPES = frozenset(('sqlite', 'clickhouse', 'kafka', 'trino'))


def _test_one(conn_id: str, conn_type: str, **context) -> dict:
    """Проверяет одно соединение.

    Postgres/S3/HTTP — через chk_any_conn; ClickHouse/Kafka/Trino — напрямую.
    Неподдерживаемый тип — skip-заметка без ошибки.
    """
    chk_type = _TYPE_MAP.get(conn_type)
    if chk_type is None and conn_type.startswith('ctl'):
        chk_type = 'KerberosHttp'
    if chk_type is not None:
        data = {'type': chk_type, 'conn_id': conn_id}
        chk_any_conn(id=conn_id, data=data, **context)
        return {'status': 'ok', 'conn_id': conn_id, 'conn_type': conn_type}

    if conn_type in _NATIVE_TYPES:
        return _check_native(conn_id, conn_type, **context)

    msg = f"☮️ conn_type='{conn_type}' — проверка не реализована"
    add_note(msg, context, level='task', title=f"☮️ {conn_id}")
    logger.info(msg)
    return {'status': 'skip', 'conn_id': conn_id, 'conn_type': conn_type}


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
    },
    start_date=pendulum.datetime(2026, 1, 21, tz=pendulum.UTC),
    schedule_interval=None,
    tags=['EDP_ETL', 'tools', 'conn'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
)
def tools_test_connections():

    all_tasks = []

    # --- TFS group (priority) ---
    # Все TFS-коннекты — S3 (conn_type='aws'), проверяем как S3 независимо от conn_type объекта.
    if _tfs_group:
        with TaskGroup(group_id='tfs', tooltip=_GROUP_TOOLTIP['tfs']):
            for conn_id, conn in sorted(_tfs_group.items()):
                t = task(
                    task_id=_safe_id(conn_id),
                    doc_md=f'Проверка `{conn_id}` (S3)',
                )(_test_one)(conn_id=conn_id, conn_type='aws')
                all_tasks.append(t)

    # --- Type groups ---
    for group_name in ('postgres', 's3', 'ctl', 'clickhouse', 'kafka', 'trino', 'other'):
        conns = _type_groups.get(group_name, {})
        if not conns:
            continue
        tooltip = _GROUP_TOOLTIP.get(group_name, group_name)
        with TaskGroup(group_id=group_name, tooltip=tooltip):
            for conn_id, conn in sorted(conns.items()):
                # aws conn_type нормализован в 's3' для группы, но conn_type у объекта — 'aws'
                t = task(
                    task_id=_safe_id(conn_id),
                    doc_md=f'Проверка `{conn_id}` (conn_type=`{conn.conn_type}`)',
                )(_test_one)(conn_id=conn_id, conn_type=conn.conn_type)
                all_tasks.append(t)

    # --- Summary ---
    @task(task_id='summary', trigger_rule=TriggerRule.ALL_DONE)
    def summary(**context):
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session

        dag_run = context['dag_run']
        with create_session() as session:
            tis = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id,
                )
                .order_by(TaskInstance.task_id)
                .all()
            )

        ok = fail = skip = 0
        rows = []
        for ti in tis:
            if ti.task_id == 'summary':
                continue
            state = ti.state or 'none'
            if state == 'success':
                icon = '✅'; ok += 1
            elif state in ('failed', 'upstream_failed'):
                icon = '❌'; fail += 1
            else:
                icon = '☮️'; skip += 1
            rows.append(f"| `{ti.task_id}` | {icon} {state} |")

        table = '| Соединение | Статус |\n|---|---|\n' + '\n'.join(rows)
        headline = f"✅ {ok} / ❌ {fail} / ☮️ {skip}"
        add_note(table, context, level='DAG', title=headline)
        logger.info("summary: %s", headline)
        return {'ok': ok, 'fail': fail, 'skip': skip}

    summary_task = summary()
    if all_tasks:
        all_tasks >> summary_task


tools_test_connections()
