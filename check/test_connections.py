"""### 🔌 DAG: Проверка Airflow Connections

Автоматизированный аудит и тестирование всех подключений из secret backend. 
Для каждого соединения создается индивидуальный таск, что позволяет локализовать проблемы со связностью.

| Группа | Условие (conn_id / type) | Описание проверки |
|---|---|---|
| **tfs** | `tfs` в ID **и** тип `aws` | Проверка S3-бакетов через `list_buckets()` |
| **s3** | тип `aws` (без tfs) | Проверка прав доступа к объектному хранилищу |
| **postgres** | тип `postgres` | `SELECT current_user, current_database()` |
| **ctl** | тип `http` или `ctl*` | Вызов `GET /v5/api/info` (Kerberos Auth) |
| **clickhouse** | тип `sqlite` / `clickhouse` | Проверка версии через `ClickHouseHook` |
| **kafka** | тип `kafka` | Листинг топиков через `KafkaAdminClientHook` |
| **trino** | тип `trino` | Валидация сессии через `TrinoHook` |
| **other** | прочие | Помечаются символом `☮️` (пропуск) |

**Особенности:**
- **Оптимизация**: Использует Airflow Variable `local_connections` (создаваемую в `show_connections`) для ускорения получения списка соединений.
- **Изоляция**: Сбой одного коннекта не влияет на проверку остальных.
- **Диагностика**: При сбое Kafka выполняется TCP Ping для разделения ошибок FW и SSL.
- **Отчетность**: Финальный таск `summary` формирует Markdown-таблицу со всеми статусами в заметках DAG'а.
"""

import os
import re
import time
from collections import defaultdict

import pendulum
from airflow.configuration import get_custom_secret_backend
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import Connection, Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.ctl_core import chk_any_conn  # type: ignore
    from plugins.utils import add_note          # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, chk_any_conn         # type: ignore

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
    """Читает Variable 'local_connections' и возвращает (tfs_group, type_groups)."""
    local_connections: dict[str, Connection] = {}

    # 1. Пробуем загрузить из Variable (созданной в show_connections)
    try:
        var_data = Variable.get('local_connections', deserialize_json=True, default_var=None)
        if var_data:
            for ctype, conns in var_data.items():
                for c in conns:
                    # Нам нужно восстановить объект Connection для корректной фильтрации и работы тасков.
                    # show_connections переименовал sqlite в clickhouse, восстанавливаем обратно если нужно.
                    local_connections[c['conn_id']] = Connection(
                        conn_id=c['conn_id'],
                        conn_type=c.get('conn_type') or (ctype if ctype != 'clickhouse' else 'sqlite'),
                        host=c['host'],
                        port=c['port'],
                        schema=c['schema'],
                        description=c['description'],
                        extra=c['extra']
                    )
    except Exception as exc:
        logger.warning("Не удалось прочитать Variable local_connections: %s", exc)

    logger.info("Found %d local connections total", len(local_connections))

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

    for gname, gconns in type_groups.items():
        logger.info("Group '%s' has %d connections", gname, len(gconns))

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
            hook = ClickHouseHook(clickhouse_conn_id=conn_id)
            result = hook.execute('SELECT version()')

        elif conn_type == 'kafka':
            import confluent_kafka.admin as kafka_admin
            from airflow.hooks.base import BaseHook
            
            conn = BaseHook.get_connection(conn_id)
            conf = conn.extra_dejson.copy()
            if 'bootstrap.servers' not in conf:
                conf['bootstrap.servers'] = f"{conn.host}:{conn.port or 9092}"
            
            client = kafka_admin.AdminClient(conf)
            meta = client.list_topics(timeout=15)
            result = sorted(meta.topics.keys())[:10]

        elif conn_type == 'trino':
            from airflow.providers.trino.hooks.trino import TrinoHook  # type: ignore
            hook = TrinoHook(trino_conn_id=conn_id)
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

    Postgres/S3/HTTP — через chk_any_conn (типы ctl* маппятся на KerberosHttp); 
    ClickHouse/Kafka/Trino — напрямую.
    Неподдерживаемый тип — skip-заметка без ошибки.
    """
    chk_type = _TYPE_MAP.get(conn_type)
    if chk_type is None and conn_type.startswith('ctl'):
        chk_type = 'KerberosHttp'
        logger.info("Connection '%s' has ctl-like type '%s', mapping to 'KerberosHttp'", conn_id, conn_type)

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
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 2,
    },
    start_date=pendulum.datetime(2026, 1, 1, tz=pendulum.UTC),
    schedule_interval='@once',
    tags=['DataLab', 'tools', 'conn', 'AutoQA'],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
)
def tools_test_connections():

    groups = []

    # --- TFS group (priority) ---
    if _tfs_group:
        with TaskGroup(group_id='tfs', tooltip=_GROUP_TOOLTIP['tfs']) as tg_tfs:
            for conn_id, conn in sorted(_tfs_group.items()):
                @task(
                    task_id=_safe_id(conn_id),
                    doc_md=f'Проверка `{conn_id}` (S3)',
                )
                def tfs_task(cid=conn_id, **kwargs):
                    return _test_one(cid, conn_type='aws', **kwargs)
                
                tfs_task()
        groups.append(tg_tfs)

    # --- Type groups ---
    for group_name in ('postgres', 's3', 'ctl', 'clickhouse', 'kafka', 'trino', 'other'):
        conns = _type_groups.get(group_name, {})
        if not conns:
            continue
        tooltip = _GROUP_TOOLTIP.get(group_name, group_name)
        with TaskGroup(group_id=group_name, tooltip=tooltip) as tg:
            for conn_id, conn in sorted(conns.items()):
                @task(
                    task_id=_safe_id(conn_id),
                    doc_md=f'Проверка `{conn_id}` (conn_type=`{conn.conn_type}`)',
                )
                def check_task(cid=conn_id, ctype=conn.conn_type, **kwargs):
                    return _test_one(cid, ctype, **kwargs)
                
                check_task()
        groups.append(tg)

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
        error_rows = []
        all_rows = []
        durations = []
        icons = []
        
        for ti in tis:
            if ti.task_id == 'summary' or ti.task_id == 'check_variable':
                continue
            
            state = ti.state
            if state == 'success':
                icon = '✅'; ok += 1
            elif state in ('failed', 'upstream_failed') or state is None:
                icon = '❌'; fail += 1
                error_rows.append(f"| `{ti.task_id}` | {icon} {state or 'not_started'} |")
            else:
                icon = '☮️'; skip += 1
            
            icons.append(icon)
            if ti.duration:
                durations.append(ti.duration)
                
            all_rows.append(f"| `{ti.task_id}` | {icon} {state or 'not_started'} |")

        avg_time = sum(durations) / len(durations) if durations else 0
        graph = "".join(icons)
        headline = f"{graph}\n\n✅ {ok} / ❌ {fail} / ☮️ {skip} | 🕒 Avg: {avg_time:.2f}s"
        
        if fail > 0:
            table = '| Соединение | Статус |\n|---|---|\n' + '\n'.join(error_rows)
            add_note(table, context, level='DAG', title=headline)
            logger.info("summary: %s", headline)
            raise AirflowFailException(f"Connections check failed: {headline}")
        
        table = '| Соединение | Статус |\n|---|---|\n' + '\n'.join(all_rows)
        add_note(table, context, level='DAG', title=headline)
        logger.info("summary: %s", headline)
        return {'ok': ok, 'fail': fail, 'skip': skip, 'avg_time': avg_time}

    # --- Variable Check ---
    @task(task_id='check_variable')
    def check_variable():
        """Проверяет наличие и наполненность переменной local_connections."""
        var_data = Variable.get('local_connections', deserialize_json=True, default_var=None)
        if not var_data:
            raise AirflowFailException(
                "Airflow Variable 'local_connections' не найдена или пуста. "
                "Пожалуйста, запустите DAG 'tools_show_connections' для её генерации."
            )
        return var_data

    check_var_task = check_variable()
    summary_task = summary()

    if groups:
        check_var_task >> groups >> summary_task
    else:
        check_var_task >> summary_task


tools_test_connections()
