"""### 🔌 Проверка всех Airflow Connections

Для каждого подключения из secret backend создаёт отдельный таск.
Таски сгруппированы по типу соединения; коннекты с `tfs` в имени — отдельная группа.

| Группа | Условие |
|---|---|
| `tfs` | `tfs` в conn_id (приоритет над типом) |
| `postgres` | conn_type == `postgres` |
| `s3` | conn_type == `aws` |
| `http` | conn_type == `http` |
| `clickhouse` | conn_type == `sqlite` или `clickhouse` |
| `kafka` | conn_type == `kafka` |
| `trino` | conn_type == `trino` |
| `other` | всё остальное |

Поддерживаемые типы проверок:
- **postgres** → `SELECT current_user, current_database(), inet_server_addr()`
- **s3** → `list_buckets()`
- **http** → `KerberosHttp GET /v5/api/info`
- остальные → `⏭ пропуск`

**Таски:** `soft_fail=True` — сбой одного не блокирует остальные.
**summary** — финальный таск, пишет таблицу ✅/❌/⏭ в DAG note.
"""

import re
from collections import defaultdict

import pendulum
from airflow.configuration import get_custom_secret_backend
from airflow.decorators import dag, task
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
    'http':       'HTTP (KerberosHttp)',
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
        if 'tfs' in cid.lower()
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

def _test_one(conn_id: str, conn_type: str, **context) -> dict:
    """Проверяет одно соединение через chk_any_conn.

    Если тип не поддерживается — пишет skip-заметку и возвращает без ошибки.
    """
    chk_type = _TYPE_MAP.get(conn_type)
    if chk_type is None:
        msg = f"⏭ conn_type='{conn_type}' — проверка не реализована"
        add_note(msg, context, level='task', title=f"⏭ {conn_id}")
        logger.info(msg)
        return {'status': 'skip', 'conn_id': conn_id, 'conn_type': conn_type}

    data = {'type': chk_type, 'conn_id': conn_id}
    chk_any_conn(id=conn_id, data=data, **context)
    return {'status': 'ok', 'conn_id': conn_id, 'conn_type': conn_type}


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
    tags=['EDP_ETL', 'tools'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
)
def tools_test_conn():

    all_tasks = []

    # --- TFS group (priority) ---
    if _tfs_group:
        with TaskGroup(group_id='tfs', tooltip=_GROUP_TOOLTIP['tfs']):
            for conn_id, conn in sorted(_tfs_group.items()):
                t = task(
                    task_id=_safe_id(conn_id),
                    soft_fail=True,
                    doc_md=f'Проверка `{conn_id}` (conn_type=`{conn.conn_type}`)',
                )(_test_one)(conn_id=conn_id, conn_type=conn.conn_type)
                all_tasks.append(t)

    # --- Type groups ---
    for group_name in ('postgres', 's3', 'http', 'clickhouse', 'kafka', 'trino', 'other'):
        conns = _type_groups.get(group_name, {})
        if not conns:
            continue
        tooltip = _GROUP_TOOLTIP.get(group_name, group_name)
        with TaskGroup(group_id=group_name, tooltip=tooltip):
            for conn_id, conn in sorted(conns.items()):
                # aws conn_type нормализован в 's3' для группы, но conn_type у объекта — 'aws'
                t = task(
                    task_id=_safe_id(conn_id),
                    soft_fail=True,
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
                icon = '⏭'; skip += 1
            rows.append(f"| `{ti.task_id}` | {icon} {state} |")

        table = '| Соединение | Статус |\n|---|---|\n' + '\n'.join(rows)
        headline = f"✅ {ok} / ❌ {fail} / ⏭ {skip}"
        add_note(table, context, level='DAG', title=headline)
        logger.info("summary: %s", headline)
        return {'ok': ok, 'fail': fail, 'skip': skip}

    summary_task = summary()
    if all_tasks:
        all_tasks >> summary_task


tools_test_conn()
