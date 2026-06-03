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
| **redis** | тип `redis` | Проверка доступности через `redis.Redis(...).ping()` |
| **other** | прочие | Помечаются символом `☮️` (пропуск) |

**Особенности:**
- **Оптимизация**: Использует Airflow Variable `local_connections` (создаваемую в `show_connections`) для ускорения получения списка соединений.
- **Изоляция**: Сбой одного коннекта не влияет на проверку остальных.
- **Отчетность**: Финальный таск `summary` формирует Markdown-таблицу со всеми статусами в заметках DAG'а.
"""

import re
import time
from collections import defaultdict

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Connection, TaskInstance, Variable
from airflow.utils.session import create_session
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.ctl_core import chk_any_conn  # type: ignore
    from plugins.utils import add_note  # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, chk_any_conn  # type: ignore

from logging import getLogger

logger = getLogger("airflow.task")


# Маппинг Airflow conn_type → тип для chk_any_conn / нативная логика
_CONN_CONFIG: dict[str, str] = {
    "postgres":   "Postgres",
    "aws":         "S3",
    "http":        "KerberosHttp",
    "sqlite":      "ClickHouse",
    "clickhouse":  "ClickHouse",
    "kafka":       "Kafka",
    "trino":       "Trino",
    "redis":       "Redis",
}

# Заголовки групп для tooltip в UI
_GROUP_TOOLTIP: dict[str, str] = {
    "tfs":        "TFS-соединения",
    "postgres":   "PostgreSQL",
    "s3":         "S3 / Object Storage",
    "ctl":        "CTL / HTTP (KerberosHttp)",
    "clickhouse": "ClickHouse",
    "kafka":      "Kafka",
    "trino":      "Trino",
    "redis":      "Redis",
    "other":      "Прочие соединения",
}


# ---------------------------------------------------------------------------
# Parse-time: читаем и группируем соединения из secret backend
# ---------------------------------------------------------------------------

def _load_groups() -> tuple[dict[str, Connection], dict[str, dict[str, Connection]]]:
    """Читает соединения из Variable и возвращает (tfs_group, type_groups)."""
    local_connections: dict[str, Connection] = {}

    try:
        var_data = Variable.get("local_connections", deserialize_json=True, default_var=None)
        if var_data:
            for ctype, conns in var_data.items():
                for c in conns:
                    local_connections[c["conn_id"]] = Connection(
                        conn_id=c["conn_id"],
                        conn_type=c.get("conn_type") or (ctype if ctype != "clickhouse" else "sqlite"),
                        host=c["host"],
                        port=c["port"],
                        schema=c["schema"],
                        description=c["description"],
                    )
    except Exception as exc:
        logger.warning("_load_groups: failed to load local_connections: %s", exc)

    logger.info("Found %d local connections total", len(local_connections))

    tfs_group = {
        cid: conn
        for cid, conn in local_connections.items()
        if "tfs" in cid.lower() and conn.conn_type == "aws"
    }

    type_groups: dict[str, dict[str, Connection]] = defaultdict(dict)
    for cid, conn in local_connections.items():
        if cid in tfs_group:
            continue
        group = conn.conn_type
        if group == "sqlite":
            group = "clickhouse"
        elif group == "aws":
            group = "s3"
        elif group == "http" or group.startswith("ctl"):
            group = "ctl"
        elif group not in _CONN_CONFIG:
            group = "other"
        type_groups[group][cid] = conn

    for gname, gconns in type_groups.items():
        logger.info("Group '%s' has %d connections", gname, len(gconns))

    return tfs_group, type_groups


_groups_cache = None


def _get_groups() -> tuple[dict[str, Connection], dict[str, dict[str, Connection]]]:
    global _groups_cache
    if _groups_cache is None:
        _groups_cache = _load_groups()
    return _groups_cache


_tfs_group, _type_groups = _get_groups()


def _safe_id(conn_id: str, seen: set[str]) -> str:
    """Приводит conn_id к безопасному идентификатору таска; при коллизии добавляет суффикс _2, _3, ..."""
    base = re.sub(r"[^a-zA-Z0-9_.\-]", "_", conn_id)
    safe = base
    i = 2
    while safe in seen:
        safe = f"{base}_{i}"
        i += 1
    seen.add(safe)
    return safe


# ---------------------------------------------------------------------------
# Объединенная функция проверки соединений
# ---------------------------------------------------------------------------

def _run_test(conn_id: str, conn_type: str, **context) -> dict:
    """Единая точка входа для проверки всех типов соединений.

    Postgres/S3/HTTP — через chk_any_conn (типы ctl* маппятся на KerberosHttp); 
    ClickHouse/Kafka/Trino/Redis — напрямую.
    """
    ti = context["ti"]
    ts = time.time()

    # 1. Маппинг типа
    chk_type = _CONN_CONFIG.get(conn_type)
    if chk_type is None and conn_type.startswith("ctl"):
        chk_type = "KerberosHttp"
        logger.info("Connection '%s' has ctl-like type '%s', mapping to 'KerberosHttp'", conn_id, conn_type)

    if chk_type is None:
        msg = f"☮️ conn_type='{conn_type}' — проверка не реализована"
        add_note(msg, context, level="task", title=f"☮️ {conn_id}")
        logger.info(msg)
        return {"status": "skip", "conn_id": conn_id, "conn_type": conn_type}

    try:
        # 2. Выполнение проверки
        if chk_type in ("Postgres", "S3", "KerberosHttp"):
            data = {"type": chk_type, "conn_id": conn_id}
            chk_any_conn(id=conn_id, data=data, **context)
            result = "Success via chk_any_conn"

        elif chk_type == "ClickHouse":
            from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook  # type: ignore
            hook = ClickHouseHook(clickhouse_conn_id=conn_id)
            result = hook.execute("SELECT version()")

        elif chk_type == "Kafka":
            import confluent_kafka.admin as kafka_admin
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(conn_id)
            conf = conn.extra_dejson.copy()
            if "bootstrap.servers" not in conf:
                conf["bootstrap.servers"] = f"{conn.host}:{conn.port or 9092}"
            conf.setdefault("socket.timeout.ms", 15000)

            client = kafka_admin.AdminClient(conf)
            meta = client.list_topics(timeout=15)
            result = sorted(meta.topics.keys())[:10]

        elif chk_type == "Trino":
            from airflow.providers.trino.hooks.trino import TrinoHook  # type: ignore
            hook = TrinoHook(trino_conn_id=conn_id)
            result = hook.get_first("SELECT current_user, current_catalog, current_schema")
            # from airflow.hooks.base import BaseHook
            # import requests
            
            # conn = BaseHook.get_connection(conn_id)
            
            # # Параметры подключения к Trino
            # host = conn.host
            # port = conn.port or 8080
            # user = conn.login or "airflow"
            # catalog = conn.schema or "hive"
            
            # url = f"http://{host}:{port}/v1/statement"
            
            # # Выполнение запроса через REST API
            # payload = {
            #     "catalog": catalog,
            #     "schema": "default",
            #     "sql": "SELECT current_user, current_catalog, current_schema"
            # }
            
            # response = requests.post(url, json=payload, timeout=15)
            # response.raise_for_status()
            # result_data = response.json()
            
            # if "data" in result_data and len(result_data["data"]) > 0:
            #     row = result_data["data"][0]
            #     result = f"user={row[0]}, catalog={row[1]}, schema={row[2]}"
            # else:
            #     result = "Trino OK (no data rows)"

        elif chk_type == "Redis":
            import redis
            from airflow.hooks.base import BaseHook

            conn = BaseHook.get_connection(conn_id)
            extra = conn.extra_dejson.copy()
            client = redis.Redis(
                host=conn.host,
                port=conn.port or 6379,
                username=conn.login or None,
                password=conn.password or None,
                db=int(extra.get("db", 0)),
                socket_timeout=int(extra.get("socket_timeout", 15)),
            )
            result = f"PONG: {client.ping()}"

        else:
            raise AirflowFailException(f"Logic for {chk_type} not implemented in _run_test")

        # 3. Логирование и выход (для нативных проверок, chk_any_conn сам пишет ноту)
        if chk_type not in ("Postgres", "S3", "KerberosHttp"):
            logger.info("🔍 %s", result)
            msg = f"✅ {time.time() - ts:.2f} sec chk_{conn_id}_conn"
            add_note({"result": str(result)}, context, title=msg)

        return {"status": "ok", "conn_id": conn_id, "conn_type": conn_type}

    except AirflowSkipException:
        raise

    except ImportError as err:
        msg = f"Провайдер не установлен — {err}"
        add_note(msg, context, level="task", title=f"☮️ {conn_id}")
        logger.warning(msg)
        raise AirflowSkipException(msg) from err

    except Exception as err:
        msg = f"❌ {time.time() - ts:.2f} sec chk_{conn_id}_conn ERROR Try {ti.try_number}"
        add_note(str(err), context, level="task,DAG", title=msg)
        raise  # re-raise оригинальное исключение, чтобы сработал retries: 1


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    doc_md=__doc__,
    default_args={
        "owner": "DataLab (CI02420667)",
        "retries": 1,
    },
    start_date=pendulum.datetime(2026, 1, 1, tz=pendulum.UTC),
    schedule="@once",
    tags=["DataLab", "tools", "conn", "AutoQA"],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
)
def tools_test_connections():  # noqa: PLR0915

    groups = []

    # --- TFS group (priority) ---
    if _tfs_group:
        with TaskGroup(group_id="tfs", tooltip=_GROUP_TOOLTIP["tfs"]) as tg_tfs:
            seen: set[str] = set()
            for conn_id, conn in sorted(_tfs_group.items()):
                @task(
                    task_id=_safe_id(conn_id, seen),
                    doc_md=f"Проверка `{conn_id}` (S3)",
                )
                def tfs_task(cid=conn_id, **kwargs):
                    return _run_test(cid, conn_type="aws", **kwargs)

                tfs_task()
        groups.append(tg_tfs)

    # --- Type groups ---
    for group_name in ("postgres", "s3", "ctl", "clickhouse", "kafka", "trino", "redis", "other"):
        conns = _type_groups.get(group_name, {})
        if not conns:
            continue
        tooltip = _GROUP_TOOLTIP.get(group_name, group_name)
        with TaskGroup(group_id=group_name, tooltip=tooltip) as tg:
            seen = set()
            for conn_id, conn in sorted(conns.items()):
                @task(
                    task_id=_safe_id(conn_id, seen),
                    doc_md=f"Проверка `{conn_id}` (conn_type=`{conn.conn_type}`)",
                )
                def check_task(cid=conn_id, ctype=conn.conn_type, **kwargs):
                    return _run_test(cid, ctype, **kwargs)

                check_task()
        groups.append(tg)

    # --- Summary ---
    @task(task_id="summary", trigger_rule=TriggerRule.ALL_DONE)
    def summary(**context):  # noqa: PLR0915
        dag_run = context["dag_run"]
        notes_map: dict[str, str] = {}

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
            try:
                from airflow.models.taskinstance import TaskInstanceNote  # noqa: PLC0415
                note_rows = (
                    session.query(TaskInstanceNote)
                    .filter(
                        TaskInstanceNote.dag_id == dag_run.dag_id,
                        TaskInstanceNote.run_id == dag_run.run_id,
                    )
                    .all()
                )
                notes_map = {n.task_id: (n.content or "") for n in note_rows}
            except Exception as e:
                logger.warning("Could not load task notes: %s", e)

        ok = fail = skip = none_count = 0
        all_rows = []
        durations = []
        icons = []

        for ti in tis:
            if ti.task_id == "summary":
                continue

            state = ti.state
            raw_note = notes_map.get(ti.task_id, "")
            # first_line = (next((ln.strip() for ln in raw_note.splitlines() if ln.strip()), "")) if raw_note else ""
            non_empty_lines = [ln.strip() for ln in raw_note.splitlines() if ln.strip()] if raw_note else []
            first_line = non_empty_lines[2] if len(non_empty_lines) > 2 else ""
            reason = first_line[:120].replace("|", "\\|") or "—"

            if state == "success":
                icon = "✅"
                ok += 1
                reason = "—"
            elif state in ("failed", "upstream_failed"):
                icon = "❌"
                fail += 1
            elif state == "skipped":
                icon = "☮️"
                skip += 1
            elif state is None:
                icon = "🔘"
                none_count += 1
                reason = "Не запущен"
            else:
                icon = "☮️"
                skip += 1

            icons.append(icon)
            if ti.duration:
                durations.append(ti.duration)

            # Выводим только ошибки и скипы
            if state != "success":
                all_rows.append(f"| `{ti.task_id}` | {icon} {state or 'not_started'} | {reason} |")

        avg_time = sum(durations) / len(durations) if durations else 0
        graph = "".join(icons)
        counts = f"✅ {ok} / ❌ {fail} / ☮️ {skip}"
        if none_count:
            counts += f" / 🔘 {none_count}"
        headline = f"{graph}\n\n{counts} | 🕒 Avg: {avg_time:.2f}s"

        table = "| Соединение | Статус | Причина |\n|---|---|---|\n" + "\n".join(all_rows)
        add_note(table, context, level="DAG", title=headline)
        logger.info("summary: %s", headline)

        if fail > 0:
            raise AirflowFailException(f"Connections check failed: {headline}")

        return {"ok": ok, "fail": fail, "skip": skip, "none": none_count, "avg_time": avg_time}

    summary_task = summary()

    if groups:
        groups >> summary_task


tools_test_connections()
