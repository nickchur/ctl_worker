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
- **`check_serialized_dag`**: отдельная проверка метабазы — падает, если в `main.serialized_dag`
  есть записи с `last_updated` за последние 30 минут (признак того, что DAG'и переразбираются
  и пересериализуются на ходу).
"""

import re
from collections import defaultdict
from datetime import datetime, timezone
from logging import getLogger

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

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
# Локальная копия plugins.ctl_core.chk_any_conn (Postgres / S3 / KerberosHttp)
# ---------------------------------------------------------------------------

def _chk_any_conn(conn_id: str, conn_type: str, context: dict) -> None:
    """Проверяет доступность соединения (Postgres / S3 / KerberosHttp).

    Самодостаточная копия `plugins.ctl_core.chk_any_conn` — чтобы тест не зависел от
    импорта ctl_core. Логика pool_slots / get_config из оригинала здесь не нужна: тест
    всегда проверяет одно соединение без пулов. При успехе пишет ноту, при ошибке —
    пробрасывает AirflowFailException.
    """
    import time

    from airflow.exceptions import AirflowFailException, AirflowSkipException

    try:
        from plugins.utils import add_note  # type: ignore
    except ImportError:
        from CI06932748.tools.utils import add_note  # type: ignore

    ti = context["ti"]
    try_number = ti.try_number
    sdt = ti.start_date.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    ts = time.time()
    try:
        if conn_type == "Postgres":
            from airflow.providers.postgres.hooks.postgres import PostgresHook  # type: ignore
            hook = PostgresHook(postgres_conn_id=conn_id)
            result = hook.get_first("SELECT current_user, current_database(), inet_server_addr()")

        elif conn_type == "S3":
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
            from botocore.config import Config  # type: ignore

            verify = S3Hook(aws_conn_id=conn_id).get_connection(conn_id).extra_dejson.get("verify", True)
            if isinstance(verify, str):
                verify = verify.lower() == "true"
            hook = S3Hook(aws_conn_id=conn_id, verify=verify, config=Config(connect_timeout=15, read_timeout=15))
            result = hook.get_conn().list_buckets()["Buckets"]

        elif conn_type == "KerberosHttp":
            from hrp_operators.utils.kerberos_http import KerberosHttpHook  # type: ignore

            hook = KerberosHttpHook(method="GET", http_conn_id=conn_id)
            verify = hook.get_connection(conn_id).extra_dejson.get("verify", True)
            if isinstance(verify, str):
                verify = verify.lower() == "true"
            response = hook.run(
                "/v5/api/info",
                headers={"Accept": "application/json"},
                extra_options={"timeout": 15, "verify": verify},
            )
            response.raise_for_status()
            result = response.json()
        else:
            result = None

        logger.info("🔍 %s", result)
        add_note({"try": try_number, "sdt": sdt}, context, title=f"✅ {time.time() - ts:.2f} sec chk_{conn_id}_conn")

    except AirflowSkipException:
        raise

    except ImportError as err:
        msg = f"☮️ {conn_id}: провайдер не установлен — {err}"
        add_note(msg, context, level="task", title=f"☮️ {conn_id}")
        logger.warning(msg)
        raise AirflowSkipException(msg) from err

    except Exception as err:
        response = getattr(err, "response", None)
        logger.error(response)
        msg = f"❌ {time.time() - ts:.2f} sec chk_{conn_id}_conn ERROR Try {try_number} {sdt}"
        add_note(err, context, level="Task,DAG", title=msg)
        raise AirflowFailException(f"{msg}: {err}") from err


# ---------------------------------------------------------------------------
# Объединенная функция проверки соединений
# ---------------------------------------------------------------------------

def _run_test(conn_id: str, conn_type: str, **context) -> dict:
    """Единая точка входа для проверки всех типов соединений.

    Postgres/S3/HTTP — через chk_any_conn (типы ctl* маппятся на KerberosHttp);
    ClickHouse/Kafka/Trino/Redis — напрямую.
    """
    import time

    from airflow.exceptions import AirflowFailException, AirflowSkipException

    try:
        from plugins.utils import add_note  # type: ignore
    except ImportError:
        from CI06932748.tools.utils import add_note  # type: ignore

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
            _chk_any_conn(conn_id, chk_type, context)
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
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
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

    # --- Serialized DAG check ---
    @task(
        task_id="check_serialized_dag",
        doc_md="Проверка `main.serialized_dag`: не пересериализовывались ли DAG'и за последние 30 минут",
        retries=0,  # окно проверки — 30 минут, ретрай через 5 минут смотрел бы почти на то же самое
    )
    def check_serialized_dag(**context) -> dict:
        """Падает, если в метабазе есть DAG'и, пересериализованные за последние 30 минут."""
        import time

        from airflow.exceptions import AirflowFailException
        from airflow.utils.session import create_session
        from sqlalchemy import text

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        count_sql = """
            SELECT COUNT(dag_id)
            FROM main.serialized_dag
            WHERE last_updated > now() - interval '30 minutes'
        """
        # Полный список уходит в XCom, в заметку попадают только первые note_rows
        list_sql = """
            SELECT last_updated, dag_id, fileloc, dag_hash
            FROM main.serialized_dag
            WHERE last_updated > now() - interval '30 minutes'
            ORDER BY last_updated DESC
            LIMIT 500
        """
        note_rows = 25  # заметка режется по MAX_NOTE_LEN, таблица на 500 строк туда не влезет

        ts = time.time()
        with create_session() as session:
            count = session.execute(text(count_sql)).scalar() or 0
            rows = session.execute(text(list_sql)).fetchall() if count else []

        elapsed = time.time() - ts
        logger.info("🔍 serialized_dag: %d записей обновлено за последние 30 минут", count)

        if not count:
            add_note(
                "Записей с last_updated за последние 30 минут нет",
                context,
                level="task,DAG",
                title=f"✅ {elapsed:.2f} sec check_serialized_dag",
            )
            return {"status": "ok", "updated": 0}

        msg = f"main.serialized_dag: {count} DAG'ов пересериализовано за последние 30 минут"
        data = [
            {"last_updated": str(last_updated), "dag_id": dag_id, "fileloc": fileloc, "dag_hash": dag_hash}
            for last_updated, dag_id, fileloc, dag_hash in rows
        ]
        add_xcom("serialized_dag", data, context)

        table = "| last_updated | dag_id | fileloc | dag_hash |\n|---|---|---|---|\n" + "\n".join(
            f"| {r['last_updated']} | `{r['dag_id']}` | `{r['fileloc']}` | `{r['dag_hash']}` |"
            for r in data[:note_rows]
        )
        if count > note_rows:
            table += (f"\n\nПоказаны первые {min(note_rows, len(data))} из {count}, "
                      f"полный список — в XCom `serialized_dag`.")
        add_note(f"{msg}\n\n{table}", context, level="task",
                 title=f"❌ {elapsed:.2f} sec check_serialized_dag")
        add_note(f"{msg}", context, level="DAG",
                 title=f"❌ {elapsed:.2f} sec check_serialized_dag")
        raise AirflowFailException(msg)

    # --- Summary ---
    @task(task_id="summary", trigger_rule=TriggerRule.ALL_DONE)
    def summary(**context):  # noqa: PLR0915
        from airflow.exceptions import AirflowFailException
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

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
    check_serialized_dag() >> summary_task

    if groups:
        groups >> summary_task


tools_test_connections()
