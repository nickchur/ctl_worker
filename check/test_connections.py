"""### 🔌 DAG: Проверка Airflow Connections
*2026-08-04 10:35 MSK · v1.0 · Чуркин Николай*

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
| **trino** | тип `trino` | Валидация сессии через `TrinoHook`; нерезолвящийся хост → `☮️` |
| **redis** | тип `redis` | Проверка доступности через `redis.Redis(...).ping()` |
| **other** | прочие | Помечаются символом `☮️` (пропуск) |

**Особенности:**
- **Оптимизация**: Использует Airflow Variable `local_connections` (создаваемую в `show_connections`) для ускорения получения списка соединений.
- **Изоляция**: Сбой одного коннекта не влияет на проверку остальных.
- **Отчетность**: Финальный таск `summary` формирует Markdown-таблицу со всеми статусами в заметках DAG'а.
- **`check_serialized_dag`**: отдельная проверка метабазы. Строит таблицу — сколько всего DAG'ов
  и у скольких менялась сериализация за год, 3 месяца, месяц, неделю, сутки и час, плюс строка
  «на последнем парсинге» (`serialized_dag.last_updated` попадает в окно последнего
  парсинга файла — `dag.last_parsed_time`). Падает именно на этой строке: значит парсинг даёт
  каждый раз новый результат и DAG'и пересериализуются на ходу.
- **`recheck_serialized_dag`**: mapped-таск поверх находок `check_serialized_dag`. На каждый
  DAG из XCom `serialized_dag` ждёт следующего парсинга (сдвига `dag.last_parsed_time`) и
  сравнивает сериализацию до и после, показывая расхождения по путям вида
  `.dag.params[0][1].schema.examples[0]`. Совпавший `dag_hash` — значит прошлое изменение было
  разовым (деплой), разошедшийся — вот конкретное поле, которое дрожит. Сам не падает.
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


def _short(value, limit: int = 60) -> str:
    """Однострочное представление значения для ячейки таблицы diff'а."""
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _json_diff(before, after, limit: int = 20) -> list[tuple[str, str, str]]:
    """Рекурсивно сравнивает две сериализации DAG'а: [(путь, было, стало), ...].

    Списки сравниваются поэлементно, а не как множества: для dag_hash порядок значим,
    и плавающий порядок списка — самая частая причина дрожания сериализации. Поэтому
    разная длина списка и расхождение по индексу — разные строки отчёта.
    """
    out: list[tuple[str, str, str]] = []

    def walk(x, y, path: str) -> None:
        if len(out) >= limit:
            return
        if type(x) is not type(y):
            # тип показываем явно: '1' и 1 в таблице выглядели бы одинаково
            out.append((path or ".", f"{_short(x)} ({type(x).__name__})",
                        f"{_short(y)} ({type(y).__name__})"))
        elif isinstance(x, dict):
            for key in dict.fromkeys(list(x) + list(y)):
                if len(out) >= limit:
                    return
                sub = f"{path}.{key}"
                if key not in x:
                    out.append((sub, "—", _short(y[key])))
                elif key not in y:
                    out.append((sub, _short(x[key]), "—"))
                else:
                    walk(x[key], y[key], sub)
        elif isinstance(x, list):
            if len(x) != len(y):
                out.append((f"{path}[]", f"{len(x)} элем.", f"{len(y)} элем."))
            for i in range(min(len(x), len(y))):
                if len(out) >= limit:
                    return
                walk(x[i], y[i], f"{path}[{i}]")
        elif x != y:
            out.append((path or ".", _short(x), _short(y)))

    walk(before, after, "")
    return out[:limit]


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

            extra = S3Hook.get_connection(conn_id).extra_dejson
            verify = extra.get("verify", True)
            if isinstance(verify, str):
                verify = verify.lower() == "true"

            # config_kwargs соединения нельзя терять: явно переданный config подменяет их
            # целиком (connection_wrapper.py: `if not self.botocore_config and config_kwargs`),
            # а секрет-бэкенд кладёт туда signature_version, payload_signing_enabled и
            # request_checksum_calculation — без них шлюз отвергает запросы.
            # merge накладывает таймауты поверх, не затирая остального.
            config = Config(**extra.get("config_kwargs", {})).merge(Config(connect_timeout=15, read_timeout=15))

            hook = S3Hook(aws_conn_id=conn_id, verify=verify, config=config)
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
        logger.error("❌ %s: %s", conn_id, err, exc_info=True)
        # is not None обязательно: у requests.Response __bool__ == False на 4xx/5xx,
        # то есть проверка на истинность отбросила бы ровно те ответы, ради которых всё и логируется
        response = getattr(err, "response", None)
        if response is not None:
            logger.error("HTTP %s: %s", getattr(response, "status_code", "?"), str(getattr(response, "text", ""))[:500])
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
            try:
                result = hook.get_first("SELECT current_user, current_catalog, current_schema")
            except Exception as err:
                # Хост не резолвится: соединение заведено под контур, где Trino нет.
                # Это не деградация связности, а отсутствие сервиса — скип, а не падение.
                # Сообщение приезжает из urllib3 внутрь requests.ConnectionError, поэтому
                # ищем по всей строке, а не сравниваем тип исключения.
                if "Name or service not known" not in str(err):
                    raise
                msg = f"☮️ {conn_id}: хост не резолвится — {err}"
                add_note(msg, context, level="task", title=f"☮️ {conn_id}")
                logger.warning(msg)
                raise AirflowSkipException(msg) from err

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
        raise  # re-raise оригинальное исключение, а не AirflowFailException — в логе видна причина


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    doc_md=__doc__,
    default_args={
        "owner": "DataLab (CI02420667)",
        # без ретраев: тест снимает срез доступности здесь и сейчас, повтор только
        # растягивает прогон и маскирует моргнувшее соединение
        "retries": 0,
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
    # Периоды от большего к меньшему, накопительно: каждый следующий — подмножество
    # предыдущего. Так по таблице сразу видно, стенд «устаканился» или переразбирается
    # прямо сейчас: у здорового стенда числа падают до нуля уже на неделе
    SERIALIZED_PERIODS = [
        ("за год", "1 year"),
        ("за 3 месяца", "3 months"),
        ("за месяц", "1 month"),
        ("за неделю", "7 days"),
        ("за сутки", "1 day"),
        ("за час", "1 hour"),
    ]

    # Читается вместе с заголовком колонки: «Изменялись на последнем парсинге».
    # Это продолжение ряда периодов — самый узкий «интервал», уже не время, а одно событие
    LAST_PARSE_ROW = "на последнем парсинге"

    @task(
        task_id="check_serialized_dag",
        doc_md=("Статистика `main.serialized_dag`: как давно менялась сериализация DAG'ов. "
                "Падает, если сериализация переписана на последнем парсинге файла"),
    )
    def check_serialized_dag(**context) -> dict:
        """Считает, у скольких DAG'ов менялась сериализация за год, 3 месяца, месяц, неделю, сутки и час.

        Строка в serialized_dag переписывается только когда меняется dag_hash, то есть
        содержимое DAG'а после парсинга файла (serialized_dag.py:170). На стабильном стенде
        последний парсинг файла не должен был ничего переписать — если переписал, значит
        парсинг даёт каждый раз новый результат: значение из БД или API на уровне модуля,
        текущее время, плавающий порядок списка. Поэтому ошибка — не «изменения за час»
        (час зависит от min_file_process_interval и ловит обычный деплой), а именно
        совпадение записи сериализации с последним парсингом.
        """
        import time

        from airflow.configuration import conf
        from airflow.exceptions import AirflowFailException
        from airflow.utils.session import create_session
        from sqlalchemy import text

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        # Окно одного парсинга: write_dag пишет last_updated раньше, чем bulk_write_to_db —
        # last_parsed_time (dagbag.py:716-723, одна транзакция), поэтому сериализация,
        # сделанная на последнем парсинге, отстаёт от last_parsed_time не больше, чем длится
        # сам парсинг. Верхняя граница — dag_file_processor_timeout, и она заведомо меньше
        # min_file_process_interval, то есть окно не дотянется до прошлого парсинга.
        parse_window = conf.getint("core", "dag_file_processor_timeout", fallback=600)
        at_parse_cond = (f"d.last_parsed_time IS NOT NULL AND "
                         f"sd.last_updated >= d.last_parsed_time - interval '{parse_window} seconds'")

        # интервалы — константы из SERIALIZED_PERIODS, parse_window — int из conf:
        # в запрос не приходит ничего извне
        stats_sql = (
            "SELECT COUNT(*) AS total, "
            + ", ".join(
                f"COUNT(*) FILTER (WHERE sd.last_updated > now() - interval '{iv}') AS p{i}"
                for i, (_, iv) in enumerate(SERIALIZED_PERIODS)
            )
            + f", COUNT(*) FILTER (WHERE {at_parse_cond}) AS at_parse"
            + " FROM main.serialized_dag sd LEFT JOIN main.dag d USING (dag_id)"
        )
        # Полный список уходит в XCom, в заметку попадают только первые note_rows
        list_sql = f"""
            SELECT sd.last_updated, sd.dag_id, sd.fileloc, sd.dag_hash, d.last_parsed_time
            FROM main.serialized_dag sd
            JOIN main.dag d USING (dag_id)
            WHERE {at_parse_cond}
            ORDER BY sd.last_updated DESC
            LIMIT 500
        """
        note_rows = 10  # заметка режется по MAX_NOTE_LEN, а таблица со статистикой важнее списка

        ts = time.time()
        with create_session() as session:
            row = session.execute(text(stats_sql)).one()
            names = [name for name, _ in SERIALIZED_PERIODS]
            total, counts = row[0], dict(zip(names, row[1:1 + len(names)]))
            at_parse = row[1 + len(names)]
            rows = session.execute(text(list_sql)).fetchall() if at_parse else []

        elapsed = time.time() - ts
        logger.info("🔍 serialized_dag: всего %d, %s, %s %d", total,
                    ", ".join(f"{name} {cnt}" for name, cnt in counts.items()),
                    LAST_PARSE_ROW, at_parse)

        def share(cnt: int) -> str:
            return f"{cnt * 100 / total:.0f}%" if total else "—"

        stats = "\n".join(
            ["| Изменялись | DAG'ов | Доля |", "|---|---:|---:|", f"| **всего** | **{total}** | 100% |"]
            + [f"| {name} | {counts[name]} | {share(counts[name])} |" for name, _ in SERIALIZED_PERIODS]
            + [f"| **{LAST_PARSE_ROW}** | **{at_parse}** | {share(at_parse)} |"]
        )
        result = {"total": total, **{name: counts[name] for name, _ in SERIALIZED_PERIODS},
                  LAST_PARSE_ROW: at_parse}

        if not at_parse:
            add_note(stats, context, level="task,DAG", title=f"✅ {elapsed:.2f} sec check_serialized_dag")
            return {"status": "ok", **result}

        msg = (f"main.serialized_dag: {at_parse} из {total} DAG'ов пересериализовано "
               f"на последнем парсинге файла")
        data = [
            {"last_updated": str(last_updated), "dag_id": dag_id, "fileloc": fileloc,
             "dag_hash": dag_hash, "last_parsed_time": str(last_parsed_time)}
            for last_updated, dag_id, fileloc, dag_hash, last_parsed_time in rows
        ]
        add_xcom("serialized_dag", data, context)

        # last_parsed_time и dag_hash в заметке не нужны: первый по условию отбора и так
        # совпадает с last_updated с точностью до парсинга, второй ни с чем не сравнить.
        # Оба остаются в XCom `serialized_dag` — там их можно смотреть построчно
        table = "| last_updated | dag_id |\n|---|---|\n" + "\n".join(
            f"| {r['last_updated']} | `{r['dag_id']}` |" for r in data[:note_rows]
        )
        if at_parse > note_rows:
            table += (f"\n\nПоказаны первые {min(note_rows, len(data))} из {at_parse}, "
                      f"полный список — в XCom `serialized_dag`.")
        add_note(f"{msg}\n\n{stats}\n\n{table}", context, level="task",
                 title=f"❌ {elapsed:.2f} sec check_serialized_dag")
        add_note(f"{msg}\n\n{stats}", context, level="DAG",
                 title=f"❌ {elapsed:.2f} sec check_serialized_dag")
        raise AirflowFailException(msg)

    # --- Перепроверка дёргающихся DAG'ов ---
    # Сколько DAG'ов из списка check_serialized_dag берём в перепроверку. Каждый ждёт
    # своего следующего парсинга, то есть до min_file_process_interval, а параллельность
    # ограничена max_active_tasks_per_dag (в проме 4) — отсюда и небольшой лимит
    RECHECK_LIMIT = 5

    @task(task_id="recheck_targets", trigger_rule=TriggerRule.ALL_DONE)
    def recheck_targets(**context) -> list[dict]:
        """Раздаёт mapped-таску список DAG'ов из XCom `serialized_dag`.

        Отдельный таск, а не expand прямо от check_serialized_dag: тот падает, когда
        список непустой, и раскрытие mapped-таска по XCom упавшего апстрима — лишний
        риск. Здесь же trigger_rule=ALL_DONE и обычный список на выходе.
        """
        import json

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        raw = context["ti"].xcom_pull(task_ids="check_serialized_dag", key="serialized_dag")
        if not raw:
            logger.info("check_serialized_dag не оставил списка — перепроверять нечего")
            return []

        # add_xcom кладёт коллекции json.dumps'ом, поэтому строка
        data = json.loads(raw) if isinstance(raw, str) else raw
        targets = [{"dag_id": r["dag_id"], "dag_hash": r["dag_hash"]} for r in data[:RECHECK_LIMIT]]
        tail = f" (из {len(data)})" if len(data) > RECHECK_LIMIT else ""
        add_note(", ".join(f"`{t['dag_id']}`" for t in targets), context, level="task",
                 title=f"🔁 На перепроверку: {len(targets)}{tail}")
        logger.info("На перепроверку %d DAG'ов%s: %s", len(targets), tail,
                    ", ".join(t["dag_id"] for t in targets))
        return targets

    @task(task_id="recheck_serialized_dag")
    def recheck_serialized_dag(target: dict, **context) -> dict:
        """Ждёт следующего парсинга DAG'а и сравнивает сериализацию до и после.

        check_serialized_dag говорит только «сериализация переписана на последнем
        парсинге». Этот таск отвечает на следующий вопрос — что именно в ней меняется:
        снимает JSON, дожидается сдвига `dag.last_parsed_time`, снимает второй и
        показывает расхождения по путям. Одинаковый dag_hash после нового парсинга
        означает, что прошлое изменение было разовым (деплой), а не дрожанием.

        Не падает: это диагностика поверх уже упавшего check_serialized_dag, и своим
        падением она только зашумила бы summary. Вердикт — в заметке и в логе.
        """
        import time

        from airflow.configuration import conf
        from airflow.exceptions import AirflowSkipException
        from airflow.models.dag import DagModel
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        dag_id = target["dag_id"]
        poke = 30
        # Худший случай — начали ждать сразу после парсинга: полный интервал плюс сам
        # разбор файла. Ждать дольше смысла нет: если за это время парсинга не случилось,
        # проблема не в дрожании, а в том, что файл вообще перестали разбирать
        timeout = (conf.getint("scheduler", "min_file_process_interval", fallback=30)
                   + conf.getint("core", "dag_file_processor_timeout", fallback=600))

        def snapshot() -> tuple:
            """(dag_hash, data, last_parsed_time); data берём через ORM — она распакует zlib."""
            with create_session() as session:
                sdm = session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).one_or_none()
                parsed = session.query(DagModel.last_parsed_time).filter(DagModel.dag_id == dag_id).scalar()
                return (sdm.dag_hash, sdm.data, parsed) if sdm else (None, None, parsed)

        hash0, data0, parsed0 = snapshot()
        if hash0 is None or parsed0 is None:
            msg = f"☮️ {dag_id}: нет в serialized_dag или в dag — сравнивать не с чем"
            add_note(msg, context, level="task", title=f"☮️ {dag_id}")
            logger.warning(msg)
            raise AirflowSkipException(msg)

        ts = time.time()
        deadline = ts + timeout
        logger.info("⏳ %s: ждём следующего парсинга, last_parsed_time=%s, таймаут %dс",
                    dag_id, parsed0, timeout)

        hash1, data1, parsed1 = hash0, data0, parsed0
        while time.time() < deadline:
            time.sleep(poke)
            hash1, data1, parsed1 = snapshot()
            if parsed1 and parsed1 > parsed0:
                break
        else:
            waited = time.time() - ts
            msg = (f"⏱️ {dag_id}: за {waited:.0f}с парсинга не было "
                   f"(last_parsed_time всё ещё {parsed0}) — файл не разбирается")
            add_note(msg, context, level="task", title=f"⏱️ {dag_id}")
            logger.warning(msg)
            return {"dag_id": dag_id, "status": "no_parse", "waited": round(waited)}

        waited = time.time() - ts
        if hash1 == hash0:
            msg = (f"✅ {dag_id}: парсинг прошёл через {waited:.0f}с, "
                   f"сериализация не изменилась (dag_hash `{hash0}`) — разовое изменение")
            add_note(msg, context, level="task", title=f"✅ {dag_id}")
            logger.info(msg)
            return {"dag_id": dag_id, "status": "stable", "waited": round(waited), "diffs": 0}

        diffs = _json_diff(data0 or {}, data1 or {})
        head = (f"❌ {dag_id}: сериализация переписана снова через {waited:.0f}с "
                f"(`{hash0}` → `{hash1}`) — разбор файла недетерминирован")
        for path, was, became in diffs:
            logger.warning("  %s: %s → %s", path, was, became)
        if diffs:
            table = ("| Путь | Было | Стало |\n|---|---|---|\n"
                     + "\n".join(f"| `{p}` | {a} | {b} |" for p, a, b in diffs[:5]))
        else:
            # data пустая при compress_serialized_dags, либо расхождение вне JSON
            table = "Расхождений в JSON не нашлось, хотя dag_hash разный"
        if len(diffs) > 5:
            table += f"\n\nПоказаны 5 из {len(diffs)}, остальные — в логе."
        add_note(f"{head}\n\n{table}", context, level="task", title=f"❌ {dag_id}")
        logger.error(head)
        return {"dag_id": dag_id, "status": "unstable", "waited": round(waited),
                "diffs": len(diffs), "paths": [p for p, _, _ in diffs]}

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

    # check_serialized_dag падает при находках, поэтому цепочка держится на ALL_DONE
    # у recheck_targets; пустой список схлопывает mapped-таск в skipped
    targets = recheck_targets()
    check_serialized_dag() >> targets
    recheck_serialized_dag.expand(target=targets) >> summary_task

    if groups:
        groups >> summary_task


tools_test_connections()
