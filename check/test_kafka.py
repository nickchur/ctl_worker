"""🧪 DAG: ручные тесты Kafka.
*2026-08-07 12:10 MSK · v1.2 · Чуркин Николай · [nschurkin@sberbank.ru](mailto:nschurkin@sberbank.ru)*

Два независимых DAG-а для изолированной проверки Kafka-связки (коннект, топик, формат
сообщения) без какого-либо прикладного пайплайна:

  📤 tools_test_kafka_snd — шлёт одно XML-сообщение `TransferFileCephRq` в топик.
  📥 tools_test_kafka_rcv — показывает сообщения из топика.

Имена топиков TFS даны с его стороны, поэтому наши направления зеркальны:

| Действие | conn_id | Топик |
|---|---|---|
| пишем | `tfs-kafka-in` | `TFS.HRPLT.IN` |
| читаем | `tfs-kafka-out` | `TFS.HRPLT.OUT` |

Дефолты параметров расставлены по этой таблице; на запуске переопределяются.

Оба DAG-а параметризуются на запуске:

| Параметр   | Описание |
|---|---|
| `conn_id`  | Airflow Kafka conn_id (kafka_config_id); выпадающий список — kafka-коннекты из Variable `local_connections`, её наполняет `tools_show_connections` |
| `topic`    | Имя топика |
| `scenario` | Только write: ScenarioId в XML |
| `filename` | Только write: имя файла в XML (`Name`) |
| `mode`     | Только read: `read_last` / `wait` |
| `timeout`  | Только read: сколько ждать сообщение, сек (окно read_last / poll_timeout) |
| `max_messages` | Только read: сколько последних сообщений читать в `read_last` |

Режимы приёма (`mode`):
  • `read_last` (по умолчанию) — читает последние `max_messages` сообщений, **уже лежащих**
    в топике (tail через seek к high_watermark−N), свежей consumer group без коммита offset.
    Порядок запуска не важен, в боевую группу не входит — ничего ни у кого не перехватывает.
  • `wait` — `ConsumeFromTopicOperator`: синхронно опрашивает топик на воркере, triggerer
    не требуется. Ловит только сообщения, опубликованные ПОСЛЕ старта опроса (consumer group,
    auto.offset.reset=latest), в пределах `timeout` секунд.
    ⚠️ Работает в consumer group коннекта, а у `tfs-kafka-out` она общая с `wait_confirm`
    боевого `er_export`: сообщение, доставшееся тесту, до сенсора уже не дойдёт. Для
    `TFS.HRPLT.OUT` запускать только при простое ER-выгрузок.

Запускать вручную (schedule=None). Для сквозной проверки: обоим DAG-ам поставить один топик
(`TFS.HRPLT.IN` — туда пишем мы, чужих слушателей там нет), и в режиме `wait` сначала
триггерить `tools_test_kafka_rcv`, а `tools_test_kafka_snd` — пока идёт опрос
(`timeout` секунд). Для `read_last` порядок любой.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

try:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore

logger = logging.getLogger("airflow.task")

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# 🔧 Дефолты Kafka (можно переопределить параметрами запуска).
# IN/OUT в именах — сторона TFS, и у соединения, и у топика: его вход — наш выход
SND_CONN  = "tfs-kafka-in"
SND_TOPIC = "TFS.HRPLT.IN"
RCV_CONN  = "tfs-kafka-out"
RCV_TOPIC = "TFS.HRPLT.OUT"


def _kafka_conn_ids() -> list[str]:
    """conn_id всех kafka-соединений из Variable `local_connections` — для выпадающего списка.

    Variable наполняет DAG `tools_show_connections`: {conn_type: [{conn_id, host, ...}]}.
    Читаем на парсинге, как это делает test_connections._load_groups. Если Variable нет
    (show_connections ещё не запускали) — остаются дефолты направлений, чтобы список не
    оказался пустым; они же всегда в списке, иначе выпадашка откроется без своего значения.
    """
    conn_ids = {SND_CONN, RCV_CONN}
    try:
        var_data = Variable.get("local_connections", deserialize_json=True, default_var=None) or {}
        conn_ids |= {c["conn_id"] for c in var_data.get("kafka", []) if c.get("conn_id")}
    except Exception as exc:
        logger.warning("Не прочитали local_connections, список conn_id только из дефолтов: %s", exc)
    return sorted(conn_ids)


KAFKA_CONN_IDS = _kafka_conn_ids()


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def produce_test_msg(scenario_id: str, file_names: list[str], throttle_delay: int = 1):
    """Генератор Kafka-сообщений: одно XML-уведомление TransferFileCephRq на каждый файл."""
    import time
    import uuid

    for file_name in file_names:
        time.sleep(throttle_delay)
        rq_uuid = str(uuid.uuid4()).replace("-", "")
        # isoformat(ms) даёт формат TFS 'YYYY-MM-DDTHH:mm:ss.SSSZ' (смещение с двоеточием, +03:00)
        rq_tm = datetime.now().astimezone().isoformat(timespec="milliseconds")
        message = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{rq_uuid}</RqUID>
    <RqTm>{rq_tm}</RqTm>
    <ScenarioInfo><ScenarioId>{scenario_id}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{file_name}</Name></FileInfo></File>
</TransferFileCephRq>"""
        logger.info("Kafka message prepared: %s", rq_uuid)
        yield None, message


def on_delivery(err: Exception | None, msg) -> None:
    """Колбэк подтверждения доставки Kafka: падает с AirflowFailException при ошибке."""
    from airflow.exceptions import AirflowFailException

    if err:
        raise AirflowFailException(f"Kafka delivery failed: {err}")
    logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def consume_msg(msg) -> None:
    """apply_function для ConsumeFromTopicOperator (режим wait): логирует и кладёт текст в XCom.

    Выполняется на воркере; get_current_context даёт доступ к ti для xcom_push.
    Отображение — в общей задаче show.
    """
    from airflow.operators.python import get_current_context

    text = msg.value().decode("utf-8", errors="replace")
    logger.info("Received Kafka message: %s", text)
    get_current_context()["ti"].xcom_push(key="message", value=text)


def _set_poll_timeout(context):
    """pre_execute: проставляет poll_timeout оператора из параметра timeout (poll_timeout не шаблонизируется)."""
    context["task"].poll_timeout = int(context["params"]["timeout"])


ON_DELIVERY = f"{__name__}.on_delivery"

_DEF_ARGS = {
    "owner":               "DataLab (CI02420667)",
    "retries":             0,
    "pool":                TOOLS_POOL,
    "on_failure_callback": on_callback,
    "on_success_callback": on_callback,
}
_TAGS = ["DataLab", "tools", "Kafka", "AutoQA"]


# ── DAG: tools_test_kafka_snd ───────────────────────────────────────────────

@dag(
    dag_id="tools_test_kafka_snd",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=_TAGS,
    default_args=_DEF_ARGS,
    doc_md=__doc__,
    params={
        "conn_id":  Param(SND_CONN, type="string", title="Kafka conn_id", examples=KAFKA_CONN_IDS),
        "topic":    Param(SND_TOPIC, type="string", title="Topic"),
        "scenario": Param("HRPLATFORM-4000", type="string", title="Scenario ID"),
        "filename": Param("test.zip", type="string", title="File name"),
    },
)
def tools_test_kafka_snd():
    ProduceToTopicOperator(
        task_id="notify",
        kafka_config_id="{{ params.conn_id }}",
        topic="{{ params.topic }}",
        producer_function=produce_test_msg,
        producer_function_args=["{{ params.scenario }}", ["{{ params.filename }}"]],
        delivery_callback=ON_DELIVERY,
        execution_timeout=timedelta(minutes=5),
    )


tools_test_kafka_snd()


# ── DAG: tools_test_kafka_rcv ────────────────────────────────────────────────

@dag(
    dag_id="tools_test_kafka_rcv",
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=_TAGS,
    default_args=_DEF_ARGS,
    doc_md=__doc__,
    params={
        "conn_id":  Param(RCV_CONN, type="string", title="Kafka conn_id", examples=KAFKA_CONN_IDS),
        "topic":    Param(RCV_TOPIC, type="string", title="Topic"),
        "mode":     Param(
            "read_last",
            type="string",
            enum=["read_last", "wait"],
            title="Receive mode",
            description="read_last = прочитать последние N сообщений, уже лежащие в топике, "
                        "свежей consumer group; wait = ConsumeFromTopicOperator (воркер, ловит "
                        "новые, без triggerer), но работает в боевой consumer group коннекта.",
        ),
        "timeout":  Param(
            180, type="integer", minimum=5, title="Timeout, sec",
            description="Сколько ждать сообщение: poll_timeout (wait), окно чтения (read_last).",
        ),
        "max_messages": Param(1, type="integer", minimum=1, title="Max messages (read_last)"),
    },
)
def tools_test_kafka_rcv():
    @task.branch(task_id="pick")
    def pick(params=None):
        return {"wait": "wait", "read_last": "read_last"}[params["mode"]]

    # режим wait: синхронный опрос топика на воркере, triggerer не нужен.
    # Ловит только сообщения, опубликованные ПОСЛЕ старта опроса (auto.offset.reset=latest),
    # в пределах poll_timeout — поэтому tools_test_kafka_snd нужно триггерить в это окно.
    # commit_cadence='never' — тест read-only: не двигаем offset и убираем warning про auto.commit.
    # Группа берётся из коннекта: у tfs-kafka-out она общая с wait_confirm боевого er_export,
    # поэтому дефолтный режим — read_last, со своей одноразовой группой.
    wait = ConsumeFromTopicOperator(
        task_id="wait",
        kafka_config_id="{{ params.conn_id }}",
        topics=["{{ params.topic }}"],
        apply_function=consume_msg,
        commit_cadence="never",
        max_messages=1,
        max_batch_size=1,
        pre_execute=_set_poll_timeout,  # poll_timeout из params.timeout
    )

    # общая задача отображения: берёт сообщение из той ветки, что отработала
    @task(task_id="show", trigger_rule="none_failed_min_one_success")
    def show(**context):
        ti = context["ti"]
        msg = (
            ti.xcom_pull(task_ids="read_last")                       # read_last: return list
            or ti.xcom_pull(task_ids="wait", key="message")  # wait: xcom_push
        )
        if not msg:
            logger.info("No Kafka message received")
            add_note("Сообщение не получено", context, level="DAG", title="📭 Kafka: no message")
            return
        items = msg if isinstance(msg, list) else [msg]
        for text in items:
            add_note(f"```\n{text}\n```", context, level="DAG", title="📨 Kafka message received")
        logger.info("Received %d Kafka message(s)", len(items))

    # режим read_last: читает последние N сообщений, уже лежащих в топике (tail),
    # через свежую consumer group + seek к high_watermark-N. Backlog не реплеит, offset не двигает.
    @task(task_id="read_last")
    def read_last(**context):
        import time
        import uuid

        from airflow.exceptions import AirflowFailException
        from airflow.hooks.base import BaseHook
        from confluent_kafka import Consumer, TopicPartition

        p = context["params"]
        topic = p["topic"]
        n = int(p.get("max_messages") or 1)

        conn = BaseHook.get_connection(p["conn_id"])
        config = dict(conn.extra_dejson)  # extra_dejson = librdkafka config (контракт Kafka-провайдера)
        config.setdefault("bootstrap.servers", conn.host)
        config["group.id"] = f"test_kafka_rcv_{uuid.uuid4().hex[:8]}"  # свежая группа — без committed offset
        config["enable.auto.commit"] = False
        config["auto.offset.reset"] = "earliest"

        consumer = Consumer(config)
        try:
            meta = consumer.list_topics(topic, timeout=10)
            if topic not in meta.topics or meta.topics[topic].error is not None:
                raise AirflowFailException(f"Topic {topic} недоступен")

            assignment, expected = [], 0
            for pid in meta.topics[topic].partitions:
                low, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=10)
                start = max(low, high - n)
                expected += high - start
                assignment.append(TopicPartition(topic, pid, start))

            if expected == 0:
                logger.info("Топик %s пуст", topic)
                return []

            consumer.assign(assignment)
            msgs, deadline = [], time.time() + int(p.get("timeout") or 180)
            while len(msgs) < expected and time.time() < deadline:
                m = consumer.poll(timeout=5)
                if m is None or m.error():
                    continue
                text = m.value().decode("utf-8", errors="replace")
                msgs.append(text)
                logger.info("Read existing message: %s", text)

            return msgs[-n:]
        finally:
            consumer.close()

    p = pick()
    targets = [wait, read_last()]
    p >> targets
    targets >> show()


tools_test_kafka_rcv()
