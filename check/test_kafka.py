"""🧪 DAG: ручные тесты Kafka.

Два независимых DAG-а для изолированной проверки Kafka-связки (коннект, топик, формат
сообщения) без какого-либо прикладного пайплайна:

  📤 test_kafka_out — шлёт одно XML-сообщение `TransferFileCephRq` в топик.
  📥 test_kafka_in  — принимает первое любое сообщение из топика и показывает его.

Оба DAG-а параметризуются на запуске:

| Параметр   | Описание |
|---|---|
| `conn_id`  | Airflow Kafka conn_id (kafka_config_id) |
| `topic`    | Имя топика |
| `scenario` | ScenarioId в XML (используется только в out) |
| `filename` | Имя файла в XML (out шлёт его как `Name`) |
| `mode`     | Только in: `consume` (воркер, без triggerer) или `await` (deferrable-сенсор, нужен triggerer) |

Режимы приёма (`mode`):
  • `consume` (по умолчанию) — `ConsumeFromTopicOperator`: синхронно опрашивает топик на
    воркере, triggerer не требуется. Ловит только сообщения, опубликованные ПОСЛЕ старта
    опроса (consumer group, auto.offset.reset=latest), в пределах poll_timeout (180 c).
  • `await` — `AwaitMessageSensor`: deferrable, ждёт сообщение через **triggerer**. Если
    triggerer не запущен, таск зависнет в `deferred` и упадёт по execution_timeout.
  • `read_last` — читает последние `max_messages` сообщений, **уже лежащих** в топике (tail
    через seek к high_watermark−N), свежей consumer group без коммита offset. Порядок запуска
    не важен: можно сначала `test_kafka_out`, потом `test_kafka_in`.

Запускать вручную (schedule=None). Для сквозной проверки `consume`/`await`: сначала trigger
`test_kafka_in` на нужный топик, затем — пока идёт опрос (≤180 c в режиме consume) —
`test_kafka_out` на тот же топик. Для `read_last` порядок любой.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor

try:
    from plugins.utils import add_note, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, on_callback  # type: ignore

logger = logging.getLogger("airflow.task")

# 🔧 Дефолты Kafka (можно переопределить параметрами запуска)
KAFKA_OUT_CONN = "tfs-kafka-out"
KAFKA_OUT_TOPIC = "TFS.HRPLT.IN"
KAFKA_IN_CONN = "tfs-kafka-in"
KAFKA_IN_TOPIC = "TFS.HRPLT.IN"  # слушаем тот же топик, куда шлёт test_kafka_out


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def produce_test_msg(scenario_id: str, file_names: list[str], throttle_delay: int = 1):
    """Генератор Kafka-сообщений: одно XML-уведомление TransferFileCephRq на каждый файл."""
    import time
    import uuid

    for file_name in file_names:
        time.sleep(throttle_delay)
        rq_uuid = str(uuid.uuid4()).replace("-", "")
        now = datetime.now().astimezone()
        rq_tm = f"{now:%Y-%m-%dT%H:%M:%S}.{now.microsecond // 1000:03d}{now:%z}"
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


def capture_msg(msg) -> str:
    """apply_function для AwaitMessageSensor (режим await): принимает любое сообщение, возвращает текст.

    Truthy-возврат останавливает сенсор; значение уходит в XCom для отображения в общей задаче show.
    """
    return msg.value().decode("utf-8", errors="replace")


def consume_msg(msg) -> None:
    """apply_function для ConsumeFromTopicOperator (режим consume): логирует и кладёт текст в XCom.

    Выполняется на воркере; get_current_context даёт доступ к ti для xcom_push.
    Отображение — в общей задаче show.
    """
    from airflow.operators.python import get_current_context

    text = msg.value().decode("utf-8", errors="replace")
    logger.info("Received Kafka message: %s", text)
    get_current_context()["ti"].xcom_push(key="message", value=text)


KAFKA_CAPTURE = f"{__name__}.capture_msg"
ON_DELIVERY = f"{__name__}.on_delivery"

_DEF_ARGS = {
    "owner":               "DataLab (CI02420667)",
    "retries":             0,
    "on_failure_callback": on_callback,
    "on_success_callback": on_callback,
}
_TAGS = ["DataLab", "tools", "Kafka", "AutoQA"]


# ── DAG: test_kafka_out ───────────────────────────────────────────────────────

@dag(
    dag_id="test_kafka_out",
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
        "conn_id":  Param(KAFKA_OUT_CONN, type="string", title="Kafka conn_id"),
        "topic":    Param(KAFKA_OUT_TOPIC, type="string", title="Topic"),
        "scenario": Param("HRPLATFORM-4000", type="string", title="Scenario ID"),
        "filename": Param("test.zip", type="string", title="File name"),
    },
)
def test_kafka_out():
    ProduceToTopicOperator(
        task_id="notify",
        kafka_config_id="{{ params.conn_id }}",
        topic="{{ params.topic }}",
        producer_function=produce_test_msg,
        producer_function_args=["{{ params.scenario }}", ["{{ params.filename }}"]],
        delivery_callback=ON_DELIVERY,
        execution_timeout=timedelta(minutes=5),
    )


test_kafka_out()


# ── DAG: test_kafka_in ────────────────────────────────────────────────────────

@dag(
    dag_id="test_kafka_in",
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
        "conn_id":  Param(KAFKA_IN_CONN, type="string", title="Kafka conn_id"),
        "topic":    Param(KAFKA_IN_TOPIC, type="string", title="Topic"),
        "scenario": Param("HRPLATFORM-4000", type="string", title="Scenario ID"),
        "filename": Param("test.zip", type="string", title="File name"),
        "mode":     Param(
            "consume",
            type="string",
            enum=["consume", "await", "read_last"],
            title="Receive mode",
            description="consume = ConsumeFromTopicOperator (воркер, ловит новые, без triggerer); "
                        "await = AwaitMessageSensor (deferrable, требует triggerer); "
                        "read_last = прочитать последние N сообщений, уже лежащие в топике.",
        ),
        "max_messages": Param(1, type="integer", minimum=1, title="Max messages (read_last)"),
    },
)
def test_kafka_in():
    @task.branch(task_id="pick")
    def pick(params=None):
        return {"consume": "wait_consume", "await": "wait_await", "read_last": "read_last"}[params["mode"]]

    # режим consume: синхронный опрос топика на воркере, triggerer не нужен.
    # Ловит только сообщения, опубликованные ПОСЛЕ старта опроса (auto.offset.reset=latest),
    # в пределах poll_timeout — поэтому test_kafka_out нужно триггерить в это окно.
    # commit_cadence='never' — тест read-only: не двигаем offset и убираем warning про auto.commit.
    wait_consume = ConsumeFromTopicOperator(
        task_id="wait_consume",
        kafka_config_id="{{ params.conn_id }}",
        topics=["{{ params.topic }}"],
        apply_function=consume_msg,
        commit_cadence="never",
        max_messages=1,
        max_batch_size=1,
        poll_timeout=180,
    )

    # режим await: deferrable-сенсор, ждёт сообщение через triggerer
    wait_await = AwaitMessageSensor(
        task_id="wait_await",
        kafka_config_id="{{ params.conn_id }}",
        topics=["{{ params.topic }}"],
        apply_function=KAFKA_CAPTURE,
        execution_timeout=timedelta(minutes=10),
    )

    # общая задача отображения: берёт сообщение из той ветки, что отработала
    @task(task_id="show", trigger_rule="none_failed_min_one_success")
    def show(**context):
        ti = context["ti"]
        msg = (
            ti.xcom_pull(task_ids="wait_await")                      # await: return capture_msg
            or ti.xcom_pull(task_ids="read_last")                   # read_last: return list
            or ti.xcom_pull(task_ids="wait_consume", key="message")  # consume: xcom_push
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
        config["group.id"] = f"test_kafka_read_{uuid.uuid4().hex[:8]}"  # свежая группа — без committed offset
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
            msgs, deadline = [], time.time() + 30
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
    targets = [wait_consume, wait_await, read_last()]
    p >> targets
    targets >> show()


test_kafka_in()
