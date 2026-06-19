"""🧪 DAG: ручные тесты Kafka.

Два независимых DAG-а для изолированной проверки Kafka-связки (коннект, топик, формат
сообщения) без какого-либо прикладного пайплайна:

  📤 test_kafka_out — шлёт одно XML-сообщение `TransferFileCephRq` в топик.
  📥 test_kafka_in  — ждёт первое любое сообщение из топика и показывает его.

Оба DAG-а параметризуются на запуске:

| Параметр   | Описание |
|---|---|
| `conn_id`  | Airflow Kafka conn_id (kafka_config_id) |
| `topic`    | Имя топика |
| `scenario` | ScenarioId в XML (используется только в out) |
| `filename` | Имя файла в XML (out шлёт его как `Name`) |

Запускать вручную (schedule=None). Для сквозной проверки: сначала trigger `test_kafka_in`
на нужный топик (сенсор повиснет в ожидании), затем `test_kafka_out` на тот же топик.
"""
from __future__ import annotations

import logging
import time
import uuid
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
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
    for file_name in file_names:
        time.sleep(throttle_delay)
        rq_uuid = str(uuid.uuid4()).replace("-", "")
        message = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{rq_uuid}</RqUID>
    <RqTm>{pendulum.now().format('YYYY-MM-DDTHH:mm:ss.SSSZ')}</RqTm>
    <ScenarioInfo><ScenarioId>{scenario_id}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{file_name}</Name></FileInfo></File>
</TransferFileCephRq>"""
        logger.info("Kafka message prepared: %s", rq_uuid)
        yield None, message


def on_delivery(err: Exception | None, msg) -> None:
    """Колбэк подтверждения доставки Kafka: падает с AirflowFailException при ошибке."""
    if err:
        raise AirflowFailException(f"Kafka delivery failed: {err}")
    logger.info("Message delivered to %s [%s]", msg.topic(), msg.partition())


def capture_msg(msg) -> str:
    """apply_function для AwaitMessageSensor: принимает любое сообщение, возвращает его текст.

    Truthy-возврат останавливает сенсор; значение уходит в XCom для отображения в задаче show.
    """
    return msg.value().decode("utf-8", errors="replace")


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
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
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
    },
)
def test_kafka_in():
    wait = AwaitMessageSensor(
        task_id="wait",
        kafka_config_id="{{ params.conn_id }}",
        topics=["{{ params.topic }}"],
        apply_function=KAFKA_CAPTURE,
        execution_timeout=timedelta(minutes=10),
    )

    @task(task_id="show")
    def show(**context):
        msg = context["ti"].xcom_pull(task_ids="wait")
        add_note(f"```\n{msg}\n```", context, level="DAG", title="📨 Kafka message received")
        logger.info("Received Kafka message: %s", msg)

    wait >> show()


test_kafka_in()
