"""🧪 DAG: ручные тесты Kafka.
*2026-08-21 12:41 MSK · v1.6 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Два независимых DAG-а для изолированной проверки Kafka-связки (коннект, топик, формат
сообщения) без какого-либо прикладного пайплайна:

  📤 tools_test_kafka_snd — шлёт в топик одно произвольное сообщение; по умолчанию
     в поле лежит готовый `TransferFileCephRq`, собранный тем же `build_message`,
     каким тракт шлёт в бою.
     ⚠️ При отправке в боевой топик ТФС идёт **мимо очереди** `export.er_sent_files`:
     файл не попадёт в счётчики лимитов маршрута, а пул `tfs_<ScenarioId>` этот таск не берёт
     (он живёт в `TOOLS_POOL`, а двух пулов у таска не бывает) — то есть возможна
     отправка одновременно с `tfs_kafka_snd`. Для разовой проверки это допустимо,
     для регулярной отправки нужен не этот даг, а очередь.
  📥 tools_test_kafka_rcv — показывает сообщения из топика.

Имена топиков TFS даны с его стороны, поэтому наши направления зеркальны:

| Действие | conn_id | Топик |
|---|---|---|
| пишем | `tfs-kafka-in` | `TFS.HRPLT.IN` |
| читаем | `tfs-kafka-out` | `TFS.HRPLT.OUT` |

Дефолты параметров расставлены по этой таблице; на запуске переопределяются.

⚠️ **Топики зависят от контура**: в таблице — сигма, на альфе маршрут ПКАП
(`TFS.PKAPHR.IN` / `TFS.PKAPHR.OUT`). Из `KAFKA_SND_TOPICS` их сюда не подставить —
этот даг выкладывается на оба контура, а `plugins/tfs_utils.py` живёт вместе с ЕР,
и жёсткий импорт сломал бы даг там, где тракта нет. На альфе топик задаётся руками.

Оба DAG-а параметризуются на запуске:

| Параметр   | Описание |
|---|---|
| `conn_id`  | Airflow Kafka conn_id (kafka_config_id); выпадающий список — kafka-коннекты из Variable `local_connections`, её наполняет `tools_show_connections` |
| `topic`    | Имя топика |
| `message`  | Только write: текст сообщения, уходит как есть. Маркеры `{RqUID}` и `{RqTm}` заменяются при отправке; по умолчанию — `TransferFileCephRq` |
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
    ⚠️ Работает в consumer group коннекта, а у `tfs-kafka-out` она общая с боевым дагом
    `tfs_kafka_rcv` — единственным штатным потребителем этого топика. Сообщение,
    доставшееся тесту, до него уже не дойдёт, а значит и до выгрузки, которая ждёт
    квитанцию: топик `TFS.HRPLT.OUT` общий на все маршруты ТФС. Запускать здесь режим
    `wait` можно только при остановленном `tfs_kafka_rcv`.
    Прочитанные им квитанции лежат в `export.tfs_receipts` — обычно смотреть надо туда,
    а не в топик.

Запускать вручную (schedule=None). Для сквозной проверки: обоим DAG-ам поставить один топик
(`TFS.HRPLT.IN` — туда пишем мы, чужих слушателей там нет), и в режиме `wait` сначала
триггерить `tools_test_kafka_rcv`, а `tools_test_kafka_snd` — пока идёт опрос
(`timeout` секунд). Для `read_last` порядок любой.
"""
from __future__ import annotations

import logging
import re
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


# 🧾 Заготовка для поля Message. Маркеры заменяются на отправке, всё остальное
# уходит в топик буквально — включая ScenarioId и имя файла.
DEF_SCENARIO = "HRPLATFORM-4000"
DEF_FILENAME = "test.zip"
RQ_UID_MARK  = "{RqUID}"
RQ_TM_MARK   = "{RqTm}"

# Запасной шаблон на случай, когда слоя тракта рядом нет (альфа): даг выкладывается
# на оба контура, и Broken DAG из-за отсутствующего импорта там недопустим.
FALLBACK_MESSAGE = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{RQ_UID_MARK}</RqUID>
    <RqTm>{RQ_TM_MARK}</RqTm>
    <ScenarioInfo><ScenarioId>{DEF_SCENARIO}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{DEF_FILENAME}</Name></FileInfo></File>
</TransferFileCephRq>"""


def _default_message() -> str:
    """Текст, который лежит в поле Message по умолчанию.

    Собирается тем же build_message, каким тракт шлёт в бою: тест должен проверять
    боевой формат, а не свою копию, иначе он перестаёт быть тестом ровно тогда, когда
    формат меняется. RqUID отдаём маркером, а сгенерированный RqTm меняем на маркер —
    оба значения должны быть свежими на каждой отправке, а не на разборе файла.
    """
    try:
        from plugins.tfs_utils import build_message  # type: ignore
    except ImportError:
        try:
            from CI06932748.tools.tfs_utils import build_message  # type: ignore
        except ImportError:
            logger.info("Слой тракта ТФС недоступен, поле Message заполняем своим шаблоном")
            return FALLBACK_MESSAGE

    message = build_message(DEF_SCENARIO, RQ_UID_MARK, DEF_FILENAME)
    return re.sub(r"<RqTm>.*?</RqTm>", f"<RqTm>{RQ_TM_MARK}</RqTm>", message, count=1)


DEFAULT_MESSAGE = _default_message()


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

def produce_test_msg():
    """Генератор Kafka-сообщений: одно сообщение из параметра `message`.

    Текст читаем из контекста, а не через шаблон в producer_function_args: у дага
    включён render_template_as_native_obj, а нативный рендер прогоняет результат через
    literal_eval. XML это переживёт, а сообщение вида {"a": 1} превратилось бы в питоновский
    dict и уехало в топик в неузнаваемом виде.

    Подстановка через str.replace, а не str.format: в произвольном сообщении бывают свои
    фигурные скобки, и format на них падает. Нет маркеров — текст уходит нетронутым.
    """
    import uuid

    from airflow.operators.python import get_current_context

    context = get_current_context()
    message = context["params"]["message"]

    if not (message or "").strip():
        # Пустое сообщение — почти наверняка промах, но и оно бывает предметом проверки,
        # поэтому предупреждаем, а не роняем.
        logger.warning("⚠️ Сообщение пустое, уйдёт как есть")

    rq_uid = uuid.uuid4().hex
    # isoformat(ms) даёт формат ТФС 'YYYY-MM-DDTHH:mm:ss.SSS+03:00'
    rq_tm = datetime.now().astimezone().isoformat(timespec="milliseconds")
    message = message.replace(RQ_UID_MARK, rq_uid).replace(RQ_TM_MARK, rq_tm)

    logger.info("Kafka message to send:\n%s", message)
    add_note(f"```\n{message}\n```", context, level="DAG", title="📤 Kafka message")
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
_TAGS = ["DataLab", "tools", "kafka", "AutoQA"]


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
        "message":  Param(
            DEFAULT_MESSAGE, type="string", format="multiline", title="Message",
            description="Уходит в топик как есть. Маркеры {RqUID} и {RqTm} заменяются "
                        "при отправке на свежий идентификатор и время; остальное — что напишете.",
        ),
    },
)
def tools_test_kafka_snd():
    ProduceToTopicOperator(
        task_id="notify",
        kafka_config_id="{{ params.conn_id }}",
        topic="{{ params.topic }}",
        producer_function=produce_test_msg,
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
