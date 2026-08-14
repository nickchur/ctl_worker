"""📨 DAG приёма обратных квитанций ТФС из Kafka в хранилище тракта.
*2026-08-14 11:25 MSK · v2.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Обратная квитанция `TransferFileCephRs` приходит по ВСЕМ маршрутам ТФС (xStream и ЕР)
и сопоставляется с отправкой по `RqUID`. Результат передачи — в `File/Status/StatusCode`,
где `0` означает успех, причина отказа — в `File/Status/StatusDesc`.

Пример сообщения:

    <TransferFileCephRs>
        <RqUID>de8a44a8410045a4a9e31ca1f95595aa</RqUID>
        <RqTm>2026-08-12T09:38:38.198+03:00</RqTm>
        <ScenarioInfo><ScenarioId>HRPLATFORM-2104</ScenarioId></ScenarioInfo>
        <File>
            <FileInfo><Name>....zip</Name></FileInfo>
            <Status><StatusCode>0</StatusCode></Status>
        </File>
    </TransferFileCephRs>

📄 **Файлов в квитанции может быть несколько.** По спеке `File` идёт `[1-N]`, а `Status`
лежит ВНУТРИ `File` — статус у каждого файла свой. Одно сообщение Kafka поэтому даёт
столько строк, сколько в нём файлов; ключ хранилища — `(rq_uid, file_name)`.

📚 **Свой сенсор на каждый топик.** Список топиков — в `KAFKA_RCV_TOPICS`
(`plugins/tfs_utils.py`), на каждый заводится отдельный таск. Добавление маршрута со
своим топиком сводится к строчке в списке; в хранилище пишется, откуда пришла квитанция.

⏱️ **Ран живёт час.** Сенсор опрашивает топик раз в 15 секунд в режиме `reschedule`
(между опросами слот воркера свободен) и держит окно до конца — так квитанция, пришедшая
на пятой минуте, не обрывает чтение на оставшиеся пятьдесят пять. В конце окна:

* пришла хоть одна квитанция → таск **зелёный**, в XCom их число за окно;
* за час тишина → таск **скипнут** (`soft_fail`), и стартует следующий ран.

Так пустой час и час с квитанциями видно в UI, не открывая логи.

⚠️ Этот даг — ЕДИНСТВЕННЫЙ потребитель этих топиков. Kafka отдаёт сообщение одному
потребителю в группе: читать те же топики откуда-то ещё значит воровать чужие квитанции.
Выгрузки ничего из Kafka не читают — они ждут появления своей квитанции по `RqUID`.

Единственный конкурент, который остался, — `tools_test_kafka_rcv` в режиме `wait`
(`ctl/check/test_kafka.py`). На этих топиках его запускать нельзя.

📌 Партиции берутся через `assign()`, а не `subscribe()`. В режиме `reschedule` процесс
между опросами умирает, и подписка вступала бы в группу заново каждые 15 секунд —
ребаланс на ровном месте, задевающий и соседние топики той же группы. `OFFSET_STORED`
сохраняет коммиченные offset'ы: группа та же, читаем с того же места.

🗄️ Куда складывать — решает слой `plugins/tfs_utils.py`: S3 всегда, ClickHouse и
Postgres зеркалом, если задан их conn_id. Даг об этом не знает, он зовёт `save_receipts`.

🔁 Доставка at-least-once: offset коммитится ПОСЛЕ успешной записи. Падение между
записью и коммитом даст повтор — хранилище снимает дубль при чтении.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue  # type: ignore

try:
    from plugins.tfs_utils import (  # type: ignore
        get_config, add_note, parse_receipt, ensure_pools, save_receipts,
        run_state_get, run_state_set,
    )
except ImportError:
    from CI06932748.tools.tfs_utils import (  # type: ignore
        get_config, add_note, parse_receipt, ensure_pools, save_receipts,
        run_state_get, run_state_set,
    )

_cfg            = get_config()
DEF_ARGS        = _cfg['DEF_ARGS']
KAFKA_RCV_CONN  = _cfg['KAFKA_RCV_CONN']
KAFKA_RCV_TOPICS = _cfg['KAFKA_RCV_TOPICS']
RCV_POOL        = _cfg['TFS_RCV_POOL']

logger = logging.getLogger("airflow.task")

# Сколько живёт ран, сек. Ровно столько сенсор держит окно, после чего зеленеет или скипается.
WINDOW = 60 * 60
# Пауза между опросами. В reschedule на это время слот воркера освобождается.
POKE_EVERY = 15
# Тишина внутри одного опроса, после которой считаем топик вычерпанным (сек).
IDLE_TIMEOUT = 5
# Потолок сообщений за один опрос: страховка от бесконечного цикла на большом отставании.
MAX_MESSAGES = 5000
# Ключ счётчика окна. Между опросами процесс умирает, память не годится, а обычный
# xcom_push стирается перед каждым опросом — держим через run_state_* (см. tfs_utils).
SEEN_KEY = 'seen'


@dag(
    dag_id="tfs_kafka_rcv",
    description="📨 Приём квитанций ТФС: Kafka → хранилище тракта",
    default_args=DEF_ARGS,
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    # Ран живёт час, следующий создаётся по расписанию и ждёт в queued из-за
    # max_active_runs=1 — стартует сразу, как текущий закончился. Разрыв в покрытии
    # равен задержке шедулера, а не длине паузы между ранами.
    schedule_interval="@hourly",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=70),
    tags=["DataLab", "CI02420667", "TFS", "kafka"],
    is_paused_upon_creation=False,
    doc_md=__doc__,
)
def tfs_kafka_rcv_dag():

    def poke_topic(topic: str, **context) -> PokeReturnValue:
        """📥 Вычитывает квитанции топика и складывает их в хранилище тракта.

        Порядок важен: сначала запись, только потом коммит offset. При обратном порядке
        падение между операциями потеряло бы квитанцию навсегда — а её ждёт выгрузка.

        Заодно заводит пулы тракта: приёмник крутится постоянно и сам сидит в default_pool,
        поэтому создаст пулы сценариев раньше, чем они понадобятся отправителю.

        ⚠️ Здесь нельзя кидать AirflowFailException: при soft_fail сенсор превращает его
        в скип, и реальная поломка притворится тишиной. Обычные исключения роняют таск.
        """
        import time

        from airflow.hooks.base import BaseHook
        from confluent_kafka import OFFSET_STORED, Consumer, TopicPartition

        ensure_pools()

        conn = BaseHook.get_connection(KAFKA_RCV_CONN)
        config = dict(conn.extra_dejson)  # extra_dejson = librdkafka config (контракт Kafka-провайдера)
        config.setdefault("bootstrap.servers", conn.host)
        config["enable.auto.commit"] = False

        consumer = Consumer(config)
        rows: list[dict] = []
        # Максимальный offset по каждой партиции. Коммитить по последнему сообщению
        # нельзя: оно относится к ОДНОЙ партиции, и остальные остались бы
        # незакоммиченными — их бы перечитывало каждый опрос.
        positions: dict[int, int] = {}

        try:
            meta = consumer.list_topics(topic, timeout=10)
            if topic not in meta.topics or meta.topics[topic].error is not None:
                raise RuntimeError(f"Топик {topic} недоступен: {meta.topics.get(topic)}")

            # OFFSET_STORED — читаем с коммиченной позиции своей группы; для партиции
            # без коммита действует auto.offset.reset из настроек соединения.
            consumer.assign([TopicPartition(topic, pid, OFFSET_STORED)
                             for pid in meta.topics[topic].partitions])
            logger.info("👂 Слушаем %s, партиций: %d", topic, len(meta.topics[topic].partitions))

            idle_until = time.time() + IDLE_TIMEOUT
            while time.time() < idle_until and len(rows) < MAX_MESSAGES:
                msg = consumer.poll(timeout=1)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning("⚠️ Kafka: %s", msg.error())
                    continue

                raw = msg.value().decode("utf-8", errors="replace")
                # Одно сообщение — несколько строк: File у ТФС идёт [1-N], статус у каждого
                # файла свой, и в хранилище они ложатся отдельными строками.
                parsed = parse_receipt(raw, msg.partition(), msg.offset())
                for row in parsed:
                    row['kafka_topic'] = topic
                rows.extend(parsed)

                positions[msg.partition()] = max(positions.get(msg.partition(), -1), msg.offset())
                idle_until = time.time() + IDLE_TIMEOUT  # пока идут сообщения — продолжаем

            if rows:
                save_receipts(rows)
                # Только теперь: квитанции уже в хранилище, повтор при падении не страшен.
                # offset + 1 — семантика Kafka: коммитим позицию СЛЕДУЮЩЕГО сообщения.
                consumer.commit(
                    offsets=[TopicPartition(topic, p, o + 1) for p, o in positions.items()],
                    asynchronous=False,
                )
        finally:
            consumer.close()

        seen = int(run_state_get(context, SEEN_KEY) or 0)

        if rows:
            seen += len(rows)
            run_state_set(context, SEEN_KEY, seen)

            failed  = [r for r in rows if r['status_code'] > 0]
            unknown = [r for r in rows if r['status_code'] < 0]
            logger.info("📨 %s: квитанций %d (за окно %d), с ошибкой передачи: %d, неразобранных: %d",
                        topic, len(rows), seen, len(failed), len(unknown))

            # Заметка короткая намеренно: за час опросов их накопится много, а add_note
            # склеивает записи и режет всё вместе по MAX_NOTE_LEN.
            line = f"+{len(rows)} (за окно {seen})"
            if failed:
                line += f", ❌ StatusCode != 0: {len(failed)}"
            if unknown:
                line += f", ⚠️ не разобрано: {len(unknown)}"
            add_note({f"📨 {topic}": line}, level='dag', context=context, title='📨 tfs_kafka_rcv')

        # Окно считаем от старта рана: оно общее для всех топиков и переживает reschedule.
        elapsed = (datetime.now(timezone.utc) - context['dag_run'].start_date).total_seconds()
        if elapsed >= WINDOW and seen:
            logger.info("✅ %s: окно закрыто, квитанций за час: %d", topic, seen)
            return PokeReturnValue(is_done=True, xcom_value=seen)

        # Ложный ответ = ждём дальше. Когда окно выйдет, а seen так и останется нулём,
        # сенсор упрётся в timeout и при soft_fail пометит таск скипнутым.
        return PokeReturnValue(is_done=False)

    for _topic in KAFKA_RCV_TOPICS:
        # timeout заведомо больше окна: сенсор проверяет его ТОЛЬКО после ложного ответа
        # (airflow/sensors/base.py), поэтому зелёный по концу окна успевает сработать
        # первым, а скип по таймауту достаётся ровно случаю «за час ничего не пришло».
        task.sensor(
            task_id=f"rcv_{_topic.lower().replace('.', '_')}",
            mode='reschedule',
            poke_interval=POKE_EVERY,
            timeout=WINDOW + 2 * POKE_EVERY,
            soft_fail=True,
            pool=RCV_POOL,
            doc_md=f"Приём квитанций из топика `{_topic}`",
        )(poke_topic)(topic=_topic)


tfs_kafka_rcv_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
