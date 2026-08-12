"""📨 DAG приёма обратных квитанций ТФС из Kafka в хранилище тракта.
*2026-08-12 14:10 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Обратная квитанция `TransferFileCephRs` приходит по ВСЕМ маршрутам ТФС (xStream и ЕР)
и сопоставляется с отправкой по `RqUID`. Результат передачи — в `Status/StatusCode`,
где `0` означает успех.

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

📚 **Топиков может быть несколько.** Список — в `KAFKA_RCV_TOPICS` (`tfs_config.py`),
одним коннектом слушаем их все сразу. Добавление маршрута со своим топиком сводится
к строчке в списке; в таблице сохраняется, из какого топика пришла квитанция.

⚠️ Этот даг — ЕДИНСТВЕННЫЙ потребитель этих топиков. Kafka отдаёт сообщение одному
потребителю в группе: читать те же топики откуда-то ещё значит воровать чужие квитанции.
Выгрузки ничего из Kafka не читают — они ждут появления своей строки
в `export.tfs_receipts` по своему `RqUID`.

Единственный конкурент, который остался, — `tools_test_kafka_rcv` в режиме `wait`
(`ctl/check/test_kafka.py`). На этих топиках его запускать нельзя.

⏱️ Расписание: раз в минуту, `max_active_runs=1`. Ран короткий: опрашивает, пока идут
сообщения, и выходит по тишине либо по потолку сообщений.

🗄️ Куда складывать — решает `STORAGE` в `plugins/tfs_utils.py`: ClickHouse, S3 или
Postgres. Даг об этом не знает, он зовёт `save_receipts`.

🔁 Доставка at-least-once: offset коммитится ПОСЛЕ успешной записи. Падение между
записью и коммитом даст повтор — все три хранилища снимают дубль при чтении.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

try:
    from plugins.tfs_utils import (  # type: ignore
        get_config, add_note, parse_receipt, ensure_pools, save_receipts,
    )
except ImportError:
    from CI06932748.tools.tfs_utils import (  # type: ignore
        get_config, add_note, parse_receipt, ensure_pools, save_receipts,
    )

_cfg            = get_config()
DEF_ARGS        = _cfg['DEF_ARGS']
KAFKA_RCV_CONN  = _cfg['KAFKA_RCV_CONN']
KAFKA_RCV_TOPICS = _cfg['KAFKA_RCV_TOPICS']
RCV_POOL        = _cfg['TFS_RCV_POOL']

logger = logging.getLogger("airflow.task")

# Сколько ждать очередное сообщение, прежде чем считать топики вычерпанными (сек).
IDLE_TIMEOUT = 15
# Потолок сообщений за ран: страховка от бесконечного цикла на большом отставании.
MAX_MESSAGES = 5000



@dag(
    dag_id="tfs_kafka_rcv",
    description="📨 Приём квитанций ТФС: Kafka → export.tfs_receipts",
    default_args=DEF_ARGS,
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    schedule_interval="*/1 * * * *",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=5),
    tags=["DataLab", "CI02420667", "TFS", "kafka"],
    is_paused_upon_creation=False,
    doc_md=__doc__,
)
def tfs_kafka_rcv_dag():

    @task(task_id="receive", pool=RCV_POOL)
    def receive(**context):
        """📥 Вычитывает квитанции из всех топиков и складывает в хранилище тракта.

        Порядок важен: сначала запись, только потом коммит offset. При обратном порядке
        падение между операциями потеряло бы квитанцию навсегда — а её ждёт выгрузка.

        Заодно заводит пулы тракта: приёмник ходит раз в минуту и сам сидит в default_pool,
        поэтому создаст tfs_send раньше, чем он понадобится отправителю.
        """
        import time

        from airflow.hooks.base import BaseHook
        from confluent_kafka import Consumer, TopicPartition

        ensure_pools()

        conn = BaseHook.get_connection(KAFKA_RCV_CONN)
        config = dict(conn.extra_dejson)  # extra_dejson = librdkafka config (контракт Kafka-провайдера)
        config.setdefault("bootstrap.servers", conn.host)
        config["enable.auto.commit"] = False

        consumer = Consumer(config)
        rows: list[dict] = []
        # Максимальный offset по каждой паре (топик, партиция). Коммитить по последнему
        # сообщению нельзя: оно относится к ОДНОЙ партиции, и остальные остались бы
        # незакоммиченными — их бы перечитывало каждый ран.
        positions: dict[tuple[str, int], int] = {}

        try:
            consumer.subscribe(list(KAFKA_RCV_TOPICS))
            logger.info("👂 Слушаем топики: %s", ", ".join(KAFKA_RCV_TOPICS))

            idle_until = time.time() + IDLE_TIMEOUT
            while time.time() < idle_until and len(rows) < MAX_MESSAGES:
                msg = consumer.poll(timeout=5)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning("⚠️ Kafka: %s", msg.error())
                    continue

                raw = msg.value().decode("utf-8", errors="replace")
                row = parse_receipt(raw, msg.partition(), msg.offset())
                row['kafka_topic'] = msg.topic()
                rows.append(row)

                key = (msg.topic(), msg.partition())
                positions[key] = max(positions.get(key, -1), msg.offset())
                idle_until = time.time() + IDLE_TIMEOUT  # пока идут сообщения — продолжаем

            if rows:
                save_receipts(rows)
                # Только теперь: квитанции уже в хранилище, повтор при падении не страшен.
                # offset + 1 — семантика Kafka: коммитим позицию СЛЕДУЮЩЕГО сообщения.
                consumer.commit(
                    offsets=[TopicPartition(t, p, o + 1) for (t, p), o in positions.items()],
                    asynchronous=False,
                )
        finally:
            consumer.close()

        if not rows:
            logger.info("📭 Новых квитанций нет")
            return 0

        failed  = [r for r in rows if r['status_code'] > 0]
        unknown = [r for r in rows if r['status_code'] < 0]

        logger.info("📨 Квитанций: %d из %d топиков, с ошибкой передачи: %d, неразобранных: %d",
                    len(rows), len({r['kafka_topic'] for r in rows}), len(failed), len(unknown))

        note: dict = {}
        if failed:
            note[f"❌ StatusCode != 0 ({len(failed)})"] = [
                f"{r['file_name']}: код {r['status_code']}" for r in failed
            ]
        if unknown:
            note[f"⚠️ Не разобрано ({len(unknown)})"] = [r['raw_xml'][:200] for r in unknown]
        note[f"📨 Получено квитанций: {len(rows)}"] = [
            f"{r['kafka_topic']} · {r['file_name']} → {r['status_code']}" for r in rows[:20]
        ]
        add_note(note, level='task,dag', context=context, title='📨 tfs_kafka_rcv')

        # Ненулевой StatusCode — не проблема приёмника: его увидит и покажет та выгрузка,
        # которая ждёт эту квитанцию. Здесь он только логируется.
        return len(rows)

    receive()


tfs_kafka_rcv_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
