"""📨 DAG сбора обратных квитанций ТФС из Kafka в ClickHouse.
*2026-08-12 11:30 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Обратная квитанция `TransferFileCephRs` приходит по ВСЕМ маршрутам ТФС (xStream и ЕР)
в один топик `TFS.HRPLT.OUT` и сопоставляется с отправкой по `RqUID`. Результат передачи —
в `Status/StatusCode`, где `0` означает успех.

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

⚠️ Этот даг — ЕДИНСТВЕННЫЙ потребитель топика. Топик общий на все маршруты, и кто первым
вычитал сообщение, тот его и забрал: читать `TFS.HRPLT.OUT` откуда-то ещё значит воровать
чужие квитанции. Выгрузки ничего из Kafka не читают — они ждут появления своей строки
в `export.tfs_receipts` по своему `RqUID`.

Единственный конкурент, который остался, — `tools_test_kafka_rcv` в режиме `wait`
(`ctl/check/test_kafka.py`). Его на этом топике запускать нельзя.

⏱️ Расписание: раз в минуту, `max_active_runs=1`. Ран короткий: опрашивает, пока идут
сообщения, и выходит по тишине либо по потолку сообщений.

🔁 Доставка at-least-once: offset коммитится ПОСЛЕ успешной вставки в ClickHouse.
Падение между вставкой и коммитом даст повтор, его схлопнет ReplacingMergeTree.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task

try:
    from CI06932748.analytics.datalab.export_er.er_config import (  # type: ignore
        get_config, add_note, parse_receipt,
    )
except ImportError:
    from er_export.er_config import get_config, add_note, parse_receipt

_cfg           = get_config()
CH_ID          = _cfg['CH_ID']
DEF_ARGS       = _cfg['DEF_ARGS']
KAFKA_RCV_CONN = _cfg['KAFKA_RCV_CONN']
KAFKA_RCV_TOPIC = _cfg['KAFKA_RCV_TOPIC']
RECEIPTS_TABLE = _cfg['RECEIPTS_TABLE']

logger = logging.getLogger("airflow.task")

# Пул сборщика — не экспортный: чтение квитанций не должно занимать слоты выгрузок.
SYNC_POOL = "default_pool"

# Сколько ждать очередное сообщение, прежде чем считать топик вычерпанным (сек).
IDLE_TIMEOUT = 15
# Потолок сообщений за ран: страховка от бесконечного цикла на большом отставании.
MAX_MESSAGES = 5000


def _q(s: str) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return str(s).replace("'", "''")


def _values(row: dict) -> str:
    """Строит VALUES-кортеж одной квитанции для INSERT."""
    rq_tm = f"toDateTime64('{row['rq_tm'].strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}', 3)" \
        if row['rq_tm'] else "NULL"
    return (
        f"('{_q(row['rq_uid'])}', '{_q(row['file_name'])}', '{_q(row['scenario_id'])}', "
        f"{int(row['status_code'])}, {rq_tm}, '{_q(row['raw_xml'])}', "
        f"{int(row['kafka_partition'])}, {int(row['kafka_offset'])})"
    )


@dag(
    dag_id="tfs_receipts_sync",
    description="📨 Обратные квитанции ТФС: Kafka TFS.HRPLT.OUT → export.tfs_receipts",
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
def tfs_receipts_dag():

    @task(task_id="collect", pool=SYNC_POOL)
    def collect(**context):
        """📥 Вычитывает квитанции из топика и складывает в ClickHouse.

        Порядок важен: сначала вставка, только потом коммит offset. При обратном порядке
        падение между операциями потеряло бы квитанцию навсегда — а её ждёт выгрузка.
        """
        import time

        from airflow.hooks.base import BaseHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
        from confluent_kafka import Consumer

        conn = BaseHook.get_connection(KAFKA_RCV_CONN)
        config = dict(conn.extra_dejson)  # extra_dejson = librdkafka config (контракт Kafka-провайдера)
        config.setdefault("bootstrap.servers", conn.host)
        config["enable.auto.commit"] = False

        consumer = Consumer(config)
        rows: list[dict] = []
        last_msg = None

        try:
            consumer.subscribe([KAFKA_RCV_TOPIC])
            idle_until = time.time() + IDLE_TIMEOUT
            while time.time() < idle_until and len(rows) < MAX_MESSAGES:
                msg = consumer.poll(timeout=5)
                if msg is None:
                    continue
                if msg.error():
                    logger.warning("⚠️ Kafka: %s", msg.error())
                    continue

                raw = msg.value().decode("utf-8", errors="replace")
                rows.append(parse_receipt(raw, msg.partition(), msg.offset()))
                last_msg = msg
                idle_until = time.time() + IDLE_TIMEOUT  # пока идут сообщения — продолжаем

            if rows:
                hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
                hook.execute(
                    f"INSERT INTO {RECEIPTS_TABLE} "
                    "(rq_uid, file_name, scenario_id, status_code, rq_tm, raw_xml, kafka_partition, kafka_offset) "
                    "VALUES " + ", ".join(_values(r) for r in rows)
                )
                # Только теперь: квитанции уже в ClickHouse, повтор при падении не страшен
                consumer.commit(message=last_msg, asynchronous=False)
        finally:
            consumer.close()

        if not rows:
            logger.info("📭 Новых квитанций нет")
            return 0

        failed  = [r for r in rows if r['status_code'] > 0]
        unknown = [r for r in rows if r['status_code'] < 0]

        logger.info("📨 Квитанций: %d, с ошибкой передачи: %d, неразобранных: %d",
                    len(rows), len(failed), len(unknown))

        note: dict = {}
        if failed:
            note[f"❌ StatusCode != 0 ({len(failed)})"] = [
                f"{r['file_name']}: код {r['status_code']}" for r in failed
            ]
        if unknown:
            note[f"⚠️ Не разобрано ({len(unknown)})"] = [r['raw_xml'][:200] for r in unknown]
        note[f"📨 Получено квитанций: {len(rows)}"] = [
            f"{r['file_name']} → {r['status_code']}" for r in rows[:20]
        ]
        add_note(note, level='task,dag', context=context, title='📨 tfs_receipts')

        # Ненулевой StatusCode — не проблема сборщика: его увидит и покажет та выгрузка,
        # которая ждёт эту квитанцию. Здесь он только логируется.
        return len(rows)

    collect()


tfs_receipts_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
