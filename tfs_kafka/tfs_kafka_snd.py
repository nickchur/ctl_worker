"""🚚 DAG отправки файлов в ТФС с соблюдением темпа маршрута.
*2026-08-12 13:20 MSK · v1.1 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Единственное место, откуда файлы ЕР уходят уведомлением в Kafka. Пакетные даги только
ставят файлы в очередь (`export.er_sent_files`, `notified_at = 0`), а разгребает её этот
даг — раз в минуту, в темпе, который декларирует ТФС.

📊 **Зачем централизованно.** ТФС отбивает лишние файлы, а лимиты заданы на маршрут:
файлов в секунду, минуту, час и сутки (см. `TFS_LIMITS` в `tfs_config.py`). Считать их
можно только там, где видно все отправки сразу. Прежняя схема — `sleep 1` внутри
`produce_msg` плюс 1-слотовый пул `tfs_{scenario}` — держала темп лишь внутри пакета:
пул сериализовал соседние `notify_tfs`, но не разводил их по времени, а минутного,
часового и суточного бюджетов не было вовсе.

Троттлинг `rate_limit()` из `plugins/ctl_utils.py` тут не годится: он живёт в памяти
процесса, а отправка шла из разных тасков на разных воркерах.

📦 **Пакет уезжает целиком.** Очередь разбирается по `package_ts`: пока не ушли все файлы
одного пакета, к следующему не переходим. Так выполняется требование ЕР «не передаётся
несколько пакетов одновременно» — раньше его держали пулы `tfs_{scenario}`.

🔒 **Пул `tfs_send` на 1 слот** остался, но с другой ролью: он больше не разводит пакеты
(это делает сама очередь), а страхует от дагов, которые шлют в ТФС **мимо** очереди.
Такой даг обязан брать тот же пул. Учёт лимитов он всё равно сломает: его файлы не
попадают в `er_sent_files` и в счётчики, поэтому правильный путь для нового отправителя —
не пул, а та же очередь.

⏳ **Бюджет кончился** — файлы остаются в очереди и уедут, когда окно откроется. Если
самая старая строка ждёт дольше `TFS_QUEUE_ALERT_MIN`, таск в конце падает: затор должен
быть виден в мониторинге. Всё, что влезло в бюджет, к этому моменту уже отправлено.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

try:
    from CI06932748.analytics.datalab.tfs_kafka.tfs_config import (  # type: ignore
        get_config, get_dict_from_ch, add_note, tfs_limits, send_budget,
    )
except ImportError:
    from tfs_kafka.tfs_config import get_config, get_dict_from_ch, add_note, tfs_limits, send_budget

_cfg             = get_config()
CH_ID            = _cfg['CH_ID']
DEF_ARGS         = _cfg['DEF_ARGS']
KAFKA_SND_CONN   = _cfg['KAFKA_SND_CONN']
KAFKA_SND_TOPIC  = _cfg['KAFKA_SND_TOPIC']
SENT_FILES_TABLE = _cfg['SENT_FILES_TABLE']
QUEUE_ALERT_MIN  = _cfg['TFS_QUEUE_ALERT_MIN']
SEND_POOL        = _cfg['TFS_SEND_POOL']

logger = logging.getLogger("airflow.task")

# Ран ограничен по времени: следующий стартует через минуту и продолжит с того же места.
# Меньше минуты, чтобы раны не накладывались даже при max_active_runs=1.
RUN_BUDGET_SEC = 50

# 🕐 Окно суточного лимита — СКОЛЬЗЯЩЕЕ (подтверждено ТФС), как и все остальные.
# Не календарные сутки: полночь бюджет не обнуляет, освобождается он постепенно, по мере
# того как отправки уходят за границу окна.
DAY_WINDOW_SQL = "notified_at > now64(3) - INTERVAL 1 DAY"


def _q(s: str) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return str(s).replace("'", "''")


def build_message(scenario_id: str, rq_uid: str, file_name: str) -> str:
    """Собирает XML-уведомление TransferFileCephRq с ГОТОВЫМ RqUID.

    RqUID приходит из очереди, а не генерируется здесь: он записан в er_sent_files
    ещё при постановке в очередь, и именно по нему потом ищется обратная квитанция.
    """
    # isoformat(ms) воспроизводит формат pendulum 'YYYY-MM-DDTHH:mm:ss.SSSZ' (смещение с двоеточием)
    rq_tm = datetime.now().astimezone().isoformat(timespec='milliseconds')
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{rq_uid}</RqUID>
    <RqTm>{rq_tm}</RqTm>
    <ScenarioInfo><ScenarioId>{scenario_id}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{file_name}</Name></FileInfo></File>
</TransferFileCephRq>"""


def order_queue(rows: list[dict]) -> list[dict]:
    """📦 Упорядочивает очередь так, чтобы файлы одного пакета шли подряд.

    Пакеты — по времени появления (package_ts, затем created_at первого файла), внутри
    пакета — по created_at. Разрывать пакет чужими файлами нельзя: ЕР не принимает
    несколько пакетов одновременно.
    """
    packages: dict = {}
    for row in rows:
        packages.setdefault((row['replica'], row['package_ts']), []).append(row)

    ordered = []
    for key in sorted(packages, key=lambda k: (k[1], min(r['created_at'] for r in packages[k]))):
        ordered.extend(sorted(packages[key], key=lambda r: r['created_at']))
    return ordered


@dag(
    dag_id="tfs_kafka_snd",
    description="🚚 Очередь отправки в ТФС: соблюдение лимитов маршрута",
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
def tfs_kafka_snd_dag():

    @task(task_id="send", pool=SEND_POOL)
    def send(**context):
        """🚚 Отправляет из очереди столько файлов, сколько позволяет бюджет маршрута.

        Пул `tfs_send` на 1 слот удерживается весь ран (до RUN_BUDGET_SEC): пока очередь
        передаёт, никто другой в ТФС не пишет. Даг, шлющий мимо очереди, обязан брать тот
        же пул — тогда он подождёт не дольше одного рана. Лимиты маршрута пул при этом
        НЕ соблюдает: файлы мимо очереди не попадают в SENT_FILES_TABLE и в счётчики.
        """
        import time

        from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

        # FINAL обязателен: отправленная строка дописывается второй версией, и без
        # схлопывания уже ушедший файл уехал бы повторно.
        pending = get_dict_from_ch(hook, f"""
            SELECT rq_uid, file_name, replica, scenario_id, package_ts, created_at
            FROM {SENT_FILES_TABLE} FINAL
            WHERE notified_at = toDateTime64(0, 3)
            ORDER BY package_ts, created_at
        """)

        if not pending:
            logger.info("📭 Очередь пуста")
            return 0

        queue = order_queue(pending)
        logger.info("📦 В очереди %d файлов, пакетов: %d",
                    len(queue), len({(r['replica'], r['package_ts']) for r in queue}))

        producer = KafkaProducerHook(kafka_config_id=KAFKA_SND_CONN).get_producer()
        deadline = time.time() + RUN_BUDGET_SEC
        sent, blocked, last_send = [], {}, {}

        for row in queue:
            if time.time() >= deadline:
                logger.info("⏱️ Время рана вышло, остальное уедет следующим раном")
                break

            scenario = row['scenario_id']
            if scenario in blocked:
                continue  # маршрут упёрся в лимит, остальные его файлы ждут

            limits = tfs_limits(scenario)
            counts = get_dict_from_ch(hook, f"""
                SELECT
                    countIf(notified_at > now64(3) - INTERVAL 1 SECOND) AS sec,
                    countIf(notified_at > now64(3) - INTERVAL 1 MINUTE) AS min,
                    countIf(notified_at > now64(3) - INTERVAL 1 HOUR)   AS hour,
                    countIf({DAY_WINDOW_SQL})                           AS day
                FROM {SENT_FILES_TABLE} FINAL
                WHERE scenario_id = '{_q(scenario)}' AND notified_at > toDateTime64(0, 3)
            """)[0]

            allowed, hit = send_budget(counts, limits)
            if not allowed:
                blocked[scenario] = hit
                logger.warning("🚦 %s: упёрлись в лимит '%s' (%s), файлы ждут",
                               scenario, hit, limits[hit])
                continue

            # Секундный лимит выдерживаем паузой: это единственное окно, которое
            # закрывается достаточно быстро, чтобы его имело смысл переждать в раме.
            gap = 1.0 / limits['sec']
            since = time.time() - last_send.get(scenario, 0)
            if since < gap:
                time.sleep(gap - since)

            producer.produce(KAFKA_SND_TOPIC, value=build_message(scenario, row['rq_uid'], row['file_name']))
            producer.flush()
            last_send[scenario] = time.time()

            # Дописываем строку с отметкой отправки — только после подтверждения доставки
            hook.execute(f"""
                INSERT INTO {SENT_FILES_TABLE}
                    (rq_uid, file_name, replica, scenario_id, package_ts, created_at, notified_at)
                SELECT rq_uid, file_name, replica, scenario_id, package_ts, created_at, now64(3)
                FROM {SENT_FILES_TABLE} FINAL
                WHERE rq_uid = '{_q(row['rq_uid'])}'
            """)
            sent.append(row)
            logger.info("📤 %s → %s (RqUID %s)", row['file_name'], scenario, row['rq_uid'])

        left = [r for r in queue if r not in sent]
        note: dict = {}
        if sent:
            note[f"📤 Отправлено ({len(sent)})"] = [r['file_name'] for r in sent[:20]]
        if blocked:
            note[f"🚦 Лимит исчерпан ({len(blocked)})"] = [
                f"{s}: {w} ({tfs_limits(s)[w]})" for s, w in blocked.items()
            ]
        if left:
            note[f"⏳ Осталось в очереди: {len(left)}"] = [r['file_name'] for r in left[:20]]
        if note:
            add_note(note, level='task,dag', context=context, title='🚚 tfs_kafka_snd')

        # Затор виден только тогда, когда о нём кто-то кричит. Падаем в самом конце:
        # всё, что влезло в бюджет, к этому моменту уже отправлено.
        if left:
            oldest = min(r['created_at'] for r in left)
            waiting = (datetime.now(timezone.utc) - oldest.replace(tzinfo=timezone.utc)).total_seconds() / 60
            if waiting > QUEUE_ALERT_MIN:
                raise AirflowFailException(
                    f"🚦 Очередь отправки ТФС стоит {waiting:.0f} мин при пороге {QUEUE_ALERT_MIN}. "
                    f"Ждут {len(left)} файлов, упёршиеся лимиты: {blocked or 'нет'}. "
                    f"Самый старый файл в очереди с {oldest}"
                )

        return len(sent)

    send()


tfs_kafka_snd_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
