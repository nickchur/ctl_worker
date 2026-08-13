"""🚚 DAG отправки файлов в ТФС с соблюдением темпа маршрута.
*2026-08-13 13:33 MSK · v2.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Единственное место, откуда файлы ЕР уходят уведомлением в Kafka. Пакетные даги только
ставят файлы в очередь, а разгребает её этот даг — в темпе, который декларирует ТФС.

📊 **Зачем централизованно.** ТФС отбивает лишние файлы, а лимиты заданы на маршрут:
файлов в секунду, минуту, час и сутки (см. `TFS_LIMITS` в `plugins/tfs_utils.py`).
Считать их можно только там, где видно все отправки сразу. Прежняя схема — `sleep 1`
внутри `produce_msg` плюс 1-слотовый пул `tfs_{scenario}` — держала темп лишь внутри
пакета: пул сериализовал соседние `notify_tfs`, но не разводил их по времени, а
минутного, часового и суточного бюджетов не было вовсе.

Троттлинг `rate_limit()` из `plugins/ctl_utils.py` тут не годится: он живёт в памяти
процесса, а отправка шла из разных тасков на разных воркерах.

⏱️ **Ран живёт час.** Сенсор разгребает очередь раз в 15 секунд в режиме `reschedule`
(между опросами слот свободен) и держит окно до конца. В конце окна:

* ушёл хоть один файл → таск **зелёный**, в XCom их число за окно;
* очередь весь час была пуста → таск **скипнут** (`soft_fail`), стартует следующий ран.

📦 **Пакет уезжает целиком.** Очередь разбирается по `package_ts`: пока не ушли все файлы
одного пакета, к следующему не переходим. Так выполняется требование ЕР «не передаётся
несколько пакетов одновременно» — раньше его держали пулы `tfs_{scenario}`.

🔒 **Пул `tfs_send` на 1 слот** остался, но с другой ролью: он больше не разводит пакеты
(это делает сама очередь), а страхует от дагов, которые шлют в ТФС **мимо** очереди.
Такой даг обязан брать тот же пул. В режиме `reschedule` слот занят только на время
опроса, а не весь час: вхолостую очередь его больше не держит. Учёт лимитов пул всё
равно не спасает — файлы мимо очереди не попадают в счётчики, поэтому правильный путь
для нового отправителя не пул, а та же очередь.

📮 **Ручная досылка** через параметры запуска не обходит очередь, а наполняет её
(`enqueue_files` в `plugins/tfs_utils.py`): так досланный руками файл попадает в учёт
лимитов и потом находится по RqUID вместе с остальными. Параметры разбираются ровно
один раз за ран — на первом опросе, иначе за час окна те же файлы уехали бы в очередь
двести сорок раз, каждый с новым RqUID.

⏳ **Бюджет кончился** — файлы остаются в очереди и уедут, когда окно откроется. Если
в конце окна самая старая строка ждёт дольше `TFS_QUEUE_ALERT_MIN`, таск падает: затор
должен быть виден в мониторинге. Всё, что влезло в бюджет, к этому моменту уже отправлено.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models import Param
from airflow.sensors.base import PokeReturnValue  # type: ignore

try:
    from plugins.tfs_utils import (  # type: ignore
        get_config, add_note, tfs_limits, send_budget, build_message, window_hits, WINDOWS,
        enqueue_files, order_queue, pending, mark_sent, sent_counts,
        run_state_get, run_state_set,
    )
except ImportError:
    from CI06932748.tools.tfs_utils import (  # type: ignore
        get_config, add_note, tfs_limits, send_budget, build_message, window_hits, WINDOWS,
        enqueue_files, order_queue, pending, mark_sent, sent_counts,
        run_state_get, run_state_set,
    )

_cfg             = get_config()
DEF_ARGS         = _cfg['DEF_ARGS']
KAFKA_SND_CONN   = _cfg['KAFKA_SND_CONN']
KAFKA_SND_TOPIC  = _cfg['KAFKA_SND_TOPIC']
QUEUE_ALERT_MIN  = _cfg['TFS_QUEUE_ALERT_MIN']
SEND_POOL        = _cfg['TFS_SEND_POOL']

logger = logging.getLogger("airflow.task")

# Сколько живёт ран, сек. Ровно столько сенсор держит окно, после чего зеленеет или скипается.
WINDOW = 60 * 60
# Пауза между опросами. В reschedule на это время слот пула освобождается.
POKE_EVERY = 15
# Бюджет одного опроса, сек: меньше шага цикла, чтобы опросы не наезжали друг на друга.
POKE_BUDGET_SEC = 10
# Ключи состояния окна. Между опросами процесс умирает, память не годится, а обычный
# xcom_push стирается перед каждым опросом — держим через run_state_* (см. tfs_utils).
SENT_KEY   = 'sent'
PARAMS_KEY = 'params_done'


@dag(
    dag_id="tfs_kafka_snd",
    description="🚚 Очередь отправки в ТФС: соблюдение лимитов маршрута",
    default_args=DEF_ARGS,
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    # Ран живёт час, следующий ждёт в queued из-за max_active_runs=1 и стартует сразу,
    # как текущий закончился: разрыв равен задержке шедулера.
    schedule_interval="@hourly",
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=70),
    tags=["DataLab", "CI02420667", "TFS", "kafka"],
    is_paused_upon_creation=False,
    doc_md=__doc__,
    params={
        'files': Param(
            [], type='array', title='Файлы',
            description='Ручная досылка: имена файлов, уже лежащих в S3. Пусто — только очередь.',
        ),
        'scenario': Param(
            None, type=['string', 'null'], title='ScenarioId',
            description='Маршрут ТФС для файлов из «Файлы». Обязателен, если список непустой.',
        ),
        'replica': Param(
            None, type=['string', 'null'], title='Реплика',
            description='Имя пакета для файлов из «Файлы». Пусто — берётся ScenarioId.',
        ),
    },
)
def tfs_kafka_snd_dag():

    def _enqueue_params_once(context) -> None:
        """📮 Разбирает параметры запуска и ставит файлы в очередь — один раз за ран.

        Признак «уже разобрали» лежит в XCom: сенсор опрашивает очередь каждые 15 секунд,
        а enqueue_files каждый раз заводит новые RqUID, так что второй проход размножил бы
        досылку. Ошибка настройки (файлы без маршрута) не должна ронять весь ран отправки —
        она в лог и в заметку, очередь при этом разгребается дальше.
        """
        if run_state_get(context, PARAMS_KEY):
            return

        run_state_set(context, PARAMS_KEY, 1)

        p = context.get('params') or {}
        files = p.get('files') or []
        if not files:
            return

        dag_run = context.get('dag_run')
        try:
            rows = enqueue_files(
                files,
                scenario_id=p.get('scenario') or '',
                replica=p.get('replica') or '',
                dag_id='tfs_kafka_snd',
                run_id=getattr(dag_run, 'run_id', '') or '',
            )
        except ValueError as exc:
            logger.error("❌ Досылка не поставлена в очередь: %s", exc)
            add_note({"❌ Ручная досылка": str(exc)}, level='dag', context=context,
                     title='🚚 tfs_kafka_snd')
            return

        logger.info("📮 Из параметров запуска в очередь добавлено файлов: %d", len(rows))

    def poke_queue(**context) -> PokeReturnValue:
        """🚚 Отправляет из очереди столько файлов, сколько позволяет бюджет маршрута.

        Пул `tfs_send` на 1 слот удерживается на время опроса: пока очередь передаёт,
        никто другой в ТФС не пишет. Даг, шлющий мимо очереди, обязан брать тот же пул.

        ⚠️ Здесь нельзя кидать AirflowFailException: при soft_fail сенсор превращает его
        в скип (airflow/sensors/base.py), и затор о себе не заявит. Для этого AirflowException.
        """
        import time

        from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

        _enqueue_params_once(context)

        total = int(run_state_get(context, SENT_KEY) or 0)
        # Окно считаем от старта рана: оно переживает reschedule, в отличие от таймеров опроса.
        elapsed = (datetime.now(timezone.utc) - context['dag_run'].start_date).total_seconds()
        last_poke = elapsed >= WINDOW

        queued = pending()
        if not queued:
            logger.info("📭 Очередь пуста")
            return _finish(context, total, left=[], blocked={}, last_poke=last_poke)

        queue = order_queue(queued)
        logger.info("📦 В очереди %d файлов, пакетов: %d",
                    len(queue), len({(r['replica'], r['package_ts']) for r in queue}))

        producer = KafkaProducerHook(kafka_config_id=KAFKA_SND_CONN).get_producer()
        deadline = time.time() + POKE_BUDGET_SEC
        sent, blocked = [], {}
        # Расход лимитов: снимок из хранилища на начало опроса + времена собственных
        # отправок. Хранилище опрашивается ОДИН раз на маршрут за опрос, а не на каждый
        # файл: на S3-раскладке это листинг префикса, и в цикле он умножался на длину
        # очереди. Свои отправки стареют сами — окна считает тот же window_hits.
        snapshot: dict[str, dict] = {}
        mine: dict[str, list] = {}

        for row in queue:
            if time.time() >= deadline:
                logger.info("⏱️ Бюджет опроса вышел, остальное уедет следующим")
                break

            scenario = row['scenario_id']
            if scenario in blocked:
                continue  # маршрут упёрся в лимит, остальные его файлы ждут

            limits = tfs_limits(scenario)
            if scenario not in snapshot:
                snapshot[scenario] = sent_counts(scenario)
                mine[scenario] = []

            now_ = datetime.now(timezone.utc)
            # Через .get, а не копией: send_budget разрешает бэкенду вернуть неполный
            # словарь окон, и наши собственные отправки прибавляются к любому из них.
            counts = {w: snapshot[scenario].get(w, 0) for w in WINDOWS}
            for ts in mine[scenario]:
                for window in window_hits(ts, now_):
                    counts[window] += 1

            allowed, hit = send_budget(counts, limits)
            if not allowed:
                blocked[scenario] = hit
                logger.warning("🚦 %s: упёрлись в лимит '%s' (%s), файлы ждут",
                               scenario, hit, limits[hit])
                continue

            # Секундный лимит выдерживаем паузой: это единственное окно, которое
            # закрывается достаточно быстро, чтобы его имело смысл переждать в опросе.
            gap = 1.0 / limits['sec']
            if mine[scenario]:
                since = (datetime.now(timezone.utc) - mine[scenario][-1]).total_seconds()
                if since < gap:
                    time.sleep(gap - since)

            producer.produce(KAFKA_SND_TOPIC, value=build_message(scenario, row['rq_uid'], row['file_name']))
            producer.flush()

            # Отметка отправки — только после подтверждения доставки
            mark_sent(row['rq_uid'])
            mine[scenario].append(datetime.now(timezone.utc))
            sent.append(row)
            logger.info("📤 %s → %s (RqUID %s)", row['file_name'], scenario, row['rq_uid'])

        left = [r for r in queue if r not in sent]

        if sent:
            total += len(sent)
            run_state_set(context, SENT_KEY, total)
            # Заметка короткая намеренно: за час опросов их накопится много, а add_note
            # склеивает записи и режет всё вместе по MAX_NOTE_LEN.
            line = f"📤 +{len(sent)} (за окно {total})"
            if blocked:
                line += f", 🚦 лимит: {', '.join(blocked)}"
            if left:
                line += f", ⏳ в очереди {len(left)}"
            add_note({"🚚 Отправка": line}, level='dag', context=context, title='🚚 tfs_kafka_snd')

        return _finish(context, total, left, blocked, last_poke)

    def _finish(context, total: int, left: list, blocked: dict, last_poke: bool) -> PokeReturnValue:
        """Итог опроса: ждать дальше, зеленеть или заявить о заторе.

        Затор проверяем ТОЛЬКО на последнем опросе окна: сработай он в середине —
        оборвал бы отправку тех файлов, которые ещё успели бы уехать в этом же окне.
        """
        if not last_poke:
            return PokeReturnValue(is_done=False)

        if left:
            oldest = min(r['created_at'] for r in left)
            waiting = (datetime.now(timezone.utc) - oldest.replace(tzinfo=timezone.utc)).total_seconds() / 60
            if waiting > QUEUE_ALERT_MIN:
                raise AirflowException(
                    f"🚦 Очередь отправки ТФС стоит {waiting:.0f} мин при пороге {QUEUE_ALERT_MIN}. "
                    f"Ждут {len(left)} файлов, упёршиеся лимиты: {blocked or 'нет'}. "
                    f"Самый старый файл в очереди с {oldest}"
                )

        if total:
            logger.info("✅ Окно закрыто, отправлено за час: %d", total)
            return PokeReturnValue(is_done=True, xcom_value=total)

        # Ничего не ушло за весь час — сенсор упрётся в timeout и пометит таск скипнутым.
        return PokeReturnValue(is_done=False)

    # timeout заведомо больше окна: сенсор проверяет его ТОЛЬКО после ложного ответа,
    # поэтому зелёный по концу окна успевает сработать первым, а скип по таймауту
    # достаётся ровно случаю «за час отправлять было нечего».
    task.sensor(
        task_id="send",
        mode='reschedule',
        poke_interval=POKE_EVERY,
        timeout=WINDOW + 2 * POKE_EVERY,
        soft_fail=True,
        pool=SEND_POOL,
        doc_md="Разбор очереди отправки в ТФС в темпе маршрута",
    )(poke_queue)()


tfs_kafka_snd_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
