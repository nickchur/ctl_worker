"""🚚 DAG отправки файлов в ТФС с соблюдением темпа маршрута.
*2026-08-28 15:40 MSK · v2.7 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Единственное место, откуда файлы ЕР уходят уведомлением в Kafka. Пакетные даги только
ставят файлы в очередь, а разгребает её этот даг — в темпе, который декларирует ТФС.

📊 **Зачем централизованно.** ТФС отбивает лишние файлы, а лимиты заданы на маршрут:
файлов в секунду, минуту, час и сутки (см. `TFS_ROUTES` в `plugins/tfs_utils.py`).
Считать их можно только там, где видно все отправки сразу. Прежняя схема — `sleep 1`
внутри `produce_msg` плюс 1-слотовый пул `tfs_{scenario}` — держала темп лишь внутри
пакета: пул сериализовал соседние `notify_tfs`, но не разводил их по времени, а
минутного, часового и суточного бюджетов не было вовсе.

Троттлинг `rate_limit()` из `plugins/ctl_utils.py` тут не годится: он живёт в памяти
процесса, а отправка шла из разных тасков на разных воркерах.

⏱️ **Ран живёт час.** Сенсор разгребает очередь раз в 15 секунд в режиме `reschedule`
(между опросами слот свободен) и держит окно до конца. В конце окна:

* ушёл хоть один файл → таск **зелёный**, отправленное в XCom записью на пакет,
  итоги окна по пакетам — в `return_value`;
* очередь весь час была пуста → таск **скипнут** (`soft_fail`), стартует следующий ран.

📦 **Пакет уезжает целиком.** Очередь разбирается по `package_ts`: пока не ушли все файлы
одного пакета, к следующему не переходим. Так выполняется требование ЕР «не передаётся
несколько пакетов одновременно» — раньше его держали пулы `tfs_{scenario}`.

🧭 **Сценарий — свой таск.** На каждый сценарий заводится сенсор `snd_<ScenarioId>`,
который везёт только свои файлы. Гранулярность не случайна: лимиты ТФС заданы на маршрут,
и пул взаимного исключения на контуре тоже заведён на маршрут. Топик берётся из
`TFS_ROUTES` (`plugins/tfs_utils.py`), а чего там нет — по умолчанию: топик контура
и `TFS_LIMITS_DEFAULT`, так что новый маршрут работает без правки кода. Пакет не рвётся —
`(replica, package_ts)` целиком принадлежит одному сценарию, то есть одному таску.

📇 **Состав тасков — из реестра.** Список сценариев руками не ведут: таск `scan_queue`
читает очередь и дописывает найденные сценарии в Variable `tfs_snd_scenarios`, а парсинг
DAG-файла разворачивает `TFS_ROUTES ∪ реестр` в таски. Новый сценарий получает свой таск
на следующем разборе файла (шедулер добавляет его и в уже идущий ран), поэтому файл
уезжает в пределах того же часа.

🗑️ **Выключатель — пул.** Удалили `tfs_<ScenarioId>` руками — `scan_queue` уберёт сценарий
из реестра, и таск исчезнет на следующем разборе файла (в идущем ране Airflow пометит его
`removed`). Сценарий из `TFS_ROUTES` так не выключить: его пул заводится заново из конфига.
Не выключить и тот, чьи файлы ещё лежат в очереди, — следующий скан заведёт всё обратно,
сначала надо разобрать очередь.

🔒 **Пул `tfs_<ScenarioId>` на 1 слот.** Имя ровно такое, какое уже заведено на контуре
(`tfs_HRPLATFORM-4000`, `tfs_KKA-407010`, …) и какое берёт `xs_export` — в этом и смысл:
пул страхует от дагов, которые шлют по тому же маршруту **мимо** очереди. Пул с другим
именем не пересекался бы с ними и не давал бы ничего. В режиме `reschedule` слот занят
только на время опроса, а не весь час. Учёт лимитов пул всё равно не спасает — файлы мимо
очереди не попадают в счётчики, поэтому правильный путь для нового отправителя не пул,
а та же очередь.

📮 **Ручная досылка** через параметры запуска не обходит очередь, а наполняет её
(`enqueue_files` в `plugins/tfs_utils.py`): так досланный руками файл попадает в учёт
лимитов и потом находится по RqUID вместе с остальными. Разбирает их `scan_queue`, ровно
один раз за ран, иначе за час окна те же файлы уехали бы в очередь шестьдесят раз, каждый
с новым RqUID. Везёт досланное, как и всё остальное, таск того сценария, который указан
в параметрах.

⏳ **Бюджет кончился** — файлы остаются в очереди и уедут, когда окно откроется. Если
в конце окна самая старая строка ждёт дольше `TFS_QUEUE_ALERT_MIN`, таск падает: затор
должен быть виден в мониторинге. Всё, что влезло в бюджет, к этому моменту уже отправлено.

⏸️ **Пауза** (`Variable tfs_snd_pause`, пульт — даг `tfs_kafka_setup`) держит файлы
маршрута, реплики или одного пакета в очереди: постановка идёт своим чередом, отправка —
нет. Придержанные строки **не считаются затором**: осознанная пауза не должна каждый час
ронять таск по `TFS_QUEUE_ALERT_MIN`. Сколько придержано и почему — в заметке (один раз
за окно, пока причина не изменилась) и в `return_value`.

📭 **Неподтверждённые.** Раз в час `scan_queue` ищет файлы, отправленные больше
`TFS_STALE_MIN` минут назад и оставшиеся без квитанции, и пишет о них предупреждение.
Без этого отказ ТФС по пакету, чей `wait_confirm` ушёл в скип из-за паузы, не увидел бы
никто.
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
        get_config, add_note, ensure_pool, tfs_limits, send_budget, build_message,
        window_hits, WINDOWS, enqueue_files, order_queue, parse_ts, pending, mark_sent,
        drop_unpooled_scenarios, remember_scenarios, route_topic, scenario_pool,
        sent_counts, task_slug, tfs_route, run_state_get, run_state_set,
        package_key, split_pending, stale_sent,
    )
except ImportError:
    from CI06932748.tools.tfs_utils import (  # type: ignore
        get_config, add_note, ensure_pool, tfs_limits, send_budget, build_message,
        window_hits, WINDOWS, enqueue_files, order_queue, parse_ts, pending, mark_sent,
        drop_unpooled_scenarios, remember_scenarios, route_topic, scenario_pool,
        sent_counts, task_slug, tfs_route, run_state_get, run_state_set,
        package_key, split_pending, stale_sent,
    )

_cfg             = get_config()
DEF_ARGS         = _cfg['DEF_ARGS']
KAFKA_SND_CONN   = _cfg['KAFKA_SND_CONN']
QUEUE_ALERT_MIN  = _cfg['TFS_QUEUE_ALERT_MIN']
SEND_SLOTS       = _cfg['TFS_SEND_SLOTS']
# Сценарии читаются НА ПАРСИНГЕ: из TFS_ROUTES и реестра-Variable, который наполняет
# таск scan_queue по очереди отправки. Список задаёт состав тасков дага.
SCENARIOS        = _cfg['SCENARIOS']
PAUSE_VAR        = _cfg['PAUSE_VAR']
STALE_MIN        = _cfg['TFS_STALE_MIN']

logger = logging.getLogger("airflow.task")

# Сколько живёт ран, сек. Ровно столько сенсор держит окно, после чего зеленеет или скипается.
WINDOW = 60 * 60
# Пауза между опросами. В reschedule на это время слот пула освобождается.
POKE_EVERY = 15
# Шаг сканирования очереди на новые сценарии: реестр меняется редко, торопиться некуда.
SCAN_EVERY = 60
# Бюджет одного опроса, сек: меньше шага цикла, чтобы опросы не наезжали друг на друга.
POKE_BUDGET_SEC = 10
# Ключи состояния окна. Между опросами процесс умирает, память не годится, а обычный
# xcom_push стирается перед каждым опросом — держим через run_state_* (см. tfs_utils).
SENT_KEY   = 'sent'
SEEN_KEY   = 'new_scenarios'
PARAMS_KEY = 'params_done'
# Причины паузы, о которых уже написали в заметку: пишем один раз на причину за окно,
# а не каждые 15 секунд.
PAUSE_KEY  = 'pause_noted'


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

        Признак «уже разобрали» лежит в state-объекте: scan_queue опрашивает очередь
        раз в минуту, а enqueue_files каждый раз заводит новые RqUID, так что второй
        проход размножил бы досылку. Ошибка настройки не должна ронять весь ран отправки —
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
            rows, missing = enqueue_files(
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

        # Не нашлись в бакете маршрута: либо ТФС уже забрал файл — и тогда переотправить
        # его нельзя в принципе, — либо в имени опечатка. Молча пропустить нельзя: человек
        # запускал досылку и должен увидеть, что уехало не всё.
        if missing:
            logger.error("❌ Нет в бакете маршрута, в очередь не поставлены: %s", missing)
            add_note({"❌ Нет в бакете (ТФС забрал файл или опечатка)": missing},
                     level='dag', context=context, title='🚚 tfs_kafka_snd')

    def _push_xcom(context, items: list) -> None:
        """📤 Отправленное — записью на ПАКЕТ, внутри {метка отправки: файл}.

        Устроено как у приёмника (tfs_kafka_rcv) и по той же причине: отдельная запись
        XCom на файл в UI Airflow 2.10 показывается как «Invalid input», хотя данные целы.
        Метка времени поэтому живёт ключом внутреннего словаря.

        Раскладывается ВЕСЬ накопленный список на каждом опросе: Airflow чистит XCom таска
        перед каждым исполнением, а в reschedule опрос — отдельное исполнение.
        """
        by_package: dict[str, dict] = {}
        for item in items:
            by_package.setdefault(item['package'], {})[item['key']] = item['row']

        for package, files in by_package.items():
            context['ti'].xcom_push(key=package, value=files)

    def _note_pause(context, scenario: str, held: list) -> None:
        """⏸️ Пишет в заметку, что и почему придержано, — по разу на причину за окно.

        Опрос идёт каждые 15 секунд, а add_note склеивает записи и режет всё вместе
        по MAX_NOTE_LEN: писать на каждом опросе значит вытеснить из заметки всё остальное.
        """
        by_reason: dict = {}
        for row in held:
            by_reason[row['pause_reason']] = by_reason.get(row['pause_reason'], 0) + 1

        noted = set(run_state_get(context, PAUSE_KEY) or [])
        fresh = sorted(set(by_reason) - noted)
        logger.info("⏸️ %s: придержано %d файлов — %s", scenario, len(held),
                    "; ".join(f"{r} ({n})" for r, n in by_reason.items()))
        if not fresh:
            return

        run_state_set(context, PAUSE_KEY, sorted(noted | set(by_reason)))
        add_note({f"⏸️ {scenario}": [f"{r} — придержано {by_reason[r]}" for r in fresh]},
                 level='dag', context=context, title='🚚 tfs_kafka_snd')

    def poke_queue(scenario: str, **context) -> PokeReturnValue:
        """🚚 Отправляет файлы своего сценария в темпе, который декларирует ТФС.

        Таск на сценарий, а не на всю очередь: лимиты заданы на маршрут, и пул
        `tfs_<scenario>` на 1 слот удерживается на время опроса — пока таск передаёт,
        по этому маршруту не пишет никто, включая дагов, которые шлют мимо очереди
        (xs_export берёт этот же пул). Пакет при этом не рвётся: `(replica, package_ts)`
        целиком принадлежит одному сценарию, то есть одному таску.

        ⚠️ Здесь нельзя кидать AirflowFailException: при soft_fail сенсор превращает его
        в скип (airflow/sensors/base.py), и затор о себе не заявит. Для этого AirflowException.
        """
        import time

        from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook

        # В состоянии список отправленного, а не счётчик: из него на каждом опросе
        # заново раскладывается XCom (Airflow чистит его перед каждым исполнением).
        sent_state = list(run_state_get(context, SENT_KEY) or [])
        _push_xcom(context, sent_state)
        # Окно считаем от старта рана: оно переживает reschedule, в отличие от таймеров опроса.
        elapsed = (datetime.now(timezone.utc) - context['dag_run'].start_date).total_seconds()
        last_poke = elapsed >= WINDOW

        queued = [r for r in pending() if r.get('scenario_id') == scenario]
        if not queued:
            logger.info("📭 %s: очередь пуста", scenario)
            return _finish(context, scenario, sent_state, left=[], blocked='', last_poke=last_poke)

        # ⏸️ Придержанные паузой уходят из работы ЦЕЛИКОМ: их не везём и в затор не считаем.
        # Иначе осознанная пауза каждый час роняла бы таск по TFS_QUEUE_ALERT_MIN, то есть
        # выглядела бы как авария.
        free, held = split_pending(queued)
        if held:
            _note_pause(context, scenario, held)
        if not free:
            logger.info("⏸️ %s: вся очередь придержана паузой (%d файлов)", scenario, len(held))
            return _finish(context, scenario, sent_state, left=[], blocked='',
                           last_poke=last_poke, held=held)

        queue = order_queue(free)
        topic = route_topic(scenario)
        limits = tfs_limits(scenario)
        logger.info("📦 %s → %s: в очереди %d файлов, пакетов: %d, лимиты %s",
                    scenario, topic, len(queue),
                    len({(r['replica'], r['package_ts']) for r in queue}), limits)

        # Предупреждаем один раз за опрос, а не в route_topic: там это четыре записи
        # в минуту на файл. Видно сразу, что справочник маршрутов отстал.
        planned = tfs_route(scenario).get('topic')
        if planned != topic:
            logger.warning("⚠️ %s: %s — файлы уходят в топик по умолчанию %s", scenario,
                           f"маршрут ведёт в {planned}, которого нет на контуре" if planned
                           else "не описан в TFS_ROUTES", topic)

        producer = KafkaProducerHook(kafka_config_id=KAFKA_SND_CONN).get_producer()
        deadline = time.time() + POKE_BUDGET_SEC
        sent, blocked = [], ''
        # Расход лимитов: снимок из хранилища на начало опроса + времена собственных
        # отправок. Хранилище опрашивается ОДИН раз за опрос, а не на каждый файл:
        # на S3-раскладке это листинг префикса, и в цикле он умножался на длину очереди.
        # Свои отправки стареют сами — окна считает тот же window_hits.
        snapshot = sent_counts(scenario)
        mine: list = []

        for row in queue:
            if time.time() >= deadline:
                logger.info("⏱️ Бюджет опроса вышел, остальное уедет следующим")
                break

            now_ = datetime.now(timezone.utc)
            # Через .get, а не копией: send_budget разрешает бэкенду вернуть неполный
            # словарь окон, и наши собственные отправки прибавляются к любому из них.
            counts = {w: snapshot.get(w, 0) for w in WINDOWS}
            for ts in mine:
                for window in window_hits(ts, now_):
                    counts[window] += 1

            allowed, hit = send_budget(counts, limits)
            if not allowed:
                blocked = hit
                logger.warning("🚦 %s: упёрлись в лимит '%s' (%s), файлы ждут",
                               scenario, hit, limits[hit])
                break  # маршрут упёрся в лимит — весь остаток очереди этого таска ждёт

            # Секундный лимит выдерживаем паузой: это единственное окно, которое
            # закрывается достаточно быстро, чтобы его имело смысл переждать в опросе.
            gap = 1.0 / limits['sec']
            if mine:
                since = (datetime.now(timezone.utc) - mine[-1]).total_seconds()
                if since < gap:
                    time.sleep(gap - since)

            producer.produce(topic, value=build_message(scenario, row['rq_uid'], row['file_name']))
            producer.flush()

            # Отметка отправки — только после подтверждения доставки
            mark_sent(row['rq_uid'])
            mine.append(datetime.now(timezone.utc))
            sent.append(row)
            logger.info("📤 %s → %s (RqUID %s)", row['file_name'], topic, row['rq_uid'])

        left = [r for r in queue if r not in sent]

        if sent:
            now_ts = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            fresh = [
                {'package': package_key(r),
                 'key': f"{now_ts}_{len(sent_state) + n:03d}",
                 'row': {'rq_uid': r['rq_uid'], 'file_name': r['file_name'],
                         'scenario_id': scenario, 'topic': topic,
                         'replica': r.get('replica'), 'package_ts': str(r.get('package_ts'))}}
                for n, r in enumerate(sent, start=1)
            ]
            sent_state += fresh
            run_state_set(context, SENT_KEY, sent_state)
            _push_xcom(context, sent_state)

            # Заметка короткая намеренно: за час опросов их накопится много, а add_note
            # склеивает записи и режет всё вместе по MAX_NOTE_LEN.
            line = f"📤 +{len(sent)} (за окно {len(sent_state)})"
            if blocked:
                line += f", 🚦 лимит: {blocked}"
            if left:
                line += f", ⏳ в очереди {len(left)}"
            add_note({f"🚚 {scenario}": line}, level='dag', context=context, title='🚚 tfs_kafka_snd')

        return _finish(context, scenario, sent_state, left, blocked, last_poke)

    def _finish(context, scenario: str, sent_state: list, left: list, blocked: str,
                last_poke: bool, held: list | None = None) -> PokeReturnValue:
        """Итог опроса: ждать дальше, зеленеть или заявить о заторе.

        Затор проверяем ТОЛЬКО на последнем опросе окна: сработай он в середине —
        оборвал бы отправку тех файлов, которые ещё успели бы уехать в этом же окне.

        held сюда приходит уже отфильтрованным из left: придержанное паузой — не простой,
        а решение человека, и заявлять о нём как о заторе нельзя.
        """
        held = held or []
        if not last_poke:
            return PokeReturnValue(is_done=False)

        # parse_ts, а не .replace(tzinfo=...): created_at приходит из S3 СТРОКОЙ,
        # и на строке этот вызов падал TypeError вместо внятного сообщения о заторе.
        waits = [ts for ts in (parse_ts(r.get('created_at')) for r in left) if ts]
        if waits:
            oldest = min(waits)
            waiting = (datetime.now(timezone.utc) - oldest).total_seconds() / 60
            if waiting > QUEUE_ALERT_MIN:
                raise AirflowException(
                    f"🚦 Очередь маршрута {scenario} стоит {waiting:.0f} мин при пороге "
                    f"{QUEUE_ALERT_MIN}. Ждут {len(left)} файлов, упёршийся лимит: "
                    f"{blocked or 'нет'}. Самый старый файл в очереди с {oldest:%Y-%m-%d %H:%M:%S} UTC"
                    + (f". Ещё {len(held)} придержано паузой (в этот счёт не входят)" if held else "")
                )

        if sent_state:
            logger.info("✅ %s: окно закрыто, отправлено за час: %d", scenario, len(sent_state))
            # return_value — только ИТОГИ, с разбивкой по пакетам: сами файлы лежат рядом,
            # записями XCom на пакет, и дублировать их списком значит хранить дважды.
            packages: dict[str, int] = {}
            for item in sent_state:
                packages[item['package']] = packages.get(item['package'], 0) + 1
            return PokeReturnValue(is_done=True, xcom_value={
                'scenario': scenario,
                'count':    len(sent_state),
                'packages': packages,
                'held':     len(held),
            })

        # Ничего не ушло за весь час — сенсор упрётся в timeout и пометит таск скипнутым.
        # Придержанное паузой попадает сюда же: отправлять было нечего, и это не ошибка.
        if held:
            logger.info("⏸️ %s: за окно не ушло ничего, придержано паузой: %d", scenario, len(held))
        return PokeReturnValue(is_done=False)

    def _check_stale(context) -> None:
        """📭 Отправленные без квитанции — раз за окно, в конце.

        Пакет, чей wait_confirm ушёл в скип (пауза или auto_confirm), квитанции не ждёт,
        и отказ ТФС по его файлам иначе не увидел бы никто. Предупреждение, а не падение:
        причина может быть и на стороне ТФС, ронять этим отправку незачем.
        """
        try:
            stale = stale_sent(STALE_MIN)
        except Exception as exc:      # noqa: BLE001 — сверка не должна ронять отправку
            logger.warning("⚠️ Сверка неподтверждённых не удалась: %s", exc)
            return

        if not stale:
            logger.info("📨 Неподтверждённых отправок старше %d мин нет", STALE_MIN)
            return

        logger.warning("📭 Без квитанции старше %d мин: %d файлов", STALE_MIN, len(stale))
        add_note({f"📭 Нет квитанции старше {STALE_MIN} мин ({len(stale)})":
                  [f"{r['file_name']} · {r['scenario_id']} · ждём {r['waiting_min']} мин"
                   for r in stale[:10]]},
                 level='dag', context=context, title='🚚 tfs_kafka_snd')

    def poke_scan(**context) -> PokeReturnValue:
        """🔎 Ищет в очереди сценарии, у которых ещё нет своего таска, и заводит их.

        Отправкой не занимается: файлы маршрута везёт ТОЛЬКО его таск — тот, что держит
        пул `tfs_<scenario>`. Возьмись за них кто-то ещё, он держал бы чужой пул, и
        взаимное исключение с отправителем мимо очереди (xs_export) перестало бы работать.

        Что делает за опрос: разбирает параметры запуска, читает очередь, дописывает
        незнакомые сценарии в реестр (Variable) и сразу заводит их пулы — таск
        с несуществующим пулом Airflow не поставит в очередь вовсе. И наоборот: сценарий,
        у которого пул удалили руками, уходит из реестра вместе с таском — пул тут
        выключатель маршрута.

        Опрос раз в минуту, а не раз в 15 секунд: реестр меняется редко, а очередь этот
        таск читает целиком. Найденный сценарий получает свой таск на следующем разборе
        DAG-файла, и шедулер добавляет его в УЖЕ идущий ран (DagRun.verify_integrity),
        так что файл уезжает в пределах того же часа.
        """
        _enqueue_params_once(context)

        rows = pending()
        found = sorted({r.get('scenario_id', '') for r in rows} - {''})
        blank = [r for r in rows if not r.get('scenario_id')]
        if blank:
            logger.warning("⚠️ В очереди %d строк без scenario_id — их не увезёт никто: %s",
                           len(blank), [r.get('file_name') for r in blank[:5]])

        new = remember_scenarios(found)
        for scenario in new:
            ensure_pool(
                scenario_pool(scenario), slots=SEND_SLOTS,
                description=(f'TFS сценарий {scenario} — макс. {SEND_SLOTS} уведомление '
                             'одновременно. Заведён автоматически по очереди отправки'),
            )
            logger.warning("🆕 Новый сценарий %s: пул %s заведён, таск появится после "
                           "разбора DAG-файла", scenario, scenario_pool(scenario))

        # ПОСЛЕ пополнения: у только что найденного сценария пул уже есть, и он не будет
        # выброшен тем же опросом. Сценарий, чьи файлы ещё в очереди, вернётся сюда
        # следующим сканом — выключатель работает по разобранной очереди.
        gone = drop_unpooled_scenarios()
        for scenario in gone:
            logger.warning("🗑️ Сценарий %s убран из реестра: пула %s больше нет. Таск "
                           "исчезнет после разбора DAG-файла", scenario, scenario_pool(scenario))

        seen = set(run_state_get(context, SEEN_KEY) or []) | set(new)
        if new or gone:
            run_state_set(context, SEEN_KEY, sorted(seen))
            note = {}
            if new:
                note["🆕 Новые сценарии"] = ", ".join(sorted(seen))
            if gone:
                note["🗑️ Убраны (нет пула)"] = ", ".join(sorted(gone))
            add_note(note, level='dag', context=context, title='🚚 tfs_kafka_snd')

        logger.info("🔎 В очереди %d файлов, сценариев %d: %s. С таском: %s",
                    len(rows), len(found), found or '—', SCENARIOS)

        elapsed = (datetime.now(timezone.utc) - context['dag_run'].start_date).total_seconds()
        if elapsed >= WINDOW:
            _check_stale(context)
            return PokeReturnValue(is_done=True, xcom_value=sorted(seen))

        return PokeReturnValue(is_done=False)

    # timeout заведомо больше окна: сенсор проверяет его ТОЛЬКО после ложного ответа,
    # поэтому зелёный по концу окна успевает сработать первым, а скип по таймауту
    # достаётся ровно случаю «за час отправлять было нечего».
    for _scen in SCENARIOS:
        task.sensor(
            task_id=f"snd_{task_slug(_scen)}",
            mode='reschedule',
            poke_interval=POKE_EVERY,
            timeout=WINDOW + 2 * POKE_EVERY,
            soft_fail=True,
            pool=scenario_pool(_scen),
            doc_md=f"Отправка файлов сценария `{_scen}` в темпе маршрута",
        )(poke_queue)(scenario=_scen)

    # Скипается (soft_fail по таймауту), когда за час новых сценариев не нашлось —
    # ровно как отправители на пустой очереди.
    task.sensor(
        task_id="scan_queue",
        mode='reschedule',
        poke_interval=SCAN_EVERY,
        timeout=WINDOW + 2 * SCAN_EVERY,
        soft_fail=True,
        doc_md="Реестр сценариев очереди: новым заводит пул, таск появится после разбора файла",
    )(poke_scan)()


tfs_kafka_snd_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
