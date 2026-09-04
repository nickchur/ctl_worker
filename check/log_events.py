"""### 📊 Сбои доставки задач: отчёт по журналу метабазы
*2026-09-04 15:53 MSK · v1.4 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Считает по таблице `log` метабазы события, которыми планировщик сообщает, что задача не
доехала до воркера или не доработала:

| Событие | Что оно значит |
|---|---|
| `stuck in queued reschedule` | Задача пролежала в `queued` дольше `task_queued_timeout` (600 с) и возвращена в `scheduled`: её отдали celery, а воркер не начал |
| `stuck in queued tries exceeded` | Перезапросов было больше `num_stuck_in_queued_retries` — планировщик сдался и пометил задачу упавшей |
| `heartbeat timeout` | Процесс задачи перестал слать хартбит (`scheduler_zombie_task_threshold`, 300 с) — так выглядит внезапная смерть процесса, а не медленный запрос |
| `state mismatch` | Исполнитель отрапортовал об окончании задачи, пока она числилась `queued` — классика для воркера, убитого сигналом |

Все четыре — про доставку и жизнь процесса, а не про логику дага. В интерфейсе они видны
по одной задаче за раз (вкладка Event Log), поэтому вопрос «это у нас или у всех» без
такого отчёта не имеет ответа.

Отдельной строкой считаются **наши собственные события** — их пишет код тракта:

| Событие | Кто пишет | Что значит |
|---|---|---|
| `ctl dag paused` | `ctl_sensor` | загрузка пропущена: даг воркфлоу на паузе, запускать нечего |

В число сбоев доставки они не входят и порога не трогают: запаузенный даг — не поломка
инфраструктуры, а решение человека. Но и молчать о них нельзя: из-за паузы загрузка стоит,
и «сколько раз за сутки» — это и есть ответ на «часто ли».

**Событие и задача — разные вещи.** О зависшей задаче планировщик пишет не однажды, а на
каждом круге проверки: одна мёртвая задача даёт тысячи записей. Поэтому рядом с числом
событий всегда стоит число различных попыток — даг, задача, прогон, индекс отображения,
номер попытки, — и доля с порогом считаются по ним. Само число событий никуда не девается:
по его отношению к числу задач залипание как раз и видно.

**Знаменатель обязателен.** Десять сбоев на четыре тысячи запусков и десять на сорок —
разные новости, поэтому рядом всегда стоит число запусков за то же окно (события
`running`) и доля. Совпадение единиц при этом неполное: часть сбоев относится к попыткам,
которые так и не стартовали, а значит в знаменатель не попали. Доля — оценка порядка, а не
строгая часть целого.

| Параметр | Описание |
|---|---|
| 🕐 `hours` | Окно отчёта, часов *(default: `24`, максимум `168` — см. замеры в коде)* |
| 📈 `days` | Глубина разбивки по дням *(default: `7`, максимум `30`)* |
| 🔝 `top` | Сколько строк «даг · задача» показать *(default: `10`)* |
| 🚨 `alert_after` | Порог: **задач** больше — таск краснеет; `0` — только показывать *(default: `0`)* |
| ⏰ `schedule` | Расписание (МСК): cron или пресет, пусто — только вручную *(default: `30 6 * * *`)* |
| 💾 `save_params` | Сохранить параметры запуска как значения по умолчанию *(default: `False`)* |

Отчёт только читает журнал: ничего не удаляет и не правит. Чистит `log` отдельный даг —
`db_cleanup`, и от его `retention_days` зависит, насколько глубоко видно разбивку по дням.

**Таски:** `params` → `collect` → `report`.
"""

from datetime import datetime, timedelta, timezone
import logging

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from CI06932748.tools.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, saved_params, store_params, valid_schedule)
except ImportError:
    from plugins.utils import (  # type: ignore
        TOOLS_POOL, add_note, ensure_pool, on_callback, saved_params, store_params, valid_schedule)

logger = logging.getLogger("airflow.task")

# Москва живёт на постоянном UTC+3 с 2014 года, переходов нет — фиксированный сдвиг
# честнее ZoneInfo: он не тянет базу правил ради константы.
MSK = timezone(timedelta(hours=3))

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# События планировщика про доставку задач. Имена — константы Airflow
# (jobs/scheduler_job_runner.py), а не наша догадка: TASK_STUCK_IN_QUEUED_RESCHEDULE_EVENT
# и соседние Log(event=...). Появится в новой версии ещё одно — добавлять сюда.
INCIDENTS = (
    'stuck in queued reschedule',
    'stuck in queued tries exceeded',
    'heartbeat timeout',
    'state mismatch',
)

# Наши собственные события — их пишет код тракта, а не планировщик. Считаются отдельно и
# в число сбоев доставки НЕ входят: запаузенный даг это не поломка инфраструктуры, а
# решение человека (или пауза с создания), и смешивать их в одном счётчике значит
# получить бессмысленную сумму.
OURS = (
    'ctl dag paused',      # сенсор пропустил загрузку: даг воркфлоу на паузе
)

# Чем задача начинается: по этим записям считаем знаменатель. Событие пишется на каждый
# запуск попытки, то есть знаменатель считает то же, что и числитель после перехода на
# задачи. Вложенность при этом неполная: попытка, застрявшая в очереди и не стартовавшая,
# в числитель попадёт, а в знаменатель нет — поэтому доля читается как оценка порядка.
STARTS = 'running'

PARAMS_VAR = 'tools_log_events_params'
SAVED = saved_params(PARAMS_VAR)

DEFAULT_SCHEDULE = '30 6 * * *'


def names_drifted() -> str:
    """Бесплатная половина сторожа: сверка с константой самого Airflow.

    Одно из четырёх имён Airflow экспортирует константой — с ней и сверяемся на импорте.
    Остальные три лежат в его коде литералами, их так не достать; для них есть дорогой
    поиск похожих событий, но он платится только при нулевом счётчике (см. SQL_UNKNOWN).
    """
    try:
        from airflow.jobs.scheduler_job_runner import TASK_STUCK_IN_QUEUED_RESCHEDULE_EVENT as ev
    except Exception:
        return "константа TASK_STUCK_IN_QUEUED_RESCHEDULE_EVENT в Airflow не найдена"
    if ev not in INCIDENTS:
        return f"Airflow пишет {ev!r}, а мы ищем {INCIDENTS[0]!r} — имена разъехались"
    return ''


def _param(key, default, **kwargs):
    """Param со значением по умолчанию из переменной, если оно там есть."""
    return Param(SAVED.get(key, default), **kwargs)


def _schedule():
    """Расписание DAG-а: из переменной, если оно осмысленное, иначе из кода."""
    value = SAVED.get('schedule', DEFAULT_SCHEDULE)
    if not valid_schedule(value):
        logger.warning(f"⚠️ {PARAMS_VAR}: расписание '{value}' не разобрано — беру {DEFAULT_SCHEDULE}")
        return DEFAULT_SCHEDULE
    return None if value in (None, '', 'None') else str(value).strip()


def _fetch(sql: str, args: dict) -> list:
    """Читает журнал метабазы и отдаёт список словарей.

    Параметры связанные, а не подставленные в текст: окно приходит из формы запуска.
    `create_session` как контекст — своей долгоживущей сессии отчёту не нужно.

    У `pg_activity` есть такой же по смыслу помощник без параметров; сводить их в общий
    имеет смысл, когда появится третий потребитель, — пока это было бы обобщение по двум
    точкам.
    """
    from airflow.utils.session import create_session
    from sqlalchemy import text

    with create_session() as session:
        # Отчёт не должен висеть на метабазе: журнал большой, а индексы по dttm и event
        # есть, так что укладываться обязан с запасом.
        session.execute(text("SET LOCAL statement_timeout = '60s'"))
        return [dict(row) for row in session.execute(text(sql), args).mappings().all()]


# Границу окна считаем один раз в Python и передаём во все запросы: иначе у числителя и
# знаменателя оказался бы свой now(), и доля «сбоев на запусков» считалась бы по разным
# отрезкам. Заодно схема указана явно — как у соседей (pg_activity, db_cleanup).
# Задача считается по адресу попытки: даг, задача, прогон, индекс отображения, номер
# попытки. `count(DISTINCT (...))` по кортежу — конструкция PostgreSQL (row value
# expression), не общий SQL; на другой БД пришлось бы склеивать ключ строкой.
#
# Общий счётчик задач берётся отдельным запросом, а не суммой по видам событий: одна
# попытка может дать и потерю хартбита, и расхождение состояния, и в сумме по видам она
# посчиталась бы дважды.
SQL_TOTALS = """
SELECT count(*) AS cnt,
       count(DISTINCT (dag_id, task_id, run_id, map_index, try_number)) AS tasks
  FROM main.log
 WHERE dttm > :cutoff
   AND event = ANY(:events)
"""

SQL_BY_EVENT = """
SELECT event, count(*) AS cnt,
       count(DISTINCT (dag_id, task_id, run_id, map_index, try_number)) AS tasks
  FROM main.log
 WHERE dttm > :cutoff
   AND event = ANY(:events)
 GROUP BY event
 ORDER BY cnt DESC
"""

# Слово INTERVAL здесь не для красоты, и убирать его нельзя. Замер на PostgreSQL 16
# для timestamptz-значения 2026-09-04 12:20:30+00:
#
#   AT TIME ZONE INTERVAL '+03:00'  → 15:20:30   верно
#   AT TIME ZONE '+03:00'           → 09:20:30   на шесть часов мимо
#   AT TIME ZONE 'Europe/Moscow'    → 15:20:30   верно, но тянет базу правил
#
# Текстовая форма разбирается как POSIX-спецификация зоны, где знак смещения
# инвертирован, — «упрощение» до неё сдвигает время молча, цифры остаются
# правдоподобными. Имя зоны считает верно, но ради константы UTC+3 базу правил
# тянуть незачем: тот же довод, что у константы MSK выше.
SQL_BY_TASK = """
SELECT coalesce(dag_id, '—') AS dag_id, coalesce(task_id, '—') AS task_id,
       count(*) AS cnt, count(DISTINCT event) AS kinds,
       count(DISTINCT (run_id, map_index, try_number)) AS tasks,
       max(dttm AT TIME ZONE INTERVAL '+03:00') AS last_seen
  FROM main.log
 WHERE dttm > :cutoff
   AND event = ANY(:events)
 GROUP BY dag_id, task_id
 ORDER BY cnt DESC
 LIMIT :top
"""

# Сутки режутся по московскому календарю, и это смена смысла: раньше границу дня
# задавал TimeZone соединения — на боевом контуре московский, на стенде UTC, то есть
# один и тот же отчёт резал сутки по-разному. Теперь одинаково везде, но при сравнении
# со старыми сводками строки за 21:00–23:59 UTC переезжают на следующий день.
SQL_BY_DAY = """
SELECT date_trunc('day', dttm AT TIME ZONE INTERVAL '+03:00')::date AS day,
       count(*) AS cnt,
       count(DISTINCT (dag_id, task_id, run_id, map_index, try_number)) AS tasks
  FROM main.log
 WHERE dttm > :cutoff
   AND event = ANY(:events)
 GROUP BY day
 ORDER BY day
"""

SQL_STARTS = """
SELECT count(*) AS cnt
  FROM main.log
 WHERE dttm > :cutoff
   AND event = :starts
"""

# Сторож имён, дорогая половина. События планировщика при обновлении Airflow могут
# переехать: счётчик станет нулём, а отчёт напишет «чисто» — худший вид ошибки. Поиск
# похожих по смыслу событий стоит дорого: ведущий `%` в ILIKE индексом по event
# воспользоваться не даёт, и запрос перебирает все строки окна — 2,5 с за сутки и 16 с за
# неделю на журнале в миллион строк. Поэтому он выполняется ТОЛЬКО когда сбоев не нашлось
# вовсе: ровно в этом случае его ответ и нужен — тишина настоящая или имена разъехались.
SQL_UNKNOWN = """
SELECT event, count(*) AS cnt
  FROM main.log
 WHERE dttm > :cutoff
   AND (event ILIKE '%stuck%' OR event ILIKE '%heartbeat%'
        OR event ILIKE '%mismatch%' OR event ILIKE '%zombie%')
   AND NOT (event = ANY(:known))
 GROUP BY event
"""


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        # 900: выше регрессионных прогонов того же пула (у них приоритета нет),
        # ниже агента CTL (999/1000) — тот двигает боевые загрузки, и обгонять
        # его диагностике незачем.
        #
        # absolute обязателен: правило по умолчанию (downstream) складывает вес
        # вниз по цепочке, и первый таск получил бы 2700 вместо 900 — сравнение с
        # соседями стало бы зависеть от длины цепочки, а не от намерения.
        'priority_weight': 900,
        'weight_rule': 'absolute',
        # Запросы идут по индексам dttm и event; пять минут — это про зависание,
        # а не про медленный ответ.
        'execution_timeout': timedelta(minutes=5),
        'on_failure_callback': on_callback,
    },
    # Часовой пояс DAG-а берётся из start_date.tzinfo, поэтому расписание московское —
    # как у соседей по каталогу (show_connections, test_connections).
    start_date=datetime(2026, 9, 4, tzinfo=MSK),
    schedule=_schedule(),
    # Тег tools важен: по нему ролевка ограничивает запуск (HRPDATALAB-15421)
    tags=['DataLab', 'tools', 'check'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    # Потолок на прогон, а не только на таск: при max_active_runs=1 зависший прогон
    # закрывает дорогу всем следующим — так же, как у сторожа метабазы.
    dagrun_timeout=timedelta(minutes=30),
    params={
        # Потолки замерены, а не выбраны на глаз. Запросы по событиям сбоев дёшевы:
        # значения редкие, планировщик берёт индекс по event — разбивка по дням за 90
        # суток укладывается в 2 мс. Дорог знаменатель: `running` пишется на каждый
        # запуск, и он идёт по индексу dttm — на журнале в миллион строк это 0,5 с за
        # сутки, 9 с за неделю и 41 с за месяц. Поэтому окно ограничено неделей: дальше
        # отчёт перестаёт быть оперативным, а цена растёт линейно с объёмом журнала.
        #
        # `count(DISTINCT (...))` цену не изменил: он считается по той же редкой выборке
        # по индексу event. Замер на журнале в 992 тысячи строк — итоги 3,8 мс, по видам
        # 0,6 мс, по дагам 0,9 мс, по дням 0,7 мс; знаменатель, куда DISTINCT не
        # добавляется, по-прежнему самый дорогой — 62 мс за сутки.
        'hours': _param('hours', 24, type='integer', minimum=1, maximum=168,
                        description='Окно отчёта, часов (максимум неделя)'),
        'days': _param('days', 7, type='integer', minimum=1, maximum=30,
                       description='Глубина разбивки по дням'),
        'top': _param('top', 10, type='integer', minimum=1, maximum=50,
                      description='Сколько строк «даг · задача» показать'),
        'alert_after': _param('alert_after', 0, type='integer', minimum=0,
                              description='Строго больше этого числа ЗАДАЧ — краснеть; 0 — только показывать'),
        'schedule': _param('schedule', DEFAULT_SCHEDULE, type=['string', 'null'],
                           description='Расписание: cron или пресет, пусто — только вручную'),
        'save_params': Param(False, type='boolean',
                             description=f'Сохранить параметры запуска в {PARAMS_VAR}'),
    },
)
def tools_log_events():

    @task(task_id='params')
    def save_params(**context) -> str:
        """💾 Сохраняет параметры запуска как значения по умолчанию."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException

        status, msg = store_params(PARAMS_VAR, SAVED, context)
        if status == 'skip':
            raise AirflowSkipException(msg)
        # Негодное расписание переменную не переписывает, и таск обязан упасть: битое
        # значение уронило бы разбор файла и убрало из UI саму форму, через которую его
        # можно починить (см. спеку check, сценарий «Негодное расписание в форме»).
        if status == 'fail':
            raise AirflowFailException(msg)
        add_note(msg, context=context, level='task')
        return msg

    @task(task_id='collect', trigger_rule=TriggerRule.NONE_FAILED)
    def collect(**context) -> dict:
        """🔎 Считает события за окно: всего, по дагам, по дням, и запуски для доли."""
        p = context['params']
        hours, days, top = int(p['hours']), int(p['days']), int(p['top'])
        events, ours = list(INCIDENTS), list(OURS)

        # Один момент времени на все запросы: иначе числитель и знаменатель считались бы
        # по разным отрезкам, а доля — это ключевая цифра отчёта.
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=hours)
        cutoff_days = now - timedelta(days=days)

        totals = _fetch(SQL_TOTALS, {'cutoff': cutoff, 'events': events})
        by_event = _fetch(SQL_BY_EVENT, {'cutoff': cutoff, 'events': events})
        by_task = _fetch(SQL_BY_TASK, {'cutoff': cutoff, 'events': events, 'top': top})
        by_day = _fetch(SQL_BY_DAY, {'cutoff': cutoff_days, 'events': events})
        starts = _fetch(SQL_STARTS, {'cutoff': cutoff, 'starts': STARTS})

        # Те же запросы по нашим событиям: отдельный счётчик и отдельный список дагов.
        ours_by_event = _fetch(SQL_BY_EVENT, {'cutoff': cutoff, 'events': ours})
        ours_by_task = _fetch(SQL_BY_TASK, {'cutoff': cutoff, 'events': ours, 'top': top})

        # Сторож имён. Бесплатная половина — всегда; дорогая — только когда сбоев ноль:
        # тогда и надо отличить настоящую тишину от разъехавшихся имён.
        if (drift := names_drifted()):
            logger.warning("⚠️ %s — проверьте INCIDENTS", drift)

        total = int(totals[0]['cnt']) if totals else 0
        tasks = int(totals[0]['tasks']) if totals else 0
        unknown = []
        if not total:
            unknown = _fetch(SQL_UNKNOWN, {'cutoff': cutoff, 'known': events + ours + [STARTS]})
            for r in unknown:
                logger.warning("⚠️ похожее событие вне наших списков: %s (%s) — "
                               "проверьте имена в INCIDENTS", r['event'], r['cnt'])

        runs = int(starts[0]['cnt']) if starts else 0
        # Доля без знаменателя не считается: ноль запусков — это не «идеальное здоровье»,
        # а «ничего не запускалось», и путать их нельзя. Считается по задачам: доля по
        # событиям мерила бы частоту опроса планировщика, а не здоровье контура.
        share = round(100.0 * tasks / runs, 2) if runs else None

        snapshot = {
            # Момент сбора — московский, как и last_seen из запросов: иначе рядом в одном
            # снимке стоят два разных пояса и сравнить их глазами нельзя. Арифметика окна
            # при этом остаётся в UTC — cutoff считается от `now`.
            'ts': now.astimezone(MSK).strftime('%Y-%m-%d %H:%M:%S'),
            'window_hours': hours,
            'window_days': days,
            'unknown_events': {r['event']: int(r['cnt']) for r in unknown},
            'total': total,
            'tasks': tasks,
            'runs': runs,
            'share_pct': share,
            'by_event': {r['event']: {'cnt': int(r['cnt']), 'tasks': int(r['tasks'])}
                         for r in by_event},
            'by_task': [
                {'dag_id': r['dag_id'], 'task_id': r['task_id'], 'cnt': int(r['cnt']),
                 'kinds': int(r['kinds']), 'tasks': int(r['tasks']),
                 'last_seen': str(r['last_seen'])[:19]}
                for r in by_task
            ],
            'by_day': {str(r['day']): {'cnt': int(r['cnt']), 'tasks': int(r['tasks'])}
                       for r in by_day},
            'ours_total': sum(int(r['cnt']) for r in ours_by_event),
            'ours_by_event': {r['event']: int(r['cnt']) for r in ours_by_event},
            # task_id здесь нет намеренно: событие пишет сенсор, а dag_id в нём — это даг
            # ВОРКФЛОУ, чью загрузку пропустили. Задача сенсора в группировке бесполезна,
            # интересен воркфлоу.
            'ours_by_task': [
                {'dag_id': r['dag_id'], 'cnt': int(r['cnt']), 'last_seen': str(r['last_seen'])[:19]}
                for r in ours_by_task
            ],
        }

        logger.info("🔎 за %d ч: событий %d, задач %d, запусков %d, доля %s%%",
                    hours, total, tasks, runs, share if share is not None else '—')
        for r in by_event:
            logger.warning("⚠️ %s: %s (задач %s)", r['event'], r['cnt'], r['tasks'])
        for r in snapshot['by_task']:
            logger.warning("   %s.%s — %d (задач %d, видов %d, последнее %s)",
                           r['dag_id'], r['task_id'], r['cnt'], r['tasks'], r['kinds'],
                           r['last_seen'])

        if snapshot['ours_total']:
            logger.warning("⏸️ наши события: %d %s", snapshot['ours_total'], snapshot['ours_by_event'])
            for r in snapshot['ours_by_task']:
                logger.warning("   %s — %d (последнее %s)", r['dag_id'], r['cnt'], r['last_seen'])
        return snapshot

    @task(task_id='report', trigger_rule=TriggerRule.NONE_FAILED)
    def report(snapshot: dict, **context) -> str:
        """🧾 Сводка в заметку; при превышении порога — красный таск и уведомление."""
        from airflow.exceptions import AirflowFailException

        p = context['params']
        limit = int(p['alert_after'])
        total, tasks, runs = snapshot['total'], snapshot['tasks'], snapshot['runs']
        share = snapshot['share_pct']

        # Оба числа рядом и в этом порядке: событий много, задач мало, и разница между
        # ними — сама по себе диагноз. Порог сравнивается с задачами (см. ниже).
        # Форма «задач N» вместо «на N задачах» — чтобы не склонять числительное:
        # «на 1 задачах» читается как опечатка, а согласовывать род и число ради одной
        # строки сводки дороже, чем обойти согласование.
        head = (f"за {snapshot['window_hours']} ч: сбоев доставки {total}, "
                f"задач {tasks}, запусков {runs}"
                + (f", доля {share}%" if share is not None else ""))

        # Наши события идут отдельной строкой и порог не трогают: запаузенный даг это не
        # поломка доставки, а решение человека. Но молчать о них нельзя — из-за паузы
        # загрузка стоит, и «сколько раз за сутки» это и есть ответ на «часто ли».
        ours = []
        if snapshot['ours_total']:
            ours = [f"⏸️ наши события: {snapshot['ours_total']}"]
            ours += [f"   {r['dag_id']} — {r['cnt']} (последнее {r['last_seen']})"
                     for r in snapshot['ours_by_task']]

        if not total:
            # Про запуски говорим и в этом случае: «ноль сбоев» при нуле запусков —
            # не здоровье, а простой, и заметка обязана их различать.
            msg = f"✅ чисто: {head}"
            add_note({msg: ours} if ours else msg,
                     context=context, level='task,dag', title='📊 log_events ')
            return msg

        lines = [f"{k}: {v['cnt']} (задач {v['tasks']})"
                 for k, v in snapshot['by_event'].items()]
        lines += [f"{r['dag_id']}.{r['task_id']} — {r['cnt']} "
                  f"(задач {r['tasks']}, последнее {r['last_seen']})"
                  for r in snapshot['by_task']]
        by_day = snapshot['by_day']
        if by_day:
            # Сколько дней реально вернулось: журнал чистит db_cleanup по своему
            # retention_days, и «за 30 д» с четырнадцатью датами читатель иначе примет
            # за отсутствие сбоев, а не за отсутствие данных.
            lines.append(f"по дням (за {snapshot['window_days']} д, события·задачи, "
                         f"дней с данными {len(by_day)}): "
                         + ", ".join(f"{d[5:]}={v['cnt']}·{v['tasks']}"
                                     for d, v in by_day.items()))

        add_note({f"⚠️ {head}": lines + ours}, context=context, level='task,dag', title='📊 log_events ')

        # Порог — по задачам, а не по событиям (почему — в docstring модуля).
        if limit and tasks > limit:
            raise AirflowFailException(f"⚠️ {head} — задач больше порога {limit}\n"
                                       + "\n".join(lines))
        return head

    snapshot = collect()
    save_params() >> snapshot >> report(snapshot)


tools_log_events()
