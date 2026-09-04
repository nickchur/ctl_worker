"""### 📊 Сбои доставки задач: отчёт по журналу метабазы
*2026-09-04 09:40 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

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

**Знаменатель обязателен.** Десять сбоев на четыре тысячи запусков и десять на сорок —
разные новости, поэтому рядом всегда стоит число запусков за то же окно (события
`running`) и доля.

| Параметр | Описание |
|---|---|
| 🕐 `hours` | Окно отчёта, часов *(default: `24`)* |
| 📈 `days` | Глубина разбивки по дням *(default: `7`)* |
| 🔝 `top` | Сколько строк «даг · задача» показать *(default: `10`)* |
| 🚨 `alert_after` | Порог: событий больше — таск краснеет; `0` — только показывать *(default: `0`)* |
| ⏰ `schedule` | Расписание: cron или пресет, пусто — только вручную *(default: `30 6 * * *`)* |
| 💾 `save_params` | Сохранить параметры запуска как значения по умолчанию *(default: `False`)* |

Отчёт только читает журнал: ничего не удаляет и не правит. Чистит `log` отдельный даг —
`db_cleanup`, и от его `retention_days` зависит, насколько глубоко видно разбивку по дням.

**Таски:** `params` → `collect` → `report`.
"""

from datetime import datetime, timezone
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

# Чем задача начинается: по этим записям считаем знаменатель. Событие пишется на каждый
# запуск таска, поэтому «сбоев на запусков» получается честной долей.
STARTS = 'running'

PARAMS_VAR = 'tools_log_events_params'
SAVED = saved_params(PARAMS_VAR)

DEFAULT_SCHEDULE = '30 6 * * *'


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
        result = session.execute(text(sql), args)
        cols = list(result.keys())
        return [dict(zip(cols, row)) for row in result.fetchall()]


SQL_BY_EVENT = """
SELECT event, count(*) AS cnt
  FROM log
 WHERE dttm > now() - make_interval(hours => :hours)
   AND event = ANY(:events)
 GROUP BY event
 ORDER BY cnt DESC
"""

SQL_BY_TASK = """
SELECT coalesce(dag_id, '—') AS dag_id, coalesce(task_id, '—') AS task_id,
       count(*) AS cnt, count(DISTINCT event) AS kinds, max(dttm) AS last_seen
  FROM log
 WHERE dttm > now() - make_interval(hours => :hours)
   AND event = ANY(:events)
 GROUP BY 1, 2
 ORDER BY cnt DESC
 LIMIT :top
"""

SQL_BY_DAY = """
SELECT date_trunc('day', dttm)::date AS day, count(*) AS cnt
  FROM log
 WHERE dttm > now() - make_interval(days => :days)
   AND event = ANY(:events)
 GROUP BY 1
 ORDER BY 1
"""

SQL_STARTS = """
SELECT count(*) AS cnt
  FROM log
 WHERE dttm > now() - make_interval(hours => :hours)
   AND event = :starts
"""


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 0,
        'on_failure_callback': on_callback,
    },
    start_date=datetime(2026, 9, 4, tzinfo=timezone.utc),
    schedule=_schedule(),
    # Тег tools важен: по нему ролевка ограничивает запуск (HRPDATALAB-15421)
    tags=['DataLab', 'tools', 'check'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    params={
        'hours': _param('hours', 24, type='integer', minimum=1, maximum=720,
                        description='Окно отчёта, часов'),
        'days': _param('days', 7, type='integer', minimum=1, maximum=90,
                       description='Глубина разбивки по дням'),
        'top': _param('top', 10, type='integer', minimum=1, maximum=50,
                      description='Сколько строк «даг · задача» показать'),
        'alert_after': _param('alert_after', 0, type='integer', minimum=0,
                              description='Событий больше — краснеть; 0 — только показывать'),
        'schedule': _param('schedule', DEFAULT_SCHEDULE, type=['string', 'null'],
                           description='Расписание: cron или пресет, пусто — только вручную'),
        'save_params': Param(False, type='boolean',
                             description=f'Сохранить параметры запуска в {PARAMS_VAR}'),
    },
)
def tools_log_events():

    @task(task_id='params')
    def save(**context) -> str:
        """💾 Сохраняет параметры запуска как значения по умолчанию."""
        from airflow.exceptions import AirflowSkipException

        status, msg = store_params(PARAMS_VAR, SAVED, context)
        if status == 'skip':
            raise AirflowSkipException(msg)
        add_note(msg, context, level='task')
        return msg

    @task(task_id='collect', trigger_rule=TriggerRule.NONE_FAILED)
    def collect(**context) -> dict:
        """🔎 Считает события за окно: всего, по дагам, по дням, и запуски для доли."""
        p = context['params']
        hours, days, top = int(p['hours']), int(p['days']), int(p['top'])
        events = list(INCIDENTS)

        by_event = _fetch(SQL_BY_EVENT, {'hours': hours, 'events': events})
        by_task = _fetch(SQL_BY_TASK, {'hours': hours, 'events': events, 'top': top})
        by_day = _fetch(SQL_BY_DAY, {'days': days, 'events': events})
        starts = _fetch(SQL_STARTS, {'hours': hours, 'starts': STARTS})

        total = sum(int(r['cnt']) for r in by_event)
        runs = int(starts[0]['cnt']) if starts else 0
        # Доля без знаменателя не считается: ноль запусков — это не «идеальное здоровье»,
        # а «ничего не запускалось», и путать их нельзя.
        share = round(100.0 * total / runs, 2) if runs else None

        snapshot = {
            'ts': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'window_hours': hours,
            'total': total,
            'runs': runs,
            'share_pct': share,
            'by_event': {r['event']: int(r['cnt']) for r in by_event},
            'by_task': [
                {'dag_id': r['dag_id'], 'task_id': r['task_id'], 'cnt': int(r['cnt']),
                 'kinds': int(r['kinds']), 'last_seen': str(r['last_seen'])[:19]}
                for r in by_task
            ],
            'by_day': {str(r['day']): int(r['cnt']) for r in by_day},
        }

        logger.info("🔎 за %d ч: событий %d, запусков %d, доля %s%%",
                    hours, total, runs, share if share is not None else '—')
        for r in by_event:
            logger.warning("⚠️ %s: %s", r['event'], r['cnt'])
        for r in snapshot['by_task']:
            logger.warning("   %s.%s — %d (видов %d, последнее %s)",
                           r['dag_id'], r['task_id'], r['cnt'], r['kinds'], r['last_seen'])
        return snapshot

    @task(task_id='report', trigger_rule=TriggerRule.NONE_FAILED)
    def report(snapshot: dict, **context) -> str:
        """🧾 Сводка в заметку; при превышении порога — красный таск и уведомление."""
        from airflow.exceptions import AirflowFailException

        p = context['params']
        limit = int(p['alert_after'])
        total, runs = snapshot['total'], snapshot['runs']
        share = snapshot['share_pct']

        head = (f"за {snapshot['window_hours']} ч: сбоев доставки {total}, "
                f"запусков {runs}" + (f", доля {share}%" if share is not None else ""))

        if not total:
            # Про запуски говорим и в этом случае: «ноль сбоев» при нуле запусков —
            # не здоровье, а простой, и заметка обязана их различать.
            msg = f"✅ чисто: {head}"
            add_note(msg, context, level='task,dag', title='📊 log_events')
            return msg

        lines = [f"{k}: {v}" for k, v in snapshot['by_event'].items()]
        lines += [f"{r['dag_id']}.{r['task_id']} — {r['cnt']} (последнее {r['last_seen']})"
                  for r in snapshot['by_task']]
        by_day = snapshot['by_day']
        if by_day:
            lines.append("по дням: " + ", ".join(f"{d[5:]}={n}" for d, n in by_day.items()))

        add_note({f"⚠️ {head}": lines}, context, level='task,dag', title='📊 log_events')

        if limit and total > limit:
            raise AirflowFailException(f"⚠️ {head} — больше порога {limit}\n" + "\n".join(lines))
        return head

    snapshot = collect()
    save() >> snapshot >> report(snapshot)


tools_log_events()
