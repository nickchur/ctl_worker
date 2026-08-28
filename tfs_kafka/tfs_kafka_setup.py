"""⏸️ DAG-пульт паузы отправки в ТФС.
*2026-08-28 13:27 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Ставит и снимает паузу отправки, не трогая ни выгрузку, ни постановку в очередь: файлы
продолжают складываться в очередь по расписанию, а `tfs_kafka_snd` их не берёт. Сняли
паузу — уехали сами, в порядке `package_ts`.

Зачем: технические работы, выкладка доработок, сбой на стороне ТФС. Выгрузка данных
длинная и ресурсоёмкая, останавливать её ради паузы в отправке незачем.

## Три уровня

| Область | Ключ | Что придерживает |
| :--- | :--- | :--- |
| `scenarios` | `HRPLATFORM-4000` | весь маршрут ТФС |
| `replicas` | `hrplatform_datalab__1` | одну группу поставок (даг `export_er__*`); ключ — имя пакета `<реплика>__<группа>` |
| `packages` | `hrplatform_datalab__1__20260828120000` | один конкретный пакет |

Ключи взять неоткуда — их показывает таск `show`: он печатает и правила, и то, что сейчас
лежит в очереди. Выпадающего списка нет намеренно: реплики и пакеты живут в очереди,
а её чтение — поход в S3, которому не место в разборе DAG-файла.

## Что происходит с пакетом на паузе

* `make_summary` ставит файлы в очередь как обычно;
* `wait_confirm` уходит в ☮️ — ждать квитанцию по файлу, который никуда не поедет,
  бессмысленно;
* `save_status` пишет `confirmed = null`, дельта двигается, следующий цикл взводится;
* отправитель не считает придержанное затором — иначе осознанная пауза каждый час
  роняла бы его таск по `TFS_QUEUE_ALERT_MIN`.

⚠️ Цена: квитанции по такому пакету никто не ждёт. Ловит их сверка неподтверждённых —
раз в час `tfs_kafka_snd` (таск `scan_queue`) ищет отправленное без квитанции.

⚠️ Долгая пауза упирается в лимиты маршрута: 500 файлов в час и 2000 в сутки. Накопленное
за сутки простоя за один час не уедет — очередь будет разбираться постепенно.

## Как пользоваться

1. **Действие** — показать / поставить / снять / убрать истёкшие.
2. **Область** и **Ключ** — что именно придерживаем.
3. **До** — ISO-время, когда пауза снимется сама (`2026-09-01T22:00:00+03:00`).
   Пусто — бессрочно; так делать не надо: забытая пауза копит очередь молча.
4. **Причина** — попадает в правило и в заметки.

Состояние живёт в Variable `tfs_snd_pause`; править её руками можно, но пульт проверяет
формат ключа и срок, а руки — нет.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param
from airflow.utils.trigger_rule import TriggerRule

try:
    from plugins.tfs_utils import (  # type: ignore
        get_config, add_note, package_key, pause_clean_expired, pause_clear, pause_rules,
        pause_set, pause_summary, pending,
    )
except ImportError:
    from CI06932748.tools.tfs_utils import (  # type: ignore
        get_config, add_note, package_key, pause_clean_expired, pause_clear, pause_rules,
        pause_set, pause_summary, pending,
    )

_cfg         = get_config()
DEF_ARGS     = _cfg['DEF_ARGS']
PAUSE_VAR    = _cfg['PAUSE_VAR']
PAUSE_SCOPES = _cfg['PAUSE_SCOPES']

logger = logging.getLogger("airflow.task")

SHOW  = '👁️ показать'
SET   = '⏸️ поставить на паузу'
CLEAR = '▶️ снять с паузы'
CLEAN = '🧹 убрать истёкшие'

# Сколько строк очереди показывать в заметке: add_note режет всё вместе по MAX_NOTE_LEN,
# а полный список всегда есть в логе.
NOTE_LIMIT = 15


@dag(
    dag_id="tfs_kafka_setup",
    description="⏸️ Пульт паузы отправки в ТФС",
    # retries=0 вместо дефолтного: даг ручной, у экрана сидит человек, а ошибка формы
    # от повтора не лечится.
    default_args={**DEF_ARGS, 'retries': 0},
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    schedule=None,               # только ручной запуск
    max_active_runs=1,
    catchup=False,
    tags=["DataLab", "CI02420667", "TFS", "kafka", "setup"],
    is_paused_upon_creation=False,
    doc_md=__doc__,
    params={
        'action': Param(
            SHOW, type='string', title='Действие',
            enum=[SHOW, SET, CLEAR, CLEAN],
            description='Показать состояние, поставить паузу, снять её или убрать из '
                        'переменной правила с прошедшим сроком.',
        ),
        'scope': Param(
            'replicas', type='string', title='Область',
            enum=list(PAUSE_SCOPES),
            description='scenarios — весь маршрут ТФС; replicas — группа поставок '
                        '(даг export_er__*); packages — один пакет.',
        ),
        'key': Param(
            None, type=['string', 'null'], title='Ключ',
            description='HRPLATFORM-4000 / hrplatform_datalab__1 / '
                        'hrplatform_datalab__1__20260828120000. Что есть в очереди — '
                        'смотрите в таске show.',
        ),
        'until': Param(
            None, type=['string', 'null'], title='До (ISO)',
            description='Когда пауза снимется сама: 2026-09-01T22:00:00+03:00. '
                        'Пусто — бессрочно, но так лучше не делать: забытая пауза копит '
                        'очередь молча.',
        ),
        'reason': Param(
            None, type=['string', 'null'], title='Причина',
            description='Зачем ставим. Попадает в правило и в заметки.',
        ),
    },
)
def tfs_kafka_setup_dag():

    @task(task_id='show')
    def show(**context) -> dict:
        """👁️ Текущие правила паузы и то, что сейчас лежит в очереди.

        Очередь читается целиком (это листинг префикса в S3), поэтому таск один и
        отдельный: правкой занимается apply, а сюда ходят за ключами.
        """
        rows = pending()
        summary = pause_summary(rows)

        # Что вообще можно поставить на паузу прямо сейчас — по каждой области
        keys = {
            'scenarios': sorted({str(r.get('scenario_id') or '') for r in rows} - {''}),
            'replicas':  sorted({str(r.get('replica') or '') for r in rows} - {''}),
            'packages':  sorted({package_key(r) for r in rows}),
        }

        logger.info("⏸️ Правила паузы (%s):\n%s", PAUSE_VAR, summary['rules'])
        logger.info("📦 В очереди %d файлов, придержано %d", len(rows), summary['held'])
        for scope, values in keys.items():
            logger.info("    %-10s %s", scope, values or '—')

        note: dict = {}
        for scope, rules in summary['rules'].items():
            if rules:
                note[f"⏸️ {scope}"] = [
                    f"{k} — {v['reason'] or 'без причины'}"
                    + (f", до {v['until']}" if v['until'] else ", бессрочно")
                    for k, v in rules.items()
                ]
        if not note:
            note["✅ Пауз нет"] = f"в очереди {len(rows)} файлов"
        elif summary['held']:
            note["📦 Придержано"] = [f"{r} — {n}" for r, n in summary['by_reason'].items()]

        note["🔑 Ключи из очереди"] = {
            scope: (values[:NOTE_LIMIT] or '—') for scope, values in keys.items()
        }
        add_note(note, level='task,dag', context=context, title='⏸️ tfs_kafka_setup')

        return {'rules': summary['rules'], 'queue': len(rows), 'held': summary['held'],
                'keys': keys}

    @task(task_id='apply')
    def apply(**context) -> str:
        """✏️ Ставит, снимает или чистит правила паузы.

        Решатели тракта возвращают (status, message) и сами ничего не бросают — разбор
        статуса стоит здесь, рядом с trigger_rule следующего таска и с текстом, который
        увидит человек. Та же договорённость, что у таска params в check/db_cleanup.py.
        """
        p       = context['params']
        action  = (p.get('action') or SHOW).strip()
        scope   = (p.get('scope') or '').strip()
        key     = (p.get('key') or '').strip()
        note_id = getattr(context.get('dag_run'), 'run_id', '') or ''

        if action == SHOW:
            raise AirflowSkipException("Действие «показать» — правок нет")

        if action == CLEAN:
            status, msg = pause_clean_expired(note=note_id)
        elif action == SET:
            status, msg = pause_set(scope, key, until=p.get('until') or '',
                                    reason=p.get('reason') or '', note=note_id)
        elif action == CLEAR:
            status, msg = pause_clear(scope, key, note=note_id)
        else:
            raise AirflowFailException(f"Неизвестное действие '{action}'")

        if status == 'skip':
            add_note({f"☮️ {action}": msg}, level='task,dag', context=context,
                     title='⏸️ tfs_kafka_setup')
            raise AirflowSkipException(msg)
        if status == 'fail':
            raise AirflowFailException(msg)

        logger.info("✅ %s: %s", action, msg)
        add_note({f"✅ {action}": msg}, level='task,dag', context=context,
                 title='⏸️ tfs_kafka_setup')
        return msg

    # NONE_FAILED, а не дефолтный ALL_SUCCESS: apply штатно пропускает себя (показ без
    # правки, нечего снимать, нечего чистить), и пропуск апстрима утянул бы в скип показ
    # результата — на этом уже спотыкались в db_cleanup.
    @task(task_id='after', trigger_rule=TriggerRule.NONE_FAILED)
    def after(**context) -> dict:
        """👁️ Правила после правки — чтобы результат был виден, не открывая Variable.

        Очередь тут заново не читаем: она не менялась, а листинг префикса стоит времени.
        """
        rules = pause_rules()
        logger.info("⏸️ Правила паузы после правки:\n%s", rules)
        add_note({"⏸️ Стало": {scope: sorted(items) or '—' for scope, items in rules.items()}},
                 level='task,dag', context=context, title='⏸️ tfs_kafka_setup')
        return rules

    show() >> apply() >> after()


tfs_kafka_setup_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
