"""⚙️ Конфигурация и утилиты тракта Kafka ↔ ТФС.

Модуль намеренно **самодостаточен**: ничего не импортирует из `er_export`. Транспорт
обслуживает не только ЕР (следующим переезжает xStream), а в проде каталоги разворачиваются
по отдельности — зависимость от чужого пакета означала бы, что приём квитанций не поднять
без выгрузок ER.

Цена самодостаточности — копии `on_callback`, `add_note` и `get_dict`. Так же поступил
и `er_config`, скопировав их из `plugins.utils`: у DAG-ов этого контура не должно быть
внешних зависимостей, кроме провайдеров Airflow.

🔗 Общий контракт с `er_export/er_config.py` — имена двух таблиц. Они продублированы
там же; менять синхронно.
"""
from __future__ import annotations

import logging
import unicodedata
from datetime import timedelta

CH_ID = 'dlab-click'

# IN/OUT в conn_id и топиках — сторона ТФС: пишем мы в его вход, читаем из его выхода.
KAFKA_SND_CONN  = 'tfs-kafka-in'
KAFKA_SND_TOPIC = 'TFS.HRPLT.IN'

# Топики квитанций общие на ВСЕ маршруты ТФС, поэтому читает их ровно один потребитель —
# даг tfs_kafka_rcv. Выгрузки сюда не ходят: они ждут строку в RECEIPTS_TABLE.
# Список, а не строка: одним коннектом слушаем несколько топиков сразу, и добавление
# нового маршрута сводится к строчке здесь.
KAFKA_RCV_CONN   = 'tfs-kafka-out'
KAFKA_RCV_TOPICS = ['TFS.HRPLT.OUT']

# 📇 Таблицы тракта. Дублируются в er_export/er_config.py — это общий контракт двух
# каталогов, и другого способа не связать их импортом нет.
RECEIPTS_TABLE   = 'export.tfs_receipts'    # квитанции из Kafka, общие для всех маршрутов
SENT_FILES_TABLE = 'export.er_sent_files'   # очередь и реестр отправок ER

# 🚦 Лимиты ТФС на маршрут: файлов в секунду / минуту / час / сутки.
# ТФС отбивает лишние файлы, соблюдать темп должны мы сами. Значения задекларированы
# в документации маршрутов; счётчики на их стороне, предположительно, считают сообщения
# в Kafka — гипотеза не доказана, поэтому числа держим здесь и правим по факту.
# Все окна скользящие: полночь суточный бюджет не обнуляет.
TFS_LIMITS_DEFAULT = {'sec': 10, 'min': 200, 'hour': 500, 'day': 2000}
TFS_LIMITS: dict[str, dict[str, int]] = {
    'HRPLATFORM-2100': {'sec': 1, 'min': 15, 'hour': 100, 'day': 500},
}

# Очередь старше этого возраста (мин) роняет даг-отправитель: затор должен быть виден
# в мониторинге, а не только в логе.
TFS_QUEUE_ALERT_MIN = 60

# 🔒 Пул на 1 слот: в ТФС пишет кто-то один. Его берёт tfs_kafka_snd и обязан брать любой
# даг, который шлёт в ТФС МИМО очереди.
#
# Что даёт и чего не даёт: взаимное исключение — да, соблюдение лимитов — нет. Отправитель
# мимо очереди не пишет в SENT_FILES_TABLE, поэтому его файлы не попадут в счётчики.
#
# Пул один общий, а не tfs_{scenario}: пул назначается таску при разборе файла, а
# tfs_kafka_snd — один таск на все сценарии и заранее не знает, чьи файлы попадутся.
TFS_SEND_POOL  = 'tfs_send'
TFS_SEND_SLOTS = 1

# Пул приёмника — отдельный, чтобы чтение квитанций не ждало отправку.
TFS_RCV_POOL   = 'default_pool'

logger = logging.getLogger("airflow.task")

def _on_callback(context, level=None):
    """Обработчик on_failure_callback/on_success_callback — формирует заметку о статусе таска/DAG в Airflow UI."""
    from airflow.utils.state import TaskInstanceState
    from datetime import datetime

    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    dag_id = ti.dag_id
    dag_state = dag_run.state

    if not level:
        if dag_state.lower() in ('success', 'failed') or not context.get('task'):
            level = 'DAG'
        else:
            level = 'task'

    if level == 'DAG':
        tis = dag_run.get_task_instances()
        finished_tis = [t for t in tis if t.end_date is not None]
        task = sorted(finished_tis, key=lambda x: x.end_date, reverse=True)[0] if finished_tis else ti
    else:
        task = ti

    task_id = task.task_id
    map_index = task.map_index
    map_ind_str = getattr(task, 'rendered_map_index', '')
    try_number = task.try_number
    state = task.state

    msg = context.get('exception')

    map_str = f"   *Map Index*: ({map_index}) **{map_ind_str}**" if str(map_index) != '-1' else ""
    try_str = f"   *Try number*: **{try_number}**" if try_number > 1 else ""
    msg_str = f"```\n{str(msg)[:1000]}\n```\n" if msg else ""

    task_msg = '❌ FAILED' if state.lower() == 'failed' else '✅ SUCCESS' if state.lower() == 'success' else state.upper()

    message = f"*{datetime.now().astimezone().strftime('%d.%m.%Y %H:%M:%S %Z')}*\n\n"

    if level == 'DAG':
        dag_msg = '❌ FAILED' if dag_state.lower() == 'failed' else '✅ SUCCESS'
        message += f"*DAG:* **{dag_id}**: **{dag_msg}**\n\n"
    else:
        message += (
            f"*Task:* **{task_id}**: **{task_msg}**\n\n"
            f"{try_str}{map_str}\n\n"
            f"{msg_str}"
        )

    add_note(message, context, level, add=True)

    if state == TaskInstanceState.SUCCESS:
        logger.info(message)
    elif state == TaskInstanceState.FAILED:
        logger.error(message)
        if level != 'DAG':
            add_note(message, context, level='DAG', add=True)
    else:
        logger.warning(message)


def on_callback(context, level=None):
    return _on_callback(context, level)


def add_note(msg, context=None, level='task', add=True, title='', compact=False):
    """📝 Добавляет структурированную заметку в Airflow UI (Task и/или DAG Run).

    level  — 'task', 'dag' или 'task,dag' (регистр не важен; макс. 2 объекта)
    add    — True = prepend к существующей заметке; False = заменить полностью
    compact — передаётся в PrettyPrinter для компактного форматирования dict/list

    Скопировано из plugins.utils для устранения внешней зависимости.
    """
    from airflow.utils.session import create_session
    from airflow.operators.python import get_current_context
    from pprint import PrettyPrinter

    MAX_NOTE_LEN = 1000

    if not context:
        try:
            context = get_current_context()
        except Exception:
            logger.warning("Could not get Airflow context for add_note")
            return

    # Если передан dict с одним ключом — распаковываем его как заголовок
    if isinstance(msg, dict) and len(msg) == 1:
        t, msg = next(iter(msg.items()))
        title += str(t) + (f' ({len(msg)})' if isinstance(msg, (dict, list, tuple, set)) else '')

    if not isinstance(msg, str):
        msg = PrettyPrinter(indent=4, compact=compact).pformat(msg).replace("'", '')
        msg = '```\n' + msg + '\n```'

    logger.info("📝 Note added to %s %s:\n%s", level, title, msg)

    with create_session() as session:
        for lvl in list(set(level.upper().split(',')))[:2]:
            new_note = msg.strip()
            obj = session.merge(context['dag_run'] if lvl == 'DAG' else context['task_instance'])
            session.expire(obj)

            if title:
                # Если title не начинается с emoji — добавляем стандартный префикс
                if not unicodedata.category(title[0]) == 'So':
                    title = "📝 " + title
                new_note = f"{title}\n---\n{new_note}"

            # Пропускаем если заметка уже начинается с того же текста (идемпотентность)
            if obj.note and obj.note.startswith(new_note[:MAX_NOTE_LEN]):
                continue

            if add:
                new_note = f"{new_note}\n\n---\n{obj.note if obj.note else ''}"

            obj.note = new_note[:MAX_NOTE_LEN]


def get_dict(ch_hook, sql: str) -> list[dict]:
    """🔍 Выполняет SQL и возвращает результат как список словарей {column: value}."""
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    return []


def tfs_limits(scenario_id: str) -> dict[str, int]:
    """🚦 Лимиты маршрута: свои из TFS_LIMITS либо общие TFS_LIMITS_DEFAULT."""
    return TFS_LIMITS.get(scenario_id, TFS_LIMITS_DEFAULT)


def send_budget(counts: dict[str, int], limits: dict[str, int]) -> tuple[int, str]:
    """🧮 Сколько файлов можно отправить прямо сейчас и какой лимит упёрся первым.

    counts — уже отправлено за окно: {'sec', 'min', 'hour', 'day'}
    limits — потолки по тем же окнам

    Возвращает (сколько можно, имя упёршегося лимита или '').
    Минимум по всем окнам: свободен тот бюджет, что кончается раньше всех.
    """
    free = {w: limits[w] - counts.get(w, 0) for w in ('sec', 'min', 'hour', 'day')}
    window = min(free, key=lambda w: free[w])
    allowed = max(free[window], 0)
    return allowed, (window if allowed == 0 else '')


def parse_receipt(raw: str, partition: int = -1, offset: int = -1) -> dict:
    """📨 Разбирает XML обратной квитанции TransferFileCephRs.

    Битый XML не роняет разбор: возвращается строка со status_code = -1 и текстом
    в raw_xml. Потерять квитанцию хуже, чем сохранить её неразобранной, а застрявшее
    сообщение заблокировало бы очередь.

    findtext с '{*}' и без: у ТФС встречаются оба варианта — с неймспейсом и без.
    """
    import xml.etree.ElementTree as ET
    from datetime import datetime

    row = {
        'rq_uid': '', 'file_name': '', 'scenario_id': '',
        'status_code': -1, 'rq_tm': None, 'raw_xml': raw,
        'kafka_partition': partition, 'kafka_offset': offset,
    }

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as err:
        logger.error("❌ Квитанция не разобрана как XML (%s): %.500s", err, raw)
        return row

    def _text(tag: str) -> str:
        return (root.findtext(f'.//{{*}}{tag}') or root.findtext(f'.//{tag}') or '').strip()

    row['rq_uid']      = _text('RqUID')
    row['file_name']   = _text('Name')
    row['scenario_id'] = _text('ScenarioId')

    code = _text('StatusCode')
    try:
        row['status_code'] = int(code)
    except ValueError:
        logger.error("❌ StatusCode '%s' не число, RqUID=%s", code, row['rq_uid'])
        return row

    rq_tm = _text('RqTm')
    if rq_tm:
        try:
            row['rq_tm'] = datetime.fromisoformat(rq_tm)
        except ValueError:
            logger.warning("⚠️ RqTm '%s' не разобран, RqUID=%s", rq_tm, row['rq_uid'])

    return row

DEF_ARGS = {
    "owner":            "DataLab (CI02420667)",
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry":   False,
    "on_failure_callback": on_callback,
}


def ensure_pools() -> None:
    """🏊 Заводит пулы тракта, если их ещё нет.

    Вызывать из таска, а не при разборе файла: иначе сессия БД открывалась бы на каждом
    обходе scheduler-ом. Делает это приёмник — он ходит раз в минуту и сам сидит
    в default_pool, поэтому создаст tfs_send до того, как отправителю понадобится слот
    (таск с несуществующим пулом Airflow просто не поставит в очередь).
    """
    from airflow.models import Pool
    from airflow.utils.session import create_session

    desc = ('Отправка в ТФС: не больше одного отправителя одновременно. Берёт tfs_kafka_snd '
            'и обязан брать любой даг, шлющий в ТФС мимо очереди. Лимиты маршрута пул '
            'НЕ соблюдает — только взаимное исключение')

    with create_session() as session:
        exists = session.query(Pool).filter(Pool.pool == TFS_SEND_POOL).first()
        if not exists:
            session.add(Pool(pool=TFS_SEND_POOL, slots=TFS_SEND_SLOTS,
                             description=desc, include_deferred=False))
            logger.info("🏊 Создан пул %s (%d слот)", TFS_SEND_POOL, TFS_SEND_SLOTS)


def get_config() -> dict:
    """📦 Снимок констант модуля для передачи в DAG-файлы."""
    return {
        'CH_ID':            CH_ID,
        'DEF_ARGS':         DEF_ARGS,
        'KAFKA_SND_CONN':   KAFKA_SND_CONN,
        'KAFKA_SND_TOPIC':  KAFKA_SND_TOPIC,
        'KAFKA_RCV_CONN':   KAFKA_RCV_CONN,
        'KAFKA_RCV_TOPICS': KAFKA_RCV_TOPICS,
        'RECEIPTS_TABLE':   RECEIPTS_TABLE,
        'SENT_FILES_TABLE': SENT_FILES_TABLE,
        'TFS_LIMITS':       TFS_LIMITS,
        'TFS_LIMITS_DEFAULT': TFS_LIMITS_DEFAULT,
        'TFS_QUEUE_ALERT_MIN': TFS_QUEUE_ALERT_MIN,
        'TFS_SEND_POOL':    TFS_SEND_POOL,
        'TFS_SEND_SLOTS':   TFS_SEND_SLOTS,
        'TFS_RCV_POOL':     TFS_RCV_POOL,
    }
