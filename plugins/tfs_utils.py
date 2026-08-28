"""⚙️ Конфигурация, утилиты и хранилище тракта Kafka ↔ ТФС.
*2026-08-28 19:30 MSK · v1.15 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Живёт в `plugins`, а не рядом с дагами, по той же причине, что `ctl_utils` и `ctl_core`:
модулем пользуются ДВА каталога — `tfs_kafka` (приём и отправка) и `er_export`
(постановка в очередь, ожидание квитанций). Держать конфиг у одного из них значило бы
либо связать каталоги между собой, либо развести настройку по копиям.

🔑 Копии здесь недопустимы принципиально. Если приёмник запишет квитанцию в ClickHouse,
а `wait_confirm` пойдёт искать её в S3 — пакет зависнет до таймаута, и причина не будет
видна ниоткуда. Поэтому источник истины ровно один, и он тут.

🗄️ **S3 — источник истины.** Пишем туда всегда и читаем только оттуда: писатель и
читатель обязаны смотреть в одно место. ClickHouse и Postgres — зеркала для аналитики
и глаз: пишем в них, если задан их conn_id, и их сбой тракт не роняет. Три реализации
дают одинаковые сигнатуры, различие только в том, кто из них обязателен.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import timedelta

# Общие хелперы Airflow берём из plugins.utils, а не держим свои копии: заметки и
# колбэки должны вести себя одинаково во всех DAG-ах контура. add_note и ensure_pool
# здесь же реэкспортируются — их импортируют соседние модули этого каталога.
try:
    from plugins.utils import add_note, ensure_pool, get_dict_from_ch, on_callback, query_to_dict  # noqa: F401  # type: ignore
    from plugins.s3_utils import s3_move_s3, s3_path_parse  # type: ignore
except ImportError:
    from CI06932748.tools.utils import add_note, ensure_pool, get_dict_from_ch, on_callback, query_to_dict  # noqa: F401  # type: ignore
    from CI06932748.tools.s3_utils import s3_move_s3, s3_path_parse  # type: ignore

CH_ID = 'dlab-click'   # зеркало тракта в ClickHouse; пусто — выключено

# 🌍 Контур. Платформа выставляет ENV_SPACE='alpha' в airflow_entrypoint.py:75, всё
# остальное считаем sigma. ENV_STAND для этого не годится: DEV и PROM есть у обоих
# контуров, и по нему альфу от сигмы не отличить.
ENV_SPACE = 'alpha' if (os.getenv('ENV_SPACE') or '').strip().lower() == 'alpha' else 'sigma'

# IN/OUT в conn_id и топиках — сторона ТФС: пишем мы в его вход, читаем из его выхода.
KAFKA_SND_CONN  = 'tfs-kafka-in'

# Топики отправки НА КОНТУР: на альфе маршрут ПКАП, на сигме — HRPLT. Список, а не
# строка: на каждый топик в tfs_kafka_snd заводится свой таск-сенсор (как у приёмника),
# и добавление маршрута сводится к строчке здесь. Какой файл в какой топик — решает
# не этот список, а топик его сценария в TFS_ROUTES.
SND_TOPICS_BY_SPACE = {
    'alpha': ['TFS.PKAPHR.IN'],
    'sigma': ['TFS.HRPLT.IN'],
}
KAFKA_SND_TOPICS = SND_TOPICS_BY_SPACE[ENV_SPACE]

# Топик по умолчанию — первый в списке. В него уезжают файлы сценариев, которых нет
# в TFS_ROUTES: маршрут может появиться раньше строчки в коде, и лучше отправить туда,
# куда ходит весь контур, чем держать файл в очереди до разбирательства.
DEFAULT_SND_TOPIC = KAFKA_SND_TOPICS[0]

# Топики квитанций общие на ВСЕ маршруты ТФС, поэтому читает их ровно один потребитель —
# даг tfs_kafka_rcv. Выгрузки сюда не ходят: они ждут строку в RECEIPTS_TABLE.
# Список, а не строка: одним коннектом слушаем несколько топиков сразу, и добавление
# нового маршрута сводится к строчке здесь.
KAFKA_RCV_CONN   = 'tfs-kafka-out'
KAFKA_RCV_TOPICS = ['TFS.HRPLT.OUT']

# 🗄️ Где держим квитанции и очередь отправки.
#
# S3 (объекты там же, где логи, см. s3_base) — обязателен: туда идёт каждая запись,
# оттуда идёт КАЖДОЕ чтение. Зеркала подключаются непустым conn_id и нужны только
# чтобы смотреть на тракт запросами:
#
#   CH_ID   — ClickHouse (DDL: tfs_kafka/tfs_receipts.sql, er_export/er_sent_files.sql)
#   PG_CONN — Greenplum или PostgreSQL, один код на оба (DDL: *_pg.sql)
#
# Сбой зеркала только предупреждает: запись в S3 к этому моменту уже прошла, и ронять
# приём квитанций из-за аналитической копии нельзя. Расхождение лечится дозаливкой.

# 📇 Таблицы тракта — для зеркал в ClickHouse и Postgres.
RECEIPTS_TABLE   = 'export.tfs_receipts'    # квитанции из Kafka, общие для всех маршрутов
SENT_FILES_TABLE = 'export.er_sent_files'   # очередь и реестр отправок ER

# Соединение зеркала в Postgres. Нужно ЗАПИСЫВАЮЩЕЕ: alpha-adb_dev_comm-read по имени
# только на чтение. Пусто — зеркало выключено.
PG_CONN = ''

# Префикс тракта внутри логового бакета.
S3_PREFIX = 'tfs'

# Класть тракт ВНУТРЬ префикса логов или в корень бакета.
#   False — {бакет}/tfs/…                      (по умолчанию)
#   True  — {бакет}/{префикс логов}/tfs/…      (как было до 2026-08-15)
# Переключение меняет ПУТЬ К ДАННЫМ: очередь и квитанции, записанные при старом значении,
# по новому пути не найдутся — файл не уедет, а wait_confirm провисит до таймаута.
# Менять только на пустом тракте либо перенеся объекты (см. tfs_kafka/README.md).
S3_UNDER_LOG_PREFIX = False

# 🚦 Лимиты ТФС на маршрут: файлов в секунду / минуту / час / сутки.
# ТФС отбивает лишние файлы, соблюдать темп должны мы сами. Значения задекларированы
# в документации маршрутов; счётчики на их стороне, предположительно, считают сообщения
# в Kafka — гипотеза не доказана, поэтому числа держим здесь и правим по факту.
# Все окна скользящие: полночь суточный бюджет не обнуляет.
TFS_LIMITS_DEFAULT = {'sec': 10, 'min': 200, 'hour': 500, 'day': 2000}

# 🗺️ Справочник маршрутов: всё про сценарий в одном месте — топик отправки и лимиты.
# Топик здесь, а не у отправителя, потому что решение «в какой топик» принимается
# по файлу в очереди, а у файла из всех признаков маршрута есть только scenario_id.
#
# conn/bucket/prefix — где лежат файлы маршрута. Нужны ручной досылке: она проверяет,
# что объект ещё в бакете, и только тогда заводит новый RqUID. Забранный ТФС файл
# переотправить нельзя — его там уже нет, и повтор вернулся бы квитанцией с ошибкой.
# У ЕР те же значения продублированы в er_config.py (S3_CONN/BUCKET/TFS_MAP): фабрика
# намеренно не импортирует этот модуль на уровне файла, чтобы неполная выкладка тракта
# не роняла все пакеты разом.
TFS_ROUTES: dict[str, dict] = {
    'HRPLATFORM-4000': {'topic': 'TFS.HRPLT.IN', 'limits': TFS_LIMITS_DEFAULT,
                        'conn': 's3-tfs-hrplt', 'bucket': 'tfshrplt',
                        'prefix': 'from/KAP802/hrpl_lm_er'},
    'HRPLATFORM-2100': {'topic': 'TFS.HRPLT.IN',
                        'limits': {'sec': 1, 'min': 15, 'hour': 100, 'day': 500}},
    # ПКАП (альфа, TFS.PKAPHR.IN): ScenarioId ещё не выдан. Пока строки нет, его файлы
    # уедут в топик по умолчанию — с предупреждением в логе (route_topic).
}

# Очередь старше этого возраста (мин) роняет даг-отправитель: затор должен быть виден
# в мониторинге, а не только в логе. Придержанные паузой строки в этот счёт НЕ идут —
# осознанная пауза не авария.
TFS_QUEUE_ALERT_MIN = 60

# ⏸️ Пауза отправки. Файлы продолжают вставать в очередь по расписанию, но отправитель
# их не берёт — очередь копится, а после снятия паузы уезжает сама. Нужна на технические
# работы, выкладку доработок и сбои на стороне ТФС: выгрузка данных длинная и дорогая,
# останавливать её ради паузы в отправке незачем.
#
# Правится дагом-пультом tfs_kafka_setup; формат — три раздела, в каждом {ключ: правило}:
#
#   {"scenarios": {"HRPLATFORM-4000": {"until": "2026-09-01T22:00:00+03:00",
#                                      "reason": "техработы ТФС"}},
#    "replicas":  {"hrplatform_datalab__1": "внедрение доработок"},
#    "packages":  {"hrplatform_datalab__1__20260828120000": "разбираемся с отказом"}}
#
# Правило — либо строка-причина, либо объект с until/reason/by. until обязательным не
# сделан, но настоятельно рекомендован: забытая пауза копит очередь молча.
PAUSE_VAR = 'tfs_snd_pause'
PAUSE_SCOPES = ('packages', 'replicas', 'scenarios')

# Сколько минут ждать квитанцию, прежде чем считать отправку неподтверждённой. Нужно
# потому, что пакет, уехавший при снятой паузе, никто не ждёт: его wait_confirm ушёл
# в скип (см. er_export._pre_await), и без этой сверки отказ ТФС остался бы незамеченным.
TFS_STALE_MIN = 180

# 🔒 Пул на 1 слот НА СЦЕНАРИЙ: файлы одного маршрута отправляет кто-то один. Пул своего
# сценария берёт таск tfs_kafka_snd и обязан брать любой даг, который шлёт в ТФС МИМО
# очереди — именно так делает xs_export (xs_common.py, tfs_out_pool).
#
# Имя ровно `tfs_<scenario_id>`: такие пулы уже заведены на контуре (tfs_HRPLATFORM-4000,
# tfs_HRPLATFORM-2100, tfs_KKA-407010, tfs_27671910…). Совместимость тут и есть смысл —
# пул с другим именем не пересекался бы с чужим отправителем того же маршрута, то есть
# не давал бы ничего.
#
# Что даёт и чего не даёт: взаимное исключение — да, соблюдение лимитов — нет. Отправитель
# мимо очереди не пишет в SENT_FILES_TABLE, поэтому его файлы не попадут в счётчики.
TFS_SEND_POOL_PREFIX = 'tfs_'
TFS_SEND_SLOTS = 1

# 📇 Реестр сценариев: {scenario_id: когда впервые увидели в очереди, ISO}. Пишет его
# таск scan_queue, читает парсинг tfs_kafka_snd — по одному таску на сценарий.
# Сценарии из TFS_ROUTES в реестре не нуждаются: они попадают в список и без него.
SCENARIOS_VAR = 'tfs_snd_scenarios'

# Пул приёмника — отдельный, чтобы чтение квитанций не ждало отправку.
TFS_RCV_POOL   = 'default_pool'

logger = logging.getLogger("airflow.task")

def tfs_route(scenario_id: str) -> dict:
    """🗺️ Запись маршрута из TFS_ROUTES; пустой словарь, если сценарий не описан."""
    return TFS_ROUTES.get(scenario_id) or {}


def tfs_limits(scenario_id: str) -> dict[str, int]:
    """🚦 Лимиты маршрута: свои из TFS_ROUTES либо общие TFS_LIMITS_DEFAULT."""
    return tfs_route(scenario_id).get('limits') or TFS_LIMITS_DEFAULT


def route_topic(scenario_id: str) -> str:
    """📮 Топик отправки маршрута; иначе DEFAULT_SND_TOPIC.

    Топик по умолчанию, а не отказ: иначе файл сценария, который ещё не завели
    в TFS_ROUTES, лежал бы в очереди до правки кода.

    Топик маршрута берётся, ТОЛЬКО если он есть на этом контуре. Иначе файл был бы ничьим
    так же, как без маршрута вовсе: таски заводятся по KAFKA_SND_TOPICS, и строку с чужим
    топиком не забрал бы ни один из них. Живой пример: на альфе HRPLATFORM-4000 указывает
    на TFS.HRPLT.IN, которого там нет.

    Функция намеренно молчит: её зовут на каждую строку очереди в каждом опросе, то есть
    четыре раза в минуту на файл. О подмене предупреждает отправитель — один раз, в момент
    отправки (`tfs_kafka_snd`), где это и видно в логе рядом с самим файлом.
    """
    topic = tfs_route(scenario_id).get('topic')
    return topic if topic in KAFKA_SND_TOPICS else DEFAULT_SND_TOPIC


def scenario_pool(scenario_id: str) -> str:
    """🔒 Пул сценария: HRPLATFORM-4000 → tfs_HRPLATFORM-4000.

    Имя не выдумано, а взято у уже существующих пулов контура — см. комментарий
    к TFS_SEND_POOL_PREFIX.
    """
    return f"{TFS_SEND_POOL_PREFIX}{scenario_id}"


def task_slug(scenario_id: str) -> str:
    """scenario_id → безопасный кусок task_id.

    Airflow допускает в task_id только [A-Za-z0-9_.-]; настоящие сценарии
    (HRPLATFORM-4000, KKA-407010, 27671910) проходят как есть, но реестр наполняется
    из очереди, а туда сценарий приходит извне — поэтому чужие символы заменяются.
    """
    return re.sub(r'[^A-Za-z0-9_.-]', '_', scenario_id)


def known_scenarios() -> list[str]:
    """📇 Сценарии, на которые заводятся таски: TFS_ROUTES ∪ реестр из Variable.

    Читается НА ПАРСИНГЕ DAG-файла, поэтому Variable берётся через try/except: без неё
    остаются сценарии из конфига, а не Broken DAG. Сценарий из очереди попадает сюда
    через scan_queue — своим таском он обзаведётся на следующем разборе файла.
    """
    from airflow.models import Variable

    found = set(TFS_ROUTES)
    try:
        found |= set(Variable.get(SCENARIOS_VAR, deserialize_json=True, default_var=None) or {})
    except Exception as exc:
        logger.warning("⚠️ Реестр сценариев %s не прочитан (%s), берём только TFS_ROUTES",
                       SCENARIOS_VAR, exc)
    return sorted(s for s in found if s)


def remember_scenarios(found: list[str]) -> list[str]:
    """📇 Дописывает незнакомые сценарии в реестр; возвращает добавленные.

    Существующие записи не трогаются: в них время ПЕРВОЙ встречи, по нему потом видно,
    когда маршрут появился.
    """
    from datetime import datetime, timezone

    from airflow.models import Variable

    known = Variable.get(SCENARIOS_VAR, deserialize_json=True, default_var=None) or {}
    new = [s for s in dict.fromkeys(found) if s and s not in known]
    if not new:
        return []

    stamp = datetime.now(timezone.utc).isoformat(timespec='seconds')
    known.update({s: stamp for s in new})
    Variable.set(SCENARIOS_VAR, known, serialize_json=True)
    return new


def drop_unpooled_scenarios() -> list[str]:
    """🗑️ Убирает из реестра сценарии, у которых удалили пул; возвращает убранные.

    Пул — выключатель маршрута. Удалили `tfs_<scenario>` руками — значит сценарий больше
    не нужен: он уходит из реестра, а его таск исчезает на следующем разборе DAG-файла
    (в идущем ране Airflow пометит его `removed`). Другого способа убрать таск нет —
    реестр наполняется автоматически, и сам по себе сценарий из него не выпадает.

    Сценарии из TFS_ROUTES так не выключить: их пул заводится заново из конфига
    (`ensure_pools`), да и таск у них есть и без реестра. Не сработает выключатель и на
    сценарии, чьи файлы ещё лежат в очереди: следующий же скан увидит их и заведёт всё
    обратно — сначала надо разобрать очередь.
    """
    from airflow.models import Pool, Variable
    from airflow.utils.session import create_session

    known = Variable.get(SCENARIOS_VAR, deserialize_json=True, default_var=None) or {}
    if not known:
        return []

    with create_session() as session:
        alive = {p.pool for p in session.query(Pool).all()}

    gone = [s for s in known if scenario_pool(s) not in alive]
    if not gone:
        return []

    for s in gone:
        known.pop(s, None)
    Variable.set(SCENARIOS_VAR, known, serialize_json=True)
    return gone


# ── ⏸️ Пауза отправки ─────────────────────────────────────────────────────────

def package_key(row: dict) -> str:
    """🔑 Имя пакета: replica + метка пакета — 'hrplatform_datalab__1__20260828120000'.

    Тем же ключом сенсор отправителя раскладывает XCom, и это не совпадение: имя пакета
    должно быть одно на весь тракт, иначе поставленную на паузу строку пришлось бы искать
    глазами по другому написанию того же самого.
    """
    digits = ''.join(c for c in str(row.get('package_ts') or '') if c.isdigit())
    return f"{row.get('replica', '')}__{digits[:14]}"


def _pause_raw() -> dict:
    """Переменная паузы как есть, с гарантированными разделами. Ничего не бросает."""
    from airflow.models import Variable

    try:
        raw = Variable.get(PAUSE_VAR, default_var={}, deserialize_json=True) or {}
    except Exception as exc:
        logger.warning("⚠️ %s не прочитана (%s) — считаем, что паузы нет", PAUSE_VAR, exc)
        raw = {}
    if not isinstance(raw, dict):
        logger.warning("⚠️ %s содержит %s, а нужен объект — считаем, что паузы нет",
                       PAUSE_VAR, type(raw).__name__)
        raw = {}
    return {scope: dict(raw.get(scope) or {}) for scope in PAUSE_SCOPES}


def pause_rules() -> dict:
    """⏸️ Действующие правила паузы: {'packages'|'replicas'|'scenarios': {ключ: правило}}.

    Нормализует строку-причину к объекту и выбрасывает истёкшие по until. **Не бросает
    ничего**: битая переменная означает «паузы нет», а не остановку тракта — эту функцию
    зовёт каждый опрос отправителя.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    out: dict = {scope: {} for scope in PAUSE_SCOPES}

    for scope, entries in _pause_raw().items():
        for key, rule in entries.items():
            if isinstance(rule, str):
                rule = {'reason': rule}
            if not isinstance(rule, dict):
                logger.warning("⚠️ %s/%s/%s: правило должно быть строкой или объектом — пропущено",
                               PAUSE_VAR, scope, key)
                continue

            until = None
            if rule.get('until'):
                until = parse_ts(rule['until'])
                if until is None:
                    logger.warning("⚠️ %s/%s/%s: until '%s' не разобран — правило пропущено",
                                   PAUSE_VAR, scope, key, rule['until'])
                    continue
                if until <= now:
                    logger.info("⌛ %s/%s/%s: пауза истекла %s — не действует",
                                PAUSE_VAR, scope, key, rule['until'])
                    continue

            out[scope][str(key)] = {
                'reason': str(rule.get('reason') or ''),
                'until':  str(rule.get('until') or ''),
                'by':     str(rule.get('by') or ''),
            }
    return out


def pause_reason(row: dict, rules: dict | None = None) -> str:
    """⏸️ Почему строка очереди стоит; пусто — ехать можно.

    Порядок от частного к общему (пакет → реплика → маршрут): в сообщении должно стоять
    самое конкретное правило, иначе «маршрут на паузе» будет написано и там, где на самом
    деле придержан один пакет.
    """
    rules = pause_rules() if rules is None else rules
    titles = {'packages': 'пакет', 'replicas': 'реплика', 'scenarios': 'маршрут'}

    for scope, key in (('packages', package_key(row)),
                       ('replicas', str(row.get('replica') or '')),
                       ('scenarios', str(row.get('scenario_id') or ''))):
        rule = rules.get(scope, {}).get(key) if key else None
        if not rule:
            continue
        text = f"{titles[scope]} {key} на паузе"
        if rule['until']:
            text += f" до {rule['until']}"
        if rule['reason']:
            text += f" — {rule['reason']}"
        return text
    return ''


def split_pending(rows: list[dict], rules: dict | None = None) -> tuple[list, list]:
    """⏸️ Очередь надвое: (свободные, придержанные). У придержанных добавлен pause_reason.

    Делить обязаны все одинаково: отправитель по этому же делению решает, что везти,
    а проверка затора — что НЕ считать простоем.
    """
    rules = pause_rules() if rules is None else rules
    free, held = [], []
    for row in rows:
        reason = pause_reason(row, rules)
        (held if reason else free).append({**row, 'pause_reason': reason} if reason else row)
    return free, held


def pause_summary(rows: list[dict] | None = None) -> dict:
    """📊 Что сейчас на паузе и сколько строк очереди этим придержано."""
    rules = pause_rules()
    summary: dict = {'rules': rules, 'held': 0, 'by_reason': {}}
    if rows is None:
        return summary

    _, held = split_pending(rows, rules)
    summary['held'] = len(held)
    for row in held:
        summary['by_reason'][row['pause_reason']] = summary['by_reason'].get(row['pause_reason'], 0) + 1
    return summary


def _pause_write(data: dict, note: str = '') -> None:
    from datetime import datetime, timedelta, timezone

    from airflow.models import Variable

    stamp = datetime.now(timezone(timedelta(hours=3))).strftime('%Y-%m-%d %H:%M:%S')
    Variable.set(PAUSE_VAR, {scope: data.get(scope) or {} for scope in PAUSE_SCOPES},
                 serialize_json=True, description=f"{stamp} MSK · {note}".strip(' ·'))


def pause_set(scope: str, key: str, until: str = '', reason: str = '', by: str = '',
              note: str = '') -> tuple[str, str]:
    """⏸️ Ставит правило паузы. Возвращает (status, message): 'ok' | 'skip' | 'fail'.

    Статусом, а не исключением: решение «скип или падение» принимает таск пульта — там же,
    где стоят trigger_rule и текст, который увидит человек (так же устроен store_params
    в plugins/utils.py).
    """
    from datetime import datetime, timezone

    if scope not in PAUSE_SCOPES:
        return 'fail', f"область '{scope}' неизвестна, можно: {', '.join(PAUSE_SCOPES)}"

    key = str(key or '').strip()
    if not key:
        return 'fail', 'не задан ключ — что именно ставим на паузу'
    if scope == 'packages' and not re.fullmatch(r'.+__\d{14}', key):
        return 'fail', (f"ключ пакета '{key}' не того вида: нужен "
                        "'<реплика>__<14 цифр метки>', например hrplatform_datalab__1__20260828120000")

    until = str(until or '').strip()
    if until:
        parsed = parse_ts(until)
        if parsed is None:
            return 'fail', f"until '{until}' не разобран; нужен ISO, например 2026-09-01T22:00:00+03:00"
        if parsed <= datetime.now(timezone.utc):
            return 'fail', f"until '{until}' уже прошёл — такая пауза не подействует ни секунды"

    rule = {k: v for k, v in {'until': until, 'reason': str(reason or '').strip(),
                              'by': str(by or '').strip()}.items() if v}
    data = _pause_raw()
    if data[scope].get(key) == (rule or {}):
        return 'skip', f"{scope}/{key}: уже стоит с теми же параметрами"

    data[scope][key] = rule
    _pause_write(data, note)
    if not until:
        logger.warning("⚠️ %s/%s поставлен на паузу БЕЗ срока — очередь будет копиться, "
                       "пока паузу не снимут руками", scope, key)
    return 'ok', f"{scope}/{key} на паузе" + (f" до {until}" if until else " бессрочно")


def pause_clear(scope: str, key: str, note: str = '') -> tuple[str, str]:
    """▶️ Снимает правило паузы. (status, message), см. pause_set."""
    if scope not in PAUSE_SCOPES:
        return 'fail', f"область '{scope}' неизвестна, можно: {', '.join(PAUSE_SCOPES)}"

    key = str(key or '').strip()
    if not key:
        return 'fail', 'не задан ключ — что именно снимаем с паузы'

    data = _pause_raw()
    if key not in data[scope]:
        return 'skip', f"{scope}/{key}: такого правила нет — снимать нечего"

    data[scope].pop(key)
    _pause_write(data, note)
    return 'ok', f"{scope}/{key} снят с паузы"


def pause_clean_expired(note: str = '') -> tuple[str, str]:
    """🧹 Убирает из переменной правила с прошедшим until.

    Действовать они перестают и сами (pause_rules их отбрасывает), но в переменной
    копятся и мешают читать, что на паузе прямо сейчас.
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    data, gone = _pause_raw(), []
    for scope, entries in data.items():
        for key, rule in list(entries.items()):
            until = rule.get('until') if isinstance(rule, dict) else None
            parsed = parse_ts(until) if until else None
            if parsed is not None and parsed <= now:
                entries.pop(key)
                gone.append(f"{scope}/{key}")

    if not gone:
        return 'skip', 'истёкших правил нет'
    _pause_write(data, note)
    return 'ok', 'убраны истёкшие: ' + ', '.join(gone)


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


def parse_rq_tm(raw: str):
    """🕐 RqTm квитанции → datetime. Пусто и неразбираемое → None.

    Своя нормализация вместо голого fromisoformat, потому что `xsd:dateTime` шире того,
    что понимает Python 3.9:

      • смещение может быть записано как 'Z' — понимать его fromisoformat научился
        только в 3.11;
      • дробная часть секунд бывает любой длины, а 3.9 принимает ровно 3 или 6 знаков:
        и '…:00.12+03:00', и '…:00.123456789+03:00' роняли разбор одинаково.

    Цена ошибки тут не нулевая: метка времени квитанции терялась молча, оставаясь NULL
    при живой в остальном строке.
    """
    from datetime import datetime

    s = (raw or '').strip()
    if not s:
        return None

    if s[-1] in 'Zz':
        s = s[:-1] + '+00:00'

    # Дробную часть приводим ровно к шести знакам: короткую дополняем, длинную режем.
    # Наносекунды ТФС мы всё равно не храним — в колонке DateTime64(3).
    frac = re.match(r'^(.*?)\.(\d+)(.*)$', s)
    if frac:
        s = f"{frac.group(1)}.{(frac.group(2) + '000000')[:6]}{frac.group(3)}"

    try:
        return datetime.fromisoformat(s)
    except ValueError:
        logger.warning("⚠️ RqTm '%s' не разобран", raw)
        return None


# Один корневой документ квитанции целиком, с неймспейсом или без. Нужен запасному пути
# разбора: ET роняет весь документ из-за любого мусора вокруг корня, а квитанция при этом
# читаемая.
_RS_DOC_RE = re.compile(
    r'<(?:[A-Za-z_][\w.-]*:)?TransferFileCephRs\b.*?</(?:[A-Za-z_][\w.-]*:)?TransferFileCephRs\s*>',
    re.S,
)


def _status_text(node) -> str:
    """Текст причины из блока Status: основной StatusDesc плюс дополнительный.

    У настоящих отказов ТФС основного StatusDesc может не быть вовсе — текст лежит
    только в AdditionalStatus, и там же более конкретный код. Пример из боя:

        <Status><StatusCode>104</StatusCode>
            <AdditionalStatus><StatusCode>601</StatusCode>
                <StatusDesc>ошибка при разархивации [stage=KAFKA_DIFF_TOPICS] …</StatusDesc>

    Поэтому дополнительный статус не отбрасывается, а дописывается с его кодом: 104
    («ошибка смежной системы») сам по себе не говорит ничего, а «601 ошибка при
    разархивации …» показывает, что именно чинить.
    """
    def _find(parent, tag):
        found = parent.find(f'{{*}}{tag}')
        return found if found is not None else parent.find(tag)

    def _own(parent, tag) -> str:
        found = _find(parent, tag)
        return (found.text or '').strip() if found is not None else ''

    if node is None:
        return ''

    parts = [_own(node, 'StatusDesc')]

    extra = _find(node, 'AdditionalStatus')
    if extra is not None:
        code, desc = _own(extra, 'StatusCode'), _own(extra, 'StatusDesc')
        if code or desc:
            parts.append(f"[{code}] {desc}".strip() if code else desc)

    return ' | '.join(p for p in parts if p)


def parse_receipt(raw: str, partition: int = -1, offset: int = -1) -> list[dict]:
    """📨 Разбирает XML обратной квитанции TransferFileCephRs — по строке на файл.

    Список, а не одна строка: по спеке ТФС агрегат `File` идёт `[1-N]`, а `Status`
    лежит ВНУТРИ `File`, то есть статус у каждого файла свой. Прежний разбор брал первый
    `Name` и первый `StatusCode` во всём документе — на квитанции с двумя файлами он
    записал бы успех первого и потерял ошибку второго, а пакет подтвердился бы, не доехав.
    Ключ таблицы квитанций — (rq_uid, file_name), несколько строк на один RqUID она держит.

    Битый XML не роняет разбор: возвращается одна строка со status_code = -1 и текстом
    в raw_xml. Потерять квитанцию хуже, чем сохранить её неразобранной, а застрявшее
    сообщение заблокировало бы очередь. Цена такой строки, впрочем, немаленькая: у неё
    пустой rq_uid, то есть ждущая выгрузка её не найдёт и досидит до таймаута с диагнозом
    «ответа нет» — поэтому разбираем настолько терпимо, насколько можем.

    findall/findtext с '{*}' и без: у ТФС встречаются оба варианта — с неймспейсом и без.
    """
    import xml.etree.ElementTree as ET

    base = {
        'rq_uid': '', 'file_name': '', 'scenario_id': '',
        'status_code': -1, 'status_desc': '', 'rq_tm': None, 'raw_xml': raw,
        'kafka_partition': partition, 'kafka_offset': offset,
    }

    try:
        root = ET.fromstring(raw)
    except ET.ParseError as err:
        # Запасной путь, а не штатный: ТФС таких сообщений не присылал, страховка стоит
        # десяти строк и срабатывает только там, где иначе теряется всё. ET роняет разбор
        # от любого мусора вокруг корня — лишний текст до объявления, хвост после
        # закрывающего тега, два документа подряд («junk after document element»).
        # Вырезаем корневые блоки и разбираем каждый отдельно.
        #
        # Терять тут дороже, чем кажется: у нечитаемой строки пустой rq_uid, ждущая
        # выгрузка её не найдёт и досидит до таймаута с диагнозом «ответа нет».
        # Условие len(d) < len(raw) — не косметика, а гарантия остановки: рекурсия идёт
        # только по строке КОРОЧЕ исходной. Без него блок, равный всему сообщению и всё
        # равно не разбирающийся, вызывал бы сам себя вечно.
        docs = [d for d in _RS_DOC_RE.findall(raw) if len(d) < len(raw)]
        if docs:
            logger.warning("⚠️ Документ не разобран целиком (%s), но найдено корневых блоков: %d",
                           err, len(docs))
            rows = []
            for doc in docs:
                rows.extend(parse_receipt(doc, partition, offset))
            return rows

        logger.error("❌ Квитанция не разобрана как XML (%s): %.500s", err, raw)
        return [base]

    def _text(node, tag: str) -> str:
        return (node.findtext(f'.//{{*}}{tag}') or node.findtext(f'.//{tag}') or '').strip()

    base['rq_uid']      = _text(root, 'RqUID')
    base['scenario_id'] = _text(root, 'ScenarioId')
    base['rq_tm']       = parse_rq_tm(_text(root, 'RqTm'))

    files = root.findall('.//{*}File') or root.findall('.//File')
    if not files:
        # Квитанции без File по спеке не бывает, но терять сообщение из-за этого нельзя:
        # разбираем документ целиком и берём статус там, где он найдётся.
        logger.warning("⚠️ В квитанции нет ни одного File, RqUID=%s: %.300s", base['rq_uid'], raw)
        files = [root]

    rows = []
    for node in files:
        status = node.find('{*}Status')
        if status is None:
            status = node.find('Status')
        row = {**base, 'file_name': _text(node, 'Name'), 'status_desc': _status_text(status)}

        # Код берём из своего Status, а не первый попавшийся: в AdditionalStatus лежит
        # второй, и порядок документа — единственное, что их различало бы.
        code = _text(status, 'StatusCode') if status is not None else _text(node, 'StatusCode')
        try:
            row['status_code'] = int(code)
        except ValueError:
            logger.error("❌ StatusCode '%s' не число, RqUID=%s, файл '%s'",
                         code, base['rq_uid'], row['file_name'])
        rows.append(row)

    return rows


def build_message(scenario_id: str, rq_uid: str, file_name: str) -> str:
    """📤 Собирает XML-уведомление TransferFileCephRq с ГОТОВЫМ RqUID.

    RqUID приходит из очереди, а не генерируется здесь: он записан при постановке
    в очередь, и именно по нему потом ищется обратная квитанция.

    Файл в сообщении ровно один, хотя спека и допускает File [1-N]: свой RqUID на файл —
    это то, что даёт сопоставление квитанции с конкретной отправкой. Пакетом ушла бы
    одна квитанция на все файлы, и разбирать её пришлось бы по именам.

    FolderSource и FolderTarget не заполняются намеренно: спека помечает их «НЕ заполнять!»,
    они требуют отдельного согласования с командой ТФС.

    Живёт рядом с parse_receipt, а не в даге-отправителе: это формат тракта, и
    оба конца — что мы пишем, что нам отвечают — должны меняться в одном месте.
    """
    from datetime import datetime
    from xml.sax.saxutils import escape

    # isoformat(ms) воспроизводит формат pendulum 'YYYY-MM-DDTHH:mm:ss.SSSZ' (смещение с двоеточием)
    rq_tm = datetime.now().astimezone().isoformat(timespec='milliseconds')

    # Экранируем то, что пришло снаружи. Имена наших архивов безопасны по построению,
    # но ручная досылка (enqueue_files) берёт имя из параметра запуска, а амперсанд
    # в имени объекта S3 — законный символ. Неэкранированный, он даёт битый XML,
    # и ТФС отвечает на него ошибкой разбора, а не отказом по файлу.
    return f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<TransferFileCephRq>
    <RqUID>{escape(rq_uid)}</RqUID>
    <RqTm>{rq_tm}</RqTm>
    <ScenarioInfo><ScenarioId>{escape(scenario_id)}</ScenarioId></ScenarioInfo>
    <File><FileInfo><Name>{escape(file_name)}</Name></FileInfo></File>
</TransferFileCephRq>"""


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

    Вызывать из таска, а не при разборе файла: ensure_pool кэширует результат на процесс,
    но лишний SELECT на каждом обходе scheduler-ом всё равно ни к чему.

    Делает это приёмник — он опрашивает топики постоянно и сам сидит в default_pool, поэтому
    создаст пулы до того, как отправителю понадобится слот: таск с несуществующим
    пулом Airflow просто не поставит в очередь.

    Пул на каждый сценарий ИЗ КОНФИГА (см. комментарий к TFS_SEND_POOL_PREFIX). Сценарии
    из реестра сюда намеренно не входят: их пул заводит scan_queue один раз, при находке,
    и удаление такого пула руками выключает маршрут (drop_unpooled_scenarios). Заводи их
    здесь — приёмник восстанавливал бы пул через полминуты и выключатель не работал бы.

    Уже существующие пулы ensure_pool не трогает, поэтому руками выставленные слоты живы.
    """
    for scenario in TFS_ROUTES:
        ensure_pool(
            scenario_pool(scenario), slots=TFS_SEND_SLOTS,
            description=(f'TFS сценарий {scenario} — макс. {TFS_SEND_SLOTS} уведомление '
                         'одновременно. Берёт tfs_kafka_snd и обязан брать любой даг, '
                         'шлющий по этому маршруту мимо очереди. Лимиты маршрута пул '
                         'НЕ соблюдает — только взаимное исключение'),
        )


# 🧠 Состояние сенсора между опросами.
#
# ⚠️ XCom для этого НЕ ГОДИТСЯ, и обе очевидные попытки разбиваются об Airflow:
#
#   • ti.xcom_push — в режиме reschedule каждый опрос это отдельное исполнение таска,
#     а TaskInstance._execute_task_with_callbacks перед запуском зовёт clear_xcom_data()
#     и стирает ВСЕ XCom своего task_id за ран (исключение сделано только для отложенных
#     задач, reschedule к ним не относится). Записанное прошлым опросом следующий не видит:
#     у отправителя это давало повторную постановку файлов в очередь на КАЖДОМ опросе,
#     у приёмника — потерянный счётчик окна;
#   • запись под соседний task_id — у таблицы xcom внешний ключ на task_instance,
#     строки для несуществующего таска СУБД не принимает (ForeignKeyViolation).
#
# Поэтому состояние живёт там же, где остальной тракт, — объектом в S3. Обращение
# всегда точечное, по ключу рана; папка по дате, как у отправленных, чтобы при желании
# чистить целыми днями. Объект крошечный, пишется только когда есть что помнить.

def _state_key(context) -> str:
    from datetime import datetime, timezone

    ti = context['ti']
    safe = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in ti.run_id)
    day = datetime.now(timezone.utc).strftime('%Y%m%d')
    return f"{s3_base()}/state/{day}/{ti.dag_id}__{ti.task_id}__{safe}.json"


def run_state_get(context, key: str, default=None):
    """📤 Значение, пережившее reschedule. См. комментарий выше — почему не XCom."""
    import json as _json

    hook, bucket, obj = _s3_hook_key(_state_key(context))
    if not hook.check_for_key(key=obj, bucket_name=bucket):
        return default
    return _json.loads(hook.read_key(key=obj, bucket_name=bucket)).get(key, default)


def run_state_set(context, key: str, value) -> None:
    """📥 Кладёт значение так, чтобы его увидел следующий опрос того же рана."""
    import json as _json

    hook, bucket, obj = _s3_hook_key(_state_key(context))
    state = {}
    if hook.check_for_key(key=obj, bucket_name=bucket):
        state = _json.loads(hook.read_key(key=obj, bucket_name=bucket))
    state[key] = value
    hook.load_string(string_data=_json.dumps(state, ensure_ascii=False),
                     key=obj, bucket_name=bucket, replace=True)


# ══════════════════════════════════════════════════════════════════════════════
# 🗄️ Хранилище тракта
#
# Три реализации за одним интерфейсом. Публичные функции внизу пишут в S3 и в
# включённые зеркала, а читают всегда из S3; вызывающему знать об этом не нужно.
#
#   save_receipts(rows)              — приёмник сложил квитанции
#   find_receipts(rq_uids)           — wait_confirm ищет свои
#   enqueue(rows)                    — файлы встали в очередь на отправку
#   pending()                        — что ещё не отправлено
#   mark_sent(rq_uid)                — отметить уход в Kafka
#   sent_counts(scenario_id)         — расход лимитов по окнам
#   queue_state(rq_uids)             — отправлено ли (для диагностики таймаута)
#
# Общая модель у всех трёх: пишем только вставками, свежая версия побеждает при чтении.
# В ClickHouse это ReplacingMergeTree + FINAL, в Postgres — DISTINCT ON по времени,
# в S3 — перенос объекта между префиксами. Так поведение одинаково везде: приём из Kafka
# идёт at-least-once, и повтор не должен ни ломать данные, ни требовать UPDATE.
# ══════════════════════════════════════════════════════════════════════════════

WINDOWS = {'sec': 1, 'min': 60, 'hour': 3600, 'day': 86400}   # секунды в окне лимита


def window_hits(sent_at, now) -> list[str]:
    """Окна лимитов, в которые попадает отправка. Все окна скользящие.

    Вынесено отдельной функцией не ради красоты: внутри счётчика время берётся из now(),
    и проверить границу окна детерминированно было бы нельзя.
    """
    from datetime import timedelta

    delta = now - sent_at
    return [w for w, sec in WINDOWS.items() if delta <= timedelta(seconds=sec)]


def _sql_str(v) -> str:
    """Экранирует одинарные кавычки для подстановки в строковый литерал SQL."""
    return str(v).replace("'", "''")


def _ts(dt) -> str:
    """datetime → литерал 'YYYY-MM-DD HH:MM:SS.mmm' для обеих СУБД."""
    return dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]


def parse_ts(value):
    """Метка времени строки очереди → aware datetime (UTC); пусто и мусор → None.

    Читателю очереди приходится принимать обе формы: в S3 (источник истины) created_at
    лежит СТРОКОЙ, записанной _ts, а из зеркал он пришёл бы datetime-ом. Наивное
    значение считаем UTC — временем UTC его и записывали.
    """
    from datetime import datetime, timezone

    if value is None or value == '':
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        logger.warning("⚠️ Метка времени не разобрана: %r", value)
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ── ClickHouse ────────────────────────────────────────────────────────────────

def _ch_hook():
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    return ClickHouseHook(clickhouse_conn_id=CH_ID)


def _ch_save_receipts(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['scenario_id'])}', "
        f"{int(r['status_code'])}, '{_sql_str(r.get('status_desc', ''))}', "
        f"{'toDateTime64(' + chr(39) + _ts(r['rq_tm']) + chr(39) + ', 3)' if r.get('rq_tm') else 'NULL'}, "
        f"'{_sql_str(r['raw_xml'])}', '{_sql_str(r.get('kafka_topic', ''))}', "
        f"{int(r.get('kafka_partition', -1))}, {int(r.get('kafka_offset', -1))})"
        for r in rows
    )
    _ch_hook().execute(
        f"INSERT INTO {RECEIPTS_TABLE} (rq_uid, file_name, scenario_id, status_code, status_desc, "
        f"rq_tm, raw_xml, kafka_topic, kafka_partition, kafka_offset) VALUES {values}"
    )


def _ch_find_receipts(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT rq_uid, file_name, status_code, status_desc, toString(rq_tm) AS rq_tm
        FROM {RECEIPTS_TABLE} FINAL WHERE rq_uid IN ({uids})
    """)


def _ch_enqueue(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['replica'])}', "
        f"'{_sql_str(r['scenario_id'])}', toDateTime64('{r['package_ts']}', 3), "
        f"'{_sql_str(r.get('dag_id', ''))}', '{_sql_str(r.get('run_id', ''))}')"
        for r in rows
    )
    _ch_hook().execute(
        f"INSERT INTO {SENT_FILES_TABLE} (rq_uid, file_name, replica, scenario_id, "
        f"package_ts, dag_id, run_id) VALUES {values}"
    )


def _ch_pending() -> list[dict]:
    # FINAL обязателен: отправленная строка дописывается второй версией, без схлопывания
    # уже ушедший файл уехал бы повторно.
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id, created_at
        FROM {SENT_FILES_TABLE} FINAL
        WHERE notified_at = toDateTime64(0, 3)
        ORDER BY package_ts, created_at
    """)


def _ch_mark_sent(rq_uid: str) -> None:
    # dag_id и run_id перечислены наравне с остальным не для красоты: отметка отправки —
    # это ДОПИСАННАЯ версия строки, а ReplacingMergeTree по ключу rq_uid оставляет
    # последнюю. Пропусти их здесь — и у каждого отправленного файла оба поля станут
    # пустыми, хотя при постановке в очередь были записаны (в Postgres-зеркале они
    # перечислены, в S3 объект переносится целиком, так что расходился только ClickHouse).
    _ch_hook().execute(f"""
        INSERT INTO {SENT_FILES_TABLE}
            (rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id,
             created_at, notified_at)
        SELECT rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id,
               created_at, now64(3)
        FROM {SENT_FILES_TABLE} FINAL WHERE rq_uid = '{_sql_str(rq_uid)}'
    """)


def _ch_sent_counts(scenario_id: str) -> dict:
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT
            countIf(notified_at > now64(3) - INTERVAL 1 SECOND) AS sec,
            countIf(notified_at > now64(3) - INTERVAL 1 MINUTE) AS min,
            countIf(notified_at > now64(3) - INTERVAL 1 HOUR)   AS hour,
            countIf(notified_at > now64(3) - INTERVAL 1 DAY)    AS day
        FROM {SENT_FILES_TABLE} FINAL
        WHERE scenario_id = '{_sql_str(scenario_id)}' AND notified_at > toDateTime64(0, 3)
    """)[0]


def _ch_queue_state(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return get_dict_from_ch(_ch_hook(), f"""
        SELECT file_name, notified_at = toDateTime64(0, 3) AS pending
        FROM {SENT_FILES_TABLE} FINAL WHERE rq_uid IN ({uids})
    """)


# ── PostgreSQL / Greenplum ────────────────────────────────────────────────────
#
# Один код на обе СУБД: провод общий, различается только DDL (DISTRIBUTED BY у GP).
#
# ON CONFLICT не используем — в Greenplum 6 (ядро PG 9.4) его нет. Вместо него дубли
# допускаются при записи и снимаются при чтении через DISTINCT ON: тот же принцип, что
# FINAL в ClickHouse. Очередь по той же причине append-only — UPDATE в GP медленный
# и раздувает таблицу.

def _pg_hook():
    # Проверка ДО импорта провайдера: иначе ошибка настройки утонет в ModuleNotFoundError,
    # и разбираться будут не с тем.
    if not PG_CONN:
        raise ValueError(
            "Зеркало в Postgres вызвано, но PG_CONN пуст. Укажите ЗАПИСЫВАЮЩЕЕ соединение "
            "в plugins/tfs_utils.py — на чтение (…-read) не подойдёт"
        )

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=PG_CONN)


def _pg_exec(sql: str) -> None:
    hook = _pg_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()


def _pg_save_receipts(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['scenario_id'])}', "
        f"{int(r['status_code'])}, '{_sql_str(r.get('status_desc', ''))}', "
        f"{chr(39) + _ts(r['rq_tm']) + chr(39) if r.get('rq_tm') else 'NULL'}, "
        f"'{_sql_str(r['raw_xml'])}', '{_sql_str(r.get('kafka_topic', ''))}', "
        f"{int(r.get('kafka_partition', -1))}, {int(r.get('kafka_offset', -1))})"
        for r in rows
    )
    _pg_exec(
        f"INSERT INTO {RECEIPTS_TABLE} (rq_uid, file_name, scenario_id, status_code, status_desc, "
        f"rq_tm, raw_xml, kafka_topic, kafka_partition, kafka_offset) VALUES {values}"
    )


def _pg_find_receipts(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return query_to_dict(_pg_hook(), f"""
        SELECT DISTINCT ON (rq_uid, file_name)
               rq_uid, file_name, status_code, status_desc, rq_tm::text AS rq_tm
        FROM {RECEIPTS_TABLE} WHERE rq_uid IN ({uids})
        ORDER BY rq_uid, file_name, received_at DESC
    """)


def _pg_enqueue(rows: list[dict]) -> None:
    values = ", ".join(
        f"('{_sql_str(r['rq_uid'])}', '{_sql_str(r['file_name'])}', '{_sql_str(r['replica'])}', "
        f"'{_sql_str(r['scenario_id'])}', '{r['package_ts']}', "
        f"'{_sql_str(r.get('dag_id', ''))}', '{_sql_str(r.get('run_id', ''))}')"
        for r in rows
    )
    _pg_exec(
        f"INSERT INTO {SENT_FILES_TABLE} (rq_uid, file_name, replica, scenario_id, "
        f"package_ts, dag_id, run_id) VALUES {values}"
    )


def _pg_pending() -> list[dict]:
    return query_to_dict(_pg_hook(), f"""
        SELECT * FROM (
            SELECT DISTINCT ON (rq_uid)
                   rq_uid, file_name, replica, scenario_id, package_ts, created_at, notified_at
            FROM {SENT_FILES_TABLE} ORDER BY rq_uid, updated_at DESC
        ) q
        WHERE notified_at IS NULL
        ORDER BY package_ts, created_at
    """)


def _pg_mark_sent(rq_uid: str) -> None:
    _pg_exec(f"""
        INSERT INTO {SENT_FILES_TABLE}
            (rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id,
             created_at, notified_at, updated_at)
        SELECT DISTINCT ON (rq_uid)
               rq_uid, file_name, replica, scenario_id, package_ts, dag_id, run_id,
               created_at, now(), now()
        FROM {SENT_FILES_TABLE} WHERE rq_uid = '{_sql_str(rq_uid)}'
        ORDER BY rq_uid, updated_at DESC
    """)


def _pg_sent_counts(scenario_id: str) -> dict:
    return query_to_dict(_pg_hook(), f"""
        SELECT
            count(*) FILTER (WHERE notified_at > now() - interval '1 second') AS sec,
            count(*) FILTER (WHERE notified_at > now() - interval '1 minute') AS min,
            count(*) FILTER (WHERE notified_at > now() - interval '1 hour')   AS hour,
            count(*) FILTER (WHERE notified_at > now() - interval '1 day')    AS day
        FROM (
            SELECT DISTINCT ON (rq_uid) scenario_id, notified_at
            FROM {SENT_FILES_TABLE} ORDER BY rq_uid, updated_at DESC
        ) q
        WHERE scenario_id = '{_sql_str(scenario_id)}' AND notified_at IS NOT NULL
    """)[0]


def _pg_queue_state(rq_uids: list[str]) -> list[dict]:
    uids = ", ".join(f"'{_sql_str(u)}'" for u in rq_uids)
    return query_to_dict(_pg_hook(), f"""
        SELECT file_name, notified_at IS NULL AS pending
        FROM (
            SELECT DISTINCT ON (rq_uid) rq_uid, file_name, notified_at
            FROM {SENT_FILES_TABLE} WHERE rq_uid IN ({uids})
            ORDER BY rq_uid, updated_at DESC
        ) q
    """)


# ── S3 ────────────────────────────────────────────────────────────────────────
#
# Раскладка под {логовый бакет}/{S3_PREFIX}/:
#
#   receipts/{rq_uid}.json                                    — квитанция целиком
#   queue/pending/{rq_uid}.json                               — ждёт отправки
#   queue/sent/{YYYYMMDD}/{scenario}/{rq_uid}__{HHMMSS}.json  — отправлено
#
# 🔑 RqUID — ПРЕФИКС имени, а не хвост. Все три вопроса тракта задаются по RqUID
# («есть квитанция?», «ещё в очереди?», «куда переносить при отправке?»), и с ним
# в начале каждый решается точечным обращением к ключу вместо обхода префикса.
# Так уже был устроен receipts/ ради wait_confirm — очередь приведена к тому же виду.
#
# 📅 Дата в пути у sent/ идёт ПЕРЕД сценарием, и это не косметика:
#   • счётчики лимитов листают только папки дня и вчера — объём ограничен суточным
#     лимитом маршрута, а не всем архивом отправок, который никто не чистит;
#   • диагностика по RqUID (queue_state) не знает сценария, зато знает дату, и с датой
#     впереди ей хватает тех же двух папок на все маршруты сразу.
# Время отправки при этом остаётся В ИМЕНИ ключа: окна считаются по именам, без чтения
# объектов. Скользящее суточное окно живёт максимум в двух соседних датах.

def s3_base() -> str:
    """Корень тракта в S3: {логовый бакет}/{S3_PREFIX}, соединение — из airflow.cfg.

    Бакет и соединение берутся у логов (remote_base_log_folder, remote_log_conn_id):
    так на каждом стенде они верные сами собой, без отдельной настройки и отдельных
    ключей доступа. remote_base_log_folder приходит как 's3://bucket/prefix', где 's3' —
    протокол, а не conn_id; в репозитории принят вид 'conn_id://bucket/key' (его разбирает
    s3_path_parse), поэтому схему подменяем на remote_log_conn_id.

    Префикс логов (часть после бакета) при S3_UNDER_LOG_PREFIX = False отбрасывается:
    тракт живёт в корне бакета, а не внутри папки логов. Так его видно с первого взгляда
    и он не попадает под правила чистки, написанные для логов.
    """
    from airflow.configuration import conf

    base = conf.get('logging', 'remote_base_log_folder').rstrip('/')
    conn = conf.get('logging', 'remote_log_conn_id')
    path = base.split('://', 1)[1]
    if not S3_UNDER_LOG_PREFIX:
        path = path.split('/', 1)[0]
    return f"{conn}://{path}/{S3_PREFIX}"


def _s3_hook_key(path: str):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    parsed = s3_path_parse(path)
    return S3Hook(aws_conn_id=parsed['conn_id']), parsed['bucket'], parsed['key']


def _s3_save_receipts(rows: list[dict]) -> None:
    """Объект на RqUID, а внутри СПИСОК строк — по одной на файл квитанции.

    Список, потому что File у ТФС идёт [1-N] и все файлы приходят под одним RqUID:
    писали бы по объекту на RqUID из одной строки — второй файл затирал бы первый,
    и ошибка одного из них исчезала бы вместе с ним.
    """
    import json as _json
    from collections import defaultdict

    by_uid: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_uid[r['rq_uid']].append({**r, 'rq_tm': _ts(r['rq_tm']) if r.get('rq_tm') else None})

    for uid, uid_rows in by_uid.items():
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/receipts/{uid}.json")
        hook.load_string(string_data=_json.dumps(uid_rows, ensure_ascii=False),
                         key=key, bucket_name=bucket, replace=True)
        # Лог записи в ИСТОЧНИК ИСТИНЫ. Без него единственным следом квитанции в логах
        # оставался INSERT зеркала: выключи ClickHouse — и в логе не видно вообще ничего.
        logger.info("💾 %s → s3://%s/%s (строк: %d)", uid, bucket, key, len(uid_rows))


def _s3_find_receipts(rq_uids: list[str]) -> list[dict]:
    """Читает объекты квитанций. Принимает обе формы: список строк и одиночную строку —
    объекты, записанные до перехода на [1-N], остаются читаемыми."""
    import json as _json

    found = []
    for uid in rq_uids:
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/receipts/{uid}.json")
        if not hook.check_for_key(key=key, bucket_name=bucket):
            continue
        data = _json.loads(hook.read_key(key=key, bucket_name=bucket))
        found.extend(data if isinstance(data, list) else [data])
    return found


def _s3_enqueue(rows: list[dict]) -> None:
    import json as _json
    from datetime import datetime, timezone

    # created_at проставляем здесь: в СУБД это делает колоночный DEFAULT, а объекту
    # его дать некому. Без него очередь не упорядочить — order_queue сортирует пакеты
    # по времени появления первого файла.
    stamp = _ts(datetime.now(timezone.utc))

    for r in rows:
        r = {'created_at': stamp, **{k: v for k, v in r.items() if v is not None}}
        # Имя ключа — только RqUID: package_ts лежит внутри объекта, и порядок пакетов
        # строится по содержимому (см. _s3_pending), а не по именам.
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/queue/pending/{r['rq_uid']}.json")
        hook.load_string(string_data=_json.dumps(r, ensure_ascii=False, default=str),
                         key=key, bucket_name=bucket, replace=True)


def _s3_pending() -> list[dict]:
    import json as _json

    hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/pending/")
    rows = []
    for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
        if key.endswith('.json'):
            rows.append(_json.loads(hook.read_key(key=key, bucket_name=bucket)))
    return sorted(rows, key=lambda r: (str(r['package_ts']), str(r.get('created_at', ''))))


def _s3_days(now) -> list[str]:
    """Даты, в которых может лежать скользящее суточное окно: сегодня и вчера (UTC)."""
    from datetime import timedelta

    return [(now - timedelta(days=d)).strftime('%Y%m%d') for d in (0, 1)]


def _s3_mark_sent(rq_uid: str) -> None:
    import json as _json
    from datetime import datetime, timezone

    # RqUID — префикс ключа, поэтому строка очереди берётся адресно, без обхода pending/
    src = f"{s3_base()}/queue/pending/{rq_uid}.json"
    hook, bucket, key = _s3_hook_key(src)
    if not hook.check_for_key(key=key, bucket_name=bucket):
        logger.warning("⚠️ %s: в очереди не найден, отметка отправки пропущена", rq_uid)
        return

    row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
    now = datetime.now(timezone.utc)
    # Время отправки — в ИМЕНИ ключа, дата — в пути: окна считаются без чтения объектов
    dst = (f"{s3_base()}/queue/sent/{now:%Y%m%d}/{row['scenario_id']}/"
           f"{rq_uid}__{now:%H%M%S}.json")
    s3_move_s3(src, dst)


def _s3_sent_counts(scenario_id: str) -> dict:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    counts = dict.fromkeys(WINDOWS, 0)

    # Только сегодня и вчера: суточное окно дальше не тянется, а весь архив отправок
    # листать нельзя — он растёт вечно и никем не чистится.
    for day in _s3_days(now):
        hook, bucket, prefix = _s3_hook_key(f"{s3_base()}/queue/sent/{day}/{scenario_id}/")
        for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
            stamp = key.split('/')[-1].removesuffix('.json').split('__')[-1]
            try:
                sent_at = datetime.strptime(day + stamp, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning("⚠️ Ключ без разбираемой метки времени, пропущен: %s", key)
                continue
            for window in window_hits(sent_at, now):
                counts[window] += 1
    return counts


def _s3_queue_state(rq_uids: list[str]) -> list[dict]:
    import json as _json
    from datetime import datetime, timezone

    out = []
    # Ещё в очереди — точечная проверка ключа: RqUID стоит в его начале.
    missing = []
    for uid in rq_uids:
        hook, bucket, key = _s3_hook_key(f"{s3_base()}/queue/pending/{uid}.json")
        if hook.check_for_key(key=key, bucket_name=bucket):
            row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
            out.append({'file_name': row.get('file_name', uid), 'pending': True})
        else:
            missing.append(uid)

    if not missing:
        return out

    # Остальные уже ушли: из pending путь один — в sent. Сценарий отсюда неизвестен,
    # зато известна дата, и с ней впереди хватает двух папок на все маршруты сразу.
    # Путь редкий (диагностика таймаута), объём ограничен суточной отправкой.
    sent: dict[str, str] = {}
    hook, bucket, _ = _s3_hook_key(f"{s3_base()}/queue/")
    for day in _s3_days(datetime.now(timezone.utc)):
        _, _, prefix = _s3_hook_key(f"{s3_base()}/queue/sent/{day}/")
        for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
            sent.setdefault(key.split('/')[-1].split('__')[0], key)

    for uid in missing:
        key = sent.get(uid)
        if not key:
            continue   # ни в очереди, ни в отправленных — сказать нечего
        row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
        out.append({'file_name': row.get('file_name', uid), 'pending': False})
    return out


def stale_sent(minutes: int | None = None) -> list[dict]:
    """📭 Отправленные больше `minutes` назад файлы, на которые нет квитанции.

    Нужна из-за паузы: пакет, чей wait_confirm ушёл в скип, никто не ждёт, и отказ ТФС
    по его файлам иначе остался бы незамеченным. Читает только папки сегодня/вчера —
    те же две, что и счётчики лимитов, — поэтому объём ограничен суточной отправкой,
    а не всем архивом, который никто не чистит.

    Только S3: это источник истины тракта, зеркала могут отставать.
    """
    import json as _json
    from datetime import datetime, timezone

    minutes = TFS_STALE_MIN if minutes is None else int(minutes)
    now = datetime.now(timezone.utc)
    out: list[dict] = []

    hook, bucket, _ = _s3_hook_key(f"{s3_base()}/queue/")
    for day in _s3_days(now):
        _, _, prefix = _s3_hook_key(f"{s3_base()}/queue/sent/{day}/")
        for key in hook.list_keys(bucket_name=bucket, prefix=prefix) or []:
            name = key.split('/')[-1].removesuffix('.json')
            uid, _, stamp = name.partition('__')
            try:
                sent_at = datetime.strptime(day + stamp, '%Y%m%d%H%M%S').replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            waiting = (now - sent_at).total_seconds() / 60
            if waiting < minutes:
                continue

            _, r_bucket, r_key = _s3_hook_key(f"{s3_base()}/receipts/{uid}.json")
            if hook.check_for_key(key=r_key, bucket_name=r_bucket):
                continue

            row = _json.loads(hook.read_key(key=key, bucket_name=bucket))
            out.append({
                'rq_uid':      uid,
                'file_name':   row.get('file_name', ''),
                'scenario_id': row.get('scenario_id', ''),
                'replica':     row.get('replica', ''),
                'sent_at':     sent_at.strftime('%Y-%m-%d %H:%M:%S'),
                'waiting_min': int(waiting),
            })
    return sorted(out, key=lambda r: r['sent_at'])


# ── Выбор бэкенда ─────────────────────────────────────────────────────────────

_BACKENDS = {
    'ch': (_ch_save_receipts, _ch_find_receipts, _ch_enqueue, _ch_pending,
           _ch_mark_sent, _ch_sent_counts, _ch_queue_state),
    'pg': (_pg_save_receipts, _pg_find_receipts, _pg_enqueue, _pg_pending,
           _pg_mark_sent, _pg_sent_counts, _pg_queue_state),
    's3': (_s3_save_receipts, _s3_find_receipts, _s3_enqueue, _s3_pending,
           _s3_mark_sent, _s3_sent_counts, _s3_queue_state),
}


def mirrors() -> list[str]:
    """Включённые зеркала: те, у кого задан conn_id. Пустой список — только S3."""
    return [name for name, conn in (('ch', CH_ID), ('pg', PG_CONN)) if conn]


def _write(idx: int, *args) -> None:
    """Запись: S3 обязателен, зеркала — по возможности.

    Ошибка S3 прокидывается: это источник истины, без него тракт слепнет. Ошибка
    зеркала только предупреждает — данные к этому моменту уже сохранены, и ронять
    приём квитанций или отправку из-за аналитической копии нельзя.
    """
    _BACKENDS['s3'][idx](*args)

    for name in mirrors():
        try:
            _BACKENDS[name][idx](*args)
        except Exception as exc:
            logger.warning("⚠️ Зеркало %s: запись не прошла (%s). В S3 записано, "
                           "расхождение лечится дозаливкой", name, exc)


def _read(idx: int, *args):
    """Чтение всегда из S3: писатель и читатель обязаны смотреть в одно место."""
    return _BACKENDS['s3'][idx](*args)


def save_receipts(rows: list[dict]) -> None:
    """Сохраняет разобранные квитанции (приёмник)."""
    return _write(0, rows)


def find_receipts(rq_uids: list[str]) -> list[dict]:
    """Квитанции по списку RqUID: rq_uid, file_name, status_code, rq_tm."""
    return _read(1, rq_uids)


def enqueue(rows: list[dict]) -> None:
    """Ставит файлы в очередь отправки (rq_uid, file_name, replica, scenario_id, package_ts…)."""
    return _write(2, rows)


def route_s3(scenario_id: str) -> dict:
    """🪣 Где лежат файлы маршрута: {'conn', 'bucket', 'prefix'}; пусто — не описано."""
    route = tfs_route(scenario_id)
    if not (route.get('conn') and route.get('bucket')):
        return {}
    return {'conn': route['conn'], 'bucket': route['bucket'],
            'prefix': str(route.get('prefix') or '').strip('/')}


def missing_in_bucket(names: list[str], scenario_id: str) -> list[str]:
    """🔍 Имена, которых НЕТ в бакете маршрута. Маршрут без бакета — проверки нет.

    Это и есть различитель «можно retry или нельзя»: файл, который ТФС уже забрал,
    из бакета исчез, и повторная отправка вернулась бы квитанцией с ошибкой вместо
    внятного отказа на нашей стороне. Заодно ловится опечатка в имени.
    """
    dst = route_s3(scenario_id)
    if not dst:
        logger.warning("⚠️ У маршрута %s не описан бакет — наличие файлов не проверяем", scenario_id)
        return []

    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    hook = S3Hook(aws_conn_id=dst['conn'])
    pref = f"{dst['prefix']}/" if dst['prefix'] else ''
    return [n for n in names if not hook.check_for_key(key=f"{pref}{n}", bucket_name=dst['bucket'])]


def enqueue_files(files: list[str], scenario_id: str, replica: str = '',
                  dag_id: str = '', run_id: str = '',
                  check_exists: bool = True) -> tuple[list[dict], list[str]]:
    """📮 Ставит в очередь готовые имена файлов из S3 — ручная досылка.

    Возвращает (поставленные строки, пропущенные имена). Пропускаются те, которых нет
    в бакете маршрута: либо ТФС уже забрал файл — и тогда переотправка невозможна
    в принципе, — либо в имени опечатка. Без этой проверки и то, и другое уезжало
    в ТФС и возвращалось квитанцией с кодом 101/108, где причина не видна.

    RqUID генерируется здесь: по нему потом ищется обратная квитанция.

    Досылка идёт через очередь, а не мимо неё: только так файл попадает в учёт
    лимитов маршрута и потом находится по RqUID вместе с остальными.

    ⚠️ Каждый вызов заводит НОВЫЕ RqUID. Повторный вызов с тем же списком поставит
    файлы в очередь ещё раз — вызывающий обязан звать это ровно один раз на запуск.
    """
    from datetime import datetime, timezone
    from uuid import uuid4

    names = [str(f).strip() for f in (files or []) if str(f).strip()]
    if not names:
        return [], []

    scenario_id = (scenario_id or '').strip()
    if not scenario_id:
        raise ValueError("Переданы файлы, но не задан scenario_id — маршрут ТФС неизвестен")

    missing = missing_in_bucket(names, scenario_id) if check_exists else []
    names = [n for n in names if n not in missing]
    if not names:
        return [], missing

    now = datetime.now(timezone.utc)
    rows = [{
        'rq_uid':      uuid4().hex,
        'file_name':   name,
        'replica':     (replica or scenario_id).strip(),
        'scenario_id': scenario_id,
        'package_ts':  now.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        'dag_id':      dag_id,
        'run_id':      run_id,
    } for name in names]

    enqueue(rows)
    return rows, missing


def order_queue(rows: list[dict]) -> list[dict]:
    """📦 Упорядочивает очередь так, чтобы файлы одного пакета шли подряд.

    Пакеты — по времени появления (package_ts, затем created_at первого файла), внутри
    пакета — по created_at. Разрывать пакет чужими файлами нельзя: ЕР не принимает
    несколько пакетов одновременно.

    🎫 Тикет пакета уходит ПОСЛЕДНИМ. Он перечисляет архивы пакета, то есть объявляет
    его полным: приди он первым — принимающая сторона увидела бы список файлов, которых
    ещё нет. Ставится в очередь тикет вместе с архивами (make_summary), поэтому порядок
    задаётся здесь, а не временем постановки.
    """
    packages: dict = {}
    for row in rows:
        packages.setdefault((row['replica'], row['package_ts']), []).append(row)

    def _order(row: dict) -> tuple:
        return (str(row.get('file_name', '')).lower().endswith('.tkt'),
                str(row.get('created_at', '')))

    ordered = []
    # get, а не [], намеренно: строка без created_at не должна ронять всю отправку —
    # она просто встанет первой в своём пакете.
    for key in sorted(packages, key=lambda k: (k[1], min(str(r.get('created_at', '')) for r in packages[k]))):
        ordered.extend(sorted(packages[key], key=_order))
    return ordered


def pending() -> list[dict]:
    """Файлы, ещё не ушедшие в Kafka, в порядке package_ts, created_at."""
    return _read(3)


def mark_sent(rq_uid: str) -> None:
    """Отмечает файл отправленным."""
    return _write(4, rq_uid)


def sent_counts(scenario_id: str) -> dict:
    """Расход лимитов маршрута: {'sec', 'min', 'hour', 'day'}. Окна скользящие."""
    return _read(5, scenario_id)


def queue_state(rq_uids: list[str]) -> list[dict]:
    """file_name + pending: отличить «ещё не отправлено» от «нет квитанции»."""
    return _read(6, rq_uids)


def get_config() -> dict:
    """📦 Снимок констант модуля для передачи в DAG-файлы."""
    return {
        'CH_ID':            CH_ID,
        'PAUSE_VAR':        PAUSE_VAR,
        'PAUSE_SCOPES':     list(PAUSE_SCOPES),
        'TFS_STALE_MIN':    TFS_STALE_MIN,
        'MIRRORS':          mirrors(),   # включённые зеркала; S3 обязателен и в список не входит
        'DEF_ARGS':         DEF_ARGS,
        'ENV_SPACE':        ENV_SPACE,
        'KAFKA_SND_CONN':   KAFKA_SND_CONN,
        'KAFKA_SND_TOPICS': KAFKA_SND_TOPICS,
        'DEFAULT_SND_TOPIC': DEFAULT_SND_TOPIC,
        'KAFKA_RCV_CONN':   KAFKA_RCV_CONN,
        'KAFKA_RCV_TOPICS': KAFKA_RCV_TOPICS,
        'RECEIPTS_TABLE':   RECEIPTS_TABLE,
        'SENT_FILES_TABLE': SENT_FILES_TABLE,
        'TFS_ROUTES':       TFS_ROUTES,
        'TFS_LIMITS_DEFAULT': TFS_LIMITS_DEFAULT,
        'TFS_QUEUE_ALERT_MIN': TFS_QUEUE_ALERT_MIN,
        'SCENARIOS_VAR':    SCENARIOS_VAR,
        'SCENARIOS':        known_scenarios(),
        'TFS_SEND_SLOTS':   TFS_SEND_SLOTS,
        'TFS_RCV_POOL':     TFS_RCV_POOL,
    }
