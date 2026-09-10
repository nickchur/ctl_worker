"""### 🧽 Чистка очереди celery от сообщений без задач
*2026-09-10 17:25 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Обходит очереди брокера, размечает каждое сообщение на живое и мусорное по метабазе и
точечно удаляет мусор. Мусор — это сообщение, чья задача либо отсутствует в метабазе, либо
уже в терминальном состоянии: исполнять его нечего, а вернуться в очередь по
`visibility_timeout` при `task_acks_late = True` оно может бесконечно.

Замер на альфе (2026-09-10): очередь `default` держала ~950 сообщений при 16 задачах,
которые Airflow считал отданными celery. Раньше в том же контуре из 914 сообщений живыми
оказались ~27, а из 254 строк `celery_taskmeta` в статусе `STARTED` у 245 не было
`task_instance` вообще.

| Параметр | Описание |
|---|---|
| `queues` | Очереди через запятую; пусто — собрать из конфигурации и метабазы *(default: пусто)* |
| `min_junk_share` | Не удалять, если мусора меньше этой доли очереди *(default: `0.5`)* |
| `max_delete` | Не удалять, если мусора больше этого числа *(default: `5000`)* |
| `keep_days` | Сколько дней держим дампы, старше — убираем *(default: `30`)* |
| `save_to_var` | Записать значения формы в Variable `tools_queue_cleanup_cfg` *(default: `False`)* |
| `purge` | Удалять размеченный мусор. **В Variable не сохраняется** *(default: `False`)* |

**Таски:** `collect` → `purge` → `report`, рядом `prune` — чистка дампов.

Дампы лежат в бакете логов, в своей папке: `queue_cleanup/<YYYY-MM-DD>/<HHMMSS>.json`.
Пишутся всегда, в том числе при подсчёте без удаления: из дампа сообщение возвращается
`rpush` по полям `key` и `payload`, и он же остаётся следом для разбора.

Расписания у дага нет: это инструмент разбора, а не регулярная чистка. Что он не заберёт —
сообщения, которые воркеры уже держат в работе: их в очереди нет. Они вернутся туда через
`visibility_timeout`, и повторный запуск их подберёт; быстрее — перезапустить поды воркеров.

> Пороги берутся из Variable `tools_queue_cleanup_cfg`, форма запуска ими предзаполняется.
> Поменять порог — запуск с галкой «Сохранить настройки».
"""

# tuple | None в сигнатуре: на 3.9 аннотация вычисляется при определении функции и
# падает без этого импорта, а контуры на разных версиях питона.
from __future__ import annotations

# Только то, что нужно на разборе файла: декораторы, Param/TriggerRule для сигнатуры,
# datetime для start_date. Всё runtime-only — брокер, S3, метабаза — внутри тасков.
from datetime import datetime, timedelta, timezone
import base64
import json
import logging
import re

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.utils.trigger_rule import TriggerRule

try:
    from CI06932748.tools.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore
except ImportError:
    from plugins.utils import TOOLS_POOL, add_note, ensure_pool, on_callback  # type: ignore

logger = logging.getLogger("airflow.task")

# Пул заводим при разборе файла: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Бакет и коннект — те же, что у логов задач, но папка своя: дампы не должны попасть под
# чистку логов (см. check/log_cleanup.py) и мешаться с ними в выдаче.
# verify=False у S3Hook ниже — как во всех дагах этого репозитория: наш S3-шлюз ходит
# по внутреннему сертификату, которого нет в бандле CA у образа. Правильное решение —
# CA в подключении; пока его нет, оставляем как есть, но помним, что это не «на всякий
# случай», а осознанный компромисс.
AWS_CONN_ID = conf.get("logging", "REMOTE_LOG_CONN_ID")
BUCKET_NAME = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1].split("/")[0]
PREFIX = "queue_cleanup/"

CFG_VAR = "tools_queue_cleanup_cfg"

# Значения по умолчанию. Всё, кроме purge: удаление не должно становиться настройкой,
# живущей между запусками, — галку ставят руками на конкретный ран.
DEFAULTS = {
    "queues": "",
    # Ниже этой доли не удаляем: значит картина не та, для которой инструмент сделан.
    # На альфе доля мусора была ~0.97, так что порог в половину очереди не мешает делу,
    # но останавливает запуск по здоровой очереди, где размечено три сообщения из девятисот.
    "min_junk_share": 0.5,
    # Верхний предел на одно удаление: страховка от ошибки в разметке, а не от объёма.
    "max_delete": 5000,
    "keep_days": 30,
}

# Состояния, из которых задача уже не вернётся. Всё остальное, включая пустое состояние,
# считаем живым: направление ошибки выбрано в пользу сохранения сообщения.
#
# Список задан явно, а не через State.finished: finished включает removed и не включает
# None, и его состав между версиями Airflow менялся. Здесь он решает, что удалять, —
# такой список должен быть виден глазами, а не подразумеваться.
TERMINAL_STATES = frozenset({"success", "failed", "skipped", "upstream_failed", "removed"})

# Партия для IN по кортежам: планировщик разбирает список из тысяч элементов заметно дольше.
STATE_BATCH = 500
# Длина даты в имени папки дампа (`YYYY-MM-DD`) — по ней же отбираются старые дампы.
DATE_LEN = 10
# Имя папки дампа. Отбор старого идёт строковым сравнением, поэтому объект, имя которого
# на дату не похоже, под чистку попадать не должен — он «меньше» любой даты.
DATE_DIR_RE = re.compile(r"^\d{4}-\d{2}-\d{2}/")
# Предел на чтение: тела читаются в память целиком, иначе разметить их нечем.
# Гигабайтная очередь — уже не случай этого инструмента, и упасть на входе честнее, чем
# по OOM в середине разбора. Проверяется по LLEN, до LRANGE, то есть даром, — и по
# сумме тоже: в памяти лежат тела всех очередей сразу, поэтому пять очередей по сорок
# тысяч ничем не лучше одной на двести.
READ_LIMIT = 50_000

# default_var={} обязателен: без него отсутствующая переменная роняет разбор файла и
# вешает Broken DAG на весь даг, а не на один запуск.
try:
    _cfg = {**DEFAULTS, **(Variable.get(CFG_VAR, default_var={}, deserialize_json=True) or {})}
except Exception:  # битый JSON в переменной не должен ломать разбор
    logger.warning("Variable %s не разобрана, берём значения по умолчанию", CFG_VAR, exc_info=True)
    _cfg = dict(DEFAULTS)


# Очереди, к которым задачи привязаны в метабазе. Смотрим не только queued: сообщение
# переживает свою задачу, и очередь давно завершившейся задачи всё ещё нужно обойти.
SQL_QUEUES = """
SELECT DISTINCT queue FROM main.task_instance WHERE queue IS NOT NULL
"""

# Состояние задач, названных в сообщениях. Отбор по первичному ключу
# (dag_id, task_id, run_id, map_index) — то есть по индексу, а не сканом: спрашиваем ровно
# про те задачи, что нашлись в очереди.
#
# Отбор по external_executor_id пришлось бы делать сканом (индекса по нему нет, замер на
# соседней карточке Health — 330 мс на боевой), и живости он всё равно не показывает:
# у завершившейся задачи там стоит идентификатор того самого сообщения, которое и нужно
# удалить. Поэтому он здесь только для сверки в отчёте.
SQL_TARGETS = """
SELECT dag_id, task_id, run_id, map_index, state, external_executor_id
  FROM main.task_instance
 WHERE (dag_id, task_id, run_id, map_index) IN :keys
"""


def _decode(raw) -> dict:
    """Разбирает сырое сообщение очереди в словарь. Ключи те, что нужны разметке."""
    text = raw.decode() if isinstance(raw, bytes) else raw
    msg = json.loads(text)
    headers = msg.get("headers") or {}
    props = msg.get("properties") or {}
    body = msg.get("body")
    if props.get("body_encoding") == "base64":
        body = base64.b64decode(body)
    # Протокол celery v2: тело — [args, kwargs, embed]. Команда Airflow лежит первым
    # позиционным аргументом execute_command. Разбираем тело, а не headers.argsrepr:
    # repr придётся парсить как питон, а тело — обычный JSON.
    args = json.loads(body)[0] if body else []
    return {
        "id": headers.get("id"),
        "task": headers.get("task"),
        "command": args[0] if args and isinstance(args[0], list) else None,
    }


def _target(command) -> tuple | None:
    """Задача, названную которой несёт команда: (dag_id, task_id, run_id, map_index).

    None — цель не читается: не `airflow tasks run`, укороченная команда, чужая задача
    celery. Такое сообщение остаётся в очереди.
    """
    if not command or command[:3] != ["airflow", "tasks", "run"] or len(command) < 6:
        return None
    map_index = -1
    if "--map-index" in command:
        try:
            map_index = int(command[command.index("--map-index") + 1])
        except (IndexError, ValueError):
            return None
    return command[3], command[4], command[5], map_index


def _states(keys: list) -> dict:
    """Состояние и идентификатор celery по ключам задач: {ключ: (state, executor_id)}.

    Порциями: список ключей приходит из очереди и на инциденте бывает в тысячи строк,
    а IN по кортежам с таким числом элементов планировщик разбирает заметно дольше.
    """
    from airflow import settings
    from sqlalchemy import bindparam, text

    out = {}
    if not keys:
        return out
    sql = text(SQL_TARGETS).bindparams(bindparam("keys", expanding=True))
    with settings.engine.connect() as conn:
        for i in range(0, len(keys), STATE_BATCH):
            rows = conn.execute(sql, {"keys": keys[i:i + STATE_BATCH]})
            for dag_id, task_id, run_id, map_index, state, eid in rows:
                out[(dag_id, task_id, run_id, map_index)] = (state, eid)
    return out


def _read_queues(names: list) -> tuple:
    """Читает все приоритетные ключи очередей. Возвращает (сообщения, префикс, ключи).

    Сообщение — dict с сырым телом и ключом, по которому его удалять и возвращать.
    """
    from airflow.providers.celery.executors.celery_executor import app

    msgs, keys = [], []
    with app.connection_for_read() as connection:
        channel = connection.default_channel
        prefix = getattr(channel, "global_keyprefix", "") or ""
        for name in names:
            for pri in getattr(channel, "priority_steps", [0]):
                base = channel._q_for_pri(name, pri)
                # kombu дописывает global_keyprefix НЕ ко всем командам redis: LLEN
                # получает, LRANGE и LREM — нет (GlobalKeyPrefixMixin, kombu 5.6.2).
                # Поэтому длину спрашиваем по короткому имени — префикс допишет клиент,
                # а содержимое и удаление идут по полному, которое собираем сами.
                full = prefix + base
                length = channel.client.llen(base)
                if length > READ_LIMIT or len(msgs) + length > READ_LIMIT:
                    raise RuntimeError(
                        f"ключ {base!r}: {length} сообщений, уже прочитано {len(msgs)}, "
                        f"предел чтения — {READ_LIMIT}. Столько этот инструмент в память не "
                        f"берёт: разбирать такую очередь нужно иначе — пересборкой ключа, а не "
                        f"удалением по одному сообщению."
                    )
                body = channel.client.lrange(full, 0, -1)
                # Не «на всякий случай»: на контуре с префиксом это дало «в очереди 0»
                # при llen = 578 — удаление молча било бы мимо очереди. Стенд без
                # префикса такую ошибку не ловит, поэтому сверка живёт в коде.
                if length != len(body):
                    raise RuntimeError(
                        f"ключ {base!r}: llen={length}, а прочитано {len(body)}. "
                        f"Ключи разошлись, разметка недостоверна — ничего не удаляем."
                    )
                keys.append({"key": full, "length": length})
                for raw in body:
                    msgs.append({"key": full, "queue": name, "priority": pri, "raw": raw})
    return msgs, prefix, keys


def classify(msgs: list, states: dict) -> tuple:
    """⤴️ Размечает сообщения. Возвращает `(status, payload)`, решает вызывающий таск.

    Живым сообщение остаётся, если названная им задача есть в метабазе и не в терминальном
    состоянии, либо если цель вообще не прочиталась. Мусор — всё остальное.
    """
    live, junk, reasons, unparsed, eid_mismatch = [], [], {}, 0, 0
    for m in msgs:
        info = m["info"]
        key = _target(info.get("command"))
        if key is None:
            unparsed += 1
            live.append(m)
            reasons["цель не прочиталась"] = reasons.get("цель не прочиталась", 0) + 1
            continue
        state, eid = states.get(key, (None, None))
        if key not in states:
            junk.append(m)
            reasons["задачи нет в метабазе"] = reasons.get("задачи нет в метабазе", 0) + 1
        elif state in TERMINAL_STATES:
            junk.append(m)
            reasons[f"задача уже {state}"] = reasons.get(f"задача уже {state}", 0) + 1
        else:
            live.append(m)
            reasons[f"задача {state or 'без состояния'}"] = reasons.get(f"задача {state or 'без состояния'}", 0) + 1
        # Сверка, а не признак: расхождение показывает, насколько разметка по состоянию
        # расходится с тем, что метабаза помнит о последнем сообщении задачи.
        if eid and eid != info.get("id"):
            eid_mismatch += 1

    if not msgs:
        return "skip", {"note": "очередь пуста — размечать нечего"}
    return "ok", {
        "live": live,
        "junk": junk,
        "reasons": reasons,
        "unparsed": unparsed,
        "eid_mismatch": eid_mismatch,
    }


params = {
    "queues": Param(
        _cfg["queues"], type="string",
        description="Очереди через запятую; пусто — собрать из конфигурации и метабазы",
    ),
    "min_junk_share": Param(
        _cfg["min_junk_share"], type="number", minimum=0, maximum=1,
        description="Не удалять, если доля мусора в очереди меньше этой",
    ),
    "max_delete": Param(
        _cfg["max_delete"], type="integer", minimum=1,
        description="Не удалять, если мусора больше этого числа",
    ),
    "keep_days": Param(
        _cfg["keep_days"], type="integer", minimum=1,
        description="Сколько дней держим дампы удалённого",
    ),
    "save_to_var": Param(
        False, type="boolean",
        description=f"Записать значения формы в Variable {CFG_VAR}",
    ),
    # Разовое действие, а не настройка: в переменную не пишется и берётся всегда из кода
    "purge": Param(
        False, type="boolean",
        description="Удалять размеченный мусор. В Variable не сохраняется",
    ),
}


@dag(
    doc_md=__doc__,
    owner_links={"DataLab (CI02420667)": "https://confluence.sberbank.ru/display/HRTECH/DataLab"},
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        "retries": 0,
        "on_failure_callback": on_callback,
    },
    start_date=datetime(2026, 9, 10, tzinfo=timezone.utc),
    tags=["DataLab", "tools", "clean"],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    # Расписания нет намеренно: инструмент разбора, а не регулярная чистка. Удаление из
    # брокера по таймеру — не то, что должно случаться без человека.
    schedule=None,
    on_failure_callback=on_callback,
    params=params,
)
def tools_queue_cleanup():

    # Своя попытка сверх нуля в default_args: collect ничего не удаляет, а метабаза на
    # дэве рвёт соединения сама по себе (падает праймери, реплика read-only). Прогон
    # делает человек, и терять его из-за обрыва в середине опроса состояний обидно.
    @task(task_id="collect", retries=1)
    def collect(**context) -> dict:
        """📸 Обход очередей, разметка по метабазе и дамп мусора в S3."""
        from airflow.exceptions import AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow import settings
        from sqlalchemy import text

        p = context["params"]
        if p.get("save_to_var"):
            Variable.set(CFG_VAR, {k: p[k] for k in DEFAULTS}, serialize_json=True)
            logger.info("💾 %s обновлена", CFG_VAR)

        names = [n.strip() for n in (p["queues"] or "").split(",") if n.strip()]
        if not names:
            names = {conf.get("operators", "default_queue", fallback="default")}
            with settings.engine.connect() as conn:
                names.update(q for (q,) in conn.execute(text(SQL_QUEUES)) if q)
            names = sorted(names)
        logger.info("очереди: %s", ", ".join(names))

        msgs, prefix, keys = _read_queues(names)
        logger.info("префикс ключей: %r, сообщений: %s", prefix, len(msgs))

        # Разбор тел до опроса метабазы: битое сообщение не должно рушить прогон — его
        # цель не прочитается, и оно останется в очереди как неопознанное.
        targets = []
        for m in msgs:
            try:
                m["info"] = _decode(m["raw"])
            except Exception as exc:
                logger.warning("сообщение не разобрано: %s", exc)
                m["info"] = {"id": None, "task": None, "command": None}
            key = _target(m["info"].get("command"))
            if key is not None:
                targets.append(key)

        status, payload = classify(msgs, _states(sorted(set(targets))))
        if status == "skip":
            add_note(payload["note"], context=context, level="DAG,Task", title="🧽 очередь")
            raise AirflowSkipException(payload["note"])

        junk, live = payload["junk"], payload["live"]

        # Дамп пишем всегда, в том числе при подсчёте без удаления: он и след разбора, и
        # единственный способ вернуть сообщение обратно. В /tmp пода его класть нельзя —
        # под перезапустят, и возвращать будет нечего.
        now = datetime.now(timezone.utc)
        dump_key = f"{PREFIX}{now:%Y-%m-%d}/{now:%H%M%S}.json"
        dump = [
            {
                "key": m["key"],
                "queue": m["queue"],
                "priority": m["priority"],
                "celery_id": m["info"].get("id"),
                "command": m["info"].get("command"),
                "payload": m["raw"].decode() if isinstance(m["raw"], bytes) else m["raw"],
            }
            for m in junk
        ]
        S3Hook(aws_conn_id=AWS_CONN_ID, verify=False).load_string(
            json.dumps(dump, ensure_ascii=False, indent=2),
            key=dump_key, bucket_name=BUCKET_NAME, replace=True,
        )
        logger.info("💾 дамп мусора: s3://%s/%s (%s)", BUCKET_NAME, dump_key, len(dump))

        snapshot = {
            "prefix": prefix,
            "queues": names,
            "keys": keys,
            "total": len(msgs),
            "live": len(live),
            "junk": len(junk),
            "unparsed": payload["unparsed"],
            "eid_mismatch": payload["eid_mismatch"],
            "reasons": payload["reasons"],
            "dump_key": dump_key,
            # Первые несколько мусорных — чтобы по отчёту было видно, что именно размечено
            "sample": [
                {"celery_id": m["info"].get("id"), "command": (m["info"].get("command") or [])[3:6]}
                for m in junk[:5]
            ],
        }
        add_note(
            "\n".join(
                [f"очередей: {len(names)} · сообщений: {len(msgs)} · живых: {len(live)} · мусора: {len(junk)}"]
                + [f"- {k}: {v}" for k, v in sorted(payload['reasons'].items(), key=lambda x: -x[1])]
            ),
            context=context, level="Task", title="🧽 разметка",
        )
        return snapshot

    @task(task_id="purge", trigger_rule=TriggerRule.NONE_FAILED)
    def purge(snapshot: dict = None, **context) -> dict:
        """🧹 Удаляет размеченный мусор. По умолчанию выключено."""
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.celery.executors.celery_executor import app

        p = context["params"]
        if not p["purge"]:
            raise AirflowSkipException("purge=False — очередь не трогаем")

        # Разметки может не быть вовсе: NONE_FAILED считает пропуск успехом, и на пустой
        # очереди (collect пропустился) сюда приходит пустой аргумент. Это не ошибка —
        # удалять нечего, поэтому пропуск, а не падение. Упавший collect сюда не пускает
        # само правило: у него состояние upstream_failed.
        if not snapshot:
            raise AirflowSkipException("разметки нет — collect пропустился, очередь была пуста")

        total, junk = snapshot["total"], snapshot["junk"]
        if not junk:
            raise AirflowSkipException("мусора не размечено — удалять нечего")

        # Пороги проверяем здесь, а не в collect: разметка должна отработать и показать
        # числа даже там, где удалять нельзя.
        share = junk / total if total else 0

        def blocked(reason: str):
            """Причина отказа — в XCom, до падения: иначе отчёт покажет «не удаляли».

            Разница существенная: «не удаляли» — это выключенная галка, а здесь удаление
            запрашивали и порог его не пустил. Возврат таска до отчёта не доходит.
            """
            context["ti"].xcom_push(key="blocked", value=reason)
            return AirflowFailException(reason)

        if share < p["min_junk_share"]:
            raise blocked(
                f"мусора {junk} из {total} — доля {share:.2f} ниже порога {p['min_junk_share']}. "
                f"Картина не та, для которой инструмент сделан: разбираться нужно руками."
            )
        if junk > p["max_delete"]:
            raise blocked(
                f"мусора {junk}, предел за один запуск — {p['max_delete']}. "
                f"Либо поднять предел осознанно, либо сначала проверить разметку по дампу."
            )

        dump = json.loads(
            S3Hook(aws_conn_id=AWS_CONN_ID, verify=False).read_key(snapshot["dump_key"], BUCKET_NAME)
        )
        removed, missing = 0, 0
        with app.connection_for_write() as connection:
            client = connection.default_channel.client
            for item in dump:
                # LREM префикса не получает — в dump лежит уже полный ключ (см. _read_queues).
                # Удаляем по точному телу: сообщение, успевшее уйти из очереди само,
                # просто не найдётся, и это не ошибка.
                count = client.lrem(item["key"], 0, item["payload"])
                removed += count
                missing += 1 if not count else 0
        logger.warning("🧹 удалено %s, не найдено %s", removed, missing)

        # Остаток считаем по коротким именам: LLEN префикс получает от клиента сам,
        # а в snapshot["keys"] лежат полные — те, что нужны LRANGE и LREM.
        left = {}
        with app.connection_for_read() as connection:
            channel = connection.default_channel
            for item in snapshot["keys"]:
                short = item["key"][len(snapshot["prefix"]):] if snapshot["prefix"] else item["key"]
                left[short] = channel.client.llen(short)

        result = {"removed": removed, "missing": missing, "left": sum(left.values())}
        add_note(
            f"удалено: {removed} · не найдено: {missing} · осталось в очередях: {result['left']}",
            context=context, level="Task", title="🧹 удаление",
        )
        return result

    @task(task_id="report", trigger_rule=TriggerRule.ALL_DONE)
    def report(snapshot: dict = None, purged: dict = None, **context) -> str:
        """📊 Сводка: что было, что размечено, что удалено, где дамп.

        Запускается при любом исходе (`ALL_DONE`), поэтому обоих словарей может не быть:
        XCom упавшего или пропущенного таска не существует, и аргумент приезжает пустым.
        Своё падение здесь хуже отсутствия сводки — дежурный получит два красных таска
        вместо объяснения, что случилось с первым.
        """
        p = context["params"]
        purged = purged or {}
        if not snapshot:
            done = context["dag_run"].get_task_instance("collect")
            state = getattr(done, "state", None)
            note = (
                "очередь пуста — размечать было нечего"
                if state == "skipped"
                else f"collect не отработал (состояние: {state}) — размётки и дампа нет, сводки тоже"
            )
            add_note(note, context=context, level="DAG,Task", title="🧽 очередь celery")
            return note

        # Отказ порога отличается от выключенной галки: удаление запрашивали, и его не
        # пустило. Причина приезжает из XCom, потому что возврат упавшего таска до сюда
        # не доходит.
        stop = context["ti"].xcom_pull(task_ids="purge", key="blocked")
        if purged.get("removed") is not None:
            deleted = purged["removed"]
        elif stop:
            deleted = "⛔️ порог не пустил"
        else:
            deleted = "☮️ не удаляли"

        # Порядок строк — по убыванию важности: заметка обрезается по MAX_NOTE_LEN, и
        # обрезаться должна разметка в хвосте, а не адрес дампа, по которому возвращают.
        lines = [
            "| Показатель | Значение |",
            "|---|---|",
            f"| сообщений в очередях | {snapshot['total']} |",
            f"| живых | {snapshot['live']} |",
            f"| мусора | {snapshot['junk']} |",
            f"| удалено | {deleted} |",
            f"| осталось в очередях | {purged.get('left', '—')} |",
            f"| дамп | `s3://{BUCKET_NAME}/{snapshot['dump_key']}` |",
            f"| очереди | {', '.join(snapshot['queues'])} |",
            f"| префикс ключей | `{snapshot['prefix'] or '—'}` |",
            f"| цель не прочиталась | {snapshot['unparsed']} |",
            f"| расхождений с external_executor_id | {snapshot['eid_mismatch']} |",
        ]
        if stop:
            lines += ["", f"> ⛔️ {stop}"]
        if snapshot["reasons"]:
            lines += ["", "**Разметка:**"]
            lines += [f"- {k}: {v}" for k, v in sorted(snapshot["reasons"].items(), key=lambda x: -x[1])]
        if not p["purge"]:
            lines += ["", "> Подсчёт без удаления. Чтобы удалить — запуск с галкой `purge`."]
        summary = "\n".join(lines)
        logger.info("📊 сводка:\n%s", summary)
        add_note(summary, context=context, level="DAG,Task", title="🧽 очередь celery")
        return summary

    @task(task_id="prune", trigger_rule=TriggerRule.ALL_DONE)
    def prune(**context) -> str:
        """🧹 Убирает дампы старше `keep_days`.

        Своими руками, а не lifecycle-правилом бакета: наш S3-шлюз не принимает
        PutBucketLifecycleConfiguration (требует Content-MD5, которого boto3 больше не шлёт).
        """
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        keep = int(context["params"]["keep_days"])
        edge = (datetime.now(timezone.utc) - timedelta(days=keep)).strftime("%Y-%m-%d")
        hook = S3Hook(aws_conn_id=AWS_CONN_ID, verify=False)
        keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=PREFIX) or []
        # Дата лежит в имени папки, поэтому отбираем строковым сравнением, а не запросом
        # метаданных на каждый объект: формат `YYYY-MM-DD` сравнивается как строка верно.
        # Имя, на дату не похожее, пропускаем: чужой объект под нашим префиксом иначе
        # сравнился бы «меньше края» и был бы удалён заодно.
        old = [
            k for k in keys
            if DATE_DIR_RE.match(k[len(PREFIX):]) and k[len(PREFIX):len(PREFIX) + DATE_LEN] < edge
        ]
        if old:
            hook.delete_objects(bucket=BUCKET_NAME, keys=old)
        msg = f"дампов: {len(keys)}, убрано старше {edge}: {len(old)}"
        logger.info("🧹 %s", msg)
        return msg

    snapshot = collect()
    purged = purge(snapshot)
    report(snapshot, purged)
    prune()


tools_queue_cleanup()
