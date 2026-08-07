"""### 🧬 DAG: Проверка сериализации DAG'ов
*2026-08-07 13:50 MSK · v2.15 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Ищет DAG'и, у которых сериализация переписывается на каждом парсинге файла, и выясняет
причину. Выделен из `test_connections` (там остались проверки соединений).

Запускается ежедневно в 23:00 MSK.

Две независимые группы: **`check_serialized`** ловит дрожание сериализации на парсинге
(может ждать часами), **`compare`** ведёт версии в S3 и показывает, что изменилось
(минуты). Итог обеих собирает `summary`.

| Таск | Что делает |
|---|---|
| **`check_serialized.check_serialized_dag`** | Считает по `main.serialized_dag`, у скольких DAG'ов менялась сериализация за год, 3 месяца, месяц, неделю, сутки и час, плюс строка «на последнем парсинге» (`last_updated` попал в окно последнего разбора файла — `dag.last_parsed_time`). **Никогда не падает**: одного замера мало, чтобы отличить дрожание от деплоя. Возвращает список подозрительных DAG'ов, статистика — в XCom `serialized_stats` |
| **`check_serialized.recheck_serialized_dag`** | Mapped-таск, по экземпляру на DAG из списка. Ждёт следующего парсинга (сдвига `dag.last_parsed_time`) и сравнивает сериализацию до и после, показывая расхождения по путям вида `.dag.params[0][1].schema.examples[0]` |
| **`compare.find_changed`** | Находит DAG'и, у которых `dag_hash` разошёлся с последней сохранённой версией, то есть изменившиеся с прошлого прогона. Ничего не скачивает: хэш виден в имени объекта |
| **`compare.snapshot_dags`** | Пишет новые версии в S3 и возвращает пары «прошлая версия → новая» для `expand`; статистика — в XCom `snapshot_stats` |
| **`compare.compare_changed`** | Mapped-таск, по экземпляру на изменившийся DAG: сравнивает две соседние версии и показывает расхождения. **Никогда не падает**, итог в XCom `compare`. В списке mapped-тасков вместо `Map Index` — `dag_id` |
| **`parse_time`** | Вне групп: разбирает все файлы DAG'ов и ищет выбросы по времени — медленнее `среднее + 3σ`. Отдельно отмечает файлы, перевалившие половину `dag_file_processor_timeout`: такой файл dag-processor бросит на полпути, и DAG'и из него исчезнут из `serialized_dag`. **Никогда не падает** |
| **`summary`** | Сводка всех веток: вердикты, время ожидания, расхождения, покрытие версиями, выбросы парсинга |

**Исключения:** DAG'и с id по префиксам из `SKIP_DAG_PREFIXES` (сейчас `deadlocker_*`)
не проверяются вовсе — ни в статистике сериализации, ни в версиях, ни в покрытии.
Нагрузочный генератор переписывает свои DAG'и сам по себе, и в отчётах это шум.
Исключение не касается `parse_time`: он меряет разбор **файлов**, а не DAG'и по id.

**Параметры:**

| Параметр | По умолчанию | Значение |
|---|---|---|
| `snapshot_limit` | `0` | Сколько DAG'ов обходить за прогон. `0` — все. Ненулевое включает ротацию: изменившиеся → без копии → с самой старой копией, полное покрытие за `ceil(всего / limit)` суток |
| `cleanup_deleted` | `False` | Удалять ли версии DAG'ов, которых больше нет в `serialized_dag`. Выключено намеренно: пропажа чаще временная (Broken DAG, неудачный парсинг, `dag_stale_not_seen_duration`), и копия как раз тогда и нужна |

**Хранилище версий:**

```
dag_snapshots/<dag_id>/00001.<dag_hash>.json.gz
dag_snapshots/<dag_id>/00002.<dag_hash>.json.gz
```

Новая версия пишется **только когда сменился `dag_hash`** — если содержимое то же, объект не
плодится, и в истории остаются ровно те точки, где DAG менялся. `dag_hash` вынесен в имя
объекта: `list_objects_v2` пользовательские метаданные не отдаёт, и иначе на каждый DAG
пришлось бы качать последнюю копию, чтобы понять, изменилось ли что-нибудь.

Бакет и соединение — те же, что у логов задач (`[logging] remote_log_conn_id` /
`remote_base_log_folder`), но префикс в корне, а не под логами: иначе версии удалял бы
`log_cleanup`.

**Вердикты `compare_changed`** (сравниваются два неизменяемых объекта в S3, поэтому
результат воспроизводим — перезапуск таска через неделю покажет то же самое):

| Статус | | Значение |
|---|---|---|
| `changed` | 📝 | Есть расхождения — в отчёте пути |
| `same_json` | ✅ | `dag_hash` разный, а JSON совпал |
| `duplicate_dag_id` | 👯 | Сменился `fileloc` — два файла на один `dag_id` |
| `snapshot_broken` | ⚠️ | Версия не читается или не разбирается |

Если экземпляр сравнения упал и не оставил XCom, `summary` считает его «немым» и пишет
об этом отдельной строкой: версия уже записана, поэтому следующий прогон этот DAG не
переоткроет — хэши совпадут. Чинится очисткой упавшего экземпляра: обе версии лежат
в S3, он перечитает те же объекты и выдаст тот же вердикт.

**Вердикты `recheck_serialized_dag`:**

| Статус | | Значение |
|---|---|---|
| `stable` | ✅ | `dag_hash` совпал — прошлое изменение было разовым (деплой) |
| `no_parse` | ⏱️ | Парсинга не дождались: очередь в dag-processor'е. Не падение |
| `unstable` | ❌ | Сериализация переписана снова — разбор файла недетерминирован |
| `duplicate_dag_id` | 👯 | Сменился `fileloc`: на один `dag_id` претендуют два файла и затирают сериализацию друг друга. Airflow такие коллизии между файлами не диагностирует — `AirflowDagDuplicatedIdException` срабатывает внутри одного `DagBag`, то есть одного файла |

Итог каждого экземпляра уходит в XCom `recheck` — до возможного падения, иначе у упавшего
таска не осталось бы return-значения. В списке mapped-тасков вместо `Map Index` `0,1,2…`
показывается `dag_id`.
"""

from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from datetime import datetime, timedelta, timezone
from logging import getLogger

try:
    from plugins.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore

logger = getLogger("airflow.task")

# Москва живёт на постоянном UTC+3 с 2014 года, переходов на летнее время нет,
# поэтому фиксированное смещение точно описывает пояс и не зависит от tz-базы
MSK = timezone(timedelta(hours=3))

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)

# Соединение и бакет берём из настроек логирования, а не именем: на DEV бакет подменяет
# airflow_entrypoint (REMOTE_BASE_LOG_FOLDER), и хардкод туда не поедет. conf читается из
# файла и переменных окружения — в метабазу на уровне модуля этот вызов не ходит
SNAP_CONN_ID = conf.get("logging", "REMOTE_LOG_CONN_ID")
SNAP_BUCKET = conf.get("logging", "REMOTE_BASE_LOG_FOLDER").split("//")[-1].split("/")[0]
# Префикс намеренно в корне бакета, а НЕ под префиксом логов: log_cleanup удаляет всё
# старше 30 дней ровно под remote_base_log_folder, и копия редко меняющегося DAG'а
# попала бы под эту метлу
SNAP_PREFIX = "dag_snapshots/"
SNAP_EXT = ".json.gz"

# DAG'и, чей id начинается с одного из этих префиксов, не проверяем вовсе: ни в
# статистике сериализации, ни в версиях. deadlocker_* — нагрузочный генератор, он
# плодит и переписывает DAG'и по своей воле, и в отчётах это шум, а не находка
SKIP_DAG_PREFIXES = ("deadlocker_",)


def _skip_dag(dag_id: str) -> bool:
    """DAG исключён из проверок по префиксу id."""
    return dag_id.startswith(SKIP_DAG_PREFIXES)


# То же условие для SQL. left(), а не LIKE: подчёркивание в LIKE — подстановочный знак,
# и 'deadlocker_%' поймал бы заодно deadlockerX. Префиксы — константы этого модуля,
# извне в запрос не приходит ничего
SKIP_SQL = " AND ".join(
    f"left(sd.dag_id, {len(p)}) <> '{p}'" for p in SKIP_DAG_PREFIXES
) or "TRUE"

# Сколько расхождений показывать в заметке. Ячейка — 110 символов (_diff_pair), строка
# таблицы с двумя ячейками и путём выходит под 270, а заметка режется по MAX_NOTE_LEN
# (1000): три строки влезают даже в худшем случае. Остальное — в логе, там лимита нет
NOTE_DIFFS = 3


def _short(value, limit: int = 60) -> str:
    """Однострочное представление значения для ячейки таблицы diff'а."""
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _diff_pair(before, after, limit: int = 110) -> tuple[str, str]:
    """Пара ячеек для таблицы: у длинных строк показывает место расхождения, а не начало.

    Обрезка с начала бесполезна там, где строки различаются в середине или в конце:
    у `doc_md` первые полсотни символов совпадают, и в отчёт попадали два одинаковых
    огрызка. Поэтому отбрасываем общий префикс и общий суффикс, оставляя вокруг
    расхождения немного контекста.
    """
    if not (isinstance(before, str) and isinstance(after, str)):
        return _short(before, limit), _short(after, limit)
    if len(before) <= limit and len(after) <= limit:
        return _short(before, limit), _short(after, limit)

    ctx = 24
    n = min(len(before), len(after))
    head = 0
    while head < n and before[head] == after[head]:
        head += 1
    tail = 0
    # tail < n - head: хвост не должен налезть на уже отброшенный префикс
    while tail < n - head and before[-1 - tail] == after[-1 - tail]:
        tail += 1

    start = max(0, head - ctx)

    def cut(s: str) -> str:
        end = min(len(s), len(s) - tail + ctx)
        return ("…" if start else "") + s[start:end] + ("…" if end < len(s) else "")

    return _short(cut(before), limit), _short(cut(after), limit)


def _json_diff(before, after, limit: int = 20, cell: int = 110) -> list[tuple[str, str, str]]:
    """Рекурсивно сравнивает две сериализации DAG'а: [(путь, было, стало), ...].

    Списки сравниваются поэлементно, а не как множества: для dag_hash порядок значим,
    и плавающий порядок списка — самая частая причина дрожания сериализации. Поэтому
    разная длина списка и расхождение по индексу — разные строки отчёта.

    Значения в ячейки кладёт `_diff_pair`: у длинных строк он показывает окрестность
    расхождения, а не начало, — иначе в отчёт попадают два одинаковых огрызка.
    """
    out: list[tuple[str, str, str]] = []

    def walk(x, y, path: str) -> None:
        if len(out) >= limit:
            return
        if type(x) is not type(y):
            # тип показываем явно: '1' и 1 в таблице выглядели бы одинаково
            out.append((path or ".", f"{_short(x, cell)} ({type(x).__name__})",
                        f"{_short(y, cell)} ({type(y).__name__})"))
        elif isinstance(x, dict):
            for key in dict.fromkeys(list(x) + list(y)):
                if len(out) >= limit:
                    return
                sub = f"{path}.{key}"
                if key not in x:
                    out.append((sub, "—", _short(y[key], cell)))
                elif key not in y:
                    out.append((sub, _short(x[key], cell), "—"))
                else:
                    walk(x[key], y[key], sub)
        elif isinstance(x, list):
            if len(x) != len(y):
                out.append((f"{path}[]", f"{len(x)} элем.", f"{len(y)} элем."))
            for i in range(min(len(x), len(y))):
                if len(out) >= limit:
                    return
                walk(x[i], y[i], f"{path}[{i}]")
        elif x != y:
            out.append((path or ".", *_diff_pair(x, y, cell)))

    walk(before, after, "")
    return out[:limit]


def _snap_hook():
    """S3Hook под копии.

    config не передаём: явный config затирает config_kwargs соединения целиком
    (signature_version, payload_signing_enabled), без них шлюз отвергает запросы.
    verify=False — как в log_cleanup для того же бакета.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # type: ignore
    return S3Hook(aws_conn_id=SNAP_CONN_ID, verify=False)


def _snap_key(dag_id: str, version: int, dag_hash: str) -> str:
    """Ключ версии: dag_snapshots/<dag_id>/00002.<dag_hash>.json.gz.

    dag_hash — часть имени, а не метаданные объекта: list_objects_v2 пользовательские
    метаданные не отдаёт, и чтобы понять «версия та же или новая», пришлось бы качать
    последнюю копию по каждому DAG'у. Из имени это видно бесплатно.
    """
    return f"{SNAP_PREFIX}{dag_id}/{version:05d}.{dag_hash}{SNAP_EXT}"


def _snap_index(hook) -> tuple[dict, list[str]]:
    """Индекс копий: ({dag_id: последняя версия}, все ключи).

    Значение — dict с version, hash, at (LastModified) и key последней версии, плюс
    versions — сколько их всего. dag_id в ключе не экранируется: Airflow допускает
    в нём только буквы, цифры, дефис, точку и подчёркивание, слэша там быть не может,
    поэтому rpartition('/') разбирает путь однозначно.
    """
    paginator = hook.get_conn().get_paginator("list_objects_v2")
    latest: dict = {}
    all_keys: list[str] = []
    for page in paginator.paginate(Bucket=SNAP_BUCKET, Prefix=SNAP_PREFIX,
                                   PaginationConfig={"PageSize": 1000}):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(SNAP_EXT):
                continue
            dag_id, _, tail = key[len(SNAP_PREFIX):].rpartition("/")
            ver_s, _, dag_hash = tail[:-len(SNAP_EXT)].partition(".")
            if not dag_id or not ver_s.isdigit() or _skip_dag(dag_id):
                # чужой объект под префиксом, копия старого формата или исключённый DAG.
                # Копии исключённых, если они успели накопиться, остаются в бакете:
                # мы их просто не видим — ни в покрытии, ни в списке удалённых
                continue
            all_keys.append(key)
            version = int(ver_s)
            cur = latest.get(dag_id)
            if cur is None or version > cur["version"]:
                # LastModified boto3 отдаёт уже как aware datetime — оборачивать не надо
                latest[dag_id] = {"version": version, "hash": dag_hash,
                                  "at": obj["LastModified"], "key": key,
                                  "versions": (cur or {}).get("versions", 0) + 1}
            else:
                cur["versions"] += 1
    return latest, all_keys


def _snapshot_targets(all_dags: list[str], snap_ages: dict, changed: list[str], limit: int) -> list[str]:
    """Кого копировать на этом прогоне: изменившиеся → без копии → с самой старой копией.

    limit=0 — без ограничения, берём всех. Сортировка выполняется и при нулевом лимите:
    она же задаёт порядок в логе, по которому видно, докуда дошла ротация.
    """
    rank = {d: 0 for d in changed}
    order = sorted(
        all_dags,
        # без копии — раньше любой существующей: None в ключе сортировки не сравнить с датой,
        # поэтому разводим их отдельным разрядом
        key=lambda d: (rank.get(d, 1), d in snap_ages, snap_ages.get(d), d),
    )
    return order if not limit else order[:limit]


@dag(
    doc_md=__doc__,
    default_args={
        "owner": "DataLab (CI02420667)",
        "pool": TOOLS_POOL,
        # без ретраев: перепроверка ждёт парсинг до 20 минут, повтор растянул бы прогон вдвое
        # и всё равно смотрел бы на тот же стенд
        "retries": 0,
        "on_failure_callback": on_callback,
    },
    # Часовой пояс DAG'а берётся из start_date.tzinfo (models/dag.py:614-628), поэтому
    # [core] default_timezone = utc не мешает: 23:00 — московские
    start_date=datetime(2026, 1, 1, tzinfo=MSK),
    schedule="0 23 * * *",
    # Дефолт из airflow.cfg — max_active_tasks_per_dag = 4, а recheck при RECHECK_LIMIT=25
    # растянулся бы на семь волн ожидания (до пары часов). Таски почти всё время спят
    # в ожидании парсинга, так что нагрузки это не добавляет — только занятые слоты
    max_active_tasks=12,
    tags=["DataLab", "tools", "dag", "AutoQA"],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    on_failure_callback=on_callback,
    params={
        # 0 — без ограничения, копируем все DAG'и. Ненулевое значение включает ротацию
        # (изменившиеся и самые старые копии вперёд): полное покрытие набирается за
        # ceil(всего / snapshot_limit) суток
        "snapshot_limit": Param(0, type="integer", minimum=0),
        # Удаление копий DAG'ов, которых больше нет в serialized_dag — только вручную:
        # пропажа чаще временная (Broken DAG, неудачный парсинг, деактивация по
        # dag_stale_not_seen_duration), и копия как раз тогда и нужна
        "cleanup_deleted": Param(False, type="boolean"),
    },
)
def tools_test_dags():

    # Имена групп нужны и в коде: внутри группы task_id получает префикс, а xcom_pull
    # и запрос состояний в summary ищут по полному имени
    CHECK_GROUP = "check_serialized"
    COMPARE_GROUP = "compare"

    # Периоды от большего к меньшему, накопительно: каждый следующий — подмножество
    # предыдущего. Так по таблице сразу видно, стенд «устаканился» или переразбирается
    # прямо сейчас: у здорового стенда числа падают до нуля уже на неделе
    SERIALIZED_PERIODS = [
        ("за год", "1 year"),
        ("за 3 месяца", "3 months"),
        ("за месяц", "1 month"),
        ("за неделю", "7 days"),
        ("за сутки", "1 day"),
        ("за час", "1 hour"),
    ]

    # Читается вместе с заголовком колонки: «Изменялись на последнем парсинге».
    # Это продолжение ряда периодов — самый узкий «интервал», уже не время, а одно событие
    LAST_PARSE_ROW = "на последнем парсинге"

    # Сколько DAG'ов отдаём на перепроверку. Каждый ждёт своего следующего парсинга, то есть
    # до min_file_process_interval, а параллельность ограничена max_active_tasks_per_dag
    # (в проме 4): при 25 целях прогон растянется на ~7 волн ожидания
    RECHECK_LIMIT = 25

    @task(
        task_id="check_serialized_dag",
        doc_md=("Статистика `main.serialized_dag`: как давно менялась сериализация DAG'ов. "
                "Не падает — отдаёт подозрительные DAG'и на перепроверку"),
    )
    def check_serialized_dag(**context) -> list[dict]:
        """Считает, у скольких DAG'ов менялась сериализация за год, 3 месяца, месяц, неделю, сутки и час.

        Строка в serialized_dag переписывается только когда меняется dag_hash, то есть
        содержимое DAG'а после парсинга файла (serialized_dag.py:170). На стабильном стенде
        последний парсинг файла не должен был ничего переписать — если переписал, значит
        парсинг либо даёт каждый раз новый результат (значение из БД или API на уровне
        модуля, текущее время, плавающий порядок списка), либо только что был деплой.

        Различить эти два случая по одному замеру нельзя, поэтому таск не падает никогда:
        он только считает статистику и возвращает список подозрительных DAG'ов, а вердикт
        выносит recheck_serialized_dag, дождавшись следующего парсинга. Возврат — список
        для expand, статистика уходит в XCom `serialized_stats` и в заметку.
        """
        import time

        from airflow.configuration import conf
        from airflow.utils.session import create_session
        from sqlalchemy import text

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        # Окно одного парсинга: write_dag пишет last_updated раньше, чем bulk_write_to_db —
        # last_parsed_time (dagbag.py:716-723, одна транзакция), поэтому сериализация,
        # сделанная на последнем парсинге, отстаёт от last_parsed_time не больше, чем длится
        # сам парсинг. Верхняя граница — dag_file_processor_timeout, и она заведомо меньше
        # min_file_process_interval, то есть окно не дотянется до прошлого парсинга.
        parse_window = conf.getint("core", "dag_file_processor_timeout", fallback=600)
        at_parse_cond = (f"d.last_parsed_time IS NOT NULL AND "
                         f"sd.last_updated >= d.last_parsed_time - interval '{parse_window} seconds'")

        # интервалы — константы из SERIALIZED_PERIODS, parse_window — int из conf:
        # в запрос не приходит ничего извне
        stats_sql = (
            "SELECT COUNT(*) AS total, "
            + ", ".join(
                f"COUNT(*) FILTER (WHERE sd.last_updated > now() - interval '{iv}') AS p{i}"
                for i, (_, iv) in enumerate(SERIALIZED_PERIODS)
            )
            + f", COUNT(*) FILTER (WHERE {at_parse_cond}) AS at_parse"
            + " FROM main.serialized_dag sd LEFT JOIN main.dag d USING (dag_id)"
            + f" WHERE {SKIP_SQL}"
        )
        # Полный список уходит в XCom, в заметку попадают только первые note_rows
        list_sql = f"""
            SELECT sd.last_updated, sd.dag_id, sd.fileloc, sd.dag_hash, d.last_parsed_time
            FROM main.serialized_dag sd
            JOIN main.dag d USING (dag_id)
            WHERE {at_parse_cond} AND {SKIP_SQL}
            ORDER BY sd.last_updated DESC
            LIMIT 500
        """
        note_rows = 10  # заметка режется по MAX_NOTE_LEN, а таблица со статистикой важнее списка

        ts = time.time()
        with create_session() as session:
            row = session.execute(text(stats_sql)).one()
            names = [name for name, _ in SERIALIZED_PERIODS]
            total, counts = row[0], dict(zip(names, row[1:1 + len(names)]))
            at_parse = row[1 + len(names)]
            rows = session.execute(text(list_sql)).fetchall() if at_parse else []

        elapsed = time.time() - ts
        logger.info("🔍 serialized_dag: всего %d, %s, %s %d", total,
                    ", ".join(f"{name} {cnt}" for name, cnt in counts.items()),
                    LAST_PARSE_ROW, at_parse)

        def share(cnt: int) -> str:
            return f"{cnt * 100 / total:.0f}%" if total else "—"

        stats = "\n".join(
            ["| Изменялись | DAG'ов | Доля |", "|---|---:|---:|", f"| **всего** | **{total}** | 100% |"]
            + [f"| {name} | {counts[name]} | {share(counts[name])} |" for name, _ in SERIALIZED_PERIODS]
            + [f"| **{LAST_PARSE_ROW}** | **{at_parse}** | {share(at_parse)} |"]
        )
        result = {"total": total, **{name: counts[name] for name, _ in SERIALIZED_PERIODS},
                  LAST_PARSE_ROW: at_parse}

        add_xcom("serialized_stats", result, context)

        if not at_parse:
            add_note(stats, context, level="task,DAG", title=f"✅ {elapsed:.2f} sec check_serialized_dag")
            return []

        msg = (f"main.serialized_dag: {at_parse} из {total} DAG'ов пересериализовано "
               f"на последнем парсинге файла")
        data = [
            {"last_updated": str(last_updated), "dag_id": dag_id, "fileloc": fileloc,
             "dag_hash": dag_hash, "last_parsed_time": str(last_parsed_time)}
            for last_updated, dag_id, fileloc, dag_hash, last_parsed_time in rows
        ]
        add_xcom("serialized_dag", data, context)

        # last_parsed_time и dag_hash в заметке не нужны: первый по условию отбора и так
        # совпадает с last_updated с точностью до парсинга, второй ни с чем не сравнить.
        # Оба остаются в XCom `serialized_dag` — там их можно смотреть построчно
        table = "| last_updated | dag_id |\n|---|---|\n" + "\n".join(
            f"| {r['last_updated']} | `{r['dag_id']}` |" for r in data[:note_rows]
        )
        if at_parse > note_rows:
            table += (f"\n\nПоказаны первые {min(note_rows, len(data))} из {at_parse}, "
                      f"полный список — в XCom `serialized_dag`.")

        targets = [{"dag_id": r["dag_id"], "dag_hash": r["dag_hash"]} for r in data[:RECHECK_LIMIT]]
        tail = f" (из {at_parse})" if at_parse > RECHECK_LIMIT else ""
        add_note(f"{msg}\n\n{stats}\n\n{table}", context, level="task",
                 title=f"⚠️ {elapsed:.2f} sec check_serialized_dag: на перепроверку {len(targets)}{tail}")
        logger.warning("%s; на перепроверку %d%s: %s", msg, len(targets), tail,
                       ", ".join(t["dag_id"] for t in targets))
        return targets

    @task(task_id="recheck_serialized_dag", map_index_template="{{ target_dag_id }}")
    def recheck_serialized_dag(target: dict, **context) -> dict:
        """Ждёт следующего парсинга DAG'а и сравнивает сериализацию до и после.

        check_serialized_dag говорит только «сериализация переписана на последнем
        парсинге». Этот таск отвечает на следующий вопрос — что именно в ней меняется:
        снимает JSON, дожидается сдвига `dag.last_parsed_time`, снимает второй и
        показывает расхождения по путям. Одинаковый dag_hash после нового парсинга
        означает, что прошлое изменение было разовым (деплой), а не дрожанием.

        Здесь и выносится вердикт, поэтому таск падает — на `unstable` (второй парсинг снова
        переписал сериализацию) и на `duplicate_dag_id` (сменился fileloc: на один dag_id
        претендуют два файла, и они затирают сериализацию друг друга). `no_parse` не падение:
        за окно ожидания парсинга могло не случиться из-за очереди в dag-processor'е.

        Итог всегда уходит в XCom `recheck` (до возможного падения — иначе при падении
        return-значения бы не осталось), оттуда его собирает summary.
        """
        import time

        from airflow.configuration import conf
        from airflow.exceptions import AirflowFailException, AirflowSkipException
        from airflow.models.dag import DagModel
        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.operators.python import get_current_context
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        dag_id = target["dag_id"]
        # В UI вместо «Map Index 0,1,2…» показываем dag_id. map_index_template рендерится
        # по контексту уже ПОСЛЕ execute (taskinstance.py:3174-3192), поэтому ключ кладём
        # в объект из get_current_context(): наш **context — отдельная копия kwargs,
        # её правка до шаблона не доедет. При скипе имя тоже проставится — рендер идёт
        # и в except-ветке
        get_current_context()["target_dag_id"] = dag_id
        poke = 30
        # Худший случай — начали ждать сразу после парсинга: полный интервал плюс сам
        # разбор файла. Ждать дольше смысла нет: если за это время парсинга не случилось,
        # проблема не в дрожании, а в том, что файл вообще перестали разбирать
        timeout = (conf.getint("scheduler", "min_file_process_interval", fallback=30)
                   + conf.getint("core", "dag_file_processor_timeout", fallback=600))

        def snapshot() -> tuple:
            """(dag_hash, data, last_parsed_time, fileloc); data берём через ORM — она распакует zlib."""
            with create_session() as session:
                sdm = session.query(SerializedDagModel).filter(SerializedDagModel.dag_id == dag_id).one_or_none()
                parsed = session.query(DagModel.last_parsed_time).filter(DagModel.dag_id == dag_id).scalar()
                # fileloc берём из колонки, а не из JSON: она есть и при compress_serialized_dags
                return (sdm.dag_hash, sdm.data, parsed, sdm.fileloc) if sdm else (None, None, parsed, None)

        hash0, data0, parsed0, loc0 = snapshot()
        if hash0 is None or parsed0 is None:
            msg = f"☮️ {dag_id}: нет в serialized_dag или в dag — сравнивать не с чем"
            add_note(msg, context, level="task", title=f"☮️ {dag_id}")
            logger.warning(msg)
            raise AirflowSkipException(msg)

        ts = time.time()
        deadline = ts + timeout
        logger.info("⏳ %s: ждём следующего парсинга, last_parsed_time=%s, таймаут %dс",
                    dag_id, parsed0, timeout)

        hash1, data1, parsed1, loc1 = hash0, data0, parsed0, loc0
        poll = 0
        while time.time() < deadline:
            time.sleep(poke)
            hash1, data1, parsed1, loc1 = snapshot()
            poll += 1
            # Пишем на каждом опросе, а не только по факту: промежуточная выгрузка лога
            # в S3 (hrp_adapter/logging/handlers.py) дёргается из emit, то есть по записи,
            # а не по таймеру. Без этой строки лог молчащего таска не обновлялся бы все
            # 20 минут ожидания, и со стороны это неотличимо от зависшего воркера
            logger.info("⏳ %s: опрос %d, ждём %.0fс из %d, last_parsed_time=%s",
                        dag_id, poll, time.time() - ts, timeout, parsed1)
            if parsed1 and parsed1 > parsed0:
                break
        else:
            waited = time.time() - ts
            msg = (f"⏱️ {dag_id}: за {waited:.0f}с парсинга не было "
                   f"(last_parsed_time всё ещё {parsed0}) — файл не разбирается")
            add_note(msg, context, level="task", title=f"⏱️ {dag_id}")
            logger.warning(msg)
            result = {"dag_id": dag_id, "status": "no_parse", "waited": round(waited), "diffs": 0}
            add_xcom("recheck", result, context)
            return result

        waited = time.time() - ts
        if hash1 == hash0:
            msg = (f"✅ {dag_id}: парсинг прошёл через {waited:.0f}с, "
                   f"сериализация не изменилась (dag_hash `{hash0}`) — разовое изменение")
            add_note(msg, context, level="task", title=f"✅ {dag_id}")
            logger.info(msg)
            result = {"dag_id": dag_id, "status": "stable", "waited": round(waited), "diffs": 0}
            add_xcom("recheck", result, context)
            return result

        diffs = _json_diff(data0 or {}, data1 or {})

        # Сменился fileloc — значит на один dag_id претендуют два файла, и каждый парсинг
        # переписывает чужую сериализацию (PK в serialized_dag — dag_id). Это не
        # недетерминированный разбор, а коллизия, и чинится она совсем иначе. Airflow сам
        # такое не ловит: AirflowDagDuplicatedIdException срабатывает внутри одного DagBag,
        # то есть одного файла, а разные файлы разбираются независимо
        if loc0 and loc1 and loc0 != loc1:
            status = "duplicate_dag_id"
            head = (f"❌ {dag_id}: два файла на один dag_id — `{loc0}` и `{loc1}`. "
                    f"Каждый парсинг переписывает чужую сериализацию; убрать или "
                    f"переименовать дубль")
        else:
            status = "unstable"
            head = (f"❌ {dag_id}: сериализация переписана снова через {waited:.0f}с "
                    f"(`{hash0}` → `{hash1}`) — разбор файла недетерминирован")

        for path, was, became in diffs:
            logger.warning("  %s: %s → %s", path, was, became)
        if diffs:
            table = ("| Путь | Было | Стало |\n|---|---|---|\n"
                     + "\n".join(f"| `{p}` | {a} | {b} |" for p, a, b in diffs[:NOTE_DIFFS]))
        else:
            # data пустая при compress_serialized_dags, либо расхождение вне JSON
            table = "Расхождений в JSON не нашлось, хотя dag_hash разный"
        if len(diffs) > NOTE_DIFFS:
            table += f"\n\nПоказаны {NOTE_DIFFS} из {len(diffs)}, остальные — в логе."
        add_note(f"{head}\n\n{table}", context, level="task", title=f"❌ {dag_id}")
        logger.error(head)
        add_xcom("recheck", {"dag_id": dag_id, "status": status, "waited": round(waited),
                             "diffs": len(diffs), "paths": [p for p, _, _ in diffs],
                             "filelocs": [loc0, loc1] if status == "duplicate_dag_id" else [loc0]},
                 context)
        raise AirflowFailException(head)

    # --- Копии сериализаций в S3 ---

    @task(
        task_id="find_changed",
        doc_md=("Ищет DAG'и, у которых `dag_hash` разошёлся с последней сохранённой версией, "
                "то есть изменившиеся с прошлого прогона. Сравнением занимается "
                "`compare_changed` — после того, как snapshot_dags запишет новую версию"),
    )
    def find_changed(**context) -> list[str]:
        """Отбирает изменившиеся DAG'и, ничего не скачивая.

        «Изменился» определяется не окном по времени, а сравнением `dag_hash` с последней
        сохранённой версией: hash другой — значит DAG менялся с тех пор, как мы его
        записали, то есть с прошлого прогона (или с того, на котором до него дошла
        ротация). Окно по часам такого не даёт: при пропущенном или задержавшемся
        прогоне оно либо теряет изменения, либо повторно показывает уже показанные.

        Хэш последней версии виден прямо в имени объекта, поэтому отбор стоит одного
        листинга бакета и одного запроса метаданных — без единого GET.
        """
        import time

        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        note_rows = 15  # заметка режется по MAX_NOTE_LEN, счётчики важнее списка

        ts = time.time()
        snaps, _ = _snap_index(_snap_hook())

        # Тянем только метаданные: data по каждому DAG'у прочитает mapped-таск, и лишь
        # для тех, у кого hash разошёлся
        with create_session() as session:
            current = [
                {"dag_id": r[0], "dag_hash": r[1], "last_updated": r[2]}
                for r in session.query(
                    SerializedDagModel.dag_id, SerializedDagModel.dag_hash,
                    SerializedDagModel.last_updated,
                ).all()
                if not _skip_dag(r[0])
            ]

        # Сравнивать не с чем, пока версии нет: такие DAG'и только считаем — на первом
        # прогоне это все, и построчно они бы залили и заметку, и XCom
        no_snapshot = [c["dag_id"] for c in current if c["dag_id"] not in snaps]
        changed = sorted(
            (c for c in current
             if c["dag_id"] in snaps and snaps[c["dag_id"]]["hash"] != c["dag_hash"]),
            key=lambda c: c["last_updated"], reverse=True,
        )
        elapsed = time.time() - ts
        logger.info("🔍 всего DAG'ов %d, с версиями %d, изменилось с прошлого прогона %d, "
                    "без версий %d", len(current), len(snaps), len(changed), len(no_snapshot))
        for c in changed:
            logger.info("  %s: версия %05d, изменён %s",
                        c["dag_id"], snaps[c["dag_id"]]["version"], c["last_updated"])

        if changed:
            table = "| DAG | Версия | Изменён |\n|---|---:|---|\n" + "\n".join(
                f"| `{c['dag_id']}` | {snaps[c['dag_id']]['version']:05d} | {c['last_updated']} |"
                for c in changed[:note_rows])
            if len(changed) > note_rows:
                table += f"\n\nПоказаны первые {note_rows} из {len(changed)}."
        else:
            table = "С прошлого прогона ни один DAG не менялся"

        dag_ids = [c["dag_id"] for c in changed]
        add_xcom("changed_dags", dag_ids, context)
        add_note(table, context, level="task",
                 title=(f"🔍 {elapsed:.2f} sec find_changed: {len(changed)} из {len(current)}"
                        + (f", без версий {len(no_snapshot)}" if no_snapshot else "")))
        add_xcom("find_stats", {"total": len(current), "changed": len(changed),
                                "snapshots": len(snaps), "no_snapshot": len(no_snapshot)}, context)
        return dag_ids

    @task(
        task_id="snapshot_dags",
        doc_md=("Складывает копии сериализаций в S3. `snapshot_limit=0` — все DAG'и, иначе "
                "ротация: изменившиеся → без копии → с самой старой копией"),
    )
    def snapshot_dags(changed: list, **context) -> list[dict]:
        """Пишет новые версии и возвращает пары «прошлая версия → новая» для expand.

        Пары идут именно return-значением: expand умеет раскрываться только по
        `return_value` и на кастомном XCom-ключе падает ещё при разборе файла
        (`mappedoperator.py:132`, «cannot map over XCom with custom key»). Поэтому
        статистика для summary уезжает в ключ `snapshot_stats`, а не наоборот.
        """
        import gzip
        import json
        import time

        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note, add_xcom, readable_size  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom, readable_size  # type: ignore

        limit = int(context["params"]["snapshot_limit"])
        cleanup = bool(context["params"]["cleanup_deleted"])

        ts = time.time()
        hook = _snap_hook()
        snaps, all_keys = _snap_index(hook)
        with create_session() as session:
            all_dags = [r[0] for r in session.query(SerializedDagModel.dag_id).all()
                        if not _skip_dag(r[0])]

        snap_ages = {d: v["at"] for d, v in snaps.items()}
        targets = _snapshot_targets(all_dags, snap_ages, changed, limit)
        logger.info("📦 всего DAG'ов %d, с копиями %d (версий %d), к обходу %d (лимит %s)",
                    len(all_dags), len(snaps), len(all_keys), len(targets), limit or "нет")

        written = first = unchanged = total_bytes = 0
        pairs: list[dict] = []
        captured_at = datetime.now(timezone.utc).isoformat()
        for dag_id in targets:
            last = snaps.get(dag_id)
            with create_session() as session:
                sdm = (session.query(SerializedDagModel)
                       .filter(SerializedDagModel.dag_id == dag_id).one_or_none())
                if sdm is None:
                    continue  # DAG исчез между листингом и обходом
                if last and last["hash"] == sdm.dag_hash:
                    # Содержимое то же — новую версию не плодим, история остаётся читаемой:
                    # в ней ровно те точки, где DAG менялся
                    unchanged += 1
                    continue
                # JSON забираем внутри сессии: data — свойство модели, оно распакует zlib.
                # dag_hash тоже кладём в локальную — за пределами сессии инстанс отцеплен
                version = (last["version"] + 1) if last else 1
                dag_hash = sdm.dag_hash
                body = gzip.compress(json.dumps({
                    "dag_id": dag_id,
                    "version": version,
                    "dag_hash": dag_hash,
                    "fileloc": sdm.fileloc,
                    "last_updated": str(sdm.last_updated),
                    "captured_at": captured_at,
                    "prev_hash": last["hash"] if last else None,
                    "data": sdm.data,
                }, ensure_ascii=False).encode("utf-8"))

            new_key = _snap_key(dag_id, version, dag_hash)
            hook.load_bytes(body, key=new_key, bucket_name=SNAP_BUCKET, replace=True)
            written += 1
            total_bytes += len(body)
            if last is None:
                first += 1  # первая версия: сравнивать не с чем, в пары не идёт
            else:
                pairs.append({"dag_id": dag_id,
                              "prev_key": last["key"], "prev_version": last["version"],
                              "new_key": new_key, "new_version": version})
            logger.info("  %s: версия %05d (%s)", dag_id, version, readable_size(len(body)))

        # DAG'и, которых больше нет в serialized_dag: удалены совсем либо пропали временно
        deleted = sorted(set(snaps) - set(all_dags))
        deleted_keys = [k for k in all_keys
                        if k[len(SNAP_PREFIX):].rpartition("/")[0] in set(deleted)]
        if deleted and cleanup:
            # ВНИМАНИЕ: у delete_objects параметр `bucket`, а не `bucket_name` — в отличие
            # от list_keys/load_bytes. Непоследовательность API провайдера.
            # Стираем все версии удалённого DAG'а, а не только последнюю
            hook.delete_objects(bucket=SNAP_BUCKET, keys=deleted_keys)
            logger.warning("стёрты версии удалённых DAG'ов: %d (объектов %d)",
                           len(deleted), len(deleted_keys))
        elif deleted:
            logger.info("удалённых DAG'ов %d (версий %d), стирание выключено "
                        "(cleanup_deleted): %s", len(deleted), len(deleted_keys),
                        ", ".join(deleted[:10]))

        # Покрытие считаем только по живым DAG'ам: версии удалённых в бакете есть, но
        # к покрытию отношения не имеют — иначе оно уезжало бы за 100%
        covered = len((set(snaps) | set(targets)) & set(all_dags))
        total = len(all_dags) or 1
        oldest = min(snap_ages.values()) if snap_ages else None
        versions = len(all_keys) + written - (len(deleted_keys) if cleanup else 0)
        elapsed = time.time() - ts

        rows = [
            f"| новых версий | {written} |",
            f"| из них первых | {first} |",
            f"| на сравнение | {len(pairs)} |",
            f"| без изменений | {unchanged} |",
            f"| объём выгрузки | {readable_size(total_bytes)} |",
            f"| покрытие | {covered} из {len(all_dags)} ({covered * 100 / total:.0f}%) |",
            f"| версий в хранилище | {versions} |",
            f"| удалённых DAG'ов | {len(deleted)}{f' (версии стёрты: {len(deleted_keys)})' if deleted and cleanup else ''} |",
            f"| самая старая копия | {oldest or '—'} |",
        ]
        if limit and covered < len(all_dags):
            days = -(-(len(all_dags) - covered) // limit)  # ceil
            rows.append(f"| полное покрытие через | ~{days} сут |")

        add_note("| | |\n|---|---:|\n" + "\n".join(rows), context, level="task",
                 title=f"📦 {elapsed:.2f} sec snapshot_dags: +{written} версий")
        logger.info("📦 новых версий %d (%s), без изменений %d, покрытие %d/%d, на сравнение %d",
                    written, readable_size(total_bytes), unchanged, covered, len(all_dags), len(pairs))

        add_xcom("snapshot_stats",
                 {"written": written, "first": first, "unchanged": unchanged,
                  "bytes": total_bytes, "covered": covered, "total": len(all_dags),
                  "versions": versions, "pairs": len(pairs), "deleted": len(deleted),
                  "cleaned": bool(deleted and cleanup)}, context)
        return pairs

    @task(task_id="compare_changed", map_index_template="{{ target_dag_id }}")
    def compare_changed(target: dict, **context) -> dict:
        """Сравнивает две соседние версии сериализации одного DAG'а.

        Обе версии — неизменяемые объекты в S3, а не «копия против текущего состояния
        БД», поэтому результат воспроизводим: перезапуск таска через неделю покажет ровно
        то же самое. Отсюда и место в цепочке — после snapshot_dags, который эту пару
        и создал.

        Не падает: изменение сериализации после деплоя — норма. Вердикт в заметке,
        в логе и в XCom `compare`.
        """
        import gzip
        import json
        import time

        from airflow.operators.python import get_current_context

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        dag_id = target["dag_id"]
        # В UI вместо «Map Index 0,1,2…» показываем dag_id. Шаблон рендерится по контексту
        # уже ПОСЛЕ execute (taskinstance.py:3174-3192), поэтому ключ кладём в объект из
        # get_current_context(): наш **context — отдельная копия kwargs
        get_current_context()["target_dag_id"] = dag_id

        ts = time.time()
        hook = _snap_hook()

        def load(key):
            raw = hook.get_key(key, bucket_name=SNAP_BUCKET).get()["Body"].read()
            return json.loads(gzip.decompress(raw))

        versions = f"{target['prev_version']:05d} → {target['new_version']:05d}"
        result = {"dag_id": dag_id, "diffs": 0, "paths": [],
                  "prev_version": target["prev_version"], "new_version": target["new_version"]}
        try:
            prev, new = load(target["prev_key"]), load(target["new_key"])
        except Exception as err:
            msg = f"⚠️ {dag_id}: версии {versions} не читаются — {err}"
            result["status"] = "snapshot_broken"
            add_note(msg, context, level="task", title=f"⚠️ {dag_id}")
            logger.warning(msg)
            add_xcom("compare", result, context)
            return result

        diffs = _json_diff(prev.get("data") or {}, new.get("data") or {})
        result["diffs"] = len(diffs)
        result["paths"] = [p for p, _, _ in diffs]
        elapsed = time.time() - ts

        if prev.get("fileloc") and new.get("fileloc") and prev["fileloc"] != new["fileloc"]:
            # два файла на один dag_id: каждый парсинг переписывает чужую сериализацию
            result["status"] = "duplicate_dag_id"
            result["filelocs"] = [prev["fileloc"], new["fileloc"]]
            head = (f"👯 {dag_id}: два файла на один dag_id — `{prev['fileloc']}` и "
                    f"`{new['fileloc']}`. Каждый парсинг переписывает чужую сериализацию; "
                    f"убрать или переименовать дубль")
            icon = "👯"
        elif diffs:
            result["status"] = "changed"
            head = f"📝 {dag_id}: версии {versions}, расхождений {len(diffs)}"
            icon = "📝"
        else:
            result["status"] = "same_json"
            head = f"✅ {dag_id}: версии {versions}, dag_hash разный, а JSON совпал"
            icon = "✅"

        for path, was, became in diffs:
            logger.info("  %s: %s → %s", path, was, became)
        if diffs:
            table = ("| Путь | Было | Стало |\n|---|---|---|\n"
                     + "\n".join(f"| `{p}` | {a} | {b} |" for p, a, b in diffs[:NOTE_DIFFS]))
            if len(diffs) > NOTE_DIFFS:
                table += f"\n\nПоказаны {NOTE_DIFFS} из {len(diffs)}, остальные — в логе."
        else:
            table = "Расхождений в JSON нет"

        add_note(f"{head}\n\n{table}", context, level="task",
                 title=f"{icon} {elapsed:.2f} sec {dag_id}")
        logger.info(head)
        add_xcom("compare", result, context)
        return result

    # --- Время парсинга (вне групп: это про файлы, а не про сериализацию) ---
    @task(
        task_id="parse_time",
        doc_md=("Разбирает все файлы DAG'ов и ищет выбросы по времени: медленнее "
                "`среднее + 3σ`. Не падает — только отчёт"),
    )
    def parse_time(**context) -> dict:
        """Ищет файлы, которые парсятся аномально долго.

        Длительности разбора в метабазе нет: `dag.last_parsed_time` — это момент, а не
        сколько заняло. Поэтому меряем сами, тем же способом, что и `airflow dags report`:
        `DagBag` при обходе папки складывает в `dagbag_stats` по `FileLoadStat` на файл
        (`models/dagbag.py:600-618`).

        Порог — среднее плюс три сигмы по всем файлам. Три сигмы имеют смысл только на
        достаточной выборке, поэтому при `MIN_FILES` файлах и меньше отчёт ограничивается
        самыми медленными без вердикта «выброс».

        Свойство метода, о котором стоит помнить: несколько тормозов сразу раздувают σ и
        могут замаскировать друг друга — два файла по 30с среди десяти по полсекунды
        поднимают порог до 40с и в выбросы не попадают. Поэтому таблица самых медленных
        печатается всегда, а не только когда выброс нашёлся.

        Разбор идёт в воркере и импортирует все файлы разом, то есть выполняет их код
        верхнего уровня. У DAG'ов ctl там обращения к метабазе и секрет-бэкенду — на
        чтение, но время прогона от этого зависит и растёт вместе с числом файлов.

        Таск вне групп: он про то, как разбираются файлы, а не про то, что получается
        в serialized_dag.
        """
        import statistics
        import time

        from airflow.configuration import conf as af_conf
        from airflow.models.dagbag import DagBag

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        MIN_FILES = 5  # noqa: N806 — меньше выборки: сигма бессмысленна
        SIGMAS = 3  # noqa: N806
        note_rows = 10

        folder = af_conf.get("core", "dags_folder")
        timeout = af_conf.getint("core", "dag_file_processor_timeout", fallback=600)

        ts = time.time()
        # include_examples=False: примеры в папке не лежат, но дефолт берётся из конфига,
        # и полагаться на него незачем. read_dags_from_db не трогаем — нам нужен разбор
        # файлов, а не чтение готовой сериализации
        bag = DagBag(dag_folder=folder, include_examples=False)
        elapsed = time.time() - ts

        rows = [{"file": s.file, "sec": s.duration.total_seconds(),
                 "dags": s.dag_num, "tasks": s.task_num, "warnings": s.warning_num}
                for s in bag.dagbag_stats]
        if not rows:
            msg = f"В {folder} не нашлось ни одного файла с DAG'ами"
            add_note(msg, context, level="task", title="🔘 parse_time")
            logger.warning(msg)
            return {"files": 0, "outliers": 0}

        secs = [r["sec"] for r in rows]
        mean = statistics.fmean(secs)
        # stdev требует минимум двух значений и считает выборочное отклонение
        sigma = statistics.stdev(secs) if len(secs) > 1 else 0.0
        threshold = mean + SIGMAS * sigma
        enough = len(rows) > MIN_FILES

        outliers = [r for r in rows if enough and sigma > 0 and r["sec"] > threshold]
        for r in outliers:
            logger.warning("🐢 %s: %.2fс при пороге %.2fс (dags %d, tasks %d)",
                           r["file"], r["sec"], threshold, r["dags"], r["tasks"])

        # Отдельно — те, кто подобрался к таймауту процессора: такой файл dag-processor
        # бросит на полпути, и DAG'и из него исчезнут из serialized_dag
        near_timeout = [r for r in rows if r["sec"] > timeout / 2]

        shown = outliers or rows[:note_rows]  # dagbag_stats уже отсортирован по убыванию
        table = "| Файл | Сек | DAG'ов | Тасков |\n|---|---:|---:|---:|\n" + "\n".join(
            f"| `{_short(r['file'], 70)}` | {r['sec']:.2f} | {r['dags']} | {r['tasks']} |"
            for r in shown[:note_rows])
        stats_block = "\n".join([
            "| | |", "|---|---:|",
            f"| файлов | {len(rows)} |",
            f"| суммарно | {sum(secs):.1f}с |",
            f"| среднее | {mean:.2f}с |",
            f"| σ | {sigma:.2f}с |",
            f"| порог (среднее + {SIGMAS}σ) | {threshold:.2f}с |",
            f"| выбросов | {len(outliers) if enough else '—'} |",
        ])
        head = (f"🐢 Медленнее порога: {len(outliers)}" if outliers else
                "Выбросов нет" if enough else
                f"Файлов {len(rows)} — для {SIGMAS}σ мало, показаны самые медленные")
        if near_timeout:
            head += (f". ⚠️ {len(near_timeout)} файлов уже за половиной "
                     f"dag_file_processor_timeout ({timeout}с)")

        add_xcom("parse_time", {"stats": {"files": len(rows), "mean": round(mean, 2),
                                          "sigma": round(sigma, 2),
                                          "threshold": round(threshold, 2)},
                                "outliers": outliers}, context)
        add_note(f"{head}\n\n{stats_block}\n\n{table}", context, level="task",
                 title=f"🕐 {elapsed:.1f} sec parse_time: {len(rows)} файлов")
        logger.info("🕐 разобрано %d файлов за %.1fс, среднее %.2fс, σ %.2fс, порог %.2fс, "
                    "выбросов %d", len(rows), elapsed, mean, sigma, threshold, len(outliers))
        return {"files": len(rows), "outliers": len(outliers), "mean": round(mean, 2),
                "sigma": round(sigma, 2), "threshold": round(threshold, 2),
                "near_timeout": len(near_timeout)}

    # --- Summary ---
    @task(task_id="summary", trigger_rule=TriggerRule.ALL_DONE)
    def summary(**context) -> dict:
        """Сводка по всем экземплярам перепроверки: вердикт, ожидание, число расхождений."""
        import json

        from airflow.exceptions import AirflowFailException
        from airflow.models import TaskInstance
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        icon_by_status = {"stable": "✅", "no_parse": "⏱️",
                          "unstable": "❌", "duplicate_dag_id": "👯"}
        dag_run = context["dag_run"]
        ti = context["ti"]

        # Вердикты берём из XCom, а не из состояний тасков: состояние скажет только
        # «failed», а здесь есть причина, время ожидания и число расхождений. Для
        # mapped-таска xcom_pull отдаёт итератор по map-индексам (taskinstance.py:3711-3715)
        raw = ti.xcom_pull(task_ids=f"{CHECK_GROUP}.recheck_serialized_dag", key="recheck")
        rechecks = [json.loads(r) if isinstance(r, str) else r for r in list(raw or [])]

        stats_raw = ti.xcom_pull(task_ids=f"{CHECK_GROUP}.check_serialized_dag",
                                 key="serialized_stats")
        stats = json.loads(stats_raw) if isinstance(stats_raw, str) else (stats_raw or {})

        # Состояния нужны только чтобы поймать экземпляры, не оставившие XCom вовсе:
        # упавший до add_xcom или не запущенный. Оба mapped-таска забираем одним запросом
        recheck_id = f"{CHECK_GROUP}.recheck_serialized_dag"
        compare_id = f"{COMPARE_GROUP}.compare_changed"
        states: dict[str, dict[int, str]] = {recheck_id: {}, compare_id: {}}
        with create_session() as session:
            for task_id, map_index, state in (
                session.query(TaskInstance.task_id, TaskInstance.map_index, TaskInstance.state)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id,
                    TaskInstance.task_id.in_([recheck_id, compare_id]),
                )
                .all()
            ):
                states[task_id][map_index] = state

        silent = sum(1 for m, s in states[recheck_id].items()
                     if m >= 0 and s in ("failed", "upstream_failed")) \
            - sum(1 for r in rechecks if r.get("status") in ("unstable", "duplicate_dag_id"))

        counts: dict[str, int] = {}
        for r in rechecks:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        bad = counts.get("unstable", 0) + counts.get("duplicate_dag_id", 0) + max(silent, 0)

        graph = "".join(icon_by_status.get(r["status"], "❔") for r in rechecks)
        headline = (f"{graph}\n\n" if graph else "") + " / ".join(
            f"{icon_by_status.get(st, '❔')} {st} {n}" for st, n in sorted(counts.items())
        ) or "перепроверять было нечего"

        # Ветка версий: у неё свои вердикты, на падение summary они не влияют —
        # изменение сериализации после деплоя это норма
        found = ti.xcom_pull(task_ids=f"{COMPARE_GROUP}.find_changed", key="find_stats") or {}
        found = json.loads(found) if isinstance(found, str) else found
        snapshot = ti.xcom_pull(task_ids=f"{COMPARE_GROUP}.snapshot_dags",
                                key="snapshot_stats") or {}
        snapshot = json.loads(snapshot) if isinstance(snapshot, str) else snapshot
        parsed = ti.xcom_pull(task_ids="parse_time") or {}
        raw_cmp = ti.xcom_pull(task_ids=f"{COMPARE_GROUP}.compare_changed", key="compare")
        compares = [json.loads(r) if isinstance(r, str) else r for r in list(raw_cmp or [])]
        cmp_counts: dict[str, int] = {}
        for r in compares:
            cmp_counts[r["status"]] = cmp_counts.get(r["status"], 0) + 1

        # compare_changed по замыслу не падает, поэтому «немой» экземпляр — это
        # неожиданное исключение. Пропустить его нельзя: версия уже записана, и
        # на следующем прогоне find_changed этот DAG не увидит — хэши совпадут.
        # Обе версии остались в S3, так что чинится очисткой экземпляра
        cmp_ran = [m for m, s in states[compare_id].items()
                   if m >= 0 and s not in (None, "skipped", "removed")]
        cmp_silent = max(len(cmp_ran) - len(compares), 0)
        if cmp_silent:
            logger.error("compare_changed: %d экземпляров не отчитались — очистите их, "
                         "иначе изменение потеряется: версии уже записаны", cmp_silent)

        parts = []
        if stats:
            parts.append("| Изменялись | DAG'ов |\n|---|---:|\n" + "\n".join(
                f"| {k} | {v} |" for k, v in stats.items()))
        if rechecks:
            parts.append("| DAG | Вердикт | Ждали | Расхождений |\n|---|---|---:|---:|\n" + "\n".join(
                f"| `{r['dag_id']}` | {icon_by_status.get(r['status'], '❔')} {r['status']} "
                f"| {r.get('waited', 0)}s | {r.get('diffs', 0)} |"
                for r in rechecks
            ))
        if compares:
            parts.append(f"С прошлого прогона изменилось {found.get('changed')} "
                         f"из {found.get('total')} DAG'ов:\n\n"
                         + "| DAG | Версии | Вердикт | Расхождений |\n|---|---|---|---:|\n"
                         + "\n".join(
                             f"| `{r['dag_id']}` | {r.get('prev_version', 0):05d} → "
                             f"{r.get('new_version', 0):05d} | {r['status']} | {r['diffs']} |"
                             for r in compares))
        elif found and not cmp_silent:
            parts.append(f"С прошлого прогона не менялся ни один из {found.get('total')} DAG'ов")
        if cmp_silent:
            parts.append(f"⚠️ **{cmp_silent}** сравнений не отчитались. Версии уже записаны, "
                         f"поэтому следующий прогон эти DAG'и не переоткроет — очистите "
                         f"упавшие экземпляры `{compare_id}`, они перечитают те же объекты "
                         f"из S3 и дадут тот же вердикт.")
        if parsed.get("files"):
            line = (f"Парсинг: {parsed['files']} файлов, среднее {parsed.get('mean')}с, "
                    f"σ {parsed.get('sigma')}с, порог {parsed.get('threshold')}с, "
                    f"выбросов **{parsed.get('outliers')}**")
            if parsed.get("near_timeout"):
                line += f", у {parsed['near_timeout']} файлов больше половины таймаута"
            parts.append(line)
        if snapshot:
            total = snapshot.get("total") or 1
            parts.append(f"Версии: покрыто **{snapshot.get('covered')}** из {snapshot.get('total')} "
                         f"DAG'ов ({snapshot.get('covered', 0) * 100 / total:.0f}%), "
                         f"записано за прогон {snapshot.get('written')}, "
                         f"всего в хранилище {snapshot.get('versions')}")

        add_note("\n\n".join(parts) or "Подозрительных DAG'ов не нашлось",
                 context, level="DAG", title=headline)
        logger.info("summary: %s", headline.replace("\n", " "))

        if bad:
            raise AirflowFailException(f"Сериализация дрожит у {bad} DAG'ов: {headline}")

        return {"stats": stats, "recheck": rechecks, "counts": counts, "found": found,
                "compare": cmp_counts, "compare_silent": cmp_silent, "snapshot": snapshot,
                "parse_time": parsed}

    summary_task = summary()

    # Таски объявлены выше, а в группы попадают в момент вызова: оператор создаётся
    # именно здесь, TaskGroupContext читается тогда же.
    # Две независимые ветки: check_serialized ждёт парсинг часами, compare отрабатывает
    # за минуты. Внутри compare сравнение идёт последним — оно сличает две версии,
    # и вторую из них создаёт как раз snapshot_dags
    with TaskGroup(group_id=CHECK_GROUP, tooltip="Дрожание сериализации на парсинге") as tg_check:
        recheck_serialized_dag.expand(target=check_serialized_dag())

    with TaskGroup(group_id=COMPARE_GROUP, tooltip="Версии в S3 и что изменилось") as tg_compare:
        # expand раскрывается только по return_value: на кастомном ключе Airflow
        # падает при разборе файла. Поэтому snapshot_dags возвращает пары,
        # а статистику кладёт в XCom snapshot_stats
        compare_changed.expand(target=snapshot_dags(find_changed()))

    # parse_time вне групп и ни от кого не зависит: он про разбор файлов, а не про
    # содержимое serialized_dag
    [tg_check, tg_compare, parse_time()] >> summary_task


tools_test_dags()
