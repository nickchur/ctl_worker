"""### 🧬 DAG: Проверка сериализации DAG'ов
*2026-08-04 14:35 MSK · v2.4 · Чуркин Николай · [nschurkin@sberbank.ru](mailto:nschurkin@sberbank.ru)*

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
| **`compare.compare_changed`** | Сравнивает DAG'и, у которых `dag_hash` разошёлся с последней сохранённой версией, то есть изменившиеся с прошлого прогона, и показывает, что именно поменялось. **Никогда не падает**, полный список в XCom `changed_dags` |
| **`compare.snapshot_dags`** | Складывает версии сериализаций в S3, чтобы завтра было с чем сравнивать. Идёт строго после `compare_changed` |
| **`summary`** | Сводка обеих веток: вердикты, время ожидания, расхождения, покрытие копиями |

**Параметры:**

| Параметр | По умолчанию | Значение |
|---|---|---|
| `snapshot_limit` | `0` | Сколько DAG'ов обходить за прогон. `0` — все. Ненулевое включает ротацию: изменившиеся → без копии → с самой старой копией, полное покрытие за `ceil(всего / limit)` суток |
| `cleanup_orphans` | `False` | Удалять ли версии DAG'ов, которых больше нет в `serialized_dag`. Выключено намеренно: пропажа чаще временная (Broken DAG, неудачный парсинг, `dag_stale_not_seen_duration`), и копия как раз тогда и нужна |

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

**Вердикты `compare_changed`:**

| Статус | Значение |
|---|---|
| `changed` | Есть расхождения — в отчёте пути |
| `same_json` | `dag_hash` разный, а JSON совпал |
| `duplicate_dag_id` | Сменился `fileloc` — два файла на один `dag_id` |
| `snapshot_broken` | Версия не читается или не разбирается |

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

import pendulum
from airflow.configuration import conf
from airflow.decorators import dag, task
from airflow.models import Param
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from logging import getLogger

logger = getLogger("airflow.task")

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


def _short(value, limit: int = 60) -> str:
    """Однострочное представление значения для ячейки таблицы diff'а."""
    text = str(value).replace("|", "\\|").replace("\n", " ")
    return text if len(text) <= limit else text[:limit - 1] + "…"


def _json_diff(before, after, limit: int = 20) -> list[tuple[str, str, str]]:
    """Рекурсивно сравнивает две сериализации DAG'а: [(путь, было, стало), ...].

    Списки сравниваются поэлементно, а не как множества: для dag_hash порядок значим,
    и плавающий порядок списка — самая частая причина дрожания сериализации. Поэтому
    разная длина списка и расхождение по индексу — разные строки отчёта.
    """
    out: list[tuple[str, str, str]] = []

    def walk(x, y, path: str) -> None:
        if len(out) >= limit:
            return
        if type(x) is not type(y):
            # тип показываем явно: '1' и 1 в таблице выглядели бы одинаково
            out.append((path or ".", f"{_short(x)} ({type(x).__name__})",
                        f"{_short(y)} ({type(y).__name__})"))
        elif isinstance(x, dict):
            for key in dict.fromkeys(list(x) + list(y)):
                if len(out) >= limit:
                    return
                sub = f"{path}.{key}"
                if key not in x:
                    out.append((sub, "—", _short(y[key])))
                elif key not in y:
                    out.append((sub, _short(x[key]), "—"))
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
            out.append((path or ".", _short(x), _short(y)))

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
            if not dag_id or not ver_s.isdigit():
                continue  # чужой объект под префиксом или копия старого формата
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
        # без ретраев: перепроверка ждёт парсинг до 20 минут, повтор растянул бы прогон вдвое
        # и всё равно смотрел бы на тот же стенд
        "retries": 0,
    },
    # Часовой пояс DAG'а берётся из start_date.tzinfo (models/dag.py:614-628), поэтому
    # [core] default_timezone = utc не мешает: 23:00 — московские
    start_date=pendulum.datetime(2026, 1, 1, tz="Europe/Moscow"),
    schedule="0 23 * * *",
    # Дефолт из airflow.cfg — max_active_tasks_per_dag = 4, а recheck при RECHECK_LIMIT=25
    # растянулся бы на семь волн ожидания (до пары часов). Таски почти всё время спят
    # в ожидании парсинга, так что нагрузки это не добавляет — только занятые слоты
    max_active_tasks=12,
    tags=["DataLab", "tools", "dag", "AutoQA"],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    params={
        # 0 — без ограничения, копируем все DAG'и. Ненулевое значение включает ротацию
        # (изменившиеся и самые старые копии вперёд): полное покрытие набирается за
        # ceil(всего / snapshot_limit) суток
        "snapshot_limit": Param(0, type="integer", minimum=0),
        # Удаление копий DAG'ов, которых больше нет в serialized_dag — только вручную:
        # пропажа чаще временная (Broken DAG, неудачный парсинг, деактивация по
        # dag_stale_not_seen_duration), и копия как раз тогда и нужна
        "cleanup_orphans": Param(False, type="boolean"),
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
        )
        # Полный список уходит в XCom, в заметку попадают только первые note_rows
        list_sql = f"""
            SELECT sd.last_updated, sd.dag_id, sd.fileloc, sd.dag_hash, d.last_parsed_time
            FROM main.serialized_dag sd
            JOIN main.dag d USING (dag_id)
            WHERE {at_parse_cond}
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
        while time.time() < deadline:
            time.sleep(poke)
            hash1, data1, parsed1, loc1 = snapshot()
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
                     + "\n".join(f"| `{p}` | {a} | {b} |" for p, a, b in diffs[:5]))
        else:
            # data пустая при compress_serialized_dags, либо расхождение вне JSON
            table = "Расхождений в JSON не нашлось, хотя dag_hash разный"
        if len(diffs) > 5:
            table += f"\n\nПоказаны 5 из {len(diffs)}, остальные — в логе."
        add_note(f"{head}\n\n{table}", context, level="task", title=f"❌ {dag_id}")
        logger.error(head)
        add_xcom("recheck", {"dag_id": dag_id, "status": status, "waited": round(waited),
                             "diffs": len(diffs), "paths": [p for p, _, _ in diffs],
                             "filelocs": [loc0, loc1] if status == "duplicate_dag_id" else [loc0]},
                 context)
        raise AirflowFailException(head)

    # --- Копии сериализаций в S3 ---

    @task(
        task_id="compare_changed",
        doc_md=("Сравнивает DAG'и, изменившиеся с прошлого прогона, с их последней "
                "сохранённой версией. Не падает: изменение после деплоя — норма"),
    )
    def compare_changed(**context) -> dict:
        """Показывает, что именно изменилось в сериализации с прошлого прогона.

        «Изменился» определяется не окном по времени, а сравнением `dag_hash` с последней
        сохранённой версией: hash другой — значит DAG менялся с тех пор, как мы его
        записали, то есть с прошлого прогона (или с того, на котором до него дошла
        ротация). Окно по часам такого не даёт: при пропущенном или задержавшемся
        прогоне оно либо теряет изменения, либо повторно показывает уже показанные.

        Копии складывает snapshot_dags, который идёт следом — иначе свежая версия
        затёрла бы ту, с которой сравниваем.
        """
        import gzip
        import json
        import time

        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note, add_xcom  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, add_xcom  # type: ignore

        note_rows = 10  # заметка режется по MAX_NOTE_LEN, счётчики важнее списка

        ts = time.time()
        hook = _snap_hook()
        snaps, _ = _snap_index(hook)

        # Тянем только метаданные: data по каждому DAG'у читаем позже и лишь для тех,
        # у кого hash разошёлся
        with create_session() as session:
            current = [
                {"dag_id": r[0], "dag_hash": r[1], "fileloc": r[2], "last_updated": r[3]}
                for r in session.query(
                    SerializedDagModel.dag_id, SerializedDagModel.dag_hash,
                    SerializedDagModel.fileloc, SerializedDagModel.last_updated,
                ).all()
            ]

        # Сравнивать не с чем, пока версии нет: такие DAG'и только считаем — на первом
        # прогоне это все, и построчно они бы залили и заметку, и XCom
        no_snapshot = [c["dag_id"] for c in current if c["dag_id"] not in snaps]
        changed = sorted(
            (c for c in current
             if c["dag_id"] in snaps and snaps[c["dag_id"]]["hash"] != c["dag_hash"]),
            key=lambda c: c["last_updated"], reverse=True,
        )
        logger.info("🔍 всего DAG'ов %d, с версиями %d, изменилось с прошлого прогона %d, "
                    "без версий %d", len(current), len(snaps), len(changed), len(no_snapshot))

        results = []
        for row in changed:
            dag_id = row["dag_id"]
            last = snaps[dag_id]
            verdict = {"dag_id": dag_id, "diffs": 0, "paths": [], "version": last["version"]}

            key = last["key"]
            try:
                raw = hook.get_key(key, bucket_name=SNAP_BUCKET).get()["Body"].read()
                snap = json.loads(gzip.decompress(raw))
            except Exception as err:
                logger.warning("версия %s не читается: %s", key, err)
                verdict["status"] = "snapshot_broken"
                results.append(verdict)
                continue

            with create_session() as session:
                sdm = (session.query(SerializedDagModel)
                       .filter(SerializedDagModel.dag_id == dag_id).one_or_none())
                now_data = sdm.data if sdm else None
                now_loc = sdm.fileloc if sdm else None

            diffs = _json_diff(snap.get("data") or {}, now_data or {})
            verdict["diffs"] = len(diffs)
            verdict["paths"] = [p for p, _, _ in diffs]
            if snap.get("fileloc") and now_loc and snap["fileloc"] != now_loc:
                # два файла на один dag_id: каждый парсинг переписывает чужую сериализацию
                verdict["status"] = "duplicate_dag_id"
                verdict["filelocs"] = [snap["fileloc"], now_loc]
            elif diffs:
                verdict["status"] = "changed"
            else:
                verdict["status"] = "same_json"
            results.append(verdict)

        counts: dict = {}
        for r in results:
            counts[r["status"]] = counts.get(r["status"], 0) + 1
        for r in results:
            logger.info("  %s: %s (версия %05d)%s", r["dag_id"], r["status"], r["version"],
                        f" {r['diffs']} расхождений: {', '.join(r['paths'][:5])}" if r["diffs"] else "")

        elapsed = time.time() - ts
        summary_line = " / ".join(f"{st} {n}" for st, n in sorted(counts.items())) or "изменений нет"
        if no_snapshot:
            summary_line += f" | без версий ещё {len(no_snapshot)}"
        if results:
            table = ("| DAG | Версия | Вердикт | Расхождений | Первые пути |\n"
                     "|---|---:|---|---:|---|\n" + "\n".join(
                         f"| `{r['dag_id']}` | {r['version']:05d} | {r['status']} | {r['diffs']} "
                         f"| {_short(', '.join(r['paths'][:3]) or '—', 60)} |"
                         for r in results[:note_rows]))
            if len(results) > note_rows:
                table += (f"\n\nПоказаны первые {note_rows} из {len(results)}, "
                          f"полный список — в XCom `changed_dags`.")
        else:
            table = "С прошлого прогона ни один DAG не менялся"

        add_xcom("changed_dags", results, context)
        add_note(f"{summary_line}\n\n{table}", context, level="task",
                 title=f"🕵️ {elapsed:.2f} sec compare_changed: {len(changed)} с прошлого прогона")
        return {"total": len(current), "changed": len(changed), "snapshots": len(snaps),
                "no_snapshot": len(no_snapshot), "counts": counts,
                "dag_ids": [r["dag_id"] for r in results]}

    @task(
        task_id="snapshot_dags",
        doc_md=("Складывает копии сериализаций в S3. `snapshot_limit=0` — все DAG'и, иначе "
                "ротация: изменившиеся → без копии → с самой старой копией"),
    )
    def snapshot_dags(compared: dict, **context) -> dict:
        """Пополняет хранилище копий, с которыми завтра будет сравнивать compare_changed."""
        import gzip
        import json
        import time

        from airflow.models.serialized_dag import SerializedDagModel
        from airflow.utils.session import create_session

        try:
            from plugins.utils import add_note, readable_size  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note, readable_size  # type: ignore

        limit = int(context["params"]["snapshot_limit"])
        cleanup = bool(context["params"]["cleanup_orphans"])

        ts = time.time()
        hook = _snap_hook()
        snaps, all_keys = _snap_index(hook)
        with create_session() as session:
            all_dags = [r[0] for r in session.query(SerializedDagModel.dag_id).all()]

        snap_ages = {d: v["at"] for d, v in snaps.items()}
        targets = _snapshot_targets(all_dags, snap_ages, compared.get("dag_ids", []), limit)
        logger.info("📦 всего DAG'ов %d, с копиями %d (версий %d), к обходу %d (лимит %s)",
                    len(all_dags), len(snaps), len(all_keys), len(targets), limit or "нет")

        written = first = unchanged = total_bytes = 0
        captured_at = pendulum.now("UTC").isoformat()
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

            hook.load_bytes(body, key=_snap_key(dag_id, version, dag_hash),
                            bucket_name=SNAP_BUCKET, replace=True)
            written += 1
            total_bytes += len(body)
            if last is None:
                first += 1
            logger.info("  %s: версия %05d (%s)", dag_id, version, readable_size(len(body)))

        orphans = sorted(set(snaps) - set(all_dags))
        orphan_keys = [k for k in all_keys
                       if k[len(SNAP_PREFIX):].rpartition("/")[0] in set(orphans)]
        if orphans and cleanup:
            # ВНИМАНИЕ: у delete_objects параметр `bucket`, а не `bucket_name` — в отличие
            # от list_keys/load_bytes. Непоследовательность API провайдера.
            # Удаляем все версии осиротевшего DAG'а, а не только последнюю
            hook.delete_objects(bucket=SNAP_BUCKET, keys=orphan_keys)
            logger.warning("удалено осиротевших DAG'ов %d (объектов %d)", len(orphans), len(orphan_keys))
        elif orphans:
            logger.info("осиротевших DAG'ов %d (объектов %d), удаление выключено "
                        "(cleanup_orphans): %s", len(orphans), len(orphan_keys),
                        ", ".join(orphans[:10]))

        # Покрытие считаем только по живым DAG'ам: осиротевшие копии в бакете есть, но
        # к покрытию отношения не имеют — иначе оно уезжало бы за 100%
        covered = len((set(snaps) | set(targets)) & set(all_dags))
        total = len(all_dags) or 1
        oldest = min(snap_ages.values()) if snap_ages else None
        versions = len(all_keys) + written - (len(orphan_keys) if cleanup else 0)
        elapsed = time.time() - ts

        rows = [
            f"| новых версий | {written} |",
            f"| из них первых | {first} |",
            f"| без изменений | {unchanged} |",
            f"| объём выгрузки | {readable_size(total_bytes)} |",
            f"| покрытие | {covered} из {len(all_dags)} ({covered * 100 / total:.0f}%) |",
            f"| версий в хранилище | {versions} |",
            f"| осиротевших DAG'ов | {len(orphans)}{' (удалены)' if orphans and cleanup else ''} |",
            f"| самая старая копия | {oldest or '—'} |",
        ]
        if limit and covered < len(all_dags):
            days = -(-(len(all_dags) - covered) // limit)  # ceil
            rows.append(f"| полное покрытие через | ~{days} сут |")

        add_note("| | |\n|---|---:|\n" + "\n".join(rows), context, level="task",
                 title=f"📦 {elapsed:.2f} sec snapshot_dags: +{written} версий")
        logger.info("📦 новых версий %d (%s), без изменений %d, покрытие %d/%d",
                    written, readable_size(total_bytes), unchanged, covered, len(all_dags))
        return {"written": written, "first": first, "unchanged": unchanged, "bytes": total_bytes,
                "covered": covered, "total": len(all_dags), "versions": versions,
                "orphans": len(orphans), "cleaned": bool(orphans and cleanup)}

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
        # упавший до add_xcom или не запущенный
        with create_session() as session:
            states = dict(
                session.query(TaskInstance.map_index, TaskInstance.state)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id,
                    TaskInstance.task_id == f"{CHECK_GROUP}.recheck_serialized_dag",
                )
                .all()
            )
        silent = sum(1 for m, s in states.items() if m >= 0 and s in ("failed", "upstream_failed")) \
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
        compared = ti.xcom_pull(task_ids=f"{COMPARE_GROUP}.compare_changed") or {}
        snapshot = ti.xcom_pull(task_ids=f"{COMPARE_GROUP}.snapshot_dags") or {}

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
        if compared.get("counts"):
            parts.append(f"С прошлого прогона изменилось {compared.get('changed')} "
                         f"из {compared.get('total')} DAG'ов:\n\n"
                         + "| Вердикт | DAG'ов |\n|---|---:|\n"
                         + "\n".join(f"| {st} | {n} |" for st, n in sorted(compared["counts"].items())))
        elif compared:
            parts.append(f"С прошлого прогона не менялся ни один из {compared.get('total')} DAG'ов")
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

        return {"stats": stats, "recheck": rechecks, "counts": counts,
                "compared": compared, "snapshot": snapshot}

    summary_task = summary()

    # Таски объявлены выше, а в группы попадают в момент вызова: оператор создаётся
    # именно здесь, TaskGroupContext читается тогда же.
    # Две независимые ветки: check_serialized ждёт парсинг часами, compare отрабатывает
    # за минуты. Внутри compare snapshot строго после сравнения — иначе свежая версия
    # затрёт ту, с которой сравниваем
    with TaskGroup(group_id=CHECK_GROUP, tooltip="Дрожание сериализации на парсинге") as tg_check:
        recheck_serialized_dag.expand(target=check_serialized_dag())

    with TaskGroup(group_id=COMPARE_GROUP, tooltip="Версии в S3 и что изменилось") as tg_compare:
        snapshot_dags(compare_changed())

    [tg_check, tg_compare] >> summary_task


tools_test_dags()
