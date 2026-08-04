"""### 🧬 DAG: Проверка сериализации DAG'ов
*2026-08-04 12:40 MSK · v2.0 · Чуркин Николай · [nschurkin@sberbank.ru](mailto:nschurkin@sberbank.ru)*

Ищет DAG'и, у которых сериализация переписывается на каждом парсинге файла, и выясняет
причину. Выделен из `test_connections` (там остались проверки соединений).

| Таск | Что делает |
|---|---|
| **`check_serialized_dag`** | Считает по `main.serialized_dag`, у скольких DAG'ов менялась сериализация за год, 3 месяца, месяц, неделю, сутки и час, плюс строка «на последнем парсинге» (`last_updated` попал в окно последнего разбора файла — `dag.last_parsed_time`). **Никогда не падает**: одного замера мало, чтобы отличить дрожание от деплоя. Возвращает список подозрительных DAG'ов, статистика — в XCom `serialized_stats` |
| **`recheck_serialized_dag`** | Mapped-таск, по экземпляру на DAG из списка. Ждёт следующего парсинга (сдвига `dag.last_parsed_time`) и сравнивает сериализацию до и после, показывая расхождения по путям вида `.dag.params[0][1].schema.examples[0]` |
| **`summary`** | Сводка по всем экземплярам: вердикт, время ожидания, число расхождений |

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

from datetime import datetime, timezone
from logging import getLogger

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

logger = getLogger("airflow.task")


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


@dag(
    doc_md=__doc__,
    default_args={
        "owner": "DataLab (CI02420667)",
        # без ретраев: перепроверка ждёт парсинг до 20 минут, повтор растянул бы прогон вдвое
        # и всё равно смотрел бы на тот же стенд
        "retries": 0,
    },
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule="@once",
    # Дефолт из airflow.cfg — max_active_tasks_per_dag = 4, а recheck при RECHECK_LIMIT=25
    # растянулся бы на семь волн ожидания (до пары часов). Таски почти всё время спят
    # в ожидании парсинга, так что нагрузки это не добавляет — только занятые слоты
    max_active_tasks=12,
    tags=["DataLab", "tools", "dag", "AutoQA"],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
)
def tools_test_dags():

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
        raw = ti.xcom_pull(task_ids="recheck_serialized_dag", key="recheck")
        rechecks = [json.loads(r) if isinstance(r, str) else r for r in list(raw or [])]

        stats_raw = ti.xcom_pull(task_ids="check_serialized_dag", key="serialized_stats")
        stats = json.loads(stats_raw) if isinstance(stats_raw, str) else (stats_raw or {})

        # Состояния нужны только чтобы поймать экземпляры, не оставившие XCom вовсе:
        # упавший до add_xcom или не запущенный
        with create_session() as session:
            states = dict(
                session.query(TaskInstance.map_index, TaskInstance.state)
                .filter(
                    TaskInstance.dag_id == dag_run.dag_id,
                    TaskInstance.run_id == dag_run.run_id,
                    TaskInstance.task_id == "recheck_serialized_dag",
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
        add_note("\n\n".join(parts) or "Подозрительных DAG'ов не нашлось",
                 context, level="DAG", title=headline)
        logger.info("summary: %s", headline.replace("\n", " "))

        if bad:
            raise AirflowFailException(f"Сериализация дрожит у {bad} DAG'ов: {headline}")

        return {"stats": stats, "recheck": rechecks, "counts": counts}

    recheck_serialized_dag.expand(target=check_serialized_dag()) >> summary()


tools_test_dags()
