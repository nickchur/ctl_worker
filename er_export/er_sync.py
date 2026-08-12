"""🔄 DAG синхронизации метаданных ER-выгрузок.
*2026-08-10 21:40 MSK · v2.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Читает таблицу export.er_wf_meta из ClickHouse и сохраняет активные записи
в Airflow Variable `datalab_er_wfs` (JSON-словарь), который используется
фабрикой er_export.py для динамической генерации DAG-ов.

Записи раскладываются по группам поставок: группа — это значение replica целиком
(суффикс после '__'), один пакет = одна группа = один DAG = один внешний тикет.
Строка с заполненной replica и пустым extract_name задаёт дефолты всей группы.

Структура Variable:
  {
    "<replica>": {                       # ключ группы, он же имя в архиве и тикете
      "schedule":    str,                # cron DAG-а группы
      "description": str,                # опционально
      "params":      str,                # JSON групповых параметров (GROUP_PARAMS)
      "tables": {
        "db_name.extract_name": {
          "schema":  str,                # целевая схема TFS
          "PK":      list[str],
          "UK":      list[str],
          "fields":  list[str],          # обязателен и явен
          "params":  str,                # JSON разрешённых параметров таблицы
          "sql_stmt_export_delta" | "sql_stmt_export_recent": {
              "from":     str,           # обязательно
              "with":     str,           # опционально — WITH-блок (CTE)
              "joins":    str,           # опционально — JOIN-clause
              "where":    str,           # опционально — WHERE-условие
              "settings": str,           # опционально — SETTINGS-блок ClickHouse
          },
          "description": str,            # опционально
        }
      }
    }
  }

Сломанная группа выглядит иначе — без таблиц, зато с причинами; фабрика делает по такой
записи даг-заглушку, который падает при запуске:
  { "<replica>": {"schedule": str, "errors": [str, ...], "tables": {}} }

⏱️ Расписание: нет, DAG запускается только вручную.

🚦 Статус таска: любая исключённая запись делает его КРАСНЫМ — иначе ошибку в метаданных
легко не заметить, а пакет уедет неполным составом. Исправные группы при этом всё равно
записываются в Variable: опечатка в одной строке не должна замораживать правки по остальным
пакетам. Полные списки ошибок и предупреждений — в логе и в XCom ('errors', 'warnings',
'summary'). При пустой таблице на не-DEV стенде падает, чтобы не затереть Variable.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

try:
    from CI06932748.analytics.datalab.export_er.er_config import (  # type: ignore
        get_config, get_dict, obj_save, add_note, ensure_pool, replica_base,
    )
except ImportError:
    from er_export.er_config import get_config, get_dict, obj_save, add_note, ensure_pool, replica_base

_cfg           = get_config()
CH_ID          = _cfg['CH_ID']
DEF_ARGS       = _cfg['DEF_ARGS']
ENV_STAND      = _cfg['ENV_STAND']
VAR_NAME       = _cfg['VAR_NAME']
POOL_NAME      = _cfg['POOL_NAME']
POOL_SLOTS     = _cfg['POOL_SLOTS']
TFS_MAP        = _cfg['TFS_MAP']
DEFAULT_PARAMS = _cfg['DEFAULT_PARAMS']
GROUP_PARAMS   = _cfg['GROUP_PARAMS']
FORMAT_MAP     = _cfg['FORMAT_MAP']

# 🧬 Поля строки-дефолта группы, наследуемые поставками. SQL и ключи (pk, uk, fields,
# sql_*) сюда намеренно не входят — они всегда описывают конкретную таблицу.
#
# is_recent тоже не наследуется, хотя это и не SQL: колонка UInt8 DEFAULT 0, и «не задано»
# от «явно delta» не отличить. При наследовании через `or` таблица в recent-группе не смогла
# бы вернуться к дельте — её sql_stmt_export_delta уехал бы под ключом recent.
#
# description обрабатывается отдельно (см. build_wfs): у него есть третий источник —
# комментарий таблицы в ClickHouse, и он должен быть приоритетнее группового текста.
INHERITED = ('schema_name',)

# Значение колонки schedule по умолчанию в DDL. Отличать его от осознанно заданного нельзя
# (ClickHouse возвращает дефолт, а не пустую строку), поэтому при сверке расписаний внутри
# пакета такое значение считаем «не задано».
DEFAULT_SCHEDULE = '55 0 * * *'

logger = logging.getLogger("airflow.task")

# Пул для таска синхронизации — намеренно не POOL_NAME,
# чтобы sync не занимал слоты экспортного пула.
SYNC_POOL = "default_pool"

# Ключи _cfg, которые не нужны в doc_md (содержат функции или слишком многословны).
_HIDDEN_CFG_KEYS = frozenset({'DEF_ARGS', 'TYPE_MAP', 'EXTRA_PRE', 'EXTRA_SUF'})
_doc_cfg = {k: v for k, v in _cfg.items() if k not in _HIDDEN_CFG_KEYS}


def _q(s: str) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return s.replace("'", "''")


def _parse_params(raw: str, where: str) -> dict:
    """Разбирает JSON-поле params; при битом JSON возвращает {} и пишет предупреждение."""
    try:
        return json.loads(raw or '{}')
    except json.JSONDecodeError as err:
        logger.warning("⚠️ %s: битый JSON в params (%s) — параметры проигнорированы", where, err)
        return {}


def split_rows(rows: list[dict]) -> tuple[dict, list[dict], set[str]]:
    """🧩 Делит выборку на дефолты групп, поставки и выключенные группы.

    Строка с пустым extract_name — дефолты своей группы. is_active=0 на ней гасит группу
    целиком, вместе со всеми поставками: иначе поставки уехали бы дальше, растеряв групповые
    параметры и вернувшись к умолчаниям из кода (где notify_kafka=1).

    Возвращает ({replica: строка-дефолт}, [поставки], {выключенные группы}).
    """
    defaults = {r["replica"]: r for r in rows if not r["extract_name"]}
    off      = {rep for rep, r in defaults.items() if not r.get("is_active", 1)}

    return (
        {rep: r for rep, r in defaults.items() if rep not in off},
        [r for r in rows
         if r["extract_name"] and r.get("is_active", 1) and r["replica"] not in off],
        off,
    )


def _explicit_schedule(row: dict) -> str:
    """Расписание, заданное осознанно. Колоночный дефолт трактуем как «не задано»."""
    sched = (row or {}).get('schedule') or ''
    return '' if sched == DEFAULT_SCHEDULE else sched


def _check_table(row: dict, key: str, errors: list[str], params: dict) -> bool:
    """Проверяет строку-поставку. Непрошедшая запись ломает всю группу, причина — в errors.

    params — уже слитые параметры (дефолты + группа + таблица): проверять надо именно их,
    иначе опечатка в params строки-дефолта проходит синк и роняет разбор файла в фабрике.
    """
    if not row["sql_from"]:
        errors.append(f"{key}: пустой sql_from")
        return False

    base = replica_base(row["replica"])
    if base not in TFS_MAP:
        errors.append(f"{key}: базовая реплика '{base}' не найдена в TFS_MAP")
        return False

    # Без схемы фабрика падает на schema_name.replace(), а падение при разборе файла
    # уносит с собой ВСЕ пакеты, а не только этот.
    if not row.get("schema_name"):
        errors.append(f"{key}: пустой schema_name — задайте его в строке-дефолте группы или в поставке")
        return False

    # Состав полей задаётся только настройкой: иначе новая колонка источника уехала бы
    # в выгрузку и в .meta сама по себе, без единого изменения конфигурации.
    fields = row["fields"] or []
    if not fields:
        errors.append(f"{key}: пустой fields — состав колонок надо задать явно")
        return False
    star = [f for f in fields if str(f).strip() == '*' or str(f).strip().endswith('.*')]
    if star:
        errors.append(f"{key}: fields содержит {star} — звёздочка запрещена, нужен явный список")
        return False

    fmt = params.get('format', DEFAULT_PARAMS['format'])
    if fmt not in FORMAT_MAP:
        errors.append(f"{key}: неизвестный format '{fmt}', допустимы {sorted(FORMAT_MAP)}")
        return False

    return True


def build_wfs(tables: list[dict], defaults: dict, ch_comments: dict) -> tuple[dict, list[str]]:
    """🧬 Раскладывает строки er_wf_meta по группам, разрешая наследование от строк-дефолтов.

    tables      — строки-поставки (extract_name непустой)
    defaults    — {replica: строка-дефолт группы}
    ch_comments — {(db, table): комментарий} для строк без description

    Возвращает (структура для Variable, ошибки, предупреждения).

    Ошибки — это исключённые записи; они ломают группу целиком и делают таск синка красным.
    Предупреждения ничего не исключают (например, расхождение cron внутри пакета) и на
    статус таска не влияют, иначе красный статус быстро обесценится.
    """
    wfs: dict = {}
    errors: dict[str, list[str]] = {}   # {replica: причины}, копим по группам
    warnings: list[str] = []

    # Ключ таблицы — (db_name, extract_name), у строк-дефолтов extract_name пуст. Пустой
    # db_name сделал бы ключ ('', '') общим для дефолтов ВСЕХ групп, и фоновый MERGE оставил
    # бы одну строку на все пакеты. Такой дефолт отбрасываем: молча раздать группе чужие
    # параметры хуже, чем не раздать никаких.
    defaults = dict(defaults)
    for rep in [rep for rep, row in defaults.items() if not row.get("db_name")]:
        errors.setdefault(rep, []).append(
            f"группа {rep}: у строки-дефолта пустой db_name — проставьте db_name = replica, "
            "иначе дефолты разных групп схлопнутся в одну строку по ключу (db_name, extract_name)"
        )
        del defaults[rep]

    # Расписание пакета считаем до цикла: оно одно на группу, и определять его первой
    # попавшейся строкой нельзя — порядок строк не гарантирует, что она осмысленная.
    schedules = {}
    for replica in {r["replica"] for r in tables}:
        own = [_explicit_schedule(r) for r in tables if r["replica"] == replica]
        schedules[replica] = (
            _explicit_schedule(defaults.get(replica, {}))
            or next((s for s in own if s), '')
            or DEFAULT_SCHEDULE
        )

    for row in tables:
        replica   = row["replica"]
        table_key = f"{row['db_name']}.{row['extract_name']}"
        grp_row   = defaults.get(replica, {})
        own_desc  = row["description"]

        # Наследование: непустое значение поставки перебивает дефолт группы.
        row = {**row, **{k: (row.get(k) or grp_row.get(k) or '') for k in INHERITED}}

        grp_params = _parse_params(grp_row.get("params", ""), f"группа {replica}")
        tbl_params = _parse_params(row["params"], table_key)
        params     = {**DEFAULT_PARAMS, **grp_params, **tbl_params}

        if not _check_table(row, table_key, errors.setdefault(replica, []), params):
            continue

        group = wfs.setdefault(replica, {
            "schedule": schedules[replica],
            "params":   json.dumps({k: v for k, v in {**DEFAULT_PARAMS, **grp_params}.items()
                                    if k in GROUP_PARAMS}, ensure_ascii=False),
            "tables":   {},
        })
        if grp_row.get("description"):
            group["description"] = grp_row["description"]

        # Cron у пакета один — пакет уезжает целиком. Расхождение не отбрасывает поставку
        # (потерять таблицу из-за косметики хуже), но должно быть видно в логе и заметке.
        own_sched = _explicit_schedule(row)
        if own_sched and own_sched != group["schedule"]:
            warnings.append(
                f"{table_key}: schedule '{own_sched}' расходится с расписанием группы "
                f"'{group['schedule']}' — у пакета расписание одно, взято групповое"
            )

        # 🔀 is_recent определяет ключ SQL-запроса: фабрика er_export.py проверяет наличие одного из двух
        sql_key = "sql_stmt_export_recent" if row["is_recent"] else "sql_stmt_export_delta"
        sql_val = {"from": row["sql_from"]}
        if row["sql_with"]:     sql_val["with"]     = row["sql_with"]
        if row["sql_join"]:     sql_val["joins"]    = row["sql_join"]
        if row["sql_where"]:    sql_val["where"]    = row["sql_where"]
        if row["sql_settings"]: sql_val["settings"] = row["sql_settings"]

        entry = {
            "schema":  row["schema_name"],
            "PK":      row["pk"],
            "UK":      row["uk"],
            "fields":  row["fields"],
            # Параметры кладём уже разрешёнными — фабрика про наследование не знает
            "params":  json.dumps({**grp_params, **tbl_params}, ensure_ascii=False),
            sql_key:   sql_val,
        }

        # Описание: своё → комментарий таблицы в CH → групповое. Групповой текст последний,
        # иначе он затёр бы осмысленные комментарии всех таблиц пакета.
        desc = (own_desc
                or ch_comments.get((row["db_name"], row["extract_name"]), "")
                or grp_row.get("description", ""))
        if desc:
            entry["description"] = desc

        group["tables"][table_key] = entry

    # 💥 Любая ошибка ломает ВЕСЬ пакет, а не одну поставку: тикет в ЕР один на группу,
    # и уехавший неполный состав — это расхождение данных на стороне КАП. Такая группа
    # попадает в Variable заглушкой: причины плюс расписание, чтобы даг-заглушка краснел
    # в том же ритме, в каком должен был ходить пакет.
    for rep, msgs in errors.items():
        if not msgs:
            continue
        wfs[rep] = {
            "schedule": schedules.get(rep, DEFAULT_SCHEDULE),
            "errors":   msgs,
            "tables":   {},
        }

    flat_errors = [m for rep in sorted(errors) for m in errors[rep]]
    return wfs, flat_errors, warnings


def _ensure_pool() -> None:
    """🏊 Создаёт Airflow Pool для ER-выгрузок, если его ещё нет.

    Вызывается внутри таска, а не при разборе DAG: ensure_pool кэширует результат
    на процесс, но лишний SELECT на каждом обходе scheduler-ом всё равно не нужен.

    Пулы тракта ТФС здесь не заводятся — их создаёт tfs_kafka (ensure_pools в приёмнике).
    Прежние tfs_{scenario_id} не нужны: требование ЕР «не передаётся несколько пакетов
    одновременно» выполняет сам отправитель, разбирая очередь пакетами целиком.
    Уже созданные tfs_* можно удалить руками, код их не использует.
    """
    ensure_pool(POOL_NAME, slots=POOL_SLOTS, description='Пул для ER-выгрузок')


@dag(
    dag_id="export_er_sync",
    description="🔄 Синхронизация export.er_wf_meta → Airflow Variable datalab_er_wfs",
    default_args=DEF_ARGS,
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    schedule_interval=None,   # только ручной запуск
    max_active_runs=1,
    catchup=False,
    tags=["DataLab", "CI02420667", "ER", "sync"],
    is_paused_upon_creation=False,
    doc_md="```\n" + json.dumps(_doc_cfg, indent=4, default=str) + "\n```",
)
def er_sync_dag():

    @task(task_id="sync", pool=SYNC_POOL)
    def sync(**context):
        """🔄 Читает er_wf_meta, собирает словарь выгрузок и сохраняет в Airflow Variable.

        🧪 DEV: создаёт таблицу er_wf_meta если её нет; пропускает обновление при пустой таблице.
        🏭 Остальные стенды: пустая таблица — ошибка (защита от затирания Variable).
        """
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        _ensure_pool()

        hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

        if ENV_STAND == 'DEV':
            # На DEV кластер datalab существует — создаём идентичную продовой реплицированную таблицу.
            hook.execute("""
                CREATE TABLE IF NOT EXISTS export.er_wf_meta ON CLUSTER datalab
                (
                    extract_name    String                    COMMENT 'Имя выгрузки (table name без схемы); ПУСТО = строка-дефолт группы',
                    db_name         String                    COMMENT 'База данных источника в ClickHouse; у строки-дефолта группы = replica, иначе дефолты групп схлопнутся по ключу',
                    replica         String                    COMMENT 'Реплика с суффиксом группы: база до "__" ищется в TFS_MAP (er_config.py); обязательное',
                    schema_name     String                    COMMENT 'Целевая схема в .meta-файле для TFS; наследуется от строки-дефолта группы',
                    pk              Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа; не наследуется',
                    uk              Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа; не наследуется',
                    fields          Array(String) DEFAULT []             COMMENT 'SELECT-выражения; ОБЯЗАТЕЛЬНО и явно, "*" и "t1.*" запрещены',
                    sql_from        String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос; у поставки обязательное',
                    sql_where       String        DEFAULT ''             COMMENT 'WHERE-условие; пустая строка — без фильтра; {condition} подставляется рантаймом',
                    sql_join        String        DEFAULT ''             COMMENT 'JOIN-clause (полное выражение: JOIN t ON ...); вставляется между FROM и WHERE',
                    sql_with        String        DEFAULT ''             COMMENT 'WITH-блок (CTE); вставляется перед SELECT',
                    sql_settings    String        DEFAULT ''             COMMENT 'SETTINGS-блок ClickHouse; вставляется в конец запроса',
                    params          String        DEFAULT '{}'           COMMENT 'JSON с параметрами выгрузки (см. GROUP_PARAMS/TABLE_PARAMS в er_config)',
                    description     String        DEFAULT ''             COMMENT 'Описание (отображается в Airflow UI); наследуется',
                    schedule        String        DEFAULT '55 0 * * *'  COMMENT 'Cron-расписание DAG-а группы; задаётся в строке-дефолте',
                    is_recent       UInt8         DEFAULT 0              COMMENT '0 = delta (sql_stmt_export_delta), 1 = recent (sql_stmt_export_recent); НЕ наследуется',
                    is_active       UInt8         DEFAULT 1              COMMENT '0 = запись игнорируется; на строке-дефолте выключает всю группу',
                    updated_at      DateTime64(3) DEFAULT now64(3)       COMMENT 'Версия строки для ReplacingMergeTree (мс-точность исключает коллизии при быстрых обновлениях)'
                )
                ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_wf_meta_{uuid}', '{replica}', updated_at)
                ORDER BY (db_name, extract_name)
            """)
            logger.info("🧪 DEV: ensured export.er_wf_meta exists")

        # is_active НЕ фильтруем в SQL: выключенную строку-дефолт надо увидеть, чтобы
        # погасить всю группу. Уйди она из выборки — поставки синхронизировались бы дальше,
        # растеряв групповые параметры и вернувшись к умолчаниям из кода (а notify_kafka
        # там 1, то есть стендовый пакет молча поехал бы в ТФС).
        rows = get_dict(hook, """
            SELECT
                extract_name, db_name, replica, schema_name,
                pk, uk, fields,
                sql_from, sql_where, sql_join, sql_with, sql_settings,
                params, description, schedule, is_recent, is_active
            FROM export.er_wf_meta FINAL
            WHERE replica != ''
            ORDER BY replica, db_name, extract_name
        """)  # порядок только для читаемости логов; ключ таблицы — (db_name, extract_name)

        if not rows:
            if ENV_STAND == 'DEV':
                logger.warning("⚠️ export.er_wf_meta is empty — skipping Variable update on DEV stand")
                return
            raise ValueError("🚫 No active workflows found in export.er_wf_meta — aborting to avoid overwriting Variable with empty dict")

        defaults, tables, off = split_rows(rows)
        if off:
            logger.info("⏸️ Группы выключены строкой-дефолтом (is_active=0): %s", ", ".join(sorted(off)))

        # 💬 Для строк без явного description подтягиваем комментарий таблицы из system.tables
        # одним батч-запросом, чтобы не делать N отдельных DESCRIBE.
        no_desc = [(r["db_name"], r["extract_name"]) for r in tables if not r["description"]]
        ch_comments: dict[tuple[str, str], str] = {}
        if no_desc:
            pairs = ", ".join(f"('{_q(db)}', '{_q(tbl)}')" for db, tbl in no_desc)
            ch_comments = {
                (r["database"], r["name"]): r["comment"]
                for r in get_dict(hook, f"SELECT database, name, comment FROM system.tables WHERE (database, name) IN ({pairs})")
            }

        wfs, errors, warnings = build_wfs(tables, defaults, ch_comments)

        if not wfs:
            raise ValueError(
                "🚫 All records were filtered out — aborting to avoid overwriting Variable with empty dict. "
                f"Причины: {errors}"
            )

        broken  = {rep for rep, grp in wfs.items() if grp.get("errors")}
        ok_wfs  = {rep: grp for rep, grp in wfs.items() if rep not in broken}
        summary = {rep: list(grp["tables"]) for rep, grp in ok_wfs.items()}
        total   = sum(len(g["tables"]) for g in ok_wfs.values())

        # 💾 Сохраняем ДО падения: опечатка в одной строке не должна замораживать правки
        # по всем остальным пакетам. obj_save пропускает запись, если данные не изменились.
        obj_save(VAR_NAME, wfs)

        # Полные списки — в лог и в XCom. Именно xcom_push, а не return: таск ниже падает,
        # а у упавшего таска return_value в XCom не сохраняется.
        ti = context['ti']
        ti.xcom_push(key='errors',   value=errors)
        ti.xcom_push(key='warnings', value=warnings)
        ti.xcom_push(key='summary',  value=summary)

        logger.info("✅ Синхронизировано %d групп / %d выгрузок", len(ok_wfs), total)
        if warnings:
            logger.warning("⚠️ Предупреждений: %d\n%s", len(warnings), "\n".join(warnings))
        if errors:
            logger.error("❌ Ошибок: %d, сломано пакетов: %d\n%s",
                         len(errors), len(broken), "\n".join(errors))

        # Одна заметка на всё: add_note режет до 1000 символов, и отдельный блок про ошибки
        # вытеснил бы сводку. Ошибки первыми — при обрезке выживает главное, полные списки
        # всё равно есть в логе и в XCom.
        note: dict = {}
        if errors:
            note[f"❌ Ошибки ({len(errors)}), сломано пакетов: {len(broken)}"] = errors
        if warnings:
            note[f"⚠️ Предупреждения ({len(warnings)})"] = warnings
        note[f"✅ Синхронизировано: {len(ok_wfs)} групп, {total} выгрузок"] = summary or "—"
        add_note(note, level='task,dag', context=context, title='🔄 er_sync')

        if errors:
            raise AirflowFailException(
                f"❌ Метаданные ER содержат {len(errors)} ошибок, сломано пакетов: {len(broken)} "
                f"({', '.join(sorted(broken))}). Исправные группы синхронизированы. "
                "Полный список — в логе и в XCom 'errors'"
            )

    sync()


er_sync_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
