"""⚙️ DAG настройки ER-выгрузок: правка `export.er_wf_meta`, проверка и синхронизация.
*2026-08-17 12:10 MSK · v1.7 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Один ран делает всё, что раньше делали два дага (`export_er_wf_edit` и `export_er_sync`):
показывает запись, проверяет её на живом ClickHouse, пишет новую версию и раскладывает
таблицу по группам в Variable `datalab_er_wfs`, откуда фабрика `er_export.py` поднимает
пакеты.

## Как пользоваться

1. **Поток** — что смотрим или правим: `— только синхронизация —`, `➕ новая запись`
   либо ключ существующей записи из списка.
2. **Патч** — JSON **только с теми полями, которые меняем**. Для новой записи — вся
   запись целиком.

Что из этого получится:

| Поток | Патч | Что происходит |
| :--- | :--- | :--- |
| не выбран | — | только синхронизация |
| выбран | пуст | **просмотр**: запись + проверка её SQL и `.meta`, в таблицу ничего не пишется |
| выбран | заполнен | состояние **до** → проверка → запись → состояние **после** → синхронизация |

Пример правки расписания у группы:

    {"schedule": "30 2 * * *"}

Пример новой поставки:

    {
      "extract_name": "lc_items_opened", "db_name": "evolution",
      "replica": "hrplatform_datalab__1", "schema_name": "learning",
      "uk": ["item_id"], "fields": ["item_id", "title"],
      "sql_from": "evolution.lc_items_opened",
      "params": "{\\"selfrun_timeout\\": 10}"
    }

Пример новой строки-дефолта группы — `extract_name` пуст, а **`db_name` равен `replica`**:

    {
      "replica": "hrplatform_datalab__2", "db_name": "hrplatform_datalab__2",
      "schema_name": "learning", "schedule": "30 2 * * *",
      "params": "{\\"notify_kafka\\": 0}"
    }

⚠️ Пустым `db_name` у дефолта быть не может. Ключ таблицы — `(db_name, extract_name)`,
`extract_name` у дефолтов пуст, и с пустым `db_name` дефолты **всех** групп получили бы
один ключ `('', '')`: фоновый MERGE оставил бы от них одну строку, а остальные пакеты
разом потеряли бы свои параметры. Проверка это ловит и записать такую строку не даёт.

**Удаление** отдельной кнопкой не сделано и не нужно: `{"is_active": 0}` выключает
запись, а у строки-дефолта группы — весь пакет.

## Что делает проверка

Собирает запрос **тем же кодом, что и выгрузка** (`er_config.export_sql`), подставляет
в него заведомо ложное окно дельты и выполняет с внешним `LIMIT 0` — данных не читает,
но разбирает и планирует запрос целиком. По колонкам результата собирает `.meta` и сверяет
их состав с `fields`. Так ошибка находится там, где её сделали, а не ночью, когда пакет
пойдёт по расписанию.

Ловит: несуществующие таблицы и колонки, кривой `sql_join`/`sql_with`/`sql_settings`,
разъехавшийся с источником `fields` и оставшийся квалификатор таблицы в именах колонок
(при двух и более JOIN-ах ClickHouse его сохраняет — подсказка с готовой строкой замены
приходит прямо в ошибку).

Не прошла — **в таблицу ничего не записано**. Когда проверка падает не по вине записи
(источник временно недоступен, таблица ещё не создана), правку можно сохранить галкой
**Записать несмотря на проверку**.

## Что синхронизация делает и когда молчит

Синхронизация идёт в каждом ране, но если контрольная сумма настройки не изменилась —
уходит в скип ☮️. Сумма считается и по строкам таблицы, и по снимку конфига: правка
умолчаний в коде тоже меняет содержимое Variable. Принудительно — галкой
**Синхронизировать принудительно** или удалением Variable `datalab_er_wf_hash`.

🚦 Любая исключённая запись делает таск синхронизации КРАСНЫМ — иначе ошибку в метаданных
легко не заметить, а пакет уедет неполным составом. Исправные группы при этом всё равно
записываются: опечатка в одной строке не должна замораживать правки по остальным пакетам.

## Где смотреть результаты

Полностью — в логе и в XCom: `before` (состояние до), `check` (запрос, колонки, `.meta`,
ошибки), `after` (состояние после), у синхронизации — `errors`, `warnings`, `summary`.
В заметках только короткие итоги: `add_note` режет их до 1000 символов.

Правка существующей записи — это **вставка новой версии**: `ReplacingMergeTree` схлопнет
её по `(db_name, extract_name)` при фоновом MERGE, читать до того — с `FINAL`.

## Чего ждать от формы

Список записей — выпадающий, а патч — отдельное поле, потому что Airflow иначе не умеет:
статическая форма не может подставить выбранное значение в соседнее поле.

Список задан через `enum`, а не `examples`. У `examples` на строковом параметре Airflow
рисует поле поиска с `datalist` и подсказкой «Start typing to see proposal values» — пока
не начнёшь печатать, список выглядит пустым. `enum` рендерится настоящим `<select>`
с поиском. Плата за это — выбрать можно только из списка.

Список строится при разборе файла из Variable `datalab_er_wf_meta`, которую наполняет
синхронизация этого же дага. В ClickHouse на каждом парсинге не ходим: это лишняя нагрузка
и Broken DAG при недоступности базы. Поэтому только что созданная запись появится
в списке через десяток секунд после рана — когда планировщик перечитает файл.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models.param import Param

try:
    from CI06932748.analytics.datalab.export_er.er_config import (  # type: ignore
        get_config, get_dict_from_ch, obj_load, obj_save, add_note, ensure_pool,
        INHERITED, replica_full, ch_error, clean_row, parse_params, explicit_schedule, check_table,
        raw_key, key_to_where, build_meta, ch_source_columns, ch_table_comments,
        check_descriptions, check_fields, cols_from_fields, export_sql, fit_descriptions,
        merge_params, probe_sql, query_columns, sql_sources, unnamed_fields,
    )
except ImportError:
    from er_export.er_config import (
        get_config, get_dict_from_ch, obj_load, obj_save, add_note, ensure_pool,
        INHERITED, replica_full, ch_error, clean_row, parse_params, explicit_schedule, check_table,
        raw_key, key_to_where, build_meta, ch_source_columns, ch_table_comments,
        check_descriptions, check_fields, cols_from_fields, export_sql, fit_descriptions,
        merge_params, probe_sql, query_columns, sql_sources, unnamed_fields,
    )

_cfg           = get_config()
CH_ID          = _cfg['CH_ID']
DEF_ARGS       = _cfg['DEF_ARGS']
VAR_NAME       = _cfg['VAR_NAME']
RAW_VAR_NAME   = _cfg['RAW_VAR_NAME']
CKSUM_VAR_NAME = _cfg['CKSUM_VAR_NAME']
POOL_NAME      = _cfg['POOL_NAME']
POOL_SLOTS     = _cfg['POOL_SLOTS']
TFS_MAP        = _cfg['TFS_MAP']
DEFAULT_PARAMS = _cfg['DEFAULT_PARAMS']
GROUP_PARAMS   = _cfg['GROUP_PARAMS']
FORMAT_MAP     = _cfg['FORMAT_MAP']

logger = logging.getLogger("airflow.task")

TABLE = 'export.er_wf_meta'

# Пункты выпадающего списка, которые не являются ключами записей.
NONE = '— только синхронизация —'
NEW  = '➕ новая запись'

# Пул намеренно не POOL_NAME: настройка не должна занимать слоты выгрузок.
SETUP_POOL = 'default_pool'

# Потолок времени на проверочный запрос, сек. LIMIT 0 обрывает конвейер до чтения, так что
# упереться в него можно разве что на планировании совсем патологического запроса —
# но пусть лучше проверка снимется сама, чем висит на воркере.
PROBE_TIMEOUT = 30

# Ключи _cfg, которые не нужны в doc_md (содержат функции или слишком многословны).
_HIDDEN_CFG_KEYS = frozenset({'DEF_ARGS', 'TYPE_MAP', 'EXTRA_PRE', 'EXTRA_SUF'})
_doc_cfg = {k: v for k, v in _cfg.items() if k not in _HIDDEN_CFG_KEYS}

# 📋 Колонки таблицы и их умолчания. Служат тремя вещами сразу: списком допустимых ключей
# патча (опечатку ловим по нему), умолчаниями для новой записи и порядком колонок INSERT.
# updated_at сюда не входит — его проставляет сама вставка.
COLUMNS: dict = {
    'extract_name': '',
    'db_name':      '',
    'replica':      '',
    'schema_name':  '',
    'pk':           [],
    'uk':           [],
    'fields':       [],
    'sql_from':     '',
    'sql_where':    '',
    'sql_join':     '',
    'sql_with':     '',
    'sql_settings': '',
    'params':       '{}',
    'description':  '',
    'schedule':     '',
    'is_recent':    0,
    'is_active':    1,
}


def _q(v) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return str(v).replace("'", "''")


def _literal(value) -> str:
    """Значение колонки → литерал ClickHouse.

    None даёт пустую строку, а не 'None': колонки таблицы String, NULL в них не бывает,
    и через str(None) в настройку оседал литерал, который потом уезжал прямо в запрос
    (`None SELECT … FROM t1 None`) и ронял выгрузку синтаксической ошибкой ClickHouse.
    """
    if value is None:
        return "''"
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(f"'{_q(v)}'" for v in value if v is not None) + "]"
    return f"'{_q(value)}'"


def _dump(obj) -> str:
    """Значение для XCom: JSON строкой, недружественное к JSON — через str().

    default=str обязателен: из ClickHouse приезжают datetime, а из формы — что угодно.
    Строкой, а не объектом, чтобы XCom нельзя было ненароком «поправить» типами.
    """
    return json.dumps(obj, ensure_ascii=False, default=str)


def _record_choices() -> list[str]:
    """Пункты выпадающего списка: сентинелы плюс ключи записей — из Variable, не из БД.

    Variable наполняет синхронизация этого же дага. Если её ещё нет (синк ни разу
    не отрабатывал), остаются два сентинела — завести первую запись это не мешает.

    Принимаем ДВЕ формы значения. Свою — объект {ключ: запись}, как её пишет синк.
    И массив записей [{...}, {...}] — так переменную набивают руками, выгружая SELECT
    из ClickHouse; ключи тогда считаем сами через raw_key. Раньше на массиве список молча
    схлопывался до одной «новой записи»: sorted() не умеет сравнивать словари, а except
    прятал TypeError в предупреждение при разборе файла, которого в UI не видно.
    """
    try:
        raw = obj_load(RAW_VAR_NAME, default={}) or {}
    except Exception as exc:
        logger.warning("Не прочитали %s, список записей только с сентинелами: %s", RAW_VAR_NAME, exc)
        return [NONE, NEW]

    if isinstance(raw, dict):
        keys = [str(k) for k in raw]
    elif isinstance(raw, list):
        keys = [raw_key(r) if isinstance(r, dict) else str(r) for r in raw]
    else:
        logger.warning(
            "%s содержит %s, а нужен объект {ключ: запись} или массив записей — "
            "список записей только с сентинелами", RAW_VAR_NAME, type(raw).__name__,
        )
        return [NONE, NEW]

    logger.info("Записей в выпадающем списке: %d (из %s)", len(keys), RAW_VAR_NAME)
    return [NONE, NEW] + sorted(keys)


def merge_patch(base: dict, patch: dict) -> dict:
    """🧩 Накладывает патч на строку, отвергая незнакомые ключи.

    Опечатка в имени поля иначе прошла бы молча: запись сохранилась бы без правки,
    а автор был бы уверен, что поменял. Поэтому неизвестный ключ — ошибка.
    """
    unknown = [k for k in patch if k not in COLUMNS]
    if unknown:
        raise AirflowFailException(
            f"Неизвестные поля в патче: {unknown}.\n"
            f"Допустимы: {sorted(COLUMNS)}"
        )
    return {**COLUMNS, **base, **patch}


def row_diff(base: dict, merged: dict) -> dict:
    """Что изменилось: {поле: 'было → стало'}. Для новой записи база пуста."""
    return {
        k: f"{base.get(k, '∅')!r} → {merged[k]!r}"
        for k in COLUMNS
        if k not in base or base[k] != merged[k]
    }


# ── 🧬 Раскладка строк таблицы по группам ────────────────────────────────────

def split_rows(rows: list[dict]) -> tuple[dict, list[dict], set[str]]:
    """🧩 Делит выборку на дефолты групп, поставки и выключенные группы.

    Строка с пустым extract_name — дефолты своей группы. is_active=0 на ней гасит группу
    целиком, вместе со всеми поставками: иначе поставки уехали бы дальше, растеряв групповые
    параметры и вернувшись к умолчаниям из кода (где notify_kafka=1).

    Возвращает ({replica: строка-дефолт}, [поставки], {выключенные группы}).

    Реплика тут же приводится к виду с суффиксом группы: 'hrplatform_datalab' →
    'hrplatform_datalab__0'. Копией, а не правкой на месте: исходные строки уходят
    в Variable для выпадающего списка, и там они обязаны совпадать с таблицей.
    """
    # clean_row здесь же: строковые 'None' из таблицы должны исчезнуть ДО проверок,
    # иначе 'None' в sql_from пройдёт как непустое значение и доедет до ClickHouse.
    rows     = [{**clean_row(r), "replica": replica_full(r["replica"])} for r in rows]
    defaults = {r["replica"]: r for r in rows if not r["extract_name"]}
    off      = {rep for rep, r in defaults.items() if not r.get("is_active", 1)}

    return (
        {rep: r for rep, r in defaults.items() if rep not in off},
        [r for r in rows
         if r["extract_name"] and r.get("is_active", 1) and r["replica"] not in off],
        off,
    )


def wf_entry(row: dict, grp_row: dict, comment: str = '') -> dict:
    """🧬 Строка таблицы + строка-дефолт группы → запись Variable.

    Возвращает {'key', 'row', 'entry', 'params', 'group_params'}:
      key          — 'db_name.extract_name'
      row          — строка после наследования (schema_name из группы, если своего нет)
      entry        — запись в том виде, в каком её читает фабрика er_export
      params       — слитые параметры: умолчания → группа → таблица
      group_params — разобранный params группы, отдельно нужен синку

    Тем же кодом пользуются синхронизация всего пакета и проверка одной записи: собери
    проверка запись по-своему — она проверяла бы не то, что потом уедет.
    """
    table_key = f"{row['db_name']}.{row['extract_name']}"
    own_desc  = row.get("description") or ''

    # Наследование: непустое значение поставки перебивает дефолт группы.
    row = {**row, **{k: (row.get(k) or grp_row.get(k) or '') for k in INHERITED}}

    grp_params = parse_params(grp_row.get("params", ""), f"группа {row['replica']}")
    tbl_params = parse_params(row.get("params", ""), table_key)

    # Групповые описания колонок достаются всем поставкам пакета, а колонка есть не у
    # каждой: оставляем только подходящие. Свои описания поставки не трогаем — лишнее
    # имя там опечатка, и её ловит check_descriptions.
    if grp_params.get('descriptions'):
        grp_params = {**grp_params,
                      'descriptions': fit_descriptions(grp_params['descriptions'], row.get('fields'))}

    # 🔀 is_recent определяет ключ SQL-запроса: фабрика проверяет наличие одного из двух
    sql_key = "sql_stmt_export_recent" if row.get("is_recent") else "sql_stmt_export_delta"
    sql_val = {"from": row["sql_from"]}
    if row.get("sql_with"):     sql_val["with"]     = row["sql_with"]
    if row.get("sql_join"):     sql_val["joins"]    = row["sql_join"]
    if row.get("sql_where"):    sql_val["where"]    = row["sql_where"]
    if row.get("sql_settings"): sql_val["settings"] = row["sql_settings"]

    entry = {
        "schema":  row["schema_name"],
        "PK":      list(row.get("pk") or []),
        "UK":      list(row.get("uk") or []),
        "fields":  list(row.get("fields") or []),
        # Параметры кладём уже разрешёнными — фабрика про наследование не знает
        "params":  json.dumps(merge_params({}, grp_params, tbl_params), ensure_ascii=False),
        sql_key:   sql_val,
    }

    # Описание: своё → комментарий таблицы в CH → групповое. Групповой текст последний,
    # иначе он затёр бы осмысленные комментарии всех таблиц пакета.
    desc = own_desc or comment or grp_row.get("description", "")
    if desc:
        entry["description"] = desc

    return {
        'key':          table_key,
        'row':          row,
        'entry':        entry,
        'params':       merge_params(DEFAULT_PARAMS, grp_params, tbl_params),
        'group_params': grp_params,
    }


def build_wfs(tables: list[dict], defaults: dict, ch_comments: dict) -> tuple[dict, list[str], list[str]]:
    """🧬 Раскладывает строки er_wf_meta по группам, разрешая наследование от строк-дефолтов.

    tables      — строки-поставки (extract_name непустой)
    defaults    — {replica: строка-дефолт группы}
    ch_comments — {(db_name, extract_name): комментарий таблицы-источника}

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
    # Умолчания у cron нет намеренно: пакет, поехавший не в своё окно, хуже непоехавшего,
    # а «55 0 * * *» из ниоткуда — ровно такой сюрприз.
    schedules = {}
    for replica in {r["replica"] for r in tables}:
        own = [explicit_schedule(r) for r in tables if r["replica"] == replica]
        schedules[replica] = (
            explicit_schedule(defaults.get(replica, {}))
            or next((s for s in own if s), '')
        )
        if not schedules[replica]:
            errors.setdefault(replica, []).append(
                f"группа {replica}: не задано расписание — проставьте cron в поле schedule "
                "строки-дефолта группы; умолчания у него нет"
            )

    for row in tables:
        replica = row["replica"]
        grp_row = defaults.get(replica, {})
        info    = wf_entry(row, grp_row,
                           ch_comments.get((row["db_name"], row["extract_name"]), ""))

        if not check_table(info['row'], info['key'],
                           errors.setdefault(replica, []), info['params']):
            continue

        group = wfs.setdefault(replica, {
            "schedule": schedules[replica],
            "params":   json.dumps({k: v for k, v in {**DEFAULT_PARAMS, **info['group_params']}.items()
                                    if k in GROUP_PARAMS}, ensure_ascii=False),
            "tables":   {},
        })
        if grp_row.get("description"):
            group["description"] = grp_row["description"]

        # Cron у пакета один — пакет уезжает целиком. Расхождение не отбрасывает поставку
        # (потерять таблицу из-за косметики хуже), но должно быть видно в логе и заметке.
        own_sched = explicit_schedule(row)
        if own_sched and own_sched != group["schedule"]:
            warnings.append(
                f"{info['key']}: schedule '{own_sched}' расходится с расписанием группы "
                f"'{group['schedule']}' — у пакета расписание одно, взято групповое"
            )

        group["tables"][info['key']] = info['entry']

    # 💥 Любая ошибка ломает ВЕСЬ пакет, а не одну поставку: тикет в ЕР один на группу,
    # и уехавший неполный состав — это расхождение данных на стороне КАП. Такая группа
    # попадает в Variable заглушкой: причины плюс расписание, чтобы даг-заглушка краснел
    # в том же ритме, в каком должен был ходить пакет.
    for rep, msgs in errors.items():
        if not msgs:
            continue
        wfs[rep] = {
            # Расписание могло и не найтись — тогда его отсутствие и есть причина ошибки.
            # Заглушка без cron не поедет вовсе, и это честнее выдуманного времени.
            "schedule": schedules.get(rep, ''),
            "errors":   msgs,
            "tables":   {},
        }

    flat_errors = [m for rep in sorted(errors) for m in errors[rep]]
    return wfs, flat_errors, warnings


def wf_checksum(rows: list[dict]) -> str:
    """🔐 Контрольная сумма настройки: строки таблицы плюс снимок конфига.

    Конфиг в сумме обязателен. Правка умолчаний в коде (DEFAULT_PARAMS, состав GROUP_PARAMS,
    FORMAT_MAP, INHERITED, TFS_MAP) меняет содержимое Variable, не трогая ни одной строки
    таблицы, — и без этой половины синк молча пропустил бы такую правку, оставив выгрузки
    жить по прежним умолчаниям.

    Строки прогоняются через json.dumps → json.loads не для красоты: Array(String) драйвер
    ClickHouse отдаёт кортежем, а из Variable он вернулся бы списком. Без нормализации
    сумма не совпала бы никогда и скип не наступил бы ни разу.
    """
    payload = {
        'rows': json.loads(json.dumps(rows, sort_keys=True, ensure_ascii=False, default=str)),
        'cfg':  {
            'DEFAULT_PARAMS': DEFAULT_PARAMS,
            'GROUP_PARAMS':   sorted(GROUP_PARAMS),
            'FORMAT_MAP':     FORMAT_MAP,
            'INHERITED':      list(INHERITED),
            'TFS_MAP':        {k: list(v) for k, v in TFS_MAP.items()},
        },
    }
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def _ensure_pool() -> None:
    """🏊 Создаёт Airflow Pool для ER-выгрузок, если его ещё нет.

    Вызывается внутри таска, а не при разборе DAG: ensure_pool кэширует результат
    на процесс, но лишний SELECT на каждом обходе scheduler-ом всё равно не нужен.

    Пулы тракта ТФС здесь не заводятся — их создаёт tfs_kafka (ensure_pools в приёмнике).
    """
    ensure_pool(POOL_NAME, slots=POOL_SLOTS, description='Пул для ER-выгрузок')


def _hook():
    """ClickHouse-хук. Импорт внутри функции: при разборе файла плагин не нужен."""
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    return ClickHouseHook(clickhouse_conn_id=CH_ID)


# Одна выборка на все нужды дага: строки-дефолты групп и поставки вперемешку.
# is_active НЕ фильтруем: выключенную строку-дефолт надо увидеть, чтобы погасить всю
# группу. Уйди она из выборки — поставки синхронизировались бы дальше, растеряв групповые
# параметры и вернувшись к умолчаниям из кода (а notify_kafka там 1, то есть стендовый
# пакет молча поехал бы в ТФС).
SQL_ALL_ROWS = f"""
    SELECT
        extract_name, db_name, replica, schema_name,
        pk, uk, fields,
        sql_from, sql_where, sql_join, sql_with, sql_settings,
        params, description, schedule, is_recent, is_active
    FROM {TABLE} FINAL
    WHERE replica != ''
    ORDER BY replica, db_name, extract_name
"""   # порядок только для читаемости логов; ключ таблицы — (db_name, extract_name)


def _group_row(hook, replica: str) -> dict:
    """Строка-дефолт группы по реплике; пустой словарь, если группы ещё нет.

    Ищем по нормализованной реплике: в таблице суффикс группы может быть не проставлен
    ('hrplatform_datalab' и 'hrplatform_datalab__0' — одно и то же), а сравнивать надо
    так же, как это делает синхронизация.
    """
    want = replica_full(replica)
    rows = get_dict_from_ch(hook, f"""
        SELECT {', '.join(COLUMNS)}
        FROM {TABLE} FINAL
        WHERE extract_name = ''
    """)
    return next((r for r in rows if replica_full(r['replica']) == want), {})


@dag(
    dag_id="export_er_setup",
    description="⚙️ Настройка ER: правка export.er_wf_meta, проверка SQL и синхронизация",
    # retries=0 вместо трёх из DEF_ARGS: даг ручной, у экрана сидит человек, и ждать
    # 15 минут ретраев ради опечатки в SQL ему незачем. Ошибки настройки от повтора
    # не лечатся, а недоступный ClickHouse честнее показать сразу.
    default_args={**DEF_ARGS, 'retries': 0},
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    schedule_interval=None,   # только ручной запуск
    max_active_runs=1,
    catchup=False,
    tags=["DataLab", "CI02420667", "ER", "setup"],
    is_paused_upon_creation=False,
    doc_md=__doc__ + "\n\n### ⚙️ Конфигурация\n\n```\n"
           + json.dumps(_doc_cfg, indent=4, default=str) + "\n```",
    params={
        'record': Param(
            NONE, type='string', title='Поток',
            description='Что смотрим или правим. Список берётся из Variable '
                        'datalab_er_wf_meta и обновляется после каждой синхронизации.',
            # Именно enum, а не examples: examples на строковом параметре Airflow рендерит
            # полем поиска с datalist («Start typing to see proposal values»), и пока не
            # начнёшь печатать, список выглядит пустым. enum даёт настоящий <select>.
            enum=_record_choices(),
        ),
        'patch': Param(
            {}, type='object', title='Патч',
            description='JSON только с изменяемыми полями. Для новой записи — вся запись. '
                        'Выключить запись: {"is_active": 0}. Оставить пустым — просмотр: '
                        'запись показывается и проверяется, но не меняется. '
                        'У строки-дефолта группы (extract_name пуст) db_name обязан быть '
                        'равен replica — иначе дефолты всех групп схлопнутся в одну строку.',
        ),
        'force_write': Param(
            False, type='boolean', title='Записать несмотря на проверку',
            description='Сохранить правку, даже если проверка SQL не прошла. Для случаев, '
                        'когда виновата не запись: источник недоступен, таблица ещё не создана.',
        ),
        'force_sync': Param(
            False, type='boolean', title='Синхронизировать принудительно',
            description='Не смотреть на контрольную сумму. Нужно, если Variable правили '
                        'руками.',
        ),
    },
)
def er_setup_dag():

    @task(task_id="prepare", pool=SETUP_POOL)
    def prepare(**context) -> dict:
        """👁️ Читает выбранную запись, накладывает патч и проверяет её структуру.

        Ничего не пишет: запись — дальше, после проверки SQL. Состояние до правки уходит
        в XCom 'before' — по нему видно, с чего начинали, даже если ран потом упал.
        """
        p      = context['params']
        record = (p.get('record') or NONE).strip()
        if record == NONE:
            raise AirflowSkipException("Поток не выбран — в этом ране только синхронизация")

        patch = p.get('patch') or {}
        # Разбираем ДО проверки на пустоту: форма отдаёт объект строкой, и '{}' — это
        # непустая строка, но пустой патч.
        if isinstance(patch, str):
            patch = json.loads(patch)

        hook = _hook()

        # Базу читаем из ClickHouse, а не из Variable: та отражает состояние на момент
        # последнего синка и могла устареть. FINAL — чтобы не поймать старую версию строки.
        base: dict = {}
        if record != NEW:
            db_name, extract_name = key_to_where(record)
            found = get_dict_from_ch(hook, f"""
                SELECT {', '.join(COLUMNS)}
                FROM {TABLE} FINAL
                WHERE db_name = '{_q(db_name)}' AND extract_name = '{_q(extract_name)}'
            """)
            if not found:
                raise AirflowFailException(
                    f"Запись '{record}' не найдена в {TABLE}. "
                    "Возможно, её удалили после последней синхронизации"
                )
            base = clean_row(found[0])

        shown = base or dict(COLUMNS)
        context['ti'].xcom_push(key='before', value=_dump(shown))
        logger.info("👁️ %s — %s", record, 'из ClickHouse' if base else 'умолчания')
        for col in COLUMNS:
            logger.info("    %-13s %r", col, shown.get(col))

        # 👁️ Пустой патч — не ошибка, а просмотр: посмотреть, что в записи сейчас, и оттуда
        # же взять поля для правки. Проверка ниже отработает всё равно — заодно скажет,
        # жива ли поставка. Для «новой записи» показывается шаблон с умолчаниями.
        if not patch:
            what = record if record != NEW else 'шаблон новой записи'
            add_note({f"👁️ {what}": {col: repr(shown.get(col)) for col in COLUMNS}},
                     level='task,dag', context=context, title='⚙️ er_setup')
            logger.info(
                "Патч пуст — правки не будет. Чтобы править, заполните «Патч», "
                "например {\"schedule\": \"30 2 * * *\"}"
            )
            return {'mode': 'view', 'record': record, 'merged': shown, 'diff': {}}

        merged = merge_patch(base, patch)
        diff   = row_diff(base, merged)
        if not diff:
            raise AirflowFailException("Патч ничего не меняет — все значения уже такие")

        # Структурная проверка — та же, что потом сделает синхронизация. Параметры и схему
        # берём УЖЕ с наследованием от строки-дефолта группы: schema_name у поставки часто
        # пуст именно потому, что задан на группе, и проверять его в одиночку нельзя.
        errors: list[str] = []
        if merged['extract_name']:
            info = wf_entry(merged, _group_row(hook, merged['replica']))
            check_table(info['row'], record, errors, info['params'])
        elif merged['db_name'] != merged['replica']:
            errors.append(
                f"строка-дефолт группы: db_name должен быть равен replica "
                f"('{merged['replica']}'), иначе дефолты разных групп схлопнутся "
                "в одну строку по ключу (db_name, extract_name)"
            )
        if errors:
            raise AirflowFailException(
                "Запись не прошла проверку, в таблицу ничего не записано:\n"
                + "\n".join(f"  • {e}" for e in errors)
            )

        logger.info("✏️ Патч принят, изменений: %d\n%s", len(diff), _dump(diff))
        add_note({f"✏️ Патч · {record}": diff},
                 level='task,dag', context=context, title='⚙️ er_setup')
        return {'mode': 'edit', 'record': record, 'merged': merged, 'diff': diff}

    @task(task_id="check", pool=SETUP_POOL)
    def check(prepared: dict, **context) -> dict:
        """🔍 Собирает запрос и .meta и выполняет запрос без данных.

        Сборка — er_config.export_sql, тот же код, что и у выгрузки. Запрос выполняется
        с ложным окном дельты и внешним LIMIT 0: источник не читается, но разбирается
        и планируется целиком, то есть ошибка в JOIN-е, CTE или SETTINGS видна здесь.

        Не прошло — таск красный и вставки не будет. Галка «Записать несмотря на проверку»
        превращает ошибки в предупреждения: иначе apply получил бы upstream_failed
        и параметр не работал бы вовсе.
        """
        record, merged, mode = prepared['record'], prepared['merged'], prepared['mode']
        force = bool(context['params'].get('force_write'))
        hook  = _hook()

        errors: list[str] = []
        warnings: list[str] = []
        result: dict = {'record': record, 'mode': mode}

        if not merged['extract_name']:
            # Строка-дефолт группы: SQL у неё нет физически — проверяем то, что есть.
            result['kind'] = 'группа'
            if merged.get('params'):
                try:
                    json.loads(merged['params'])
                except json.JSONDecodeError as err:
                    errors.append(f"params не разбирается как JSON: {err}")
            if not explicit_schedule(merged):
                warnings.append("не задан schedule — группа не синхронизируется")
            result['group_params'] = merged.get('params')
        else:
            info = wf_entry(merged, _group_row(hook, merged['replica']))
            result['kind'] = 'поставка'
            check_table(info['row'], info['key'], errors, info['params'])

            if not errors:
                q = export_sql(info['entry'], info['params'], info['key'])
                probe = probe_sql(q['sql_export'])
                result['sql'] = q['sql_export']
                logger.info("🔍 Проверочный запрос:\n%s", probe)

                # Ошибку запроса кладём в отчёт, а не даём ей выпасть наружу: рядом с ней
                # должны быть и сам запрос, и остальные находки, а решение «падать или
                # писать» принимается ниже, одним местом на все ошибки сразу.
                try:
                    _, qcols = hook.execute(probe, with_column_types=True,
                                            settings={'max_execution_time': PROBE_TIMEOUT})
                    result['columns'] = [f"{n} {t}" for n, t in qcols]
                except Exception as err:
                    errors.append(f"запрос выгрузки не выполнился: {ch_error(err)}")

            # .meta собираем, только если сам запрос выполнился: строить схему по запросу,
            # который не разбирается, нечем, а вторая та же ошибка в отчёте лишняя.
            if not errors:
                # Комментарии колонок — из таблиц, к которым обращается запрос
                # (from → with → joins); у результата подзапроса комментариев нет.
                # Ни одной таблицы не нашлось — .meta соберётся без описаний.
                sql_parts = info['entry'].get(q['sql_key'])
                sources = sql_sources(sql_parts if isinstance(sql_parts, dict) else {},
                                      merged['db_name'], merged['extract_name'])
                ch_cols, found = ch_source_columns(hook, sources)
                result['sources'] = [f"{db}.{t}" for db, t in found]
                if not found:
                    # Предупреждение, а не ошибка: описания колонок — приятное дополнение,
                    # а не условие выгрузки. Ошибкой это было бы, будь имя выгрузки
                    # обязано быть таблицей, — оно не обязано.
                    warnings.append(
                        "таблиц-источников не нашлось (кандидаты: "
                        + (", ".join(f"{db}.{t}" for db, t in sources) or "нет")
                        + ") — .meta соберётся без описаний колонок"
                    )

                # Состав .meta считаем по запросу БЕЗ служебных колонок — ровно так же,
                # как это делает build_meta в выгрузке.
                try:
                    if q['sql_meta']:
                        _, mcols = hook.execute(probe_sql(q['sql_meta']), with_column_types=True,
                                                settings={'max_execution_time': PROBE_TIMEOUT})
                        data_cols = query_columns(mcols, ch_cols)
                    else:
                        data_cols = cols_from_fields(info['entry']['fields'], ch_cols, [(n,) for n in ch_cols])
                except Exception as err:
                    errors.append(f"состав колонок .meta не собрался: {ch_error(err)}")
                    data_cols = []

                if data_cols:
                    names = [c['column_name'] for c in data_cols]
                    errors += check_fields(info['entry']['fields'], names, info['key'])
                    # Описание колонки с опечаткой молча не доедет до КАП — ловим здесь же,
                    # рядом со сверкой состава: причина у них одна.
                    errors += check_descriptions(info['params'].get('descriptions'),
                                                 names, info['key'])
                    if unnamed := unnamed_fields(info['entry']['fields']):
                        warnings.append(
                            f"выражения без алиаса, имена колонок для них не проверены: {unnamed}"
                        )

                    result['meta'] = build_meta(
                        {'schema_name': info['entry']['schema'], 'tbl': merged['extract_name'],
                         'description': info['entry'].get('description', ''),
                         'strategy': info['params']['strategy'], 'PK': info['entry']['PK'],
                         'UK': info['entry']['UK'], 'format': info['params']['format'],
                         'descriptions': info['params'].get('descriptions')},
                        data_cols,
                    )

        result['errors'], result['warnings'] = errors, warnings
        context['ti'].xcom_push(key='check', value=_dump(result))
        logger.info("🔍 Результат проверки:\n%s", json.dumps(result, ensure_ascii=False,
                                                             indent=2, default=str))

        cols = len(result.get('meta', {}).get('columns', []))
        head = f"{'❌' if errors else '🔍'} Проверка · {record}"
        add_note({head: (errors + warnings) if (errors or warnings)
                  else f"✅ колонок в .meta: {cols}"},
                 level='task,dag', context=context, title='⚙️ er_setup')

        if errors:
            if not force:
                raise AirflowFailException(
                    "Проверка не прошла, в таблицу ничего не записано:\n"
                    + "\n".join(f"  • {e}" for e in errors)
                    + "\nЕсли виновата не запись (источник недоступен, таблица ещё не "
                      "создана) — повторите с галкой «Записать несмотря на проверку»"
                )
            logger.warning("⚠️ Проверка не прошла, но запись форсирована: %s", errors)
            add_note({f"⚠️ Запись форсирована · {record}": errors},
                     level='task,dag', context=context, title='⚙️ er_setup')

        return prepared

    @task(task_id="apply", pool=SETUP_POOL)
    def apply(prepared: dict, **context) -> str:
        """✏️ Пишет новую версию строки и перечитывает её из таблицы.

        Правка — это ВСТАВКА: ReplacingMergeTree схлопнет версии по (db_name, extract_name)
        при фоновом MERGE, поэтому состояние после читается с FINAL.
        """
        if prepared['mode'] == 'view':
            raise AirflowSkipException("Просмотр — в таблицу ничего не пишем")

        merged, record = prepared['merged'], prepared['record']
        hook = _hook()
        hook.execute(
            f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
            f"VALUES ({', '.join(_literal(merged[c]) for c in COLUMNS)})"
        )

        key   = raw_key(merged)
        after = get_dict_from_ch(hook, f"""
            SELECT {', '.join(COLUMNS)}, updated_at
            FROM {TABLE} FINAL
            WHERE db_name = '{_q(merged['db_name'])}'
              AND extract_name = '{_q(merged['extract_name'])}'
        """)
        context['ti'].xcom_push(key='after', value=_dump(after[0] if after else {}))

        logger.info("✅ %s: %s\n%s", 'создана' if record == NEW else 'обновлена', key,
                    _dump(after[0] if after else {}))
        add_note({f"{'➕ Создана' if record == NEW else '✏️ Изменена'}: {key}": prepared['diff']},
                 level='task,dag', context=context, title='⚙️ er_setup')
        return key

    # trigger_rule: синхронизация обязана отработать и тогда, когда всё выше пропущено —
    # ран «только синхронизация» именно так и выглядит. Упавшая проверка её не запускает:
    # писать было нечего, значит и раскладывать нечего.
    @task(task_id="sync", pool=SETUP_POOL, trigger_rule='none_failed')
    def sync(**context):
        """🔄 Читает er_wf_meta, собирает словарь выгрузок и сохраняет в Airflow Variable.

        Пустая таблица — ошибка на любом стенде: защита от затирания Variable.

        Таблицу синк не создаёт: DDL живёт в er_wf_meta.sql и накатывается отдельно.
        Держать вторую копию схемы в коде значило бы разъезд с боевым файлом.
        """
        _ensure_pool()
        hook = _hook()
        rows = get_dict_from_ch(hook, SQL_ALL_ROWS)

        if not rows:
            # Пустая выборка на любом стенде — ошибка, а не повод тихо выйти: Variable
            # осталась бы от прошлой синхронизации, и фабрика продолжила бы поднимать
            # даги по устаревшей настройке, ничем этого не показывая.
            raise ValueError("🚫 No active workflows found in export.er_wf_meta — aborting to avoid overwriting Variable with empty dict")

        # 🔐 Скип, если менять нечего. Проверка на пустую VAR_NAME обязательна: удалённую
        # руками переменную иначе не восстановил бы никто — сумма-то сходится.
        checksum = wf_checksum(rows)
        known    = (obj_load(CKSUM_VAR_NAME, default={}) or {}).get('hash')
        if (not context['params'].get('force_sync')
                and checksum == known and obj_load(VAR_NAME, default={})):
            raise AirflowSkipException(
                f"☮️ Настройка не менялась с последней синхронизации (sha256 {checksum[:12]}). "
                "Принудительно — галка «Синхронизировать принудительно» или удаление "
                f"Variable {CKSUM_VAR_NAME}"
            )

        defaults, tables, off = split_rows(rows)
        if off:
            logger.info("⏸️ Группы выключены строкой-дефолтом (is_active=0): %s", ", ".join(sorted(off)))

        # 💬 Для строк без явного description подтягиваем комментарий ТАБЛИЦЫ-ИСТОЧНИКА
        # (первой из sql_from), а не таблицы с именем выгрузки: имя выгрузки таблице
        # соответствовать не обязано. Один батч-запрос на все записи.
        sources_by_key = {
            (r["db_name"], r["extract_name"]): sql_sources(
                {'from': r.get("sql_from"), 'with': r.get("sql_with"), 'joins': r.get("sql_join")},
                r["db_name"], r["extract_name"],
            )
            for r in tables if not r["description"]
        }
        ch_comments = ch_table_comments(hook, sources_by_key) if sources_by_key else {}

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

        # Сырые строки — для выпадающего списка записей: он строится при разборе файла
        # и в ClickHouse ходить не может. Пишем ВСЕ строки, включая выключенные:
        # править надо уметь и их.
        obj_save(RAW_VAR_NAME, {raw_key(r): r for r in rows})

        # Сумму запоминаем ТОЛЬКО при чистом прогоне. Иначе сломанный пакет отрапортовал
        # бы о себе однажды и замолчал: следующий ран увидел бы совпадение и ушёл в скип.
        if not errors:
            obj_save(CKSUM_VAR_NAME, {
                'hash':   checksum,
                'ts':     datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
                'rows':   len(rows),
                'groups': len(ok_wfs),
            })

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
        add_note(note, level='task,dag', context=context, title='🔄 er_setup sync')

        if errors:
            raise AirflowFailException(
                f"❌ Метаданные ER содержат {len(errors)} ошибок, сломано пакетов: {len(broken)} "
                f"({', '.join(sorted(broken))}). Исправные группы синхронизированы. "
                "Полный список — в логе и в XCom 'errors'"
            )

    apply(check(prepare())) >> sync()


er_setup_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
