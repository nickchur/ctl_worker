"""✏️ DAG правки настройки ER-выгрузок — export.er_wf_meta из UI.
*2026-08-12 16:20 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Заводит новые записи и правит существующие, чтобы не ходить в `clickhouse-client`.

## Как пользоваться

1. **Запись** — что правим: `➕ новая запись` либо ключ существующей из списка.
2. **Патч** — JSON **только с теми полями, которые меняем**. Для новой записи —
   вся запись целиком.
3. **Синхронизировать** — запустить `export_er_sync` после успешной записи.

Пример правки расписания у существующей группы:

    {"schedule": "30 2 * * *"}

Пример новой поставки:

    {
      "extract_name": "lc_items_opened", "db_name": "evolution",
      "replica": "hrplatform_datalab__1", "schema_name": "learning",
      "uk": ["item_id"], "fields": ["item_id", "title"],
      "sql_from": "evolution.lc_items_opened", "sql_where": "{condition}",
      "params": "{\\"selfrun_timeout\\": 10}"
    }

**Удаление** отдельной кнопкой не сделано и не нужно: `{"is_active": 0}` выключает
запись, а у строки-дефолта группы — весь пакет.

## Чего ждать от формы

Список записей — выпадающий, а патч — отдельное поле, потому что Airflow иначе не умеет:
параметр с `examples` рендерится списком **вместо** свободного поля, и подставить
выбранное значение в соседнее поле статическая форма не может.

Список строится при разборе файла из Variable `datalab_er_wf_meta`, которую наполняет
`export_er_sync`. В ClickHouse на каждом парсинге не ходим: это лишняя нагрузка и Broken
DAG при недоступности базы. Поэтому только что созданная кем-то запись появится
в списке после ближайшего синка — но править её можно и до того, вручную указав ключ
через conf.

## Что проверяется до записи

Ровно то же, что потом проверит `export_er_sync`: непустой `sql_from`, реплика
в `TFS_MAP`, непустой `schema_name`, явный `fields` без звёздочки, известный `format`,
а у строки-дефолта — `db_name`, равный `replica`. Битая запись в таблицу не попадает:
ошибку видно там, где её сделали, а не через два дага.

Правка существующей записи — это ВСТАВКА новой версии: `ReplacingMergeTree` схлопнет
её по `(db_name, extract_name)` при фоновом MERGE, читать до того — с `FINAL`.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

try:
    from CI06932748.analytics.datalab.export_er.er_config import (  # type: ignore
        get_config, get_dict_from_ch, obj_load, add_note,
        check_table, parse_params, raw_key, key_to_where,
    )
except ImportError:
    from er_export.er_config import (
        get_config, get_dict_from_ch, obj_load, add_note,
        check_table, parse_params, raw_key, key_to_where,
    )

_cfg           = get_config()
CH_ID          = _cfg['CH_ID']
DEF_ARGS       = _cfg['DEF_ARGS']
RAW_VAR_NAME   = _cfg['RAW_VAR_NAME']
DEFAULT_PARAMS = _cfg['DEFAULT_PARAMS']

logger = logging.getLogger("airflow.task")

TABLE    = 'export.er_wf_meta'
NEW      = '➕ новая запись'
SYNC_DAG = 'export_er_sync'

# Пул синка, а не экспортный: правка настройки не должна занимать слоты выгрузок.
EDIT_POOL = 'default_pool'

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
    'schedule':     '55 0 * * *',
    'is_recent':    0,
    'is_active':    1,
}


def _record_choices() -> list[str]:
    """Ключи существующих записей для выпадающего списка — из Variable, не из БД.

    Variable наполняет export_er_sync. Если её ещё нет (синк ни разу не отрабатывал),
    остаётся один пункт «новая запись» — завести первую запись это не мешает.
    """
    try:
        rows = obj_load(RAW_VAR_NAME, default={}) or {}
        return [NEW] + sorted(rows)
    except Exception as exc:
        logger.warning("Не прочитали %s, список записей только с «новой»: %s", RAW_VAR_NAME, exc)
        return [NEW]


def _q(v) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return str(v).replace("'", "''")


def _literal(value) -> str:
    """Значение колонки → литерал ClickHouse."""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(f"'{_q(v)}'" for v in value) + "]"
    return f"'{_q(value)}'"


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


@dag(
    dag_id="export_er_config",
    description="✏️ Правка и создание записей в export.er_wf_meta",
    default_args=DEF_ARGS,
    start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
    schedule_interval=None,   # только ручной запуск
    max_active_runs=1,
    catchup=False,
    tags=["DataLab", "CI02420667", "ER", "config"],
    is_paused_upon_creation=False,
    doc_md=__doc__,
    params={
        'record': Param(
            NEW, type='string', title='Запись',
            description='Что правим. Список берётся из Variable datalab_er_wf_meta '
                        'и обновляется после каждого export_er_sync.',
            examples=_record_choices(),
        ),
        'patch': Param(
            {}, type='object', title='Патч',
            description='JSON только с изменяемыми полями. Для новой записи — вся запись. '
                        'Выключить запись: {"is_active": 0}.',
        ),
        'run_sync': Param(
            True, type='boolean', title='Синхронизировать',
            description='Запустить export_er_sync после записи. Выключите, если правок '
                        'подряд несколько и синхронизировать хотите один раз в конце.',
        ),
    },
)
def er_wf_meta_edit_dag():

    @task(task_id="apply", pool=EDIT_POOL)
    def apply(**context):
        """✏️ Накладывает патч на выбранную запись, проверяет и пишет новую версию."""
        from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

        p      = context['params']
        record = (p.get('record') or NEW).strip()
        patch  = p.get('patch') or {}

        if not patch:
            raise AirflowFailException(
                "Патч пуст — менять нечего. Укажите поля, например {\"schedule\": \"30 2 * * *\"}"
            )
        if isinstance(patch, str):     # форма может отдать JSON строкой
            patch = json.loads(patch)

        hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

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
                    "Возможно, её удалили после последнего export_er_sync"
                )
            base = found[0]

        merged = merge_patch(base, patch)
        diff   = row_diff(base, merged)
        if not diff:
            raise AirflowFailException("Патч ничего не меняет — все значения уже такие")

        # Та же проверка, что потом сделает синк: ошибку надо показать здесь, а не после
        # записи в таблицу. Для строки-дефолта группы check_table не годится — у неё нет
        # ни sql_from, ни fields, зато есть своё правило про db_name.
        errors: list[str] = []
        if merged['extract_name']:
            check_table(merged, record, errors, {**DEFAULT_PARAMS, **parse_params(merged['params'], record)})
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

        hook.execute(
            f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
            f"VALUES ({', '.join(_literal(merged[c]) for c in COLUMNS)})"
        )

        key = raw_key(merged)
        logger.info("✅ %s: %s", 'создана' if record == NEW else 'обновлена', key)
        add_note(
            {f"{'➕ Создана' if record == NEW else '✏️ Изменена'}: {key}": diff},
            level='task,dag', context=context, title='✏️ er_wf_meta',
        )

        if p.get('run_sync', True):
            from airflow.api.common.trigger_dag import trigger_dag

            trigger_dag(dag_id=SYNC_DAG, conf={}, replace_microseconds=False)
            logger.info("🔄 Запущен %s", SYNC_DAG)
        else:
            logger.info("🔄 %s не запускался: правка вступит в силу после синхронизации", SYNC_DAG)

        return key

    apply()


er_wf_meta_edit_dag()  # вызов регистрирует DAG в globals() через декоратор @dag
