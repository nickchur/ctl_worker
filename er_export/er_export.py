"""🚀 DAG-фабрика ER-выгрузок (ClickHouse → S3 → TFS).
*2026-09-01 08:48 MSK · v3.20 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Один DAG — один пакет — одна группа поставок — один внешний тикет. Пакет задаётся парой
`replica` + `dag_group` (двумя колонками `export.er_wf_meta`), а даг называется
`export_er__<replica>__<dag_group>`; внутри — по TaskGroup на таблицу:

  make_ts → <схема>__<таблица>: init → [build_meta, export_to_s3] → pack_zip
                                                                      ↓
  make_summary → wait_confirm → save_status → schedule_next

Имена файлов пакета: `[реплика]__[ts]__[группа]__[таблица]__[часть]_[всего]_[строк].zip`
и `[реплика]__[ts].tkt` — группа стоит за меткой времени, а в тикете её нет вовсе.
Метку выдаёт make_ts, по одному таску за раз на реплику (пул на один слот).

Состояние дельты живёт в своей таблице `export.er_extract_history`: выгрузка опознаётся
тройкой (replica, schema_name, extract_name), окно следующей выгрузки считает next_window
здесь же, в коде. До 01.09.2026 состояние лежало в общей с xStream `export.extract_history`
под составным именем `<dag_id>.<extract_name>`, а окно считала чужая вью.

Метаданные выгрузок хранятся в Airflow Variable `datalab_er_wfs` (JSON-словарь, ключ —
dag_id пакета), который синхронизируется DAG-ом export_er_setup из таблицы
export.er_wf_meta.

Поддерживаемые режимы выгрузки:
  📈 delta  — инкрементальный, окно [time_from, time_to] от достигнутого состояния
  🔄 recent — скользящее окно [now() - recent_interval, now()], без сохранения состояния
  📅 период — ручной запуск с date_from/date_to; состояние дельты при этом не двигается
"""
from __future__ import annotations

import ast
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Param
from airflow.utils.task_group import TaskGroup

try:
    from CI06932748.analytics.datalab.export_er.er_config import (  # type: ignore
        get_config, get_dict_from_ch, obj_load, add_note, get_params,
        norm_group, ts_pool,
        build_meta, ch_source_columns, check_descriptions, check_fields,
        cols_from_fields, export_sql, parse_s3_target, query_columns, sql_sources,
        unnamed_fields,
    )
except ImportError:
    from er_export.er_config import (  # type: ignore
        get_config, get_dict_from_ch, obj_load, add_note, get_params,
        norm_group, ts_pool,
        build_meta, ch_source_columns, check_descriptions, check_fields,
        cols_from_fields, export_sql, parse_s3_target, query_columns, sql_sources,
        unnamed_fields,
    )


def _tfs():
    """Слой тракта ТФС — общий с tfs_kafka: STORAGE живёт в одном месте, иначе писатель
    и читатель разъедутся и пакет зависнет без внятной причины.

    Импорт ЛЕНИВЫЙ, на уровне модуля его нет намеренно. Во-первых, транспорт нужен только
    таскам постановки в очередь и ожидания квитанции, а при `notify_kafka: 0` не нужен
    вовсе. Во-вторых, модуль этот — фабрика: жёсткий импорт при неполной выкладке ронял
    Broken DAG'ом ВСЕ пакеты ЕР разом, включая те, что в ТФС ничего не шлют.
    """
    try:
        from plugins import tfs_utils  # type: ignore
    except ImportError:
        from CI06932748.tools import tfs_utils  # type: ignore
    return tfs_utils

logger = logging.getLogger("airflow.task")

# ── Configuration & Constants ────────────────────────────────────────────────

# TYPE_MAP, EXTRA_PRE/SUF, LIMITS и ENV_STAND сюда больше не берутся: они нужны только
# сборке запроса и .meta, а та переехала в er_config целиком.
_cfg = get_config()
CH_ID          = _cfg['CH_ID']
DEF_ARGS       = _cfg['DEF_ARGS']
BUCKET         = _cfg['BUCKET']
TFS_MAP         = _cfg['TFS_MAP']
S3_CONN         = _cfg['S3_CONN']
VAR_NAME        = _cfg['VAR_NAME']
FORMAT_MAP      = _cfg['FORMAT_MAP']
HIST_TABLE      = _cfg['HIST_TABLE']
HIST_CURRENT_VW = _cfg['HIST_CURRENT_VW']
CH_HIST_COLS    = tuple(_cfg['CH_HIST_COLS'])


def sql_cur_state(replica: str, schema: str, tbl: str) -> str:
    """SQL: последнее состояние поставки из export.er_extract_current_vw.

    Возвращает СЫРЫЕ значения, а не готовые SQL-литералы: окно следующей выгрузки
    считает next_window здесь же, в коде. Раньше его считала вью export.extract_current_vw
    (чужая, общая с xStream), и арифметику окна нельзя было ни увидеть в репозитории,
    ни поправить иначе как ALTER-ом на боевом кластере.

    Ключ — тройка (replica, schema_name, extract_name); группы в нём нет, поэтому одна
    таблица в двух группах запрещена настройкой (er_config.state_conflicts).
    """
    return (f"SELECT * FROM {HIST_CURRENT_VW} "
            f"WHERE replica = '{_sql_str(replica)}' "
            f"AND schema_name = '{_sql_str(schema)}' "
            f"AND extract_name = '{_sql_str(tbl)}'")


def _parse_ts(value) -> datetime:
    """Момент времени из настройки или из ClickHouse → наивный datetime в UTC.

    lower_bound в er_wf_meta пишут и датой ('2026-01-01'), и датой со временем, и через
    'T' — принимаем всё три вида: значение это человеческое, а падать разбором файла
    из-за пропущенных часов не за что.
    """
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    s = str(value).strip().replace('T', ' ')
    for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise AirflowFailException(
        f"Не разобрал момент времени '{value}'. Форматы: 'ГГГГ-ММ-ДД' или 'ГГГГ-ММ-ДД ЧЧ:ММ:СС'"
    )


def next_window(reached: datetime, increment: int, overlap: int,
                now: datetime | None = None) -> tuple[datetime, datetime, bool]:
    """🪟 Окно следующей выгрузки: (time_from, time_to, догнали ли текущее время).

    Повторяет арифметику прежней вью export.extract_current_vw один в один:

        time_from  = reached - overlap        (СЕКУНДЫ, нахлёст назад под задержки CDC)
        time_to    = min(reached + increment, now)   (increment — МИНУТЫ)
        is_current = reached + increment >= now

    Единицы разные и такими остаются: ровно так они заданы в er_wf_meta и в форме
    запуска, и ровно так их считала вью (toIntervalSecond / toIntervalMinute).

    increment и overlap берутся из НАСТРОЙКИ, а не из строки состояния. Прежде их брала
    вью из истории, и правка шага в er_wf_meta ни на что не влияла, пока не пройдёт
    полный цикл, — а у поставок, заведённых bootstrap-ом, там и вовсе лежало значение
    в других единицах (минуты, умноженные на 60).

    Время наивное, в UTC: ровно в таком виде его хранит ClickHouse и печатает _ch_ts.
    """
    now = (now or datetime.now(timezone.utc)).replace(tzinfo=None)
    if reached.tzinfo is not None:
        reached = reached.astimezone(timezone.utc).replace(tzinfo=None)
    edge = reached + timedelta(minutes=int(increment))
    return reached - timedelta(seconds=int(overlap)), min(edge, now), edge >= now


# ── Helpers ───────────────────────────────────────────────────────────────────

def _fmt_val(v: Any) -> str:
    """None → 'null', иначе → SQL-строковый литерал в одинарных кавычках.

    Кавычки внутри значения удваиваются: сюда попадают поля состояния дельты, и хотя
    сейчас это даты и числа, литерал есть литерал — одиночная кавычка ломала бы запрос
    в месте, где причину пришлось бы искать по трассировке ClickHouse.
    """
    return 'null' if v is None else f"'{_sql_str(v)}'"


def _ch_ts(dt: datetime) -> str:
    """🕐 Момент времени так, как его принимает ClickHouse: 'ГГГГ-ММ-ДД ЧЧ:ММ:СС'.

    Намеренно не isoformat(): у tz-aware времени он даёт '2026-08-12T12:47:47+00:00',
    а смещение ClickHouse не разбирает вовсе — такую строку он отказывается и сравнивать
    с DateTime64 (TYPE_MISMATCH при построении WHERE), и вставлять в колонку этого типа
    (CANNOT_PARSE_TEXT в er_extract_history). Разделитель 'T' сам по себе допустим, ломает
    именно хвост со смещением, но пробел заодно совпадает с тем, что отдаёт toString()
    в состоянии дельты: export_time не должен выглядеть по-разному в зависимости от
    режима выгрузки, принимающая сторона парсит его одним форматом.

    Наивное время считаем UTC, а не локальным: ровно таким его отдаёт ClickHouse
    (состояние дельты), а astimezone на наивном значении подставил бы зону сервера —
    в бою это МСК, то есть окно уехало бы на три часа, причём молча.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def _pkg_ts(context) -> datetime:
    """🕐 Единая точка отсчёта пакета — метка времени из XCom таска make_ts.

    От неё строятся имена ВСЕХ файлов пакета: и архивов по каждой таблице, и общего тикета,
    а ЕР требует у них одинаковый ts. Поэтому значение обязано быть неизменным — его и
    считает один-единственный таск в начале рана, а все остальные только читают.

    Не logical_date, хотя она у рана неизменна: суффикс группы из имени тикета убран, а
    logical_date у групп одной реплики на общем cron совпадает до секунды — тикеты получили
    бы одно имя и затёрли друг друга в S3. Подробности — в docstring _er_make_ts.

    Пусто = падаем. Молчаливый откат на logical_date вернул бы ровно то совпадение имён,
    ради которого make_ts и заведён.
    """
    return datetime.fromisoformat(_xcom(context, '', 'make_ts', key='package_ts'))


def _enqueue_files(gcfg: dict, files: list[str], context) -> list[dict]:
    """📮 Ставит файлы пакета в очередь отправки, выдавая каждому свой RqUID.

    Отправкой занимается отдельный даг tfs_kafka_snd — он один видит все выгрузки сразу
    и потому только он может соблюдать лимиты маршрута. Пакетный даг лишь регистрирует
    намерение.

    RqUID сохраняется здесь же: по нему потом ищется обратная квитанция.
    Куда именно ложится очередь — ClickHouse, S3 или Postgres — решает STORAGE
    в plugins/tfs_utils.py; здесь это неважно.
    """
    import uuid

    package_ts = _pkg_ts(context).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    run_id     = getattr(context.get('dag_run'), 'run_id', '') or ''

    rows = [{
        'rq_uid':      uuid.uuid4().hex,
        'file_name':   f,
        # В очередь идёт ИМЯ ПАКЕТА (реплика + группа): тракт ТФС и пауза отправки видят
        # пакет именно так, и это ровно та строка, что стояла здесь до выноса группы
        # в свою колонку.
        'replica':     gcfg['pkg_name'],
        'scenario_id': gcfg['scenario'],
        'package_ts':  package_ts,
        'dag_id':      gcfg['dag_id'],
        'run_id':      run_id,
    } for f in files]

    _tfs().enqueue(rows)
    logger.info("📮 В очередь отправки поставлено %d файлов пакета %s", len(rows), gcfg['replica'])
    return rows


def _sql_str(s) -> str:
    """Экранирует одинарные кавычки для подстановки в ClickHouse-строковый литерал."""
    return str(s).replace("'", "''")


def _xcom(context, tg: str, task_id: str, key: str = 'return_value', required: bool = True):
    """📤 Тянет XCom таска внутри TaskGroup — id там составной: '<group>.<task>'.

    required=True превращает отсутствие значения в падение: молчаливый None выше по
    течению даёт не ошибку, а пустой пакет с тикетом на пустой список архивов.
    """
    full_id = f"{tg}.{task_id}" if tg else task_id
    val = context['ti'].xcom_pull(task_ids=full_id, key=key)
    if val is None and required:
        raise AirflowFailException(f"XCom '{full_id}' (key={key}) пуст — таск не отработал")
    return val


def _state_to_params(cur: dict, cfg: dict) -> dict:
    """Строка состояния из вью + настройка → словарь SQL-литералов для шаблонов.

    Окно считается здесь (next_window), а не читается готовым: вью отдаёт только
    достигнутое состояние (reached = верхняя граница прошлого окна). Шаг и нахлёст
    берутся из НАСТРОЙКИ поставки, а не из истории — правка increment в er_wf_meta
    должна применяться со следующего же рана.

    extract_time = reached: это «версия» состояния и ключ строки истории; новая верхняя
    граница уезжает в time_to. Так было и в общей таблице — ключ там (extract_name,
    extract_time), и по нему же argMax находит последнюю строку.
    """
    tf   = str(cur.get('time_field') or cfg.get('time_field') or '').strip("'")
    inc  = int(cfg.get('increment', 60) or 60)
    ovl  = int(cfg.get('overlap', 0) or 0)
    ec   = cur['extract_count']
    reached = cur['reached']
    time_from, time_to, is_current = next_window(reached, inc, ovl)
    return {
        'num_state':       str(cur.get('num_state', 0)),
        'extract_time':    f"'{_ch_ts(reached)}'",
        'extract_count':   'null' if ec is None else str(ec),
        'loaded':          _fmt_val(cur.get('loaded')) if ec is not None else 'null',
        'sent':            _fmt_val(cur.get('sent')) if ec is not None else 'null',
        'confirmed':       _fmt_val(cur.get('confirmed')) if ec is not None else 'null',
        'increment':       str(inc),
        'overlap':         str(ovl),
        'time_field':      f"'{tf}'",
        'time_from':       f"'{_ch_ts(time_from)}'",
        'time_to':         f"'{_ch_ts(time_to)}'",
        'condition':       f"'{_ch_ts(time_from)}' < {tf} and {tf} <= '{_ch_ts(time_to)}'",
        'is_current':      'True' if is_current else 'False',
        'recent_interval': '0',
        'mode':            "'delta'",
    }


def _pause_reason(gcfg: dict, context) -> str:
    """⏸️ Причина, по которой файлы этого пакета сейчас придержаны в очереди; пусто — едут.

    Строка собирается той же формы, что лежит в очереди отправки, — тогда правило паузы
    ищется общим кодом тракта (tfs_utils.pause_reason), и «пакет на паузе» здесь и у
    отправителя означает ровно одно и то же.
    """
    return _tfs().pause_reason({
        'scenario_id': gcfg['scenario'],
        # Именно pkg_name (реплика + группа), а НЕ голая реплика: ровно эта строка лежит
        # в очереди отправки, и по ней же считается ключ пакета. Возьми мы здесь replica —
        # правило паузы, поставленное на пакетный даг, отправитель бы видел, а ожидание
        # квитанций нет, и пакет досидел бы до таймаута.
        'replica':     gcfg['pkg_name'],
        'package_ts':  _pkg_ts(context).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
    })


def _pre_await(gcfg, context):
    """Пропускает ожидание подтверждения: auto_confirm, notify_kafka=0, пустой пакет или пауза.

    Вызывается первым делом в wait_confirm — так один таск заменяет
    EmptyOperator/ожидание switch.

    ⏸️ Пауза отправки: ждать квитанцию по файлу, который никуда не поедет, бессмысленно —
    час занятого слота в пуле выгрузок и красный ран в конце. Уходим в скип, а
    save_status при этом отработает с confirmed = null: дельта двигается, следующий цикл
    взводится, пакет уедет, когда паузу снимут. Цена — квитанции по нему никто не ждёт;
    ловит их сверка неподтверждённых в tfs_kafka_snd (stale_sent).
    """
    p = context['params']
    if p.get('auto_confirm', False):
        raise AirflowSkipException("Auto confirm enabled, skipping wait")
    if not p.get('notify_kafka', True):
        raise AirflowSkipException("Kafka notification disabled (notify_kafka=0)")
    summary_tkt = context['ti'].xcom_pull(task_ids="make_summary", key='summary_tkt_name')
    if not summary_tkt:
        raise AirflowSkipException("No data exported, skipping wait")

    if reason := _pause_reason(gcfg, context):
        add_note({"⏸️ Отправка на паузе": reason}, level='task,dag', context=context,
                 title='📨 TFS confirm')
        raise AirflowSkipException(f"Отправка на паузе, квитанций не ждём: {reason}")
# ── Tasks ───────────────────────────────────────────────────────────────────

class _ZipReader:
    """Адаптер stream_zip (генератор байт) → file-like object для S3 multipart upload.

    ⚠️ Ветка read(-1) собирает весь поток в память — это единственное место, где стриминг
    перестал бы быть стримингом. В нашем пути она не вызывается: load_file_obj отдаёт
    объект в upload_fileobj, а тот читает кусками фиксированного размера. Ветка оставлена
    ради полноты файлового интерфейса — уберёшь её, и адаптер перестанет быть file-like
    для любого другого потребителя.
    """
    def __init__(self, g): self._g, self._b = g, bytearray()
    def read(self, n=-1):
        if n < 0:
            self._b.extend(b''.join(self._g))
            d, self._b = bytes(self._b), bytearray()
            return d
        while len(self._b) < n:
            try: self._b.extend(next(self._g))
            except StopIteration: break
        chunk, self._b = bytes(self._b[:n]), self._b[n:]
        return chunk


@task(task_id='init')
def _er_init(cfg, **context):
    """⚙️ Инициализирует состояние выгрузки и возвращает словарь SQL-литералов для шаблонов.

    Delta-режим: читает export.extract_current_vw; при первом запуске создаёт bootstrap-состояние
    с time_from/time_to = lower_bound.
    Recent-режим: вычисляет окно [now() - recent_interval, now()] без обращения к CH.
    Период: date_from/date_to из DAG Params перебивают состояние и помечают ран как ad_hoc.

    Возвращаемый словарь (XCom "return_value") используется всеми downstream-тасками
    через _xcom(context, cfg['tg'], 'init') — внутри TaskGroup id составной.

    Снятый флаг `tbl_<tg>` в форме запуска скипает всю TaskGroup таблицы: downstream
    с all_success уходит в skipped следом, XCom не появляется, а save_status
    и schedule_next таблицу без XCom пропускают — состояние дельты остаётся на месте.
    """
    from airflow.exceptions import AirflowSkipException
    from airflow.hooks.base import BaseHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # 🚦 Флаги таблиц. Все снятые — это не «пропустить всё», а ошибка запуска: пустой
    # пакет означал бы тикет без единого файла, чего ТФС от нас не ждёт. Исключение —
    # пакет, у которого ВСЕ поставки приостановлены настройкой (is_paused): там снятые
    # флаги не ошибка человека, а само это состояние.
    flags = {k: v for k, v in context['params'].items() if k.startswith('tbl_')}
    if flags and not any(flags.values()) and not cfg.get('all_paused'):
        raise AirflowFailException(
            "Сняты флаги всех таблиц пакета — грузить нечего. "
            "Отметьте хотя бы одну таблицу либо не запускайте пакет"
        )
    if not context['params'].get(f"tbl_{cfg['tg']}", True):
        why = ("поставка приостановлена настройкой (is_paused=1)" if cfg.get('paused')
               else "флаг таблицы снят — поставка пропущена")
        add_note({f"⏭️ {cfg['schema_name']}.{cfg['tbl']}": why}, level='task', context=context)
        raise AirflowSkipException(f"{cfg['schema_name']}.{cfg['tbl']}: {why}")

    # 🔌 Коннекты проверяем САМИ, до первого обращения. Провайдер amazon на отсутствующий
    # conn_id не падает: пишет «Unable to find AWS Connection ID …, switching to empty»
    # и переключается на дефолтную стратегию boto3 — то есть уходит в настоящий AWS
    # и валится там NoCredentialsError из глубины botocore, где ни conn_id, ни стенда
    # уже не видно. AirflowFailException, а не обычная ошибка: отсутствующий коннект
    # за четыре ретрая сам не появится, а это 20 минут ожидания на ровном месте.
    missing = []
    for conn_id in (S3_CONN, CH_ID):
        try:
            BaseHook.get_connection(conn_id)
        except Exception:
            missing.append(conn_id)
    if missing:
        raise AirflowFailException(
            f"Не найдены Airflow connection: {', '.join(missing)}. "
            f"Заведите их на стенде — выгрузка ходит в S3 '{S3_CONN}' и ClickHouse '{CH_ID}'"
        )

    s3 = S3Hook(aws_conn_id=S3_CONN)
    # 🪣 Бакет заводится заранее, вместе с коннектом, и на лету НЕ создаётся. Отсутствие
    # бакета означает ошибку в настройке контура — не тот стенд, не тот endpoint, опечатка
    # в имени. Молча созданный бакет такую ошибку прячет: выгрузка отработает «успешно»,
    # файлы лягут в пустоту, которую ТФС не читает, и обнаружится это только по
    # неприходящим квитанциям.
    if not s3.check_for_bucket(bucket_name=BUCKET):
        raise AirflowFailException(
            f"Бакет '{BUCKET}' не найден в S3 '{S3_CONN}'. Он должен существовать заранее: "
            f"проверьте endpoint коннекта и имя бакета в er_config.py"
        )
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)

    # `or ''`, а не значение по умолчанию у get: пустая строка приезжает в cfg как None.
    # DAG собран с render_template_as_native_obj=True, а Jinja в native-режиме отдаёт None
    # для шаблона без единого узла — то есть ровно для ''. Без этого time_field таблицы
    # с full_export уходил в историю строкой 'None'. Пустым он бывает только там:
    # у дельты его отсутствие не пропускает check_table.
    tf  = cfg.get('time_field') or ''
    lb  = cfg.get('lower_bound') or '1970-01-01 00:00:00'

    # Параметры, общие для delta и recent: передаются оператору экспорта и сохраняются в историю
    reg = {
        'lower_bound':        f"'{lb}'",
        'selfrun_timeout':    str(cfg.get('selfrun_timeout', 10)),
        'max_file_size':      cfg.get('max_file_size', ''),
        'pg_array_format':    cfg.get('pg_array_format', 'False'),
        'format_params':      cfg.get('format_params', ''),
        'xstream_sanitize':   cfg.get('xstream_sanitize', 'False'),
        'sanitize_array':     cfg.get('sanitize_array', 'False'),
        'sanitize_list':      cfg.get('sanitize_list', ''),
        'increment':          str(cfg.get('increment', 60)),
        'overlap':            str(cfg.get('overlap', 0)),
        'time_field':         f"'{tf}'",
    }

    if cfg.get('full_export'):
        # 📚 Полная выгрузка: окна нет, состояние дельты не ведём. Применимо и к таблицам
        # без поля времени — condition в SQL не подставляется, а '1=1' лежит здесь на
        # случай, если {condition} всё-таки написан в sql_where руками.
        now_s = _ch_ts(datetime.now(timezone.utc))
        reg.update({
            'extract_time':    f"'{now_s}'",
            'extract_count':   'null',
            'loaded':          'null',
            'sent':            'null',
            'confirmed':       'null',
            # Границы окна условны и нужны только истории: выгружено всё, что было на now.
            'time_from':       f"'{lb}'",
            'time_to':         f"'{now_s}'",
            'condition':       '1=1',
            # Всегда актуально: догонять нечего, следующий ран придёт по расписанию,
            # а schedule_next при is_current=True цикл не взводит.
            'is_current':      'True',
            'recent_interval': '0',
            'num_state':       '0',
            'mode':            "'full'",
        })
        result = reg
        logger.info("📚 %s.%s: полная выгрузка, окно дельты не применяется", cfg['schema_name'], cfg['tbl'])
    elif cfg['sql_get_current']:
        cur_res = get_dict_from_ch(hook, cfg['sql_get_current'])
        if not cur_res:
            # 🌱 Первый запуск: достигнутым состоянием считаем lower_bound, дальше окно
            # считается общей формулой. Первое окно поэтому не пустое, как было раньше:
            # прежний bootstrap записывал time_from = time_to = lower_bound, то есть
            # первый ран новой поставки уходил впустую, только чтобы завести строку.
            logger.warning("First execution for %s. Bootstrapping from lower_bound=%s.", cfg['tbl'], lb)
            state = {
                'num_state': 0, 'reached': _parse_ts(lb),
                'extract_count': None, 'loaded': None, 'sent': None, 'confirmed': None,
                'time_field': tf,
            }
            result = {**reg, **_state_to_params(state, cfg)}
        else:
            result = {**reg, **_state_to_params(cur_res[0], cfg)}
    else:
        ri  = int(cfg.get('recent_interval', 60))
        now = datetime.now(timezone.utc).replace(microsecond=0)
        t0  = now - timedelta(minutes=ri)
        now_s, t0_s = _ch_ts(now), _ch_ts(t0)
        reg.update({
            'extract_time':    f"'{now_s}'",
            'extract_count':   'null',
            'loaded':          'null',
            'sent':            'null',
            'confirmed':       'null',
            'time_from':       f"'{t0_s}'",
            'time_to':         f"'{now_s}'",
            'condition':       f"'{t0_s}' < {tf} and {tf} <= '{now_s}'",
            'is_current':      'True',
            'recent_interval': str(ri),
            'num_state':       '0',
            'mode':            "'recent'",
        })
        result = reg

    # Переопределения из DAG Params (ручной запуск): применяются поверх состояния дельты
    p = context['params']
    key_map = {
        'selfrun_timeout': str,
        'strategy':        str,
        'max_file_size':   str,
        'notify_kafka':    lambda v: 'True' if v else 'False',
        'auto_confirm':    lambda v: 'True' if v else 'False',
    }
    for key, transform in key_map.items():
        if p.get(key) not in (None, '', 'None'):
            result[key] = transform(p[key])

    # is_current — галка, а не тристейт: снятая означает «не переопределять», поэтому
    # False здесь ничего не делает, иначе каждый ран форсил бы неактуальное состояние
    # и бесконечно взводил следующий цикл.
    if p.get('is_current'):
        result['is_current'] = 'True'

    # 📅 Ручная выгрузка за период. Задаётся на весь пакет и перебивает состояние дельты.
    date_from = str(p.get('date_from') or '').strip()
    date_to   = str(p.get('date_to') or '').strip()
    if date_from or date_to:
        if not (date_from and date_to):
            raise AirflowFailException(
                "Для выгрузки за период нужны обе даты: date_from и date_to. "
                "Одной границы недостаточно — окно задаётся парой"
            )
        if date_to <= date_from:
            raise AirflowFailException(f"date_to ({date_to}) должна быть больше date_from ({date_from})")

        result.update({
            'extract_time': f"'{date_to}'",
            'time_from':    f"'{date_from}'",
            'time_to':      f"'{date_to}'",
            'condition':    f"'{date_from}' < {tf} and {tf} <= '{date_to}'",
            # Состояние дельты не двигаем: save_status пропустит запись в историю,
            # а is_current=True не даст schedule_next запустить следующий цикл. Иначе разовая
            # доливка за прошлый месяц отбросила бы регулярный поток назад.
            'is_current':   'True',
            'ad_hoc':       'True',
            'mode':         "'ad_hoc'",
        })
        logger.info("📅 Разовая выгрузка за период %s .. %s, состояние дельты не сохраняется", date_from, date_to)

    # extract_time, condition и increment перечислены явно: из формы они больше не правятся
    # (у каждой таблицы своё состояние), но в заметке нужны — смотрят именно на них.
    shown = list(key_map) + ['is_current', 'extract_time', 'condition', 'increment',
                             'time_from', 'time_to', 'ad_hoc']
    note = {k: result.get(k) for k in shown}

    # 🐢 Сколько ранов осталось до текущего времени. Окно двигается на increment минут за
    # ран (schedule_next взводит следующий сам), поэтому отставание в неделю — это сотни
    # циклов. Человеку это надо видеть сразу: лечится либо разовой выгрузкой за период,
    # либо временно увеличенным increment, но никак не ожиданием.
    if str(result.get('is_current')).lower() != 'true':
        behind = (datetime.now(timezone.utc).replace(tzinfo=None)
                  - _parse_ts(str(result['time_to']).strip("'")))
        note['отставание'] = f"{behind.days} дн {behind.seconds // 3600} ч"
        note['циклов до текущего времени'] = (
            int(behind.total_seconds() // 60 // max(int(result['increment']), 1)) + 1)

    add_note(note, level='task', context=context,
             title=f"⚙️ Delta State · {cfg['schema_name']}.{cfg['tbl']}")
    return result


@task(task_id='build_meta')
def _er_build_meta(cfg, **context):
    """🗂️ Строит .meta JSON с описанием структуры таблицы для ЕР/TFS.

    Порядок колонок: export_time (PRE) + data_cols + ctl_action, ctl_validfrom (SUF).

    Состав data_cols — два DESCRIBE:
      • имена и типы — `DESCRIBE (<итоговый запрос>)`, ровно то, что TSVWithNames
        пишет в заголовок CSV (учитывает JOIN, алиасы, вычисляемые выражения);
      • description — `DESCRIBE TABLE <источник>`, подмешивается по имени колонки,
        так как у результата подзапроса комментариев нет.
    Если запрос задан строкой или со своим списком fields, cfg['sql_meta'] пуст —
    состав берётся только по DESCRIBE TABLE источника.

    Типы: parse_ch_type → TYPE_MAP; для FixedString/Decimal извлекаются
    length/precision/scale. UK передаётся плоским массивом: ['id'] (стандарт ЕР).

    Сама сборка — в er_config: тем же кодом проверяет настройку даг export_er_setup.
    """
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
    dp = _xcom(context, cfg['tg'], 'init')
    hook = ClickHouseHook(clickhouse_conn_id=CH_ID)
    # 💬 Описания колонок берём у таблиц, к которым обращается САМ ЗАПРОС: from → with →
    # joins. Имя выгрузки для этого не годится — из одной таблицы делают несколько выгрузок
    # с разными джойнами, и таблицы с именем выгрузки может не быть вовсе.
    sources        = sql_sources(cfg.get('sql_parts') or {})
    ch_cols, found = ch_source_columns(hook, sources)
    if found:
        logger.info("💬 Описания колонок из %s (описано %d из %d)",
                    ", ".join(f"{db}.{t}" for db, t in found),
                    sum(1 for c in ch_cols.values() if c['description']), len(ch_cols))
    else:
        logger.warning("⚠️ Ни одной таблицы-источника не нашлось (кандидаты: %s) — "
                       ".meta соберётся без описаний колонок",
                       ", ".join(f"{db}.{t}" for db, t in sources) or "нет")

    def _cols_from_query(sql: str) -> list[dict]:
        """Колонки по DESCRIBE итогового запроса — так же, как их видит TSVWithNames.

        DESCRIBE запрос не выполняет, но разобрать его обязан, поэтому окно дельты
        и время подставляются заведомо безопасными значениями (probe_sql), а внешний
        LIMIT 0 срезается: DESCRIBE его не примет.
        """
        q = sql.replace('{export_time}', 'now64(6)').replace('{condition}', '1=0')
        qrows, _ = hook.execute(f"DESCRIBE ({q})", with_column_types=True)
        return query_columns(qrows, ch_cols)

    def _cols_from_table() -> list[dict]:
        """Запасной путь: колонки по первой таблице-источнику, без учёта JOIN и выражений.

        Не нашлось ни одной — брать состав неоткуда, и честнее упасть с внятным текстом,
        чем отдать в КАП .meta, собранный неизвестно из чего.
        """
        if not found:
            raise AirflowFailException(
                f"{cfg['schema_name']}.{cfg['tbl']}: состав колонок .meta взять неоткуда — "
                f"DESCRIBE по запросу не удался, а таблиц-источников не нашлось "
                f"(кандидаты: {', '.join(f'{db}.{t}' for db, t in sources) or 'нет'}). "
                f"Проверьте запрос дагом export_er_setup"
            )
        # Только первая таблица: у запасного пути состав колонок берётся из ОДНОГО
        # источника, иначе в .meta уехали бы колонки, которых в выгрузке нет.
        first, _ = ch_source_columns(hook, found[:1])
        # Псевдострок DESCRIBE достаточно: cols_from_fields смотрит в них только имя.
        return cols_from_fields(cfg.get('fields', ['*']), first, [(name,) for name in first])

    sql_meta = cfg.get('sql_meta')
    if sql_meta:
        try:
            data_cols = _cols_from_query(sql_meta)
        except Exception as err:
            # Схема из DESCRIBE TABLE лучше, чем упавший DAG, но она может разойтись с CSV
            logger.warning("DESCRIBE по запросу не удался, откат на DESCRIBE TABLE: %s", err)
            add_note(f"DESCRIBE по запросу не удался, схема взята по DESCRIBE TABLE {cfg['schema_name']}.{cfg['tbl']}\n\n{err}",
                     level='task', context=context, title='⚠️ build_meta fallback')
            data_cols = _cols_from_table()
    else:
        data_cols = _cols_from_table()

    # 🔍 Состав колонок обязан совпадать с настройкой: новая колонка в источнике не должна
    # доезжать до КАП сама, только через правку fields. Выгрузке нужен красный таск —
    # ретраить тут нечего, от повтора настройка не изменится.
    key = f"{cfg['schema_name']}.{cfg['tbl']}"
    names = [c['column_name'] for c in data_cols]
    # Обе сверки — про одно: настройка разошлась с тем, что уедет. Ретраить нечего,
    # поэтому ошибки собираются вместе и падают одним внятным сообщением.
    if errors := check_fields(cfg['fields'], names, key) + \
            check_descriptions(cfg.get('descriptions'), names, key):
        raise AirflowFailException("\n".join(errors))

    if unnamed := unnamed_fields(cfg['fields']):
        logger.warning(
            "Выражения без алиаса, имена колонок для них не проверены: %s. "
            "Добавьте 'as <имя>', чтобы сверка работала полностью", unnamed,
        )

    meta = build_meta(cfg, data_cols, strategy=dp.get('strategy', ''))
    context["ti"].xcom_push(key="meta_json", value=json.dumps(meta, ensure_ascii=False))
    add_note({f"🗂️ build_meta · {cfg['tbl']}": [c["column_name"] for c in meta["columns"]]},
             level='task', context=context)


@task(task_id='make_ts')
def _er_make_ts(gcfg, **context):
    """🕐 Выдаёт пакету метку времени — от неё строятся имена ВСЕХ его файлов.

    Раньше меткой служила logical_date рана, и этого хватало, пока суффикс группы стоял
    в имени тикета. Теперь тикет называется `[базовая реплика]__[ts].tkt`, а logical_date
    у групп одной реплики на общем cron совпадает до секунды — два пакета получили бы
    один тикет и затёрли друг друга в S3 (replace=True).

    Развести их и обязан этот таск. Он сидит в пуле ts_pool(replica) — одном на всю
    базовую реплику и ровно на ОДИН слот, — так что группы проходят его по очереди,
    а секундная пауза в конце гарантирует, что следующая возьмёт другое значение now():
    слот освобождается только по концу таска.

    Порядок «сначала запомнить, потом поспать» не декоративный: метка обязана быть той,
    что взята на входе, иначе пауза не даёт ничего.

    Метка живёт в XCom, поэтому clear одной таблицы или make_summary имена не меняет.
    А вот clear всего рана перезапустит и этот таск: пакет поедет с новой меткой, а архивы
    прошлой попытки останутся в S3 сиротами — раньше они перезаписывались.
    """
    import time

    ts = datetime.now(timezone.utc)
    context['ti'].xcom_push(key='package_ts', value=ts.isoformat())
    logger.info("🕐 Метка времени пакета %s: %s", gcfg['replica'], ts.strftime('%Y%m%d%H%M%S'))
    add_note(f"🕐 метка времени пакета: {ts.strftime('%Y%m%d%H%M%S')}",
             level='task,dag', context=context)

    # Пауза под удерживаемым слотом пула: соседняя группа стартует не раньше чем через
    # секунду и получит другую метку. Секунды хватает — в имени файла она младший разряд.
    time.sleep(1)


@task(task_id='pack_zip')
def _er_pack_zip(cfg, **context):
    """📦 Упаковывает выгруженные файлы одной таблицы в ZIP-архивы формата ЕР.

    Каждый файл данных оборачивается в отдельный ZIP (стриминг, без буферизации в памяти):
      [база]__[ts].tkt      — `filename;rowcount` (TKT внутри архива, стандарт ЕР)
      [schema]__[table]__[ts].meta        — JSON-схема колонок
      [schema]__[table]__[ts].csv|.json   — данные из S3, расширение по формату выгрузки

    Сам архив — [база]__[ts]__[группа]__[table]__[часть]_[всего]_[строк].zip. Базовая
    реплика первой, суффикс группы ЗА меткой времени; в тикете суффикса нет вовсе,
    его пакеты разводит по именам сама метка (см. _er_make_ts).

    Имена файлов — строго нижний регистр, расширение архива .zip (стандарт ЕР).
    После упаковки исходные файлы удаляются из S3.

    Общий тикет пакета здесь НЕ пишется: архивов в пакете много, тикет один, и собирает
    его make_summary после того, как отработают все таблицы группы.
    """
    from stat import S_IFREG
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from stream_zip import ZIP_32, stream_zip # type: ignore

    ti  = context["ti"]
    tg  = cfg['tg']
    ext = FORMAT_MAP[cfg['format']]['ext']

    # Куда класть архивы: параметр запуска перебивает штатный путь ТФС. Читать части
    # выгрузки надо ОТТУДА ЖЕ, куда их положил оператор, — он получил тот же параметр
    # через шаблон, поэтому цель одна на весь пакет.
    dst = parse_s3_target(context['params'].get('export_path'), S3_CONN, BUCKET, cfg['s3_prefix'])

    # s3_key_list пуст при нулевой дельте — это штатно. А вот row_count_list при непустом
    # списке ключей обязан быть: без него zip() молча даст TypeError вместо внятной ошибки.
    s3_keys = _xcom(context, tg, 'export_to_s3', key='s3_key_list',   required=False)
    counts  = _xcom(context, tg, 'export_to_s3', key='row_count_list', required=bool(s3_keys))
    meta_s  = _xcom(context, tg, 'build_meta',   key='meta_json')

    base_ts = _pkg_ts(context)
    ts      = base_ts.strftime("%Y%m%d%H%M%S")
    mtime   = base_ts.replace(tzinfo=None)

    if not s3_keys:
        # Только из настройки таблицы: параметра формы у send_empty нет намеренно —
        # у пакета много таблиц, и одно значение в UI перебило бы настройку каждой.
        if not cfg['send_empty']:
            ti.xcom_push(key="zip_name_list",   value=[])
            ti.xcom_push(key="total_row_count", value=0)
            add_note({f"📦 pack_zip · {cfg['tbl']}": "пусто, send_empty=0 — архив не создан"},
                     level='task', context=context)
            return

        # Пустой пакет по требованию ТФС. У TSV это файл с одной строкой заголовка,
        # у NDJSON заголовка нет вовсе — пустой файл нулевой длины.
        meta_obj = json.loads(meta_s)
        body     = ("\t".join(c["column_name"] for c in meta_obj["columns"]) + "\n").encode() \
                   if FORMAT_MAP[cfg['format']]['header'] else b""
        data_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__0_1_0.{ext}".lower()
        meta_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__0_1_0.meta".lower()
        tkt_n  = f"{cfg['replica']}__{ts}.tkt".lower()
        zip_n  = (f"{cfg['replica']}__{ts}__{cfg['dag_group']}__"
                  f"{cfg['tbl']}__0_1_0.zip").lower()
        members = [
            (tkt_n,  mtime, S_IFREG | 0o600, ZIP_32, [f"{data_n};0".encode()]),
            (meta_n, mtime, S_IFREG | 0o600, ZIP_32, [meta_s.encode()]),
            (data_n, mtime, S_IFREG | 0o600, ZIP_32, [body]),
        ]
        hook_e = S3Hook(aws_conn_id=dst['conn_id'])
        hook_e.load_file_obj(_ZipReader(stream_zip(members)), key=f"{dst['key_prefix']}{zip_n}",
                             bucket_name=dst['bucket'], replace=True)
        ti.xcom_push(key="zip_name_list",   value=[zip_n])
        ti.xcom_push(key="total_row_count", value=0)
        add_note({f"📦 pack_zip · {cfg['tbl']} (empty)": [zip_n]}, title="rows=0 send_empty=True",
                 level='task', context=context)
        return

    hook, total = S3Hook(aws_conn_id=dst['conn_id']), len(s3_keys)
    uploaded = []

    for i, (key, rows) in enumerate(zip(s3_keys, counts)):
        data_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__{i+1}_{total}_{rows}.{ext}".lower()
        meta_n = f"{cfg['schema_name']}__{cfg['tbl']}__{ts}__{i+1}_{total}_{rows}.meta".lower()
        tkt_n  = f"{cfg['replica']}__{ts}.tkt".lower()
        zip_n  = (f"{cfg['replica']}__{ts}__{cfg['dag_group']}__"
                  f"{cfg['tbl']}__{i+1}_{total}_{rows}.zip").lower()

        s3_body = hook.get_key(key=key, bucket_name=dst['bucket']).get()["Body"]
        members = [
            (tkt_n,  mtime, S_IFREG | 0o600, ZIP_32, [f"{data_n};{rows}".encode()]),
            (meta_n, mtime, S_IFREG | 0o600, ZIP_32, [meta_s.encode()]),
            (data_n, mtime, S_IFREG | 0o600, ZIP_32, s3_body.iter_chunks(chunk_size=8*1024*1024)),
        ]
        hook.load_file_obj(_ZipReader(stream_zip(members)), key=f"{dst['key_prefix']}{zip_n}",
                           bucket_name=dst['bucket'], replace=True)
        hook.delete_objects(bucket=dst['bucket'], keys=[key])
        uploaded.append(zip_n)

    total_rows = sum(int(r) for r in counts)
    ti.xcom_push(key="zip_name_list",   value=uploaded)
    ti.xcom_push(key="total_row_count", value=total_rows)
    add_note({f"📦 pack_zip · {cfg['tbl']}": uploaded}, title=f"rows={total_rows} files={total}",
             level='task', context=context)


@task(task_id='make_summary', trigger_rule='none_failed')
def _er_make_summary(gcfg, **context):
    """🧾 Собирает общий тикет пакета — один .tkt на все архивы группы.

    Тикет `[база]__[ts].tkt` перечисляет ZIP-файлы всех таблиц пакета, по имени в строке.
    Суффикса группы в его имени нет (требование ЕР), поэтому уникальность держится
    на метке времени: её выдаёт make_ts, по одному таску за раз на базовую реплику.
    Внутри группы двум пакетам не дают родиться одновременно ещё и max_active_runs=1.

    trigger_rule=none_failed: падение любой таблицы блокирует пакет целиком — тикет
    на неполный список архивов ушёл бы в ЕР как полноценная поставка.
    """
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook

    ti = context["ti"]
    zips: list[str] = []
    for tg in gcfg['groups']:
        zips.extend(_xcom(context, tg, 'pack_zip', key='zip_name_list', required=False) or [])

    if not zips:
        # Пусто во всех таблицах и send_empty=0: уведомлять ЕР не о чем.
        ti.xcom_push(key="summary_tkt_name", value="")
        ti.xcom_push(key="zip_name_list",    value=[])
        add_note("пакет пуст — тикет не создан, уведомление пропущено",
                 level='task,dag', context=context, title='🧾 make_summary')
        return

    ts          = _pkg_ts(context).strftime("%Y%m%d%H%M%S")
    summary_tkt = f"{gcfg['replica']}__{ts}.tkt".lower()
    dst = parse_s3_target(context['params'].get('export_path'), S3_CONN, BUCKET, gcfg['s3_prefix'])
    S3Hook(aws_conn_id=dst['conn_id']).load_bytes(
        "\n".join(zips).encode(), key=f"{dst['key_prefix']}{summary_tkt}",
        bucket_name=dst['bucket'], replace=True,
    )
    if context['params'].get('export_path'):
        # Не выключаем отправку и не трогаем дельту (осознанное решение), но предупреждаем:
        # маршрут ТФС ждёт файлы строго по своему префиксу в своём бакете, и уведомление
        # о файлах, лежащих в другом месте, он не подтвердит.
        logger.warning("⚠️ Пакет выгружен в %s://%s/%s — не туда, где его ждёт ТФС (%s://%s/%s)",
                       dst['conn_id'], dst['bucket'], dst['prefix'], S3_CONN, BUCKET, gcfg['s3_prefix'])

    # 📮 Ставим файлы в очередь отправки. RqUID генерируется ЗДЕСЬ и сохраняется: именно
    # по нему потом ищется обратная квитанция. Раньше он рождался внутри produce_msg
    # и умирал там же, поэтому сопоставить отправку с квитанцией было нечем.
    files = [summary_tkt] + zips
    queued = _enqueue_files(gcfg, files, context) if context['params'].get('notify_kafka', True) else []
    if not queued:
        logger.info("📭 notify_kafka=0 — файлы в очередь отправки не ставим")

    ti.xcom_push(key="summary_tkt_name", value=summary_tkt)
    ti.xcom_push(key="zip_name_list",    value=zips)
    ti.xcom_push(key="rq_uids",          value=[q['rq_uid'] for q in queued])
    add_note({f"🧾 {summary_tkt}": zips}, title=f"архивов в пакете: {len(zips)}, в очереди: {len(queued)}",
             level='task,dag', context=context)


@task(task_id='wait_confirm', trigger_rule='none_failed')
def _er_wait_confirm(gcfg, **context):
    """⏳ Ждёт обратные квитанции ТФС по всем файлам пакета.

    Kafka здесь больше не читается. Топик квитанций общий на все маршруты ТФС, и прежнее
    прямое чтение брало ЛЮБОЕ сообщение: пакет подтверждался чужой квитанцией, настоящий
    адресат её терял, а StatusCode не проверялся вовсе. Теперь топик вычитывает один
    даг tfs_kafka_rcv, а мы ждём появления СВОИХ строк по своим RqUID.

    Любой status_code != 0 роняет таск сразу, не дожидаясь остальных файлов: пакет уже
    не доедет целиком, ждать бессмысленно.

    ⏸️ Паузу отправки смотрим ДВАЖДЫ и оба раза вне цикла: на входе (ждать нечего, файлы
    никуда не поедут) и по истечении таймаута (тогда это скип, а не падение). Внутри цикла
    не смотрим намеренно: пауза, поставленная и снятая за то же окно, пакета не касается —
    файлы уедут, квитанции придут, — а проверка в цикле погасила бы такой пакет на ровном
    месте. Заодно это два чтения Variable на таск вместо сотен.
    """
    import time

    _pre_await(gcfg, context)  # auto_confirm / notify_kafka / нет данных / пауза → skip

    rq_uids = context['ti'].xcom_pull(task_ids="make_summary", key='rq_uids') or []
    if not rq_uids:
        raise AirflowFailException(
            "В XCom make_summary нет rq_uids — файлы не встали в очередь отправки"
        )

    # Из params, а не из gcfg: значение в форме запуска должно работать. Потолок ему всё
    # равно ставит execution_timeout, посчитанный при разборе файла.
    timeout = int(context['params'].get('confirm_timeout') or gcfg['confirm_timeout'])
    deadline = time.time() + timeout * 60

    while True:
        got = _tfs().find_receipts(rq_uids)

        bad = [r for r in got if r['status_code'] != 0]
        if bad:
            def _why(r):
                """Код плюс описание из StatusDesc: без него причину пришлось бы искать
                в raw_xml, а ТФС кладёт туда внятный текст (до 1000 символов)."""
                desc = (r.get('status_desc') or '').strip()
                return f"{r['file_name']}: StatusCode={r['status_code']}" + (f" — {desc}" if desc else "")

            add_note({"❌ Квитанции с ошибкой": [_why(r) for r in bad]},
                     level='task,dag', context=context, title='📨 TFS confirm')
            raise AirflowFailException(
                "ТФС отверг файлы пакета:\n" + "\n".join(f"  • {_why(r)}" for r in bad)
            )

        # Сравниваем ПО RqUID, а не по числу строк: в одной квитанции ТФС может прислать
        # несколько File, и тогда строк придёт больше, чем отправленных файлов.
        if not set(rq_uids) - {r['rq_uid'] for r in got}:
            add_note({"✅ Квитанции получены": [f"{r['file_name']} @ {r['rq_tm']}" for r in got]},
                     level='task,dag', context=context, title='📨 TFS confirm')
            return datetime.now(timezone.utc).isoformat()

        if time.time() >= deadline:
            break
        time.sleep(10)

    # Таймаут. Сначала пауза: если её поставили, пока мы ждали, это не авария, а решение
    # человека — уходим в скип, как если бы она стояла на входе.
    if reason := _pause_reason(gcfg, context):
        add_note({"⏸️ Отправка на паузе": f"{reason}; квитанций нет, ждать перестали"},
                 level='task,dag', context=context, title='📨 TFS confirm')
        raise AirflowSkipException(
            f"Квитанций нет за {timeout} мин, но отправка на паузе: {reason}"
        )

    # Дальше — настоящий таймаут. Различаем два диагноза: очередь стоит или ТФС молчит,
    # лечатся они по-разному.
    missing = list(set(rq_uids) - {r['rq_uid'] for r in got})
    queued  = _tfs().queue_state(missing)
    not_sent  = [r['file_name'] for r in queued if r['pending']]
    no_answer = [r['file_name'] for r in queued if not r['pending']]

    raise AirflowFailException(
        f"Квитанции ТФС не пришли за {timeout} мин.\n"
        + (f"  Ещё не отправлены (очередь стоит): {not_sent}\n" if not_sent else "")
        + (f"  Отправлены, ответа нет: {no_answer}\n" if no_answer else "")
        + "  Смотреть: хранилище тракта (STORAGE в plugins/tfs_utils.py), даги tfs_kafka_snd и tfs_kafka_rcv"
    )


@task(task_id='save_status', trigger_rule='none_failed')
def _er_save_status(gcfg, **context):
    """💾 Записывает результат по каждой таблице пакета в export.er_extract_history.

    Одна вставка на весь пакет: строк столько, сколько таблиц отработало. Выгрузка
    опознаётся тройкой (replica, schema_name, extract_name) — своими колонками, а не
    составным именем внутри одного поля, как это было в общей с xStream таблице
    export.extract_history (до 01.09.2026).

    Колонки перечисляет CH_HIST_COLS, а не этот код: строка состояния ДОПИСЫВАЕТСЯ при
    смене статуса (ReplacingMergeTree по ключу окна), и колонка, забытая во второй
    вставке, обнулилась бы — так уже терялись dag_id и run_id в er_sent_files.

    trigger_rule=none_failed: запускается при успехе или при skipped-тасках
    (wait_confirm пропускается при auto_confirm=True или отсутствии данных).
    confirmed — время квитанции ТФС из wait_confirm; null, если ждать не стали
    (auto_confirm=1) или пакет был пуст.
    extract_time берётся из XCom init как SQL-литерал (уже в кавычках) и означает
    ДОСТИГНУТОЕ до этого рана состояние; новая верхняя граница уезжает в time_to —
    из неё следующий ран и посчитает своё окно.

    Разовая выгрузка за период (ad_hoc) состояние не двигает — иначе следующая штатная
    дельта оттолкнулась бы от вручную заданных границ.
    """
    from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

    # wait_confirm вернул время получения квитанций; при skip (auto_confirm=1) — None
    confirmed_at = context['ti'].xcom_pull(task_ids='wait_confirm')
    confirmed = f"toDateTime64('{confirmed_at[:23].replace('T', ' ')}', 3)" if confirmed_at else 'null'
    pkg_ts    = _pkg_ts(context).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    run_id    = _sql_str(str(context.get('run_id') or context['ti'].run_id))

    selects, noted, skipped = [], {}, []
    for tg, tbl in gcfg['tables'].items():
        dp = _xcom(context, tg, 'init', required=False)
        if not dp:
            continue
        if str(dp.get('ad_hoc')).lower() == 'true':
            skipped.append(tbl)
            continue

        rows = _xcom(context, tg, 'pack_zip', key='total_row_count', required=False) or 0
        zips = _xcom(context, tg, 'pack_zip', key='zip_name_list',   required=False) or []
        zip_arr = "[" + ", ".join(f"'{_sql_str(z)}'" for z in zips) + "]"

        # id таск-группы собран как '<schema>__<table>', а двойное подчёркивание внутри
        # частей ключа запрещено проверкой имён (check_name) — поэтому первое же '__'
        # делит его однозначно.
        schema = tg.partition('__')[0]

        # Значения по колонкам, а не позиционным списком: порядок задаёт CH_HIST_COLS,
        # и разъехаться им негде.
        vals = {
            'replica':         f"'{_sql_str(gcfg['replica'])}'",
            'schema_name':     f"'{_sql_str(schema)}'",
            'extract_name':    f"'{_sql_str(tbl)}'",
            'extract_time':    dp['extract_time'],
            'dag_group':       f"'{_sql_str(gcfg['dag_group'])}'",
            'extract_count':   str(rows),
            'loaded':          'now64(3)',
            'sent':            'now64(3)',
            'confirmed':       confirmed,
            'increment':       dp['increment'],
            'overlap':         dp['overlap'],
            'recent_interval': dp['recent_interval'],
            'time_field':      dp['time_field'],
            'time_from':       dp['time_from'],
            'time_to':         dp['time_to'],
            'exported_files':  zip_arr,
            'mode':            dp.get('mode', "'delta'"),
            'dag_id':          f"'{_sql_str(gcfg['dag_id'])}'",
            'run_id':          f"'{run_id}'",
            'package_ts':      f"toDateTime64('{pkg_ts}', 3)",
            'updated_at':      'now64(3)',
        }

        # Алиас у КАЖДОЙ колонки обязателен, хотя INSERT и подставляет их по позиции.
        # Неименованную константу ClickHouse называет текстом выражения, поэтому у таблицы
        # с нулём строк extract_count, overlap и recent_interval превращаются в три колонки
        # с именем '0', а у таблицы с данными таких только две. Ветки UNION ALL сверяются
        # по именам, и пакет из двух и более поставок падал с AMBIGUOUS_COLUMN_NAME
        # («Block structure mismatch»). На пакете из одной таблицы UNION ALL не возникает —
        # оттого и не всплывало.
        selects.append("SELECT " + ", ".join(f"{vals[c]} AS {c}" for c in CH_HIST_COLS))
        noted[tbl] = {"time_from": dp['time_from'], "time_to": dp['time_to'], "rows": rows, "zips": zips}

    if selects:
        ClickHouseHook(clickhouse_conn_id=CH_ID).execute(
            f"INSERT INTO {HIST_TABLE} ({', '.join(CH_HIST_COLS)}) "
            + ' UNION ALL '.join(selects)
        )

    if skipped:
        add_note(f"📅 разовая выгрузка за период — состояние не сохранено: {', '.join(skipped)}",
                 level='task,dag', context=context, title='💾 save_status')
    if noted:
        add_note({"💾 save_status": noted}, level='task,dag', context=context)


@task(task_id='schedule_next')
def _er_schedule_next(gcfg, **context):
    """⏭️ Запускает следующий цикл, если хотя бы одна таблица пакета отстаёт от текущего времени.

    Пакет ходит целиком, поэтому и решение об автозапуске одно на группу: догонять
    отставшую таблицу отдельным раном нельзя — тикет формируется на весь пакет.
    Запуск откладывается на selfrun_timeout минут, чтобы избежать гонки с источником.
    Для recent-режима и для выгрузки за период is_current всегда True — автозапуск не нужен.
    """
    from airflow.api.common.trigger_dag import trigger_dag

    behind = []
    for tg, tbl in gcfg['tables'].items():
        dp = _xcom(context, tg, 'init', required=False)
        if dp and str(dp.get('is_current')).lower() not in ('true', 't', '1'):
            behind.append(tbl)

    if not behind:
        add_note("✅ delta is current — next run not scheduled", level='task,dag', context=context)
        return

    # Из params, а не из gcfg: иначе поле «Selfrun timeout» в форме запуска ничего не меняет.
    delay = context['params'].get('selfrun_timeout') or gcfg['selfrun_timeout']
    next_run = datetime.now(timezone.utc) + timedelta(minutes=int(delay))
    trigger_dag(dag_id=gcfg['dag_id'], execution_date=next_run, conf={}, replace_microseconds=False)
    add_note(f"⏭️ next run scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC\n\nотстают: {', '.join(behind)}",
             level='task,dag', context=context)

# ── DAG Factory ───────────────────────────────────────────────────────────────

def _table_cfg(table_key: str, entry: dict, gcfg: dict) -> dict:
    """🗂️ Конфиг одной поставки внутри пакета.

    table_key — ключ поставки внутри группы, 'схема.имя_выгрузки': по ключу er_wf_meta
                (replica, dag_group, schema_name, extract_name) эта пара уникальна в пакете
    entry     — запись из Variable: schema, PK, UK, fields, params (уже разрешённые
                наследованием от строки-дефолта группы), необязательный is_paused и ровно
                один из sql_stmt_export_delta / sql_stmt_export_recent
    gcfg      — конфиг пакета: dag_id, replica, dag_group, s3_prefix

    Базы источника в конфиге нет с 28.08.2026: колонка db_name убрана, и таблицы-источники
    находятся по квалифицированным именам в самом запросе (sql_sources).
    """
    p = get_params(entry)
    schema, tbl = entry['schema'], table_key.split('.', 1)[-1]

    if p['format'] not in FORMAT_MAP:
        raise AirflowFailException(
            f"{table_key}: неизвестный формат '{p['format']}', допустимы {sorted(FORMAT_MAP)}"
        )

    # Сборка запроса — в er_config: тем же кодом собирает проверку даг export_er_setup.
    # Там же и запрет на '*' в fields: состав колонок задаётся только настройкой, иначе
    # новая колонка источника уедет в выгрузку и в КАП сама, без единой правки er_wf_meta.
    q = export_sql(entry, p, table_key)
    is_delta = q['sql_key'] == 'sql_stmt_export_delta'

    if p['format'] != 'TSVWithNames' and (p['pg_array_format'] or p['xstream_sanitize']):
        logger.warning(
            "%s: pg_array_format/xstream_sanitize не применяются к формату %s — "
            "orjson сериализует массивы и экранирует спецсимволы сам", table_key, p['format'],
        )

    return {
        # ── Идентификация ────────────────────────────────────────────────────
        # id TaskGroup, он же префикс XCom и хвост имени параметра формы 'tbl_<tg>'
        'tg':              f"{schema}__{tbl}",
        'tbl':             tbl,
        'schema_name':     schema,
        'replica':         gcfg['replica'],
        'dag_group':       gcfg['dag_group'],
        's3_prefix':       gcfg['s3_prefix'],
        # ⏸️ Приостановленная поставка: флаг в форме запуска снят по умолчанию, поэтому
        # таск создаётся и штатно скипается — но его можно включить галкой на один ран.
        'paused':          bool(entry.get('is_paused')),
        # Проставляется после сборки всех поставок (см. create_export_dag): init по нему
        # отличает «настройка приостановила весь пакет» от «человек снял все флаги».
        'all_paused':      False,
        # ── SQL ──────────────────────────────────────────────────────────────
        'sql_export':      q['sql_export'],
        # None → состояние дельты не читается: так работают recent и full_export
        'sql_get_current': (sql_cur_state(gcfg['replica'], schema, tbl)
                            if (is_delta and not p['full_export']) else None),
        'sql_meta':        q['sql_meta'],
        # Части запроса как есть — из них build_meta достаёт таблицы-источники, чтобы
        # взять описания колонок. Имя выгрузки для этого не годится: таблицы с таким
        # именем может не быть вовсе.
        'sql_parts':       entry.get(q['sql_key']) if isinstance(entry.get(q['sql_key']), dict) else {},
        # ── Схема ────────────────────────────────────────────────────────────
        'fields':          entry['fields'],
        'PK':              entry.get('PK', []),
        'UK':              entry.get('UK', []),
        'description':     entry.get('description', ''),
        # Описания колонок из настройки: перебивают комментарии источника в build_meta
        'descriptions':    p['descriptions'],
        # ── Параметры таблицы ────────────────────────────────────────────────
        'format':          p['format'],
        'strategy':        p['strategy'],
        'full_export':     bool(p['full_export']),
        'increment':       p['increment'],
        'selfrun_timeout': p['selfrun_timeout'],
        'lower_bound':     p['lower_bound'],
        'time_field':      p['time_field'],
        'overlap':         p['overlap'],
        'recent_interval': p['recent_interval'],
        'export_timeout':  p['export_timeout'],
        'max_file_size':    p['max_file_size'],
        'format_params':    p['csv_format_params'],
        'pg_array_format':  'True' if p['pg_array_format'] else 'False',
        'xstream_sanitize': 'True' if p['xstream_sanitize'] else 'False',
        'sanitize_array':   'True' if p['sanitize_array'] else 'False',
        'sanitize_list':    p['sanitize_list'],
        'send_empty':       bool(p['send_empty']),
    }


def _dag_params(gp: dict, tables: dict) -> dict:
    """🎛️ DAG Params пакета. tables — {tg: (имя выгрузки, приостановлена ли)}.

    Состав намеренно скупой. Табличные настройки (формат, санитизация, стратегия слияния,
    признак recent) живут в er_wf_meta и правятся дагом export_er_setup — как параметр
    ПАКЕТА они применяли бы одно значение ко ВСЕМ таблицам сразу, то есть чинили одну
    поставку и молча ломали остальные. Рычаги правки состояния дельты (extract_time,
    condition, increment) убраны по той же причине, усиленной тем, что состояние у каждой
    таблицы своё и сохраняется в er_extract_history.

    Все умолчания подобраны так, чтобы автозапуск (`schedule_next` → trigger_dag(conf={}))
    отрабатывал обычный цикл дельты без единого заполненного поля.
    """
    return {
        # ── Окно выгрузки ────────────────────────────────────────────────────
        'date_from': Param(
            None, type=['string', 'null'], title='Дата с',
            description='Разовая выгрузка за период. Формат «ГГГГ-ММ-ДД» или '
                        '«ГГГГ-ММ-ДД ЧЧ:ММ:СС», например 2026-08-01 00:00:00. '
                        'Граница строгая: берётся то, что БОЛЬШЕ неё. '
                        'Задавать вместе с «Дата по», обе — в одном формате. '
                        'Состояние дельты при этом НЕ сохраняется.',
        ),
        'date_to': Param(
            None, type=['string', 'null'], title='Дата по',
            description='Верхняя граница периода, включительно: берётся то, что МЕНЬШЕ '
                        'ИЛИ РАВНО. Формат тот же, например 2026-08-31 23:59:59. '
                        'За сутки 1 августа: с 2026-08-01 по 2026-08-02.',
        ),
        # Именно type='boolean', а не ['boolean','null']: чекбокс в форме Airflow рисуется
        # по сравнению schema.type == "boolean" строкой, и с типом-списком поле выпадает
        # в обычный текстовый ввод. Поэтому «не переопределять» выражено снятой галкой,
        # а не пустым значением: срабатывает только True.
        # ── Куда кладём ──────────────────────────────────────────────────────
        'export_path': Param(
            None, type=['string', 'null'], title='Путь выгрузки',
            description=(
                'Куда положить файлы пакета: conn_id://bucket/dir, например '
                's3-archive://dataplatform-monitoring-dev/er_dump. Пусто — штатный путь '
                'маршрута ТФС. Действует на весь пакет: и на выгрузку, и на ZIP, и на тикет. '
                '⚠️ Уведомление в ТФС при этом всё равно уходит, а состояние дельты всё равно '
                'сдвигается — маршрут ТФС ждёт файлы у себя и такой пакет не подтвердит. '
                'Для разовой выгрузки на сторону снимайте «Notify Kafka» и задавайте период.'
            ),
        ),
        'is_current': Param(
            False, type='boolean', title='Пометить актуальным',
            description='Отметить — состояние считается актуальным и следующий цикл '
                        'не запускается. Снято = признак берётся из состояния дельты.',
        ),
        # ── Групповые: одни на весь пакет ────────────────────────────────────
        'notify_kafka': Param(
            bool(gp['notify_kafka']), type='boolean', title='Notify Kafka',
            description='True = отправлять уведомление в Kafka; False = пропустить (стенд).',
        ),
        'auto_confirm': Param(
            bool(gp['auto_confirm']), type='boolean', title='Auto confirm',
            description='True = не ждать Kafka-подтверждения от TFS.',
        ),
        'confirm_timeout': Param(
            gp['confirm_timeout'], type='integer', title='Confirm timeout (мин)',
            description=(
                'Максимальное время ожидания подтверждения из Kafka. Сверху ограничено '
                f"execution_timeout таска ({gp['confirm_timeout'] + 5} мин), он считается "
                'при разборе файла и через форму запуска не меняется.'
            ),
        ),
        # Таймаута отправки здесь нет: шлёт файлы tfs_kafka_snd, у него свой темп.
        'selfrun_timeout': Param(
            gp['selfrun_timeout'], type='integer', title='Selfrun timeout (мин)',
            description='Задержка до следующего автозапуска, если дельта не догнала текущее время.',
        ),
        # ── Табличное: пусто = взять из er_wf_meta ───────────────────────────
        # Пустой вариант — строка 'None', а не сам None: форма рендерит опции как
        # <option value="{{ option }}">, и None превращается в 'None', которую схема
        # потом отвергает («'None' is not one of [None, 'FULL_UK', ...]»). Строковый
        # 'None' — принятая в репозитории договорённость, key_map отбрасывает его
        # наравне с пустотой, см. tools/s3_checker.py.
        'strategy': Param(
            'None', type=['string'], title='Strategy',
            enum=['None', 'FULL_UK', 'FULL_NO_UK', 'INC', 'APPEND'],
            description=(
                'Стратегия загрузки TFS; применяется ко ВСЕМ таблицам пакета. '
                'None — у каждой таблицы своя, из er_wf_meta. '
                'FULL_UK — полное обновление snp с дедубликацией по UK; строки с ctl_action=D отбрасываются TFS. '
                'FULL_NO_UK — полное обновление без дедубликации; строки с ctl_action=D отбрасываются TFS. '
                'INC — инкрементальное обновление: ctl_action=D+UK→удаление, ctl_action=D без UK→отброс, '
                'остальные+UK→обновление, остальные без UK→вставка. '
                'APPEND — только добавление; ctl_action игнорируется TFS, всегда I.'
            ),
        ),
        'max_file_size': Param(
            None, type=['string', 'null'], title='Max file size',
            description=("Предел размера одного файла данных: '500MB', '10GB' или число байт. "
                         'Применяется ко ВСЕМ таблицам пакета; пусто — у каждой своя, из er_wf_meta.'),
        ),
        # ── Что грузим: по флагу на таблицу ──────────────────────────────────
        # Снятый флаг скипает TaskGroup таблицы целиком, состояние её дельты не двигается.
        # Нужно, чтобы перелить одну сломавшуюся поставку, не гоняя весь пакет.
        # ⏸️ У приостановленной поставки (is_paused в настройке) флаг снят по умолчанию:
        # таск создаётся и штатно скипается, но его можно включить на один ран галкой.
        **{
            f'tbl_{tg}': Param(
                not paused, type='boolean',
                title=f"Грузить {tbl}" + (' ⏸️' if paused else ''),
                description=('Поставка приостановлена настройкой (is_paused=1). '
                             if paused else '')
                            + 'Снять — эта таблица в текущем ране пропускается: '
                              'в ZIP и тикет не попадёт, её дельта останется на месте.',
            )
            for tg, (tbl, paused) in tables.items()
        },
    }


def create_export_dag(dag_id: str, group: dict) -> tuple[str, DAG]:
    """🏭 Создаёт Airflow DAG на одну группу поставок — то есть на один пакет ЕР.

    dag_id — ключ группы из Variable, он же имя дага ('export_er__<реплика>__<группа>');
             в историю он пишется отдельной колонкой, ключом состояния не является
    group  — {replica, dag_group, schedule, is_paused, description, params (групповые),
              tables: {'схема.имя_выгрузки': entry}}

    Возвращает (dag_id, dag) для регистрации в globals().
    """
    from hrp_operators.clickhouse_to_s3 import HrpClickNativeToS3ListOperator # type: ignore

    replica   = group['replica']
    dag_group = norm_group(group.get('dag_group'))
    if replica not in TFS_MAP:
        raise AirflowFailException(f"{dag_id}: реплика '{replica}' не найдена в TFS_MAP")
    scen, prefix = TFS_MAP[replica]

    gp     = get_params(group)
    tables = group.get('tables') or {}
    if not tables:
        raise AirflowFailException(f"{dag_id}: в группе нет ни одной поставки")

    gcfg = {
        'dag_id':          dag_id,
        'replica':         replica,
        'dag_group':       dag_group,
        # Имя пакета в очереди отправки: тракт ТФС и пауза видят пакет именно так,
        # и это ровно та строка, что стояла в replica до выноса группы в свою колонку.
        'pkg_name':        f"{replica}__{dag_group}",
        'scenario':        scen,
        's3_prefix':       prefix,
        'confirm_timeout': gp['confirm_timeout'],
        'selfrun_timeout': gp['selfrun_timeout'],
    }

    cfgs = {tk: _table_cfg(tk, entry, gcfg) for tk, entry in tables.items()}
    # Соответствие «TaskGroup → имя выгрузки»: по нему групповые таски собирают XCom
    gcfg['groups'] = [c['tg'] for c in cfgs.values()]
    gcfg['tables'] = {c['tg']: c['tbl'] for c in cfgs.values()}
    # Все поставки на паузе — это не «человек снял все флаги», а настройка: init такой
    # пакет не роняет, он просто весь скипается.
    gcfg['all_paused'] = all(c['paused'] for c in cfgs.values())
    for cfg in cfgs.values():
        cfg['all_paused'] = gcfg['all_paused']

    schemas = sorted({c['schema_name'].replace(' ', '_').lower() for c in cfgs.values()})

    dag = DAG(
        dag_id=dag_id,
        description=(group.get('description')
                     or f"ER: пакет {replica}/{dag_group} ({len(cfgs)} табл.)"),
        doc_md=(
            f"### Пакет ЕР `{replica}`, группа `{dag_group}` — "
            f"{len(cfgs)} поставок, один тикет\n\n"
            f"Групповые параметры:\n```json\n{json.dumps(gp, indent=2, default=str)}\n```\n\n"
            + "\n".join(
                f"**{tk}** — формат `{c['format']}`, стратегия `{c['strategy']}`, "
                f"колонок {len(c['fields'])}"
                for tk, c in cfgs.items()
            )
        ),
        default_args=DEF_ARGS, start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
        # Расписание обязательное: синк не пропускает группу без cron, и подставлять
        # что-то своё здесь нельзя — пакет поехал бы не в своё окно.
        schedule_interval=group['schedule'],
        max_active_tasks=int(gp['max_active_tasks']), max_active_runs=1, catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'ER', replica, *schemas],
        render_template_as_native_obj=True,
        # На паузе создаётся любой новый пакет, а при is_paused в настройке паузу ещё
        # и дожимает синк — на уже созданный даг этот флаг сам по себе не действует.
        is_paused_upon_creation=True,
        params=_dag_params(gp, {c['tg']: (c['tbl'], c['paused']) for c in cfgs.values()}),
        # Путь выгрузки задаётся параметром запуска, а поля оператора выставляются при
        # РАЗБОРЕ файла — подменить их можно только шаблоном. Отсюда макрос: он зовёт тот же
        # parse_s3_target, что и питоновские таски, поэтому цель у оператора, упаковки
        # и тикета всегда одна.
        user_defined_macros={
            's3_target': lambda path, part, _p=prefix: parse_s3_target(path, S3_CONN, BUCKET, _p)[part],
        },
    )

    def _make_pre_exp(tcfg):
        """pre_execute для export_to_s3: подставляет состояние дельты в SQL и параметры оператора."""
        def _pre_exp(ctx):
            dp = _xcom(ctx, tcfg['tg'], 'init')
            op = ctx['task']
            # Шаблон берём из настройки таблицы, а НЕ с оператора (op.sql): подстановка
            # одноразовая, после неё плейсхолдеров в op.sql уже нет, и повторная отрисовка
            # в том же процессе взяла бы старое окно дельты. Заодно хук перестаёт зависеть
            # от того, донёс ли оператор это поле до запуска.
            #
            # replace, а не format: в SQL встречаются фигурные скобки (JSON-функции, map(),
            # литерал '{}'), и str.format на них падает KeyError/IndexError.
            op.sql = (tcfg['sql_export']
                      .replace('{export_time}', dp['extract_time'])
                      .replace('{condition}', dp['condition']))

            # max_size оператор ждёт СТРОКОЙ: '100MB' либо просто число байт. Разбирает он
            # её сам, в _init_check через parse_size(), а тот сразу зовёт .strip() — int
            # роняет его с AttributeError: 'int' object has no attribute 'strip'.
            # None безопасен: _init_check подставит вместо него дефолт (10 ГБ на PROM,
            # 1 ГБ иначе), так что до сравнения размеров в execute() None не доживает.
            # Ноль тоже отдаём как None: parse_size('0') вернёт 0, а оператор считает это
            # неверным форматом и падает уже своей ошибкой.
            raw = str(dp.get('max_file_size') or '').strip().upper()
            num = re.fullmatch(r'(\d+)\s?(MB|KB|GB|B)?', raw)
            op.max_size = raw if num and int(num.group(1)) > 0 else None

            op.pg_array_format  = dp['pg_array_format'] == 'True'
            op.xstream_sanitize = dp.get('xstream_sanitize', 'False') == 'True'
            op.sanitize_array   = dp.get('sanitize_array', 'False') == 'True'
            op.sanitize_list    = dp.get('sanitize_list') or ''
            try:
                op.format_params = ast.literal_eval(dp['format_params'])
            except (ValueError, SyntaxError):
                op.format_params = {}
        return _pre_exp

    with dag:
        # Метка времени пакета — первым делом и на весь ран: имена файлов строятся от неё,
        # а пул на базовую реплику разводит по секундам пакеты разных групп.
        t_ts = _er_make_ts.override(pool=ts_pool(replica))(gcfg=gcfg)

        packed = []
        for table_key, tcfg in cfgs.items():
            fmt = FORMAT_MAP[tcfg['format']]
            with TaskGroup(group_id=tcfg['tg']):
                t_init, t_meta = _er_init(cfg=tcfg), _er_build_meta(cfg=tcfg)
                t_exp = HrpClickNativeToS3ListOperator(
                    task_id='export_to_s3',
                    s3_bucket="{{ s3_target(params.export_path, 'bucket') }}",
                    # Ключ обязан различать и таблицу, и группу: s3_prefix общий у всех групп
                    # одной базовой реплики, ts_nodash совпадает у пакетов на одном cron, а
                    # оператор лишь дописывает номер части. Только имени таблицы мало —
                    # одноимённые таблицы из разных баз (tg = db__tbl) или из разных групп
                    # писали бы в один ключ и затирали друг друга при replace=True.
                    s3_key=("{{ s3_target(params.export_path, 'key_prefix') }}"
                            f"{tcfg['replica']}__{tcfg['tg']}__{{{{ ts_nodash }}}}.{fmt['ext']}"),
                    aws_conn_id="{{ s3_target(params.export_path, 'conn_id') }}",
                    clickhouse_conn_id=CH_ID,
                    sql=tcfg['sql_export'], fmt=fmt['fmt'], header=fmt['header'],
                    compression=None, replace=True, post_file_check=False,
                    pre_execute=_make_pre_exp(tcfg),
                    execution_timeout=timedelta(minutes=tcfg['export_timeout']),
                )
                t_zip = _er_pack_zip(cfg=tcfg)
                t_ts >> t_init >> [t_meta, t_exp] >> t_zip
                packed.append(t_zip)

        # Отправки здесь нет: make_summary только ставит файлы в очередь, а шлёт их
        # tfs_kafka_snd — он один видит все выгрузки и потому только он может
        # соблюдать лимиты маршрута (файлов в секунду, минуту, час и сутки).
        t_sum = _er_make_summary(gcfg=gcfg)
        # execution_timeout с запасом к собственному дедлайну таска: снимать его должен
        # он сам, с внятной ошибкой, а не Airflow по таймауту
        t_wait = _er_wait_confirm.override(
            execution_timeout=timedelta(minutes=gcfg['confirm_timeout'] + 5),
        )(gcfg=gcfg)

        packed >> t_sum >> t_wait >> _er_save_status(gcfg=gcfg) >> _er_schedule_next(gcfg=gcfg)

    return dag_id, dag

@task(task_id='config_error')
def _er_config_error(replica: str, errors: list, **context):
    """💥 Единственный таск даг-заглушки: сообщает, что пакет сломан, и падает.

    Не EmptyOperator: зелёный таск создавал бы ощущение работающей выгрузки. Причины
    дублируются в лог и в заметки, чтобы их было видно и из списка ранов, и из алертов,
    а не только в описании DAG.
    """
    for err in errors:
        logger.error("❌ %s: %s", replica, err)

    add_note({f"❌ Пакет {replica} сломан ({len(errors)})": errors},
             level='task,dag', context=context, title='🚫 Ошибки настройки er_wf_meta')

    raise AirflowFailException(
        f"Пакет {replica} не собран: {len(errors)} ошибок в настройке export.er_wf_meta.\n"
        + "\n".join(f"  • {e}" for e in errors)
        + "\nПоправьте настройку и перезапустите export_er_setup"
    )


def create_broken_dag(dag_id: str, errors: list, schedule=None) -> tuple[str, DAG]:
    """🚧 DAG-заглушка вместо пакета, который не удалось собрать.

    dag_id тот же, что у рабочего пакета: DAG не двоится, а подменяется — в списке сразу
    видно, что выгрузки нет. Расписание группы сохраняется, поэтому поломка продолжает
    сигналить красным раном в том же ритме, в каком должен был ходить пакет.
    """
    replica = dag_id.removeprefix('export_er__')
    dag = DAG(
        dag_id=dag_id,
        description=f"❌ Пакет сломан: {len(errors)} ошибок в настройке er_wf_meta",
        doc_md=(
            f"## ❌ Пакет `{replica}` не собран\n\n"
            f"Выгрузка не работает: в `export.er_wf_meta` {len(errors)} ошибок.\n\n"
            + "\n".join(f"- {e}" for e in errors)
            + "\n\nПоправьте настройку и перезапустите `export_er_setup`."
        ),
        # Ретраить ошибку настройки бессмысленно; пул экспорта заглушке не нужен,
        # а on_failure_callback оставляем — он пишет штатную заметку о падении.
        default_args={
            'owner': DEF_ARGS['owner'],
            'retries': 0,
            'email_on_failure': False,
            'on_failure_callback': DEF_ARGS['on_failure_callback'],
        },
        start_date=datetime(2024, 12, 18, tzinfo=timezone.utc),
        schedule_interval=schedule, max_active_runs=1, catchup=False,
        tags=['DataLab', 'CI02420667', 'ER', replica, 'BROKEN'],
        is_paused_upon_creation=True,
    )
    with dag:
        _er_config_error(replica=replica, errors=errors)

    return dag_id, dag

# ── Dynamic DAG Registration ──────────────────────────────────────────────────

try:
    workflows = obj_load(VAR_NAME)
except Exception as e:
    logger.error("Failed to load workflows: %s", e)
    workflows = {}

for _dag_id, _group in workflows.items():
    # Сломанную группу подменяем заглушкой, а не роняем разбор файла: раньше одна битая
    # запись уносила ВСЕ ER-пакеты. Причины видны в описании DAG, в логе и в заметках,
    # а таск заглушки падает — молча пропущенной выгрузки не остаётся.
    try:
        if _group.get('errors'):
            dag_id, dag_obj = create_broken_dag(_dag_id, _group['errors'], _group.get('schedule'))
        else:
            dag_id, dag_obj = create_export_dag(_dag_id, _group)
    except Exception as e:
        logger.error("DAG generation failed for %s: %s", _dag_id, e)
        dag_id, dag_obj = create_broken_dag(_dag_id, [f"Ошибка сборки DAG: {e}"],
                                            _group.get('schedule'))
    globals()[dag_id] = dag_obj
