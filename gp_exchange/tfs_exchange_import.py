# HRPDATALAB-13047
import pendulum
import json
import logging
import re
from datetime import datetime, date, timedelta

from airflow import DAG, Dataset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
# from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from hrp_operators import HrpS3ToClickhouseTableOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.providers.common.sql.operators.sql import BranchSQLOperator
# from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.operators.python import BranchPythonOperator
from airflow.exceptions import AirflowFailException

from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from airflow.utils.task_group import TaskGroup

# from airflow.models.xcom_arg import XComArg

# from airflow.utils.dates import days_ago
# from random import randint

# Двойной импорт, как во всём репозитории: на контуре общий модуль приезжает боевым
# пакетом, на стенде и в свежем клоне — соседним файлом каталога. Жёсткий импорт держался
# на заглушке CI06932748 в PYTHONPATH стенда; без неё файл не разбирался вовсе.
try:
    from CI06932748.analytics.datalab.gp_exchange.tfs_exchange_common import ( # type: ignore
        ON_CLUSTER,
        REPLICATED,
        TFS_IN_DATASET,
        TFS_IN_CONN_ID,
        TFS_IN_BUCKET,
        TFS_IN_PREFIX,
        default_args,
        CH_BD,
        CH_ID,
        GP_EXCHANGE
    )
except ImportError:
    from gp_exchange.tfs_exchange_common import ( # type: ignore
        ON_CLUSTER,
        REPLICATED,
        TFS_IN_DATASET,
        TFS_IN_CONN_ID,
        TFS_IN_BUCKET,
        TFS_IN_PREFIX,
        default_args,
        CH_BD,
        CH_ID,
        GP_EXCHANGE
    )


_IDENTIFIER_RE = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')


def validate_identifier(name: str) -> str:
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid SQL identifier: {name!r}")
    return name


_TYPE_RE = re.compile(r'^[A-Za-z0-9_]+(\([A-Za-z0-9_, ]*\))?$')


def validate_type(name: str) -> str:
    """Тип ClickHouse для JSONExtract: имя, необязательно со скобками.

    Пропускает 'String', 'Int64', 'Nullable(DateTime)' и режет всё остальное.
    Тип приезжает из JSONType над присланными данными, то есть управляется чужой
    стороной, и подставляется в запрос текстом.
    """
    if not _TYPE_RE.match(name):
        raise ValueError(f"Invalid ClickHouse type: {name!r}")
    return name


def json_fields(rows: list, on_bad: str = 'raise') -> tuple:
    """Собирает список выражений JSONExtract по описанию полей из данных.

    Имя поля и его тип приходят из самого сообщения (JSONExtractKeys / JSONType),
    то есть их задаёт присылающая сторона, а подставляются они в запрос текстом —
    без проверки ключ вида "x') as y, (select …" встраивается в SQL. Проверяем оба.

    Args:
        rows: строки с ключами 'fld' и 'type'.
        on_bad: 'raise' — падать на негодном поле (загрузка в целевую таблицу),
            'skip' — пропустить его, вернув вторым элементом список пропущенных
            (разведка неизвестного потока: один странный ключ не должен её отменять).

    Returns:
        Кортеж (строка с выражениями, список пропущенных полей).
    """
    parts, skipped = [], []
    for row in rows:
        try:
            fld = validate_identifier(row['fld'])
            typ = validate_type(row['type'])
        except ValueError as e:
            if on_bad == 'raise':
                raise AirflowFailException(f"Bad field in incoming data: {e}") from e
            logging.warning("Пропускаю поле: %s", e)
            skipped.append(f"{row['fld']}: {row['type']}")
            continue
        parts.append(f"\n, JSONExtract(a._gp_data, '{fld}', '{typ}') as {fld}")
    return ''.join(parts), skipped


def sql_fields(wf_name: str) -> str:
    """Запрос, определяющий поля потока и их типы по самим данным.

    Тип берётся из JSONType по первым строкам. Одно поле в разных строках может
    приехать разным типом — целое в одной, дробное в другой, — и прежний
    group_concat склеивал их в 'Double,Int64'. Такого типа не существует:
    JSONExtract с ним падает, а на разведке нового потока падала вся задача.
    Разошлись типы — берём String: он вмещает любое значение, а точный тип всё
    равно задаётся вручную при создании целевой таблицы.

    Object и Array тоже уходят в String — это было и раньше.
    """
    return f"""
                with srs as (
                    select JSONExtractString(a._gp_data) _gp_data, _gp_id
                    from {CH_BD}.gp_ue_exchange a where _gp_name = '{escape_str(wf_name)}' order by _gp_id
                ), fld as (
                    select fld, max(i) ind
                    from (select distinct JSONExtractKeys(_gp_data) arr from srs limit 10)
                    array join arr as fld, arrayEnumerate(arr) as i
                    group by 1 order by 2
                ), typed as (
                    select fld.ind as ind, fld.fld as fld, srs._gp_id as _gp_id
                        , multiIf(JSONType(srs._gp_data, fld.fld) in ('Object', 'Array'), 'String',
                                  JSONType(srs._gp_data, fld.fld)) as tp
                    from fld, srs
                    where JSONType(srs._gp_data, fld.fld) != 'Null'
                )
                select ind, fld
                    , if(uniqExact(tp) > 1, 'String', any(tp)) as type
                    , max(_gp_id) as id
                from typed
                group by 1, 2
                order by 1, 2
                limit 1000
    """


def escape_str(value: str) -> str:
    return value.replace("'", "''")


def process_any(fields, tbl_bd='', table='', ins_str='', engine='', drop_src=False, clear_trg=None, _gp_key='_gp_key', chk_out=True, chk_uniq=None, **context):
    """
    Универсальный обработчик для загрузки данных из gp_ue_exchange в целевую таблицу.

    Выполняет:
    - Автоматическое определение схемы полей из JSON (при fields='*').
    - Создание целевой таблицы при отсутствии (с указанным движком).
    - Очистку по стратегии: DROP PARTITION, TRUNCATE, DELETE, OPTIMIZE.
    - Вставку данных с фильтрацией по новым _gp_id.
    - Логирование в gp__exchange_log.
    - Проверку уникальности записей (если задано).

    Аргументы:
        fields (str): SQL-выражение для извлечения полей из JSON. Если '*', — определяется автоматически.
        tbl_bd (str): База данных назначения (по умолчанию CH_BD).
        table (str): Имя целевой таблицы (если не задано — выводится из task_id).
        ins_str (str): Список полей для INSERT (если не задан — определяется через system.columns).
        engine (str): DDL-выражение для создания таблицы (включая движок и ORDER BY).
        drop_src (bool): Удалить ли исходную партицию в gp_ue_exchange после обработки.
        clear_trg (str|None): Стратегия очистки перед вставкой:
            - 'drop': удалить партиции по _gp_key
            - 'truncate': полностью очистить таблицу
            - 'delete': удалить строки по _gp_key
            - 'optimize': выполнить OPTIMIZE TABLE
            - 'final': выполнить OPTIMIZE TABLE FINAL
            - None: ничего не делать
        _gp_key (str): Ключ партиционирования (по умолчанию '_gp_key').
        chk_out (bool): Проверять ли количество записей после загрузки.
        chk_uniq (str): Выражение для проверки уникальности (например, 'id').

    XCom:
        - stat: статистика по загруженным данным (row_cnt, min/max _gp_id)
        - uniq: результат проверки уникальности (список дублей или "passed")

    Raises:
        AirflowFailException: при провале проверок, пустом имени таблицы или дублях.
    """
    # ClickHouse hook и TaskInstance
    ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti = context.get('ti') or context.get('task_instance')
    now = context["execution_date"].in_tz('UTC').strftime('%Y-%m-%d %H:%M:%S')

    # Таблица и партиция
    wf_name = ti.task_id.replace('process_', '') if ti else ''
    wf_name = wf_name.split('.')[-1] if '.' in wf_name else wf_name
    if not table:
        table = 'gp_' + wf_name
    if not tbl_bd:
        tbl_bd = CH_BD

    validate_identifier(tbl_bd)
    validate_identifier(table)
    validate_identifier(_gp_key)
    wf_name = escape_str(wf_name)

    logging.info(f"Processing table: {tbl_bd}.{table} ({wf_name})")

    try:
        exists = (ch_hook.execute(f"EXISTS TABLE {tbl_bd}.{table}")[0][0] == 1)
    except Exception as e:
        logging.error(f"Failed to check table existence {tbl_bd}.{table}: {e}")
        raise AirflowFailException(f"Failed to check table existence {tbl_bd}.{table}: {e}") from e

    # Формируем SELECT для вставки в целевую таблицу

    if fields.strip() == '*':
        if exists:
            sql=f"""
            select 
                -- multiIf, а не CASE: CASE в ClickHouse — это transform, а он требует
                -- константных веток, тогда как здесь в каждой ветке printf по колонкам
                -- system.columns. С CASE запрос падает с 'Argument at index 2 for
                -- function transform must be constant', то есть поток с fields='*'
                -- не грузился вовсе, когда целевая таблица уже создана.
                multiIf(
                    type = 'DateTime', printf($$, parseDateTimeBestEffort(JSONExtract(a._gp_data, '%s', 'String')) as %s$$, a.name, a.name),
                    type = 'Nullable(DateTime)', printf($$, parseDateTimeBestEffortOrNull(JSONExtract(a._gp_data, '%s', 'Nullable(String)')) as %s$$, a.name, a.name),
                    printf($$, JSONExtract(a._gp_data, '%s', '%s') as %s$$, a.name, a.type, a.name)
                ) as fld
            from system.columns a
            where a.database = '{tbl_bd}' and a.table = '{table}'
                and a.name not in ('_gp_ts', '_gp_key', '_gp_id', '_gp_hash')
            order by a.position
            limit 1000
            """
            res = select_dic(ch_hook, sql)
            # ti.xcom_push(key=f'{wf_name}_fields', value=json_out(res)[:500])

            fields_list = [f"\n{k['fld']}" for k in res]
            fields_str = ''.join(fields_list)
        else:
            sql=f"""
                {sql_fields(wf_name)}
            """
            res = select_dic(ch_hook, sql)
            # ti.xcom_push(key=f'{wf_name}_fields', value=json_out(res)[:500])

            fields_str, _ = json_fields(res)
    else:
        fields_str = fields

    # Формируем SELECT для вставки в целевую таблицу
    sel_str = f"""
        select a._gp_ts
            {fields_str}
            , a._gp_key
            , a._gp_id
            , a._gp_hash
        from {CH_BD}.gp_ue_exchange a final
        where a._gp_name = '{wf_name}'
    """
    logging.info(sel_str)

    # Создаём таблицу при необходимости, выполняем очистки/вставку и логирование
    if exists:
        logging.info(f"Table {tbl_bd}.{table} already exists")
        res = ch_hook.execute(f""" select name, create_table_query, engine_full FROM system.tables where database='{CH_BD}' and table = '{table}' """)
        engine = res[0][2]
        create_sql = res[0][1]
        logging.info(create_sql)

    else:
        logging.info(f"Creating table {tbl_bd}.{table}")

        trg = now[:10].replace('-', '')
        # trg = f"MergeTree('/clickhouse/tables/{table}_{trg}'"+",'{replica}',"
        trg = f"MergeTree('/clickhouse/tables/{table}_"+"{uuid}','{replica}',"
        create_sql = f"""
            CREATE TABLE {tbl_bd}.{table} {ON_CLUSTER}
            {engine}
            as
            {sel_str}
            limit 0
        """.replace('MergeTree(', trg).replace(',)', ')')
        logging.info(create_sql)

        # if engine and not exists: ch_hook.execute(create_sql)
        raise AirflowFailException(f"Table {tbl_bd}.{table} does not exist. Please create it manually: {create_sql}")


    # Определение нужно ли FINAL
    final_types = [
        'ReplacingMergeTree',
        'SummingMergeTree',
        'AggregatingMergeTree',
        'CollapsingMergeTree',
        'VersionedCollapsingMergeTree',
    ]
    engine = engine or ''
    final = ' FINAL' if engine and any(t in engine for t in final_types) else ''

    # Ниже везде _gp_name = wf_name, а не table: в gp_ue_exchange лежит имя потока
    # ('_exchange_log'), тогда как table — имя целевой таблицы ('gp__exchange_log').
    # С именем таблицы выборка ключей пуста, а DROP PARTITION молча не находит партицию:
    # очистка и удаление источника не делали ничего и не жаловались.
    if clear_trg == 'drop':
        res = ch_hook.execute(f""" select distinct {_gp_key} from {CH_BD}.gp_ue_exchange where _gp_name = '{wf_name}' """)
        for key in res:
            logging.info(f"clear_trg: drop partition {key[0]}")
            ch_hook.execute(f""" ALTER TABLE {tbl_bd}.{table} {ON_CLUSTER} DROP PARTITION '{key[0]}' """)

    elif clear_trg == 'truncate':
        logging.info(f"clear_trg: truncate {table}")
        ch_hook.execute(f"""TRUNCATE TABLE {tbl_bd}.{table} {ON_CLUSTER}""")

    elif clear_trg == 'delete':
        res = ch_hook.execute(f"""select distinct {_gp_key} from {CH_BD}.gp_ue_exchange where _gp_name = '{wf_name}' """)
        for key in res:
            where = f"WHERE {_gp_key} = cast('{key[0]}', toTypeName({_gp_key}))"
            logging.info(f"clear_trg: delete {where}")
            ch_hook.execute(f""" ALTER TABLE {tbl_bd}.{table} {ON_CLUSTER} DELETE {where} """)

    elif clear_trg in ('optimize', 'final'):
        pass  # optimize выполнится после insert

    elif clear_trg is not None:
        logging.error(f"Unknown clear_trg option: {clear_trg}")
        raise AirflowFailException(f"Unknown clear_trg option: {clear_trg}")

    logging.info(f"insert into {tbl_bd}.{table}")
    if not ins_str: ins_str = ch_hook.execute(f"""
        select concat('(',groupConcat(', ')(a.name),')')
        from (
            select * from system.columns
            where database = '{tbl_bd}' and table = '{table}'
            order by position 
        ) a
    """)[0][0]
    logging.info(ins_str)
    ins = ch_hook.execute(f""" 
        insert into {tbl_bd}.{table} 
        {ins_str}
        {sel_str} 
        and a._gp_id > (select max(_gp_id) from {tbl_bd}.{table})
    """)
    logging.info(f"ins: {ins}")

    if clear_trg == 'optimize': ch_hook.execute(f""" OPTIMIZE TABLE {tbl_bd}.{table} {ON_CLUSTER} """)
    if clear_trg == 'final': ch_hook.execute(f""" OPTIMIZE TABLE {tbl_bd}.{table} {ON_CLUSTER} FINAL """)

    # проверка выходных данных (опционально)
    if chk_out or chk_out is None:
        do_chk(chk_type='right'
               , chk_src='_exchange_log'
               , wf_name=wf_name
               , chk_prm=['only_err', 'raise', 'not_empty', 'now', 'last_trg']
               , **context)

    # логируем факт загрузки в контрольную таблицу gp__exchange_log
    ch_hook.execute(f"""
        insert into {CH_BD}.gp__exchange_log 
        select toDateTime('{now}') -- _gp_ts
            , 'IN_log' -- wf_type
            , '{wf_name}' -- wf_name
            , toJSONString(map(
                'bd', '{tbl_bd}'
                , 'table', '{table}' 
                , 'wf_key', a._gp_id::text
                , 'cnt', count(*)::text
                --, 'sum_len', sum(length(concat(a.*)))::text
                --, 'min_len', min(length(concat(a.*)))::text
                --, 'max_len', max(length(concat(a.*)))::text
                , 'time', toString(nowInBlock()-now())
            )) -- _gp_data
            , a._gp_key
            , 0 as _gp_id
            , generateUUIDv4() as _gp_hash
        from {tbl_bd}.{table} a {final}
        where a._gp_id > 0
            and a._gp_ts = toDateTime('{now}')
        group by a._gp_id, a._gp_key
    """)

    # Возвращаем базовую статистику по таблице
    sql = f"""
        select 'all' type, count(1) row_cnt
            , min(_gp_id) min_ctl, max(_gp_id)  max_ctl
            --, count(distinct _gp_id) cnt_ctl
            , min(_gp_ts) min_its, max(_gp_ts)  max_its
            --, count(distinct _gp_ts) cnt_its
        from {tbl_bd}.{table} {final}
        union all
        select 'now' type, count(1) row_cnt
            , min(_gp_id) min_ctl, max(_gp_id)  max_ctl
            --, count(distinct _gp_id) cnt_ctl
            , min(_gp_ts) min_its, max(_gp_ts)  max_its
            --, count(distinct _gp_ts) cnt_its
        from {tbl_bd}.{table} {final}
        where _gp_ts = toDateTime('{now}')
        order by 1 desc
    """
    ret = select_dic(ch_hook, sql)
    jsn = json_out(ret)

    if drop_src: ch_hook.execute(f""" ALTER TABLE {CH_BD}.gp_ue_exchange {ON_CLUSTER} DROP PARTITION '{wf_name}' """)
    ti.xcom_push(key=f'stat', value=jsn[:500])

    if chk_uniq:
        uniq_sql = f"""
            select {chk_uniq}, count(*)
            from {tbl_bd}.{table} {final}
            group by {chk_uniq}
            having count(*) > 1
            limit 10
        """
        res = select_dic(ch_hook, uniq_sql)
        if res:
            jsn = json_out(res)
            logging.error(f"Unique check failed: {jsn}", extra={'data':jsn})
            ti.xcom_push(f'uniq', jsn[:500])
            raise AirflowFailException("Unique check failed")
        else:
            logging.info("Unique check passed")
            ti.xcom_push(f'uniq', [json.dumps({'chk_uniq': 'passed'})][:500])
    else:
        logging.info("Unique check skipped")
        ti.xcom_push(f'uniq', [json.dumps({'chk_uniq': 'skipped'})][:500])

def do_list_files(**context) -> list:
    """
    Сканирует S3-бакет на наличие CSV-файлов с префиксом ue_exchange_.
    Проверяет расширение .csv, собирает метаданные (размер, дата), пушит в XCom.
    Возвращает список ключей S3 для загрузки.
    """

    # Инициализация схемы gp_ue_exchange_load
    # ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti=context.get('ti') or context.get('task_instance')

    # ch_hook.execute(SQL_INIT)

    s3_hook = S3Hook(aws_conn_id=TFS_IN_CONN_ID)
    res = s3_hook.list_keys(
        bucket_name=TFS_IN_BUCKET,
        prefix=TFS_IN_PREFIX + 'ue_exchange_',
        max_items=500,
    )
    logging.info(f"Found {len(res)} files: {res}")

    info = []
    for key in res:
        s3_obj = s3_hook.get_key(key, bucket_name=TFS_IN_BUCKET)
        file = {
            # 'bucket': TFS_IN_BUCKET,
            'key': key.replace(TFS_IN_PREFIX, ''),
            'size': readable_size(s3_obj.content_length),
            'modified': str(s3_obj.last_modified),
        }
        logging.info(f"Found file: {file}")
        info.append(str(file))

    ti.xcom_push(key='files', value=info[:500])

    return res

def do_chk(chk_type='full', chk_src='', chk_trg='', wf_name='', chk_prm=None, **context):
    """
    Выполняет сверку количества записей между источником и приёмником.

    Используется для контроля целостности данных на разных этапах ETL.

    Args:
        chk_type (str): тип JOIN ('full', 'right', 'left').
        chk_src (str): имя источника (например, 'ue_exchange_load').
        chk_trg (str): имя приёмника (например, 'ue_exchange').
        wf_name (str): фильтр по имени задачи.
        chk_prm (list): опции проверки:
            - 'only_err': возвращать только ошибки
            - 'raise': вызывать исключение при ошибке
            - 'not_empty': требовать непустой результат
            - 'now': фильтровать по времени запуска DAG
            - 'final': использовать FINAL при выборке
            - 'last_src', 'last_trg': фильтровать по последнему _gp_id
    """
    if chk_prm is None:
        chk_prm = ['only_err', 'raise', 'not_empty']  # 'final', 'last_src', 'last_trg', 'now', 'no_key', 'no_id'

    if chk_trg == '':
        chk_trg = wf_name

    if chk_src:
        validate_identifier(chk_src)
    if chk_trg:
        validate_identifier(chk_trg)
    wf_name = escape_str(wf_name)

    ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti=context.get('ti') or context.get('task_instance')
    now = context["execution_date"].in_tz('UTC').strftime('%Y-%m-%d %H:%M:%S')

    if chk_src in ('ue_exchange', '_exchange_log'):
        src_final = 'final'
    else:
        src_final = ''

    srs_where = ""
    if chk_src in ('ue_exchange', 'ue_exchange_load'):
        srs_where += " and _gp_name = '_exchange_log'\n"

    if wf_name != '':
        srs_where += f" and JSONExtractString(JSONExtractString(_gp_data), 'wf_name') = '{wf_name}'\n"

    if 'last_src' in chk_prm:
        srs_where += f" and _gp_id = (select max(_gp_id) from {CH_BD}.gp_{chk_src} {src_final})\n"

    if 'now' in chk_prm:
        srs_where += f" and _gp_ts = toDateTime('{now}')\n"

    srs_key = "''" if 'no_key' in chk_prm else "JSONExtractString(JSONExtractString(_gp_data), 'wf_key')"

    id_str = "''" if 'no_id' in chk_prm else "_gp_id"

    sql_srs = f"""
                select {id_str} as _id
                    , JSONExtractString(JSONExtractString(_gp_data), 'wf_name') as _name
                    , {srs_key} as _key
                    , JSONExtractString(JSONExtractString(_gp_data), 'type') as _type
                    , JSONExtractInt(JSONExtractString(_gp_data), 'cnt') 
                        + (JSONExtractString(JSONExtractString(_gp_data), 'type') = 'SUM')::int
                        as cnt
                    , _gp_data
                from {CH_BD}.gp_{chk_src} {src_final}
                where JSONExtractString(JSONExtractString(_gp_data), 'type') in ('OUT', 'SUM')
                    {srs_where}
                order by 1 desc, 2, 3 desc
    """

    if chk_trg in ('ue_exchange', '_exchange_log'):
        trg_final = 'FINAL'
    elif 'final' in chk_prm:
        trg_final = 'FINAL'
    else:
        trg_final = ''

    trg_where = ""
    if chk_trg in ('ue_exchange', 'ue_exchange_load'):
        trg_name = '_gp_name'
        if wf_name != '':
            trg_where += f" and _gp_name = '{escape_str(wf_name)}'\n"
    else:
        trg_name = f"'{chk_trg}'"

    if 'last_trg' in chk_prm:
        trg_where += f" and _gp_id = (select max(_gp_id) from {CH_BD}.gp_{chk_trg} {trg_final})\n"

    if 'now' in chk_prm:
        trg_where += f"and _gp_ts = toDateTime('{now}')\n"

    trg_key = "''" if ('no_key' in chk_prm) else "_gp_key"

    sql_trg = f"""
            select {id_str}  as _id
                , {trg_name} as _name
                , {trg_key}  as _key
                , count(*) as row_cnt
            from {CH_BD}.gp_{chk_trg} {trg_final}
            where true 
                {trg_where}
            GROUP by 1,2,3
    """
    sql = f"""
        select (a.chk_cnt == b.row_cnt)::bool as is_ok
            , coalesce(nullif(a._id, 0), b._id) as _id
            , coalesce(nullif(a._name, ''), b._name) as _name
            , coalesce(nullif(a._key, ''), b._key) as _key
            , a.chk_cnt
            , b.row_cnt
        from (
            select _id, _name, _key, sum(cnt) chk_cnt
            from ( {sql_srs} 
            ) a
            group by 1,2,3
            order by 1 desc, 2, 3 desc
        ) a
        {chk_type} join ( {sql_trg} 
        ) b on a._id = b._id and a._key = b._key and a._name = b._name
        order by 1, 2 desc, 3, 4 desc
    """
    res = select_dic(ch_hook, sql)
    empty = (res is None) or (not res)

    if 'only_err' in chk_prm:
        res = [row for row in res if not row['is_ok']]

    jsn = json_out(res)
    logging.info(f"chk {chk_type}: {jsn}")

    # Проверка результата: если хоть одна строка содержит is_ok == false — фейлим таск
    if 'raise' in chk_prm:
        if 'not_empty' in chk_prm and empty:
            logging.error(f"Data check failed in chk task res is empty {chk_prm}")
            ti.xcom_push(key='chk', value='Empty')
            raise AirflowFailException(f"Data check failed in chk task res is empty {chk_prm}")
        elif res and [row for row in res if not row['is_ok']]:
            logging.error(f"Data check failed in chk task {chk_prm}: {res}", extra={'data': res})
            ti.xcom_push(key='chk', value=jsn[:500])
            raise AirflowFailException(f"Data check failed in chk task {chk_prm}")
        else:
            logging.info(f"Data check passed in chk task {chk_prm}")
            ti.xcom_push(key='chk', value=(jsn[:500] if res else 'Ok'))
    else:
        ti.xcom_push(key=f'chk', value=(jsn[:500] if res else 'Ok'))

def do_load_exchange(**context):
    """
    Загружает данные из gp_ue_exchange_load в gp_ue_exchange.
    Дедуплицирует, нормализует JSON, добавляет хэши и временную метку.
    Выполняет OPTIMIZE FINAL для консистентности.
    Возвращает информацию о партициях.
    """

    ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti=context.get('ti') or context.get('task_instance')
    now = context["execution_date"].in_tz('UTC').strftime('%Y-%m-%d %H:%M:%S')

    ch_hook.execute(f"""
        INSERT INTO {CH_BD}.gp_ue_exchange
        select distinct 
            toDateTime('{now}') as _gp_ts
            , a._gp_id
            , a._gp_name
            , a._gp_key
            , JSONExtractString(a._gp_data) as _gp_data
            , toUUID(HEX(MD5(a._gp_data))) as _gp_hash
        from {CH_BD}.gp_ue_exchange_load a
        -- where a._gp_id not in (select distinct _gp_id from {CH_BD}.gp_ue_exchange)
    """)

    ch_hook.execute(f""" OPTIMIZE TABLE {CH_BD}.gp_ue_exchange {ON_CLUSTER} FINAL """)

    sql = f"""
        SELECT a.partition, formatReadableQuantity(a.rows) cnt, formatReadableDecimalSize(a.bytes_on_disk) on_disk
        FROM system.parts a
        WHERE a.database = 'support' AND a.table= 'gp_ue_exchange' AND a.active
        order by 1
        limit 50
    """
    res = select_dic(ch_hook, sql)
    jsn = json_out(res)
    logging.info(f"load_exchange: {jsn}")
    ti.xcom_push(key='load_exchange', value=(jsn[:500] if res else 'Empty'))

def do_clear_load(**context):
    """
    Очищает промежуточную таблицу gp_ue_exchange_load по _gp_id.
    Удаляет партиции, уже перенесённые в основную таблицу.
    """

    ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti=context.get('ti') or context.get('task_instance')
    # now = context["execution_date"].in_tz('UTC').strftime('%Y-%m-%d %H:%M:%S')

    keys = ch_hook.execute(f""" 
        select distinct _gp_id 
        from {CH_BD}.gp_ue_exchange_load 
        where _gp_id in (select distinct _gp_id from {CH_BD}.gp_ue_exchange)
    """)
    logging.info(f"keys: {keys}")
    for key in keys:
        if key[0] > 0:
            logging.info(f"Dropping partition: {key[0]}")
            ch_hook.execute(f""" ALTER TABLE {CH_BD}.gp_ue_exchange_load {ON_CLUSTER} DROP PARTITION '{key[0]}' """)

    sql = f"""
        SELECT a.partition
            , formatReadableQuantity(a.rows) rows
            , formatReadableDecimalSize(a.bytes_on_disk) on_disk
        FROM system.parts a
        WHERE a.database = 'support' AND a.table= 'gp_ue_exchange_load' AND a.active
        order by 1
        limit 50
    """
    res = select_dic(ch_hook, sql)
    jsn = json_out(res)
    ti.xcom_push(key='clear_load', value=(jsn[:500] if res else 'Empty'))

def do_branch(**context):
    """
    Определяет, какие процессные задачи запускать, на основе _gp_name в gp_ue_exchange.

    Работает следующим образом:
    - Получает список всех _gp_name из буфера gp_ue_exchange.
    - Исключает служебные таблицы (начинающиеся с '_').
    - Формирует список задач `process.<имя>`, зарегистрированных в DAG.
    - Если обнаружены неизвестные типы — направляет в `process__other`.

    Returns:
        list: Список task_id для запуска (ветвление DAG).
    """
    ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti = context.get('ti') or context.get('task_instance')
    # now = context["execution_date"].in_tz('UTC').strftime('%Y-%m-%d %H:%M:%S')

    # Получаем метрики по каждой _gp_name (кол-во, min/max id и ключи и т.п.)
    sql= f"""
        select _gp_name
            , row_cnt
            , min_id
            , max_id
            , cnt_id
            , min_key
            , max_key
            , cnt_key
            , ids
        from (
            select _gp_name 
                , groupArray(10)(distinct _gp_id)::text ids
                , count(*) wf_cnt, count(*) row_cnt
                , min(_gp_id) min_id, max(_gp_id) max_id, count(distinct _gp_id) cnt_id
                , min(_gp_key) min_key, max(_gp_key) max_key, count(distinct _gp_key) cnt_key
            from {CH_BD}.gp_ue_exchange a final
            -- left join, а не join: поток, по которому в журнале ещё нет ни одной
            -- записи IN_log, inner join отсекал, а запись появляется только после его
            -- обработки — первый пакет нового потока не грузился никогда, пока журнал
            -- не засеют вручную. Отсутствующий максимум даёт 0, и проходит любой _gp_id.
            left join (
                select b.wf_name wf, max(toInt64OrZero(JSONExtractString(b._gp_data, 'wf_key'))) as max
                from {CH_BD}.gp__exchange_log b
                join (
                    select c.wf_name, max(c._gp_ts) max_ts
                    from {CH_BD}.gp__exchange_log c
                    where c.wf_type = 'IN_log'
                    group by 1
                ) c on b.wf_name = c.wf_name and b._gp_ts = c.max_ts
                where b.wf_type = 'IN_log'
                group by 1
            ) b on a._gp_name = b.wf 
            where a._gp_id > coalesce(b.max, 0)
            group by 1
        ) a
        order by _gp_name asc
    """
    tables = select_dic(ch_hook, sql)
    ti.xcom_push(key='tables', value=json_out(tables)[:500])  # полезно для отладки и тестирования

    # downstream_task_ids — набор доступных дочерних тасков, фильтруем по ним
    downstream = context['task'].downstream_task_ids

    # игнорируем контрольную запись '_exchange_log'
    branches = ["process.process_" + row['_gp_name'] for row in tables
                if ("process.process_" + row['_gp_name']) in downstream
                    and not row['_gp_name'].startswith('_')]

    other = [row['_gp_name'] for row in tables
             if ("process.process_" + row['_gp_name']) not in downstream
                and not row['_gp_name'].startswith('_')]
    if other:
        logging.warning(f"Other tables found: {other}")
        ti.xcom_push(key='other', value=other[:500])
        # с префиксом группы: задача называется 'process.process__other', и без него
        # BranchPythonOperator не нашёл бы её среди downstream — падал бы первый же
        # пакет с незнакомым потоком, то есть ровно тот случай, ради которого разведка
        # и заведена
        branches.append('process.process__other')

    # Пустой список означал бы skip у всех process_* и, по цепочке, у end_task с его
    # one_success: Dataset не публикуется, зависимые DAG-и молча не запускаются, а сам
    # прогон при этом зелёный. Пустая ветка доводит DAG до конца честно.
    return branches or ['process.process__empty']

def do_end_task(**context):
    if context['ti'].xcom_pull(key='chk', task_ids='chk') == 'Ok':
        return 'end'
    else:
        raise AirflowFailException("Data check failed")

def do_process_other(**context):
    """
    Обрабатывает ранее неучтённые таблицы (не зарегистрированные в DAG).
    Для каждой:
    - Определяет схему полей
    - Формирует пример данных (первые 15 строк)
    - Сохраняет в XCom для анализа
    """
    ch_hook = ClickHouseHook(clickhouse_conn_id=CH_ID, database=CH_BD)
    ti = context.get('ti') or context.get('task_instance')

    other = ti.xcom_pull(key='other', task_ids='branch')
    for table in other:
        validate_identifier(table)
        logging.info(f"Find new table: {table}")
        sql = f"""
            {sql_fields(table)}
        """
        res = select_dic(ch_hook, sql)
        jsn = json_out(res)

        logging.info(f"Table {table}: {jsn}")
        ti.xcom_push(key=f'{table}', value=jsn[:500])

        fields_str, skipped = json_fields(res, on_bad='skip')
        if skipped:
            ti.xcom_push(key=f'{table}_skipped', value=skipped[:50])
        # ti.xcom_push(key=f'{table}_fields', value=fields_list[:500])

        sql = f"""
            select a._gp_ts
                {fields_str}
                , a._gp_key
                , a._gp_id
                , a._gp_hash::text _gp_hash
            from {CH_BD}.gp_ue_exchange a final
            where a._gp_name = '{table}'
            limit 15
        """
        res = select_dic(ch_hook, sql)
        jsn = json_out(res)

        logging.info(f"Table {table} sample: {jsn}")
        ti.xcom_push(key=f'{table}_sample', value=jsn[:500])

    return other

def readable_size(size_bytes, base = 1024):
    """
    Convert a file size from bytes to a human-readable string (KB, MB, GB, etc.).
    """
    if size_bytes == 0: return "0 B"

    # Define the units and the base (1024 for binary prefixes)
    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]
    base = 1024

    # Calculate the appropriate unit index using logarithm
    import math
    i = int(math.floor(math.log(size_bytes, base)))

    if i >= len(units): i = len(units) - 1

    # Calculate the human-readable value and unit
    size_value = round(size_bytes / (base ** i), 2)
    return f"{size_value} {units[i]}"

def select_dic(ch_hook, sql):
    logging.info(sql)
    res, cols = ch_hook.execute(sql , with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    else:
        return []

def json_out(rows, fmt='json'):
    """
    Преобразует список словарей в список JSON-строк.
    Безопасно обрабатывает datetime, date, None, объекты и др.
    Не мутирует исходные данные.
    """
    if not rows:
        return []

    def serialize_value(val):
        if val is None:
            return None
        elif isinstance(val, (datetime, date, timedelta)):
            return val.isoformat()
        elif isinstance(val, (int, float, str, bool)):
            return val
        elif hasattr(val, 'isoformat'):  # для datetime-like объектов
            return val.isoformat()
        elif hasattr(val, '__str__'):
            return str(val)
        else:
            return repr(val)

    def serialize_row(row):
        if isinstance(row, dict):
            return {k: serialize_value(v) for k, v in row.items()}
        else:
            return serialize_value(row)

    if fmt == 'json':
        serialized_rows = [serialize_row(row) for row in rows]
        return [json.dumps(row, ensure_ascii=False) for row in serialized_rows]
    else:
        return [str(row).replace("'", "") for row in rows]


"""
DAG: import_gp_ue_exchange

Описание:
    Цель DAG — приём и обработка универсальных сообщений из внешней системы (ПКАП) через S3 в ClickHouse.
    Сообщения передаются в формате CSV с JSON-данными и содержат метаданные о потоках, загрузках, проверках и т.д.

Основные этапы:
    1. list_files — сканирует S3 на наличие новых файлов `ue_exchange_*.csv`.
    2. import_files — загружает CSV в промежуточную таблицу `gp_ue_exchange_load`.
    3. delete_files — удаляет успешно обработанные файлы из S3.
    4. load_exchange — нормализует данные в `gp_ue_exchange`: извлекает `_gp_name`, `_gp_key`, генерирует хэши.
    5. clear_load — очищает промежуточную таблицу.
    6. chk_all — проверяет соответствие входных и выходных данных.
    7. branch — анализирует `_gp_name` и определяет, какие `process_*` задачи запускать.
    8. process_<table> — универсальная загрузка в целевые таблицы `gp_vw_*`.
    9. process__other — обрабатывает неизвестные типы сообщений для анализа.
    10. end_task — завершает DAG и сигнализирует зависимым процессам через Dataset.

Ключевые особенности:
    - Поддержка динамического ветвления: DAG адаптируется под новые типы сообщений.
    - Автоматическое создание таблиц: схема определяется из JSON.
    - Контроль целостности: проверки на дубли, объём, уникальность.
    - Логирование: все действия фиксируются в `gp__exchange_log`.
    - Отказоустойчивость: `catchup=False`, `max_active_runs=1`.

Автор: Чуркин Николай
Теги: ['DataLab', 'import', 'tfs', 'CI02420667', 'pkap', 'exchange']
"""

with DAG(
    dag_id='import_gp_ue_exchange',
    description="Загрузка универсального обмена из ПКАП",
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 20),
    schedule=[TFS_IN_DATASET],
    tags=['DataLab', 'import', 'TFS', 'CI02420667', 'PKAP', 'exchange'],
    catchup=False,
    is_paused_upon_creation=True,
    render_template_as_native_obj=True,
    max_active_runs=1,
    max_active_tasks=1,
) as dag_import:

    with TaskGroup("load", tooltip="Обработка всех таблиц") as load:
        list_files = PythonOperator(
            task_id = 'list_files',
            python_callable = do_list_files,
            do_xcom_push = True
        )

        import_files = HrpS3ToClickhouseTableOperator.partial(
            task_id='import_files',
            table_name='gp_ue_exchange_load',
            schema=CH_BD,
            fmt='CustomSeparatedWithNames',
            compression=None,
            settings={
                'format_custom_escaping_rule': 'CSV',
                'format_custom_field_delimiter': '\t',
                'format_csv_allow_double_quotes': False,
                'input_format_with_names_use_header': False,
            },
        ).expand(s3_key=list_files.output)

        chk_load = PythonOperator(
            task_id='chk_load',
            python_callable=do_chk,
            op_kwargs = {
                'chk_src': 'ue_exchange_load',
                'chk_trg': 'ue_exchange_load',
                'chk_type': 'full',
                # именно chk_prm: 'params' — зарезервированное имя контекста Airflow,
                # список уезжал в **context, а проверка молча шла с набором по умолчанию
                'chk_prm': ['raise', 'not_empty', 'only_err'],
            }
        )

        # Удаляем файлы из S3
        delete_files: object = S3DeleteObjectsOperator.partial(
            task_id='delete_files',
        ).expand(keys=list_files.output)

        load_exchange = PythonOperator(
            task_id="load_exchange",
            python_callable=do_load_exchange,
        )

        clear_load = PythonOperator(
            task_id='clear_load',
            python_callable=do_clear_load,
        )

        chk_all = PythonOperator(
            task_id='chk_all',
            python_callable=do_chk,
            op_kwargs = {
                'chk_src': '_exchange_log',
                'chk_trg': 'ue_exchange',
                'chk_type': 'right',
                'chk_prm': ['only_err', 'raise', 'not_empty', 'now'],
            }
        )

        process__exchange_log = PythonOperator(
            python_callable=process_any,
            task_id="process__exchange_log",
            # trigger_rule='all_done',
            outlets=Dataset(f'{GP_EXCHANGE}_log'),
            op_kwargs = {
                'table' : 'gp__exchange_log',
                'fields': """
                    , 'CH_IN' as wf_type
                    , JSONExtract(a._gp_data, 'wf_name', 'String') wf_name
                    , a._gp_data
                """,
                'ins_str': '(_gp_ts, wf_type, wf_name, _gp_data, _gp_key, _gp_id, _gp_hash)',
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY (wf_name, _gp_id, _gp_key, _gp_hash)
                """,
                'drop_src': True,
                'clear_trg': 'optimize',
                'chk_out': True,
                'chk_uniq': 'wf_name, _gp_id, _gp_key, _gp_hash',

            }
        )

        list_files >> import_files >> chk_load >> delete_files
        chk_load >> load_exchange >> clear_load
        load_exchange >>process__exchange_log >> chk_all

    #################################################################

    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=do_branch,
    )

    end_task = EmptyOperator(task_id='end_task', outlets=Dataset(f'{GP_EXCHANGE}'), trigger_rule='one_success')

    # end_task = BranchPythonOperator(
    #     task_id='end_task',
    #     python_callable=do_end_task,
    #     outlets=Dataset(f'{GP_EXCHANGE}'),
    #     trigger_rule='one_success',
    # )

    #################################################################

    with TaskGroup("process", tooltip="Обработка всех таблиц") as process:

        process__other = PythonOperator(
            python_callable=do_process_other,
            task_id="process__other",
        )

        # Ветка на случай, когда обрабатывать нечего: без неё ветвление возвращает
        # пустой список, и end_task не выполняется вовсе (см. do_branch).
        process__empty = EmptyOperator(task_id="process__empty")

        tasks = {
            "vw_exchange_log": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY (wf_name, id , wf_key, wf_data)
                """,
                'fields': """
                    , JSONExtract(a._gp_data, 'wf_name', 'String') wf_name
                    , JSONExtract(a._gp_data, 'id', 'Int32') id             
                    , JSONExtract(a._gp_data, 'wf_key', 'String') wf_key
                    , JSONExtract(a._gp_data, 'cnt', 'Int32') cnt
                    , JSONExtract(a._gp_data, 'sum_len', 'Int32') sum_len
                    , JSONExtract(a._gp_data, 'min_len', 'Int32') min_len
                    , JSONExtract(a._gp_data, 'max_len', 'Int32') max_len
                    , JSONExtract(a._gp_data, 'time', 'String') time
                    , JSONExtract(a._gp_data, 'completed', 'Bool') completed
                    , JSONExtract(a._gp_data, 'wf_data', 'String') wf_data
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'wf_name, id , wf_key, wf_data',
            },
            "vw_log_ctl_entity": {
                'fields': """ 
                    , JSONExtract(a._gp_data, 'id', 'Int64') as id
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'ts', 'String')) as ts
                    , JSONExtract(a._gp_data, 'url', 'String') as url
                    , JSONExtract(a._gp_data, 'name', 'String') as name
                    , JSONExtract(a._gp_data, 'path', 'String') as path
                    , JSONExtract(a._gp_data, 'storage', 'String') as storage
                    , JSONExtract(a._gp_data, 'parentid', 'Int64') as parentid
                    , JSONExtract(a._gp_data, 'msg', 'String') as msg
                """,
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY id
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'id',
            },
            "vw_log_ctl_loading": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY id
                """,
                'fields': """
                    , JSONExtract(a._gp_data, 'id', 'Int64') as id
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'ts', 'String')) as ts
                    , JSONExtract(a._gp_data, 'url', 'String') as url
                    , JSONExtract(a._gp_data, 'alive', 'String') as alive
                    , JSONExtract(a._gp_data, 'auto', 'Bool') as auto
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'start_dttm', 'String')) as start_dttm
                    , parseDateTimeBestEffortOrNull(JSONExtract(a._gp_data, 'end_dttm', 'String')) as end_dttm
                    , JSONExtract(a._gp_data, 'profile', 'String') as profile
                    , JSONExtract(a._gp_data, 'wf_id', 'Int64') as wf_id
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'status_dttm', 'String')) as status_dttm
                    , JSONExtract(a._gp_data, 'status', 'String') as status
                    , JSONExtract(a._gp_data, 'status_log', 'String') as status_log
                    , JSONExtract(a._gp_data, 'msg', 'String') as msg
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'id',
            },
            "vw_log_ctl_wf": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY id
                """,
                'fields': """
                    , JSONExtract(a._gp_data, 'id', 'Int32') as id
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'ts', 'String')) as ts
                    , JSONExtract(a._gp_data, 'url', 'String') as url
                    , JSONExtract(a._gp_data, 'profile', 'String') as profile
                    , JSONExtract(a._gp_data, 'category', 'String') as category
                    , JSONExtract(a._gp_data, 'name', 'String') as name
                    , JSONExtract(a._gp_data, 'scheduled', 'Bool') as scheduled
                    , JSONExtract(a._gp_data, 'deleted', 'Bool') as deleted
                    , JSONExtract(a._gp_data, 'singleloading', 'Bool') as singleloading
                    , JSONExtract(a._gp_data, 'engine', 'String') as engine
                    , JSONExtract(a._gp_data, 'type', 'String') as type
                    , JSONExtract(a._gp_data, 'connected', 'String') as connected
                    , JSONExtract(a._gp_data, 'eventawaitstrategy', 'String') as eventawaitstrategy
                    , JSONExtract(a._gp_data, 'wf_sched', 'String') as wf_sched
                    , JSONExtract(a._gp_data, 'param', 'String') as param
                    , JSONExtract(a._gp_data, 'statusnotifications', 'String') as statusnotifications
                    , JSONExtract(a._gp_data, 'msg', 'String') as msg
                    , JSONExtract(a._gp_data, 'wf_interval', 'String') as wf_interval
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'id',
            },
            "vw_log_workflow": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY start_id
                """,
                'fields': """
                    , JSONExtract(a._gp_data, 'start_id', 'Int32') as start_id
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'start_ts', 'String')) as start_ts
                    , JSONExtract(a._gp_data, 'start_action', 'String') as start_action
                    , JSONExtract(a._gp_data, 'workflow', 'String') as workflow
                    , JSONExtract(a._gp_data, 'end_id', 'Int32') as end_id
                    , parseDateTimeBestEffortOrNull(JSONExtract(a._gp_data, 'end_ts', 'String')) as end_ts
                    , JSONExtract(a._gp_data, 'end_action', 'String') as end_action
                    , JSONExtract(a._gp_data, 'duration', 'String') as duration
                    , JSONExtract(a._gp_data, 'message', 'String') as message
                    , JSONExtract(a._gp_data, 'rows_count', 'Int64') as rows_count
                    , JSONExtract(a._gp_data, 'period_name', 'String') as period_name
                    , JSONExtract(a._gp_data, 'period_from', 'String') as period_from
                    , JSONExtract(a._gp_data, 'period_to', 'String') as period_to
                    , JSONExtract(a._gp_data, 'load_name', 'String') as load_name
                    , JSONExtract(a._gp_data, 'load_min', 'String') as load_min
                    , JSONExtract(a._gp_data, 'load_max', 'String') as load_max
                    , JSONExtract(a._gp_data, 'key_name', 'String') as key_name
                    , JSONExtract(a._gp_data, 'key_min', 'String') as key_min
                    , JSONExtract(a._gp_data, 'key_max', 'String') as key_max
                    , JSONExtract(a._gp_data, 'zscore', 'Float32') as zscore
                    , JSONExtract(a._gp_data, 'ztest_ok', 'Bool') as ztest_ok
                    , JSONExtract(a._gp_data, 'confidence', 'Float32') as confidence
                    , JSONExtract(a._gp_data, 'key_date', 'String') as key_date
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'start_id',
            },
            "vw_swf_ctl_log": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY beg_id
                """,
                'fields': """
                    , JSONExtract(a._gp_data, 'beg_id', 'Int32') as beg_id 
                    , JSONExtract(a._gp_data, 'beg_ts', 'String')::timestamp(9)::timestamp as beg_ts 
                    , JSONExtract(a._gp_data, 'beg_action', 'String') as beg_action 
                    , JSONExtract(a._gp_data, 'duration', 'String') as duration 
                    , JSONExtract(a._gp_data, 'end_id', 'Int32') as end_id 
                    , parseDateTimeBestEffortOrNull(JSONExtract(a._gp_data, 'end_ts', 'String')) as end_ts 
                    , JSONExtract(a._gp_data, 'end_action', 'String') as end_action 
                    , JSONExtract(a._gp_data, 'beg_msg', 'String') as beg_msg 
                    , JSONExtract(a._gp_data, 'end_msg', 'String') as end_msg            
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'beg_id',
            },
            "vw_swf_chk_log": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY beg_id
                """,
                'fields': """
                    , JSONExtract(a._gp_data, 'beg_id', 'Int32') as beg_id
                    , parseDateTimeBestEffort(JSONExtract(a._gp_data, 'beg_ts', 'String')) as beg_ts
                    , JSONExtract(a._gp_data, 'beg_action', 'String') as beg_action
                    , JSONExtract(a._gp_data, 'obj', 'String') as obj
                    , JSONExtract(a._gp_data, 'sch', 'String') as sch
                    , JSONExtract(a._gp_data, 'end_id', 'Int32') as end_id
                    , parseDateTimeBestEffortOrNull(JSONExtract(a._gp_data, 'end_ts', 'String')) as end_ts
                    , JSONExtract(a._gp_data, 'duration', 'String') as duration
                    , JSONExtract(a._gp_data, 'end_action', 'String') as end_action
                    , JSONExtract(a._gp_data, 'res', 'Int32') as res
                    , JSONExtract(a._gp_data, 'msg', 'String') as msg
                    , JSONExtract(a._gp_data, 'value', 'String') as value
                    , JSONExtract(a._gp_data, 'beg_message', 'String') as beg_message
                    , JSONExtract(a._gp_data, 'end_message', 'String') as end_message
                """,
                'clear_trg': 'optimize',
                'chk_uniq': 'beg_id',
            },
            "vw_ztest": {
                'engine': f"""
                    ENGINE = {REPLICATED}ReplacingMergeTree(_gp_id)
                    ORDER BY (object, ts)
                """,
                'fields': """ * """,
                'clear_trg': 'optimize',
                'chk_uniq': 'object, ts',
            },
        }

        task_list = list()
        for task_name, task_params in tasks.items():
            task_list.append(
                PythonOperator(
                    python_callable=process_any,
                    task_id=f"process_{task_name}",
                    outlets=Dataset(f'{GP_EXCHANGE}_{task_name}'),
                    op_kwargs = task_params,
                )
            )

load >> branch >> process >> end_task
