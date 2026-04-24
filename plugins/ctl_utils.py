"""### 🛠️ Утилиты CTL (`plugins/ctl_utils.py`)

Базовый модуль для всех DAG'ов CTL.

| Функция | Описание |
|---|---|
| `get_config()` | Ленивая загрузка `ctl_config` из Airflow Variable (кешируется) |
| `ctl_api()` | HTTP-клиент CTL с Kerberos, retry и rate_limit |
| `pg_exe()` | SQL в Airflow metadata DB через `create_session` |
| `gp_exe()` | SQL в Greenplum с retry |
| `gp_upload_s3_csv()` | Потоковая загрузка CSV/ZIP/GZ из S3 в Greenplum |
| `ctl_obj_load/save()` | JSON/YAML объекты в S3 + Airflow Variables |
| `add_note()` | Заметки в UI Airflow (DAG run / task instance) |
| `eval_delta()` | Временные смещения (`+1 hour`, `weekday=1`) |
| `rate_limit()` | Ограничение частоты вызовов API (≤100/сек) |
| `category_recursive()` | Иерархия задач по категориям CTL |

> Импортируйте `get_config`, а не `config` напрямую.
"""

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import Variable, Pool

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook # type: ignore
from airflow.utils.session import create_session
from hrp_operators.utils.kerberos_http import KerberosHttpHook # type: ignore
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError

import json
import yaml
import io
import hashlib
import time
import pendulum
from datetime import timedelta
from pprint import PrettyPrinter
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from sqlalchemy import text

from plugins.utils import query_to_dict, add_note, readable_size, pool_slots # type: ignore #, on_callback
from plugins.s3_utils import s3_IterStream

from logging import getLogger
logger = getLogger("airflow.task")


CTL_API_CALS = 100 #в секунду


def ctl_get_config():
    """Читает Airflow Variable `ctl_config` и возвращает её как dict. При отсутствии возвращает {}."""
    config = Variable.get("ctl_config", default_var={}, deserialize_json=True)
    return config

_config = None

def get_config():
    """Ленивый синглтон конфигурации. При первом вызове загружает `ctl_config` из Airflow Variables и кеширует на весь lifetime процесса."""
    global _config
    if _config is None:
        _config = ctl_get_config()
    return _config

def eval_delta(dt:str, delta:str)->str:
    """Вычисляет новое время, применяя delta к dt.

    delta — строка с одним или несколькими через запятую выражениями:
      'time=HH:MM:SS', 'date=YYYY-MM-DD', 'weekday=N' (0=пн),
      'hours=+N', 'minutes=-N', 'HH:MM:SS' (прибавить к dt).
    Возвращает строку 'YYYY-MM-DD HH:mm:ss'.
    """
    if delta is None: return dt
        
    new_dt = pendulum.parse(dt)
    # Разбиение delta на части
    for d in [d.lower().strip() for d in delta.split(',')]:
        p = [p.lower().strip() for p in d.split('=')]
        if len(p) == 1:
            p = [p.lower().strip() for p in d.split(' ')]
            if len(p) == 2:
                p[1] = p[1] if p[1][-1]=='s' else p[1]+'s'
                d = f'{p[1]}={p[0]}'
                p = [p[1],p[0]]
            else:
                d = p[0]
                p = ['', p[0]]
        if p[0] == 'time':
            kk = ['hour', 'minute', 'second', 'microsecond']
            dtd = {'hour':0, 'minute':0, 'second':0, 'microsecond':0}
            for k, v in enumerate(p[1].split(':')): dtd[kk[k]] = int(v)
            new_dt = new_dt.replace(**dtd)
        elif p[0] == 'date':
            if '.' in p[1]:
                kk, sp = ['day', 'month', 'year',], '.'
            elif '-' in p[1]:
                kk, sp = ['year', 'month', 'day',], '-'
            for k, v in enumerate(p[1].split(sp)): 
                new_dt = new_dt.replace(**{kk[k]: int(v)})
        elif p[0] == 'weekday':
            new_dt += timedelta(days=int(p[1])-new_dt.weekday())
        elif p[0] == '' and p[1]:
            kk = ['hours', 'minutes', 'seconds']
            for k, v in enumerate(p[1].split(':')): 
                new_dt += timedelta(**{kk[k]: int(v)})
        elif p[1].lstrip('+-').isdigit():
            new_dt += timedelta(**{p[0]: int(p[1])})
            
    return new_dt.format('YYYY-MM-DD HH:mm:ss') 

def logging(msg, action='info', obj='', log=None):
    """Унифицированный логгер с эмодзи-префиксами.

    Форматирует msg (str или dict) в структурированный словарь {obj, action, ...}
    и пишет в logger на нужном уровне (critical/error/warning/debug/info).
    Если передан log — дополнительно добавляет заметку в Airflow через add_note.
    """
    if isinstance(msg, dict):
        log_msg = { 'obj': obj, 'action': action, **msg }
    else:
        log_msg = { 'obj': obj, 'action': action, 'msg': msg }
        
    if action == 'critical':
        logger.critical(f'🛑 {log_msg}')
    elif action == 'error': 
        logger.error(f'❌ {log_msg}')
    elif action == 'warning': 
        logger.warning(f'⚠️ {log_msg}')
    elif action == 'debug': 
        logger.debug(f'🐞 {log_msg}')
    else:
        logger.info(f'ℹ️ {action}: {log_msg}')
    
    if log:
        add_note(log_msg, log)

def log_retry_attempt(retry_state):
    """Tenacity before_sleep callback: логирует номер попытки, исключение и задержку до следующей попытки."""
    logger.warning(
        f"⚠️ Попытка {retry_state.attempt_number} не удалась. "
        f"Ошибка: {retry_state.outcome.exception()}. "
        f"Следующая попытка через {retry_state.next_action.sleep} сек..."
    )

def pg_exe(sql='select 1', timeout=300):
    """Выполняет SQL в Airflow DB (PostgreSQL) через create_session и возвращает список dict-строк."""
    ts_start = time.time()
    with create_session() as session:
        session.execute(text("SET LOCAL search_path = main"))
        if timeout:
            session.execute(text(f"SET LOCAL statement_timeout = '{timeout}s'"))
        result = session.execute(text(sql))
        cols = [col for col in result.keys()]
        records = [dict(zip(cols, row)) for row in result.fetchall()]
    logger.info(f"✅ {len(records)} records loaded in {time.time() - ts_start:.2f}s")
    return records


import threading

_last_call_time = {}
_lock = threading.Lock()

def rate_limit(pool_name='ctl_pool', ctl_api_calls=100):
    """Thread-safe throttle: гарантирует не более ctl_api_calls вызовов в секунду на pool_name.

    При необходимости блокирует поток на недостающий интервал (sleep).
    Использует глобальный dict _last_call_time и threading.Lock.
    """
    # global _last_call_time
    
    # Интервал между запросами (например, 1/100 = 0.01 сек)
    # Если нужно учитывать кол-во слотов, можно оставить вашу логику
    min_interval = 1.0 / ctl_api_calls 

    with _lock:
        current_time = time.time()
        last_time = _last_call_time.get(pool_name, 0)
        elapsed = current_time - last_time
        
        if elapsed < min_interval:
            wait_time = min_interval - elapsed
            logger.debug(f"🕒 Throttling {pool_name}: ждем {wait_time:.3f} сек")
            time.sleep(wait_time)
            # Обновляем время после сна
            _last_call_time[pool_name] = current_time
        else:
            _last_call_time[pool_name] = current_time
    

@retry(
    stop=stop_after_attempt(3), # Максимум 3 попытки
    wait=wait_exponential(multiplier=1, min=1, max=5), # Паузы 1с, 2с, 4с
    # retry=retry_if_exception_type(Exception), # Ре траим на любые ошибки, кроме AirflowFailException
    retry=retry_if_exception_type((ReadTimeout, ConnectTimeout, ConnectionError)),
    before_sleep=log_retry_attempt,
    reraise=True
)
def ctl_api(url='/v5/api/info', method='GET', data={}, json={}, timeout=None, check_response=False, skip=True):
    """HTTP-клиент к CTL API через KerberosHttpHook с rate-limiting и retry.

    4xx → AirflowSkipException (skip=True) или AirflowFailException (skip=False).
    5xx / таймаут → retry tenacity (до 3 раз, пауза 1–5 сек).
    GET-запросы (кроме /statval, /tmpl) логируются в Greenplum через pr_log_ctl.
    Возвращает распарсенный JSON или текст ответа.
    """
    
    con_id = get_config().get('conns', {}).get('ctl', {}).get('conn_id', 'ctl')
    if not timeout:
        timeout = get_config().get('conns', {}).get('ctl', {}).get('timeout', (10, 30))
        
    method = method.upper()
    msg = { 'method': method, 'url': url, 'data': data, 'json': json }
    logger.debug(f"🔍\n"+PrettyPrinter(indent=4, width=80, compact=True).pformat(msg))
    
    hook = KerberosHttpHook(method=method, http_conn_id=con_id)
    rate_limit() # Ограничение нагрузки (Throttling)
    ts = time.time()
    try:
        response = hook.run(endpoint=url, data=data, json=json,
            headers={'Accept': 'application/json', 'Content-Type': 'application/json'},
            extra_options = {'timeout': timeout,'check_response': check_response },)
        response.raise_for_status()
        logger.debug(f"{time.time()-ts:.2f} sec ✅ {method} {url} {response.status_code}")
    except Exception as e:
        status_code = getattr(getattr(e, 'response', None), 'status_code', None)
        response = getattr(e, 'response', None)
        resp_text = getattr(response, 'text', str(e))
        
        # Логируем ошибку с деталями
        error_msg = f"{resp_text[:200]} ❌ Error: {status_code} HTTP {method} {url}"
        logger.error(error_msg)
        
        # Если это ошибка клиента (4xx, кроме таймаутов 408/429), повторы не помогут
        if resp_text.startswith('4') or 300 <= status_code < 500:
            add_note(error_msg, level='Task,DAG', title='Ошибка API')
            if skip:
                raise AirflowSkipException(error_msg)
            else:
                raise AirflowFailException(error_msg)
        else:
            # Для остальных случаев (5xx или таймаут) кидаем исключение, чтобы Tenacity сделал retry
            raise e # Пробрасываем исключение дальше для Tenacity
    
    try:
        out = response.json()
    except:
        out = response.text
    
    if method == 'GET': 
        if 'tmpl' not in url and 'statval' not in url and not pool_slots('gp_pool'):
            
            ts = time.time()
            url = url[7:] if url.startswith('/v5/api') or url.startswith('/v4/api') else url
            try:
                import json
                gp_exe(f"select pr_log_ctl('{url}', $jsn${json.dumps(out)}$jsn$)")
                logger.debug(f"{time.time()-ts:.2f} sec ✅ Записан лог в GP")
            except Exception as e:
                logger.error(f"{time.time()-ts:.2f} sec ❌ Ошибка записи лога в GP: {str(e)}")
        else:
            logger.debug(f"✅ Запрос к API без логирования")

    return out


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30),
    # Ретраим только на системные/сетевые ошибки БД
    # Не ретраим на SyntaxError или ProgrammingError (ошибки в SQL)
    retry=retry_if_exception_type((OperationalError, InterfaceError, ConnectionError)),
    before_sleep=log_retry_attempt,
    reraise=True
)
def gp_exe(sql, val=None, ti=None, autocommit=True, timeout=None):
    """Выполняет SQL в Greenplum и возвращает первое значение первой строки (или None).

    Устанавливает statement_timeout и search_path на уровне сессии.
    Если передан ti — пушит gp_pid в XCom для возможного pg_cancel_backend.
    Retry на OperationalError/InterfaceError; DatabaseError → AirflowFailException.
    """
    
    # if not gp_hook:
    gp = get_config().get('conns', {}).get('gp', {})
    gp_schema = gp.get('gp_schema', 's_grnplm_vd_hr_edp_srv_wf')
    timeout = timeout or gp.get('timeout') or  60*10 # 10 минут
    timeout = timeout * 1000 # в миллисекундах
    
    gp_hook = PostgresHook(postgres_conn_id=gp['conn_id'])
    
    # if isinstance(gp_hook, str):
    #     gp_hook = PostgresHook(postgres_conn_id=gp_hook)
        
    ts_start = time.time()
    try:
        with gp_hook.get_conn() as conn:
            conn.autocommit = autocommit
            with conn.cursor() as cur:
                # Установка таймаута на уровне сессии GP
                cur.execute(f"set statement_timeout = {timeout}; set search_path to {gp_schema}")
                if ti: 
                    cur.execute(f"SELECT pg_backend_pid()")
                    pid = dict(pid=cur.fetchone()[0], timeout=timeout/1000, autocommit=autocommit)
                    ti.xcom_push(key='gp_pid', value=pid)
                
                logger.debug(f"🚀 Выполнение SQL (timeout {timeout/1000} сек.)...")
                cur.execute(sql, val)
                
                res = cur.fetchone()
                duration = time.time() - ts_start
                logger.debug(f"✅ SQL выполнен успешно за {duration:.2f} сек.")
                
                return res[0] if res else None

    except (OperationalError, InterfaceError) as e:
        # Эти ошибки перехватит tenacity и сделает retry
        logger.error(f"📡 Сетевая ошибка Greenplum: {str(e)}")
        raise e
    except DatabaseError as e:
        # Ошибки синтаксиса или прав (SQL Error) — ретрай тут не поможет
        logger.error(f"❌ Критическая ошибка SQL (ретрай пропущен): {str(e)}")
        raise AirflowFailException(f"GP SQL Error: {e}")
    except Exception as e:
        logger.error(f"💥 Непредвиденная ошибка в gp_exe: {str(e)}")
        raise


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=5, max=30),
    retry=retry_if_exception_type((OperationalError, InterfaceError, ConnectionError)),
    before_sleep=log_retry_attempt,
    reraise=True
)
def gp_upload_s3_csv(table, key, options=None, gp_schema=None, truncate=False, timeout=None):
    """Потоково загружает CSV/ZIP/GZ из S3 в таблицу Greenplum через COPY FROM STDIN.

    ZIP-архивы распаковываются через stream_unzip (cp866), GZ — через gzip.GzipFile.
    Диск не используется. Retry на сетевые ошибки (до 3 раз, пауза 5–30 сек).
    """
    from contextlib import closing
    
    # Конфиги и параметры
    gp = get_config()['conns']['gp']
    gp_schema = gp_schema or 's_grnplm_vd_hr_edp_dia'
    timeout = (timeout or 60 * 30) * 1000
    options = options if options else "CSV HEADER DELIMITER ',' "

    s3 = get_config()['conns']['files']
    bucket = s3['bucket']
    
    # Инициализируем хуки внутри, чтобы при ретрае они пересоздавались
    gp_hook = PostgresHook(postgres_conn_id=gp['conn_id'])
    s3_hook = S3Hook(aws_conn_id=s3['conn_id']) # Или ваш ID
    
    # Получаем объект S3 
    s3_obj = s3_hook.get_key(key, bucket_name=bucket)
    # body = s3_obj.get()['Body']
    # Используем closing для автоматического закрытия стрима S3
    with closing(s3_obj.get()['Body']) as body:
        add_note(f"📥 Загрузка файла {key}")
            
        # Декомпрессия
        if key.lower().endswith(".zip"):
            from stream_unzip import stream_unzip # type: ignore

            for f_name_byte, f_size, chunks in stream_unzip(body):
                f_name = f_name_byte.decode('cp866', errors='replace').strip('/')
                if not f_name or f_name.endswith('/'): 
                    continue
                add_note(f"📥 Файл из архива {f_name} ({readable_size(f_size)})")

                csv_stream = s3_IterStream(chunks)
                gp_from_stream(gp_hook, csv_stream, table, options, gp_schema, truncate, timeout )

        elif key.lower().endswith(".gz"):
            import gzip
            csv_stream = gzip.GzipFile(fileobj=body, mode="rb")
            gp_from_stream(gp_hook, csv_stream, table, options, gp_schema, truncate, timeout )
        else:
            csv_stream = body
            gp_from_stream(gp_hook, csv_stream, table, options, gp_schema, truncate, timeout )


def gp_from_stream(gp_hook, csv_stream, table, options=None, gp_schema=None, truncate=False, timeout=None):
    """Выполняет COPY gp_schema.table FROM STDIN из file-like объекта csv_stream.

    При truncate=True предварительно очищает таблицу. Логирует количество строк и время.
    OperationalError/InterfaceError пробрасывается для retry в вызывающей функции.
    """
    ts_start = time.time()
    try:
        with gp_hook.get_conn() as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f"SET statement_timeout = {timeout};")
                
                if truncate:
                    cur.execute(f"TRUNCATE TABLE {gp_schema}.{table};")
                    add_note(f"🧹 Таблица {gp_schema}.{table} очищена.", level='Task') #, DAG
                
                # Используем полное имя таблицы
                sql_copy = f"COPY {gp_schema}.{table} FROM STDIN WITH {options}"
                
                add_note(f"🚀 Начинаю загрузку в {gp_schema}.{table}", level='Task')
                cur.copy_expert(sql_copy, csv_stream)
                
                rows = readable_size(cur.rowcount,1000)
                
                duration = time.time() - ts_start
                add_note(f"✅ Загрузка завершена успешно {rows} за {duration:.2f} сек.", level='Task')
                
                return rows

    except (OperationalError, InterfaceError) as e:
        add_note(f"📡 Сетевая ошибка Greenplum (будет ретрай): {str(e)}", level='Task,DAG')
        raise e # Tenacity поймает это
    except Exception as e:
        # Для AirflowFailException ретраи не делаются, задача сразу падает в Fail
        msg = f"❌ GP Upload Error: {e}"
        add_note(msg, level='Task,DAG')
        raise AirflowFailException(msg)


@retry(
    stop=stop_after_attempt(3), # Максимум 3 попытки
    wait=wait_exponential(multiplier=1, min=2, max=10), # Паузы 2с, 4с, 8с
    retry=retry_if_exception_type(Exception), # Ре траим на любые ошибки, кроме AirflowFailException
    before_sleep=log_retry_attempt,
    reraise=True
)
def ctl_obj_load(key):
    """Загружает объект по ключу: сначала из Airflow Variable, затем из S3 (JSON).

    Возвращает dict или {} при отсутствии объекта.
    Retry на любые исключения (до 3 раз, пауза 2–10 сек).
    """
    
    key_ext = f'{key}.json'

    data  = Variable.get(key, default_var={}, deserialize_json=True)
    if data:
        logger.info(f'Объект {key} загружен из Airflow.')
        # content = json.dumps(data, indent=4, ensure_ascii=False).encode('utf-8')
        # new_md5 = hashlib.md5(content).hexdigest()
        return data #, new_md5
    
    s3 = get_config().get('conns', {}).get('s3', {})
    bucket = s3.get('bucket', 'edpetl-ctl')

    if bucket:
        hook = S3Hook(aws_conn_id=s3.get('conn_id', 's3'))
        try:
            # Получаем объект из S3
            s3_obj = hook.get_key(key=key_ext, bucket_name=bucket)
            if not s3_obj:
                return {} #, None

            # Читаем метаданные (ETag)
            response = s3_obj.get()
            etag = response.get('ETag', '').strip('"')

            # Читаем тело файла через потоковый ридер
            with io.TextIOWrapper(response['Body'], encoding='utf-8') as reader:
                logger.info(f"Объект {key} успешно загружен из S3.")
                data = json.load(reader)
            response['Body'].close()  # явно закрываем
            return data #, etag

        except Exception as e:
            logger.error(f"Ошибка при чтении {key_ext}: {e}")
            # Возвращаем пустой словарь и None в случае битого JSON или отсутствия файла
            return {} #, None
    else:
        logger.error(f"Бакет для {key} не указан в конфигурации.")
        return {}

    
@retry(
    stop=stop_after_attempt(3), # Максимум 3 попытки
    wait=wait_exponential(multiplier=1, min=2, max=10), # Паузы 2с, 4с, 8с
    retry=retry_if_exception_type(Exception), # Ре траим на любые ошибки, кроме AirflowFailException
    before_sleep=log_retry_attempt,
    reraise=True
)
def ctl_obj_etag(key, ext='json'):
    """
    Возвращает только ETag (md5) объекта из S3 без загрузки тела файла.
    """
    from botocore.exceptions import ClientError

    s3 = get_config().get('conns', {}).get('s3', {})
    bucket = s3.get('bucket', 'edpetl-ctl')

    hook = S3Hook(aws_conn_id=s3.get('conn_id', 's3'))
    # key_ext = f'{key}.json'
    key_ext = f'{key}.{ext}'
    try:
        # head_object возвращает только заголовки (метаданные)
        response = hook.get_conn().head_object(Bucket=bucket, Key=key_ext)
        # ETag в S3 всегда в двойных кавычках, например '"d41d8cd9..."'. Очищаем их.
        return response.get('ETag', '').strip('"')
    
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            logger.warning(f"Файл {key_ext} не найден в бакете {bucket}")
            return None
        raise e


@retry(
    stop=stop_after_attempt(3), # Максимум 3 попытки
    wait=wait_exponential(multiplier=1, min=2, max=10), # Паузы 2с, 4с, 8с
    retry=retry_if_exception_type(Exception), # Ре траим на любые ошибки, кроме AirflowFailException
    before_sleep=log_retry_attempt,
    reraise=True
)
def ctl_obj_save(key, data, var=False, ext='json'):
    """
    Сохраняет объект в S3 и обновляет переменную Airflow, если указано.
    """
    
    s3 = get_config().get('conns', {}).get('s3', {})
    bucket = s3.get('bucket', 'edpetl-ctl')

    if bucket:
        hook = S3Hook(aws_conn_id=s3.get('conn_id', 's3'))
        key_ext = f'{key}.{ext}'
            
        # 1. Готовим контент в памяти
        if ext in ['json','jsn']:
            content = json.dumps(data, indent=4, ensure_ascii=False).encode('utf-8')
        elif ext in ['yml','yaml']:
            content = yaml.safe_dump(data, sort_keys=False).encode('utf-8')
        elif ext in ['txt','log']:
            content = data.encode('utf-8')
        else:
            content = data
            
        new_md5 = hashlib.md5(content).hexdigest()

        # 2. Проверяем текущий ETag в S3 (boto3 хранит MD5 в ETag для обычных загрузок)
        if hook.check_for_key(key_ext, bucket_name=bucket):
            s3_obj = hook.get_key(key_ext, bucket_name=bucket)
            # ETag обычно возвращается в кавычках, например '"d41d8cd9..."'
            current_etag = s3_obj.e_tag.strip('"')
            
            # Примечание: ETag == MD5 только для обычных (не multipart) загрузок
            if current_etag == new_md5:
                logger.info(f"Объект {key_ext} не изменился (MD5 match). Пропускаем загрузку.")
                return False

        # 3. Если хеши разные или файла нет — загружаем
        with io.BytesIO(content) as buffer:
            hook.load_file_obj(
                file_obj=buffer,
                key=key_ext,
                bucket_name=bucket,
                replace=True,
                # extra_args={ 
                #     "Cache-Control": "max-age=604800",  # 7 дней в секундах
                #     "Expires": pendulum.now().add(days=7).to_rfc1123_string() 
                # },
            )
    else:
        logger.warning(f"Бакет для {key} не указан в конфигурации.")
    
    if var:
        msg ={
            'ts': pendulum.now(get_config()['tz']).format('YYYY-MM-DD HH:mm:ss'),
            'len': len(data),
            'size': readable_size(len(str(data))),
            # 'md5': new_md5,
        }
        Variable.set(key, data, description=str(msg), serialize_json=True) 
        logger.info(f"Переменная {key} успешно обновлена в Airflow.")
            
    logger.info(f"Объект {key_ext} успешно обновлен в S3.")
    return True


def category_recursive(cat_id=1):
    """Рекурсивно строит TaskGroup с EmptyOperator-задачами для workflows категории cat_id.

    Вложенные TaskGroup создаются для дочерних категорий. Используется при генерации DAG-структуры CTL.
    """
    from airflow.utils.task_group import TaskGroup
    from airflow.operators.empty import EmptyOperator
    from airflow import Dataset
    
    cat_dict = ctl_obj_load('ctl_categories')
    wfs_dict = ctl_obj_load('ctl_workflows')
    cat_name = cat_dict[cat_id]['name'].split('.')[-1]

    with TaskGroup(
        group_id=cat_name, 
        default_args={  
            'pool': 'ctl_pool',
            # 'retries':100,
            # 'retry_delay': pendulum.timedelta(seconds=10),
            # 'retry_exponential_backoff':True,
            # 'max_retry_delay': pendulum.timedelta(minutes=15), 
            # 'max_active_runs': 1, 
            # 'priority_weight':1000,
        }
    ) as group:

        for wf in [w for w in wfs_dict.values() if w['category'] == cat_id]:
            EmptyOperator(
                task_id=wf['name'],
                outlets=[Dataset(f'CTL/wf/{wf["id"]}/{wf["name"]}')],
            )
        
        children = [c for c in cat_dict.values() if c['parent'] == cat_id]
        for child in children:
            category_recursive(child['id'])

    return group

    # cat_root_id = [c['id'] for c in cat_dict.values() if c['name'] == config['root_category']][0]
    # chk_conn >> ctl_sensor >> category_recursive(cat_root_id) >> end

