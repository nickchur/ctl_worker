"""### Утилиты S3 (`plugins/s3_utils.py`)

Расширенные функции для работы с S3 в системе CTL.

| Функция | Описание |
|---|---|
| `get_s3_list()` | Ленивая инициализация списка бакетов по всем соединениям (кешируется) |
| `s3_set_ttl` / `s3_get_ttl` / `s3_del_ttl` | Управление жизненным циклом объектов |
| `s3_create_bucket` / `s3_get_buckets` / `s3_bucket_size` | Операции с бакетами |
| `s3_path_parse` | Разбор `conn_id://bucket/prefix/mask` |
| `s3_to_s3` / `s3_move_s3` / `s3_done` | Копирование, перемещение, `.done`-файлы |
| `s3_from_zip` | Потоковое извлечение из ZIP без сохранения на диск |
| `s3_keys` | Список объектов по маске (`*`, `?`) |
| `s3_IterStream` / `s3_gzip_stream` | Потоковые обёртки для загрузки в S3 |

> Вызывайте `get_s3_list()` вместо обращения к модульной переменной `s3_list`.
"""

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from urllib.parse import urlparse
from stream_unzip import stream_unzip # type: ignore

from  plugins.utils import readable_size, get_conns_by_type

import pendulum
from fnmatch import fnmatch
import time
import io
import os
import gzip

from logging import getLogger
logger = getLogger("airflow.task")



def s3_set_ttl(conn, bucket, days, prefix='', status='Enabled'):
    """Создаёт или обновляет lifecycle-правило '{prefix}DeleteAfter' на удаление объектов через days дней."""

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    
    # 1. Пытаемся получить текущие правила
    try:
        current_cfg = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        rules = current_cfg.get('Rules', [])
    except ClientError as e:
        # Если правил еще нет, начинаем с пустого списка
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            rules = []
        else:
            raise e

    # 2. Создаем новое правило
    new_rule_id = f'{prefix}DeleteAfter'
    new_rule = {
        'ID': new_rule_id,
        'Status': status,
        'Filter': {'Prefix': prefix},
        'Expiration': {'Days': days},
    }

    # 3. Обновляем существующее правило с таким же ID или добавляем новое
    # (Чтобы не плодить дубликаты при повторном запуске)
    rule_exists = False
    for i, rule in enumerate(rules):
        if rule.get('ID') == new_rule_id:
            rules[i] = new_rule
            rule_exists = True
            break
    
    if not rule_exists:
        rules.append(new_rule)

    # 4. Отправляем обновленный список правил
    response = client.put_bucket_lifecycle_configuration(
        Bucket=bucket,
        LifecycleConfiguration={'Rules': rules}
    )
    return response
    
def s3_get_ttl(conn, bucket):
    """Возвращает список lifecycle-правил бакета. При отсутствии правил — []."""

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    
    try:
        response = client.get_bucket_lifecycle_configuration(Bucket=bucket)
        return response.get('Rules', [])
    except ClientError as e:
        # Если правил жизненного цикла нет, AWS вернет ошибку NoSuchLifecycleConfiguration
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        raise e

def s3_del_ttl(conn, bucket):
    """Удаляет всю lifecycle-конфигурацию бакета. Игнорирует ошибку NoSuchLifecycleConfiguration."""

    hook = S3Hook(aws_conn_id=conn)
    client = hook.get_conn()
    
    try:
        # Используем специальный метод для удаления всей конфигурации
        response = client.delete_bucket_lifecycle(Bucket=bucket)
        return response
    except ClientError as e:
        # Если правил и так нет, S3 может вернуть ошибку — игнорируем её
        if e.response['Error']['Code'] == 'NoSuchLifecycleConfiguration':
            return []
        raise e

def s3_create_bucket(conn, bucket):
    """Создаёт бакет bucket в соединении conn, если он ещё не существует."""
    
    hook = S3Hook(aws_conn_id=conn)
    if not hook.check_for_bucket(bucket):
        hook.create_bucket(bucket_name=bucket)

def s3_bucket_size(conn, bucket, readable=False):
    """Обходит все объекты бакета (paginator) и возвращает {size, objs, min_date, max_date}.

    readable=True — размер и количество в человекочитаемом формате (KB/MB/K/M).
    """
    total_size = 0
    total_objs = 0
    min_date = None
    max_date = None
    
    _, paginator = s3_get_pages(conn, bucket)
    
    for page in paginator:
        if contents := page.get("Contents"):
            for obj in contents:
                last_mod = obj.get("LastModified")
                size = obj.get("Size", 0)
                
                total_size += size
                total_objs += 1
                
                # Обновляем мин/макс даты
                if last_mod:
                    if min_date is None or last_mod < min_date:
                        min_date = last_mod
                    if max_date is None or last_mod > max_date:
                        max_date = last_mod
    
    result = {
        "size": readable_size(total_size) if readable else total_size,
        "objs": readable_size(total_objs, base=1000) if readable else total_objs,
        "min_date": min_date.isoformat() if min_date else None,
        "max_date": max_date.isoformat() if max_date else None
    }
    return result

def s3_get_buckets(conn):
    """Возвращает список имён всех бакетов в соединении conn."""
    
    hook = S3Hook(aws_conn_id=conn, verify=False)
    s3_client = hook.get_conn() 
        
    response = s3_client.list_buckets()
    return [bucket['Name'] for bucket in response['Buckets']]

def s3_get_pages(conn, bucket, prefix='', page_size=1000, max_items=10000):
    """Создаёт S3-paginator для list_objects_v2 и возвращает (hook, pages_iterator)."""
    
    hook = S3Hook(aws_conn_id=conn)
    s3_client = hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=bucket, 
        Prefix=prefix,
        PaginationConfig={'MaxItems': max_items, 'PageSize': page_size}
    )
    return hook, pages

def s3_path_parse(path: str) -> dict:
    """
    Разбирает S3 путь на составляющие и определяет наличие маски.
    """
    parsed = urlparse(path)
    conn_id = parsed.scheme
    bucket = parsed.netloc
    full_path = parsed.path.lstrip('/')
    
    # Поиск спецсимволов
    star_pos = full_path.find('*')
    qmark_pos = full_path.find('?')
    is_mask = star_pos != -1 or qmark_pos != -1
    
    # Находим позицию самого первого спецсимвола
    special_chars = [pos for pos in [star_pos, qmark_pos] if pos != -1]
    wild_pos = min(special_chars) if special_chars else len(full_path)
    
    if is_mask:
        # Ищем последний слеш ДО первого спецсимвола
        last_slash = full_path.rfind('/', 0, wild_pos)
        if last_slash == -1:
            prefix = ""
            mask = full_path
        else:
            prefix = full_path[:last_slash]
            mask = full_path[last_slash+1:]
    else:
        # Если маски нет, стандартное деление на папку и файл
        prefix = os.path.dirname(full_path)
        mask = os.path.basename(full_path)

    name, ext = os.path.splitext(mask)
    
    return {
        "path":    path,
        "conn_id": conn_id,
        "bucket":  bucket,
        "key":     full_path,
        "prefix":  (prefix + '/') if prefix else '',
        "file":    mask,
        "name":    name,
        "ext":     ext,
        "is_mask": is_mask,
    }
    
class s3_IterStream(io.RawIOBase):
    """Оборачивает генератор байтов в file-like объект (RawIOBase) для boto3 upload_fileobj.

    Буферизует чанки из итератора по мере чтения. Защита от OOM:
    предупреждение при буфере > 100 МБ, MemoryError при > 512 МБ.
    total_read — суммарное количество прочитанных байт.
    """
    def __init__(self, iterator):
        self.iterator = iterator
        self.buffer = b''
        self.total_read = 0  

    def readable(self): 
        return True

    def read(self, size=-1):
        # Пороги памяти
        WARN_THRESHOLD = 100 * 1024 * 1024  # 100 MB - предупреждение в логи
        MAX_THRESHOLD = 512 * 1024 * 1024   # 512 MB - жесткая остановка (защита воркера)

        if size == -1:
            data = self.buffer + b''.join(self.iterator)
            if len(data) > MAX_THRESHOLD:
                raise MemoryError(f"🚨 Buffer exceeded MAX_THRESHOLD: {readable_size(len(data))}")
            self.buffer = b''
            self.total_read += len(data)
            return data

        while len(self.buffer) < size:
            try:
                chunk = next(self.iterator)
                if not chunk: 
                    break
                self.buffer += chunk
                
                curr_len = len(self.buffer)
                if curr_len > MAX_THRESHOLD:
                    raise MemoryError(f"🚨 Critical Memory Limit! Buffer: {readable_size(curr_len)}")
                elif curr_len > WARN_THRESHOLD:
                    logger.warning(f"⚠️ IterStream buffer is large: {readable_size(curr_len)}")
                # else:
                #     logger.debug(f"📦 IterStream buffer: {readable_size(curr_len)}")
            except StopIteration:
                break

        res = self.buffer[:size]
        self.buffer = self.buffer[size:]
        self.total_read += len(res)
        return res

def s3_gzip_stream(iterator, compresslevel=6):
    """Генератор: потоково сжимает чанки из iterator в GZIP без буферизации всего файла в памяти."""
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode='wb', compresslevel=compresslevel) as gz:
        for chunk in iterator:
            gz.write(chunk)
            res = buffer.getvalue()
            if res:
                yield res
                buffer.seek(0)
                buffer.truncate()
    yield buffer.getvalue()

def s3_to_s3(src_path, dst_path, compress=False):
    """Копирует объект S3→S3. Одно соединение без сжатия — нативная copy_object.

    В остальных случаях — потоковое копирование через воркер с опциональным GZIP.
    Сжатие отключается автоматически для .parquet/.zip/.gz.
    """
    src = s3_path_parse(src_path)
    dst = s3_path_parse(dst_path)

    if src['ext'].lower() in  ['.parquet', '.zip', '.gz']:
        if compress:
            logger.info(f"⏩ Compression disabled for {src['ext']} (already compressed)")
        compress = False
    
    s_hook = S3Hook(aws_conn_id=src['conn_id'])
    
    final_key = dst['key']
    
    # ОПТИМИЗАЦИЯ: Если одно подключение и сжимать не надо
    if src['conn_id'] == dst['conn_id'] and not compress:
        logger.info(f"🚀 S3 Native Copy {src_path} to {final_key}")
        s_hook.copy_object(
            source_bucket_key=src['key'],
            dest_bucket_key=dst['key'],
            source_bucket_name=src['bucket'],
            dest_bucket_name=dst['bucket']
        )
        return
    # В ОСТАЛЬНЫХ СЛУЧАЯХ: Потоковое копирование через воркер
    d_client = S3Hook(aws_conn_id=dst['conn_id']).get_conn()
    
    # Поток из источника
    body_stream = s_hook.get_key(src['key'], src['bucket']).get()['Body']
    
    data_iter = body_stream # По умолчанию просто поток байтов
    
    if compress:
        data_iter = s3_gzip_stream(body_stream)
        if not final_key.endswith('.gz'):
            final_key += '.gz'
            dst_path += '.gz'

    config = TransferConfig(use_threads=False)
    d_client.upload_fileobj(s3_IterStream(data_iter), dst['bucket'], final_key, Config=config)
    logger.info(f"✅ Copied {src_path} to {dst_path}")

def s3_delete(path):
    """Удаляет объект из S3 по пути вида conn_id://bucket/key."""
    s3 = s3_path_parse(path)
    hook = S3Hook(aws_conn_id=s3['conn_id'])
    hook.delete_objects(keys=s3['key'],  bucket=s3['bucket'])
    logger.info(f"🗑️ Deleted: {path}")

def s3_move_s3(src_path, dst_path, compress=False, done=False):
    """Перемещает объект S3→S3: копирует через s3_to_s3, затем удаляет источник.

    done=True или done=<путь> — дополнительно создаёт .done-маркер.
    """
    s3_to_s3(src_path, dst_path, compress=compress)
    if done:
        s3_done(done if isinstance(done, str) else dst_path )
    s3_delete(src_path)
    logger.info(f"✅ Moved: {src_path} to {dst_path}")

def s3_done(path):
    """Создаёт пустой файл-маркер {path}.done в S3."""
    s3 = s3_path_parse(path)
    hook = S3Hook(aws_conn_id=s3['conn_id'])
    hook.load_string(
            bucket_name=s3['bucket'],
            key=s3['key'] + '.done',
            string_data='',
            replace=True
        )
    logger.info(f"✅ Done-файл создан: {path}.done")
    
def s3_from_zip(src_path, dst_prefix, compress=True):
    """Потоково извлекает файлы из ZIP в S3 и загружает их в dst_prefix (без сохранения на диск).

    compress=True — каждый файл дополнительно сжимается в GZIP (кроме .parquet/.zip/.gz).
    Имена файлов декодируются из cp866. Возвращает (total_bytes, total_files, stats_dict).
    """
    start_time = time.time()
    src = s3_path_parse(src_path)
    dst = s3_path_parse(dst_prefix)
    
    s_hook = S3Hook(aws_conn_id=src['conn_id'])
    d_client = S3Hook(aws_conn_id=dst['conn_id']).get_conn()
    
    body = s_hook.get_key(src['key'], src['bucket']).get()['Body']
    config = TransferConfig(use_threads=False)

    total_uploaded_size = 0
    total_raw_size = 0
    total_count = 0

    for f_name_byte, f_size, chunks in stream_unzip(body):
        f_name = f_name_byte.decode('cp866', errors='replace').strip('/')
        if not f_name or f_name.endswith('/'): 
            continue
        
        target_key = f"{dst['key']}/{f_name}"
        
        # Если f_size известен заранее, плюсуем его
        if f_size: total_raw_size += f_size
        
        # Проверяем расширение текущего файла f_name
        not_zip = True
        if any(f_name.lower().endswith(ext) for ext in ['.parquet', '.zip', '.gz']):
            not_zip = False
            if compress:
                logger.info(f"⏩ Compression disabled for {f_name} (already compressed)")
        
        if compress and not_zip:
            final_gen = s3_gzip_stream(chunks)
            if not target_key.endswith('.gz'):
                target_key += ".gz"
        else:
            final_gen = chunks

        stream = s3_IterStream(final_gen)
        d_client.upload_fileobj(stream, dst['bucket'], target_key, Config=config)

        total_uploaded_size += stream.total_read
        total_count += 1
        
        # Если f_size был None (потоковый ZIP), считаем по факту прочитанного из IterStream (без сжатия)
        if not f_size and not compress:
            total_raw_size += stream.total_read

    # Расчет статистики
    duration = time.time() - start_time
    # Скорость в сек (по исходным данным)
    speed = total_raw_size / duration if duration > 0 else 0
    
    stats = {
        "count": readable_size(total_count, 1000),
        "size": readable_size(total_uploaded_size),
        "raw_size": readable_size(total_raw_size),
        "duration_sec": round(duration, 2),
        "speed_in_s": readable_size(speed)
    }

    logger.info(
        f"🏁 Done: {stats['count']} files | "
        f"Uploaded: {stats['size']} | "
        f"Speed: {stats['speed_in_s']} in sec"
    )

    return total_uploaded_size, total_count, stats

def s3_keys(path) -> dict:
    """Возвращает dict {key: 'дата / размер'} объектов S3, соответствующих маске в path.

    path вида conn_id://bucket/prefix/mask, поддерживает * и ?. Пропускает директории и .error.
    """
    
    s3 = s3_path_parse(path)
    hook = S3Hook(aws_conn_id=s3['conn_id'])

    # Получаем список объектов с их метаданными
    # list_objects_v2 возвращает список словарей, где есть 'Key' и 'LastModified'
    s3_client = hook.get_conn()
    paginator = s3_client.get_paginator('list_objects_v2')
    
    objects = {}
    
    for page in paginator.paginate(
            Bucket=s3['bucket'], 
            Prefix=s3['prefix'],
            PaginationConfig={'PageSize': 1000, 'MaxItems': 10_000, }
        ):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if key.endswith('/') or key.endswith('.error'): 
                    continue
                logger.debug(f"🔍 {key} {obj}")
                
                # Фильтруем по wildcard (аналог wildcard_match)
                if fnmatch(key, s3['key']):
                    dt = pendulum.instance(obj['LastModified'])
                    objects[key] = dt.format('YYYY-MM-DD HH:mm:ss zz') + f" / {readable_size(obj['Size'])}"
                    logger.info(f"✅ {key}: {objects[key]}")
    
    return objects

_s3_list = None

def get_s3_list():
    """Ленивый синглтон: список URI всех S3-бакетов по всем aws-соединениям вида 'conn_id://bucket/'.

    Выполняет сетевые запросы только при первом вызове, далее возвращает кеш.
    """
    global _s3_list
    if _s3_list is None:
        _s3_list = []
        for conn in get_conns_by_type(conn_type='aws'):
            try:
                for bucket in s3_get_buckets(conn):
                    _s3_list.append(f"{conn}://{bucket}/")
            except Exception:
                _s3_list.append(f"{conn}://")
    return _s3_list

