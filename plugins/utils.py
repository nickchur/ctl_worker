"""###🛠️ Утилиты Airflow (`plugins/utils.py`)

Вспомогательные функции, используемые во всех DAG'ах CTL.

| Функция | Описание |
|---|---|
| `add_note()` | Структурированные заметки в Airflow UI (DAG/Task) |
| `on_callback()` | Обработчик событий success/failure/retry |
| `pool_slots()` / `get_current_load()` | Управление слотами пула |
| `md5_hash()` | Хеш JSON-совместимых структур |
| `readable_size()` / `readable()` | Форматирование байтов, datetime, timedelta |
| `str2timedelta()` | Парсинг строки в timedelta (`'minutes=5'`) |
| `safe_eval()` | Безопасное вычисление математических выражений |
| `get_conns_by_type()` / `get_conn()` | Получение соединений по типу |
| `query_to_dict()` | SQL → список словарей (Greenplum) |
| `update_dag_pause()` | Программная пауза/возобновление DAG'а |
"""

from airflow.models import DagModel, TaskInstance, Pool
from airflow.utils.session import provide_session, create_session
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from airflow.operators.python import get_current_context

from pprint import PrettyPrinter
from datetime import timedelta, datetime
import pendulum
import json
import hashlib

from logging import getLogger
logger = getLogger("airflow.task")

MAX_NOTE_LEN = 1000


# === Утилиты ===
def sign(x):
    return (x > 0) - (x < 0)

def md5_hash(data):
    return hashlib.md5(json.dumps(data, sort_keys=True, default=str).encode('utf-8')).hexdigest()

@provide_session
def pool_slots(pool_name, slots=None, session=None):

    pool = session.query(Pool).filter(Pool.pool == pool_name).first()

    if isinstance(slots, (tuple, list)):
        min_val, max_val = slots[0], slots[-1]
        load = get_current_load(pool_name, pool=False, session=session)
        
        # Считаем текущую потребность
        demand = load['queued'] + load['running'] + load['scheduled']
        
        # Берем текущие слоты (если пул существует) или min_val
        current_slots = pool.slots if pool else min_val
        
        # Шагаем в сторону demand: +1, -1 или 0 (если demand == current_slots)
        delta = demand - current_slots + 1
        new_slots = current_slots + (int(delta/10) or sign(delta))
        
        # Ограничиваем диапазоном [min, max]
        slots = max(min_val, min(max_val, new_slots))
        
        
    if pool:
        # Если slots не передан, берем из существующего пула
        if slots is None:
            slots = pool.slots
        else:
            # Если передан, обновляем (с защитой от отрицательных чисел)
            slots = 0 if slots < 0 else slots
            pool.slots = slots
    else:
        # Если пула нет, создаем новый (минимум 0)
        slots = 0 if (slots is None or slots < 0) else slots
        new_pool = Pool(
            pool=pool_name,
            slots=slots,
            description='CTL_worker',
            include_deferred=False
        )
        session.add(new_pool)
        logger.warning(f'Pool {pool_name} created')
    
    session.commit()  # Autocommit
    
    if slots > 0:
        logger.info(f'Pool {pool_name} set to {slots} slots')
    else:
        logger.warning(f'Pool {pool_name} deactivated')
    
    return slots

@provide_session
def get_current_load(pool_name, pool=True, session=None):
    # Считаем задачи в очереди и в работе для этого пула
    queued = session.query(TaskInstance).filter(
        TaskInstance.pool == pool_name, 
        TaskInstance.state == State.QUEUED
    ).count()
    
    running = session.query(TaskInstance).filter(
        TaskInstance.pool == pool_name, 
        TaskInstance.state == State.RUNNING
    ).count()

    scheduled = session.query(TaskInstance).filter(
        TaskInstance.pool == pool_name, 
        TaskInstance.state == State.SCHEDULED
    ).count()
    
    res = dict(queued=queued, running=running, scheduled=scheduled)
    if pool: res['pool_slots'] = pool_slots(pool_name)

    return res
    
def add_note(msg, context=None, level='task', add=True, title='', compact=False):

    if not context:
        context = get_current_context()
        
    if isinstance(msg, dict) and len(msg) == 1:
        t, msg = next(iter(msg.items()))
        title += str(t) + (f' ({len(msg)})' if isinstance(msg, (dict, list, tuple, set)) else '')
                    
    if type(msg) is not str:
        # Настройка для красоты:
        # indent=4 — отступ
        # width=80 — стараться не делать строку длиннее 80 символов
        # compact=False — каждое значение на новой строке
        msg = PrettyPrinter(indent=4, compact=compact).pformat(msg).replace("'", '')
        msg = '```\n' + msg + '\n```'

    logger.info(f"📝 Note added to {level} {title}:\n{msg}")
    
    # Используем новый контекстный менеджер для чистой сессии
    with create_session() as session:
        for l in list(set(level.upper().split(',')))[:2]:
            new_note = msg.strip()
            # Определяем объект (DagRun или TaskInstance)
            if l == 'DAG':
                obj = session.merge(context['dag_run'])
            else:
                obj = session.merge(context['task_instance'])
            
            # Логика заголовка
            if title:
                import unicodedata
                # Если первый символ не эмодзи, то добавляем эмодзи
                if not unicodedata.category(title[0]) == 'So':
                    title = "📝 " + title

                new_note = f"{title}\n---\n{new_note}"

            if obj.note and obj.note.startswith(new_note[:MAX_NOTE_LEN]):
                continue
                
            # Логика склейки заметки
            # if add and obj.note:
            if add:
                new_note = f"{ new_note}\n\n---\n{obj.note if obj.note else '' }"
                
            # Лимит длины
            obj.note = new_note[:MAX_NOTE_LEN]

def on_callback(context, level=None): return _on_callback(context, level)

default_args = {
    'owner': 'EDP.ETL',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['p1080@sber.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'pool': 'default_pool',
    # 'xcom_push': True,  
    # 'execution_timeout': timedelta(minutes=15),  
    'on_failure_callback': on_callback,
    # 'on_success_callback': on_callback,
    # 'on_retry_callback': on_callback,
    # 'on_execute_callback': None,
}

def _on_callback(context, level=None):
    """ 
    Обработчик события on_callback
    """
    from airflow.utils.state import TaskInstanceState
    import pendulum
    import pendulum
    
    ti = context.get('task_instance')
    dag_run = context.get('dag_run')
    dag_id = ti.dag_id
    dag_state = dag_run.state
    
    if not level:
        if dag_state.lower() in ['success', 'failed'] or not context.get('task'):
            level = 'DAG'
        else:
            level = 'task'
    
    
    if level == 'DAG':
        tis = dag_run.get_task_instances()
        finished_tis = [ti for ti in tis if ti.end_date is not None ][-10:]
        last_ti = sorted(finished_tis, key=lambda x: x.end_date, reverse=True)[0] if finished_tis else []
        # failed_tis = [ti for ti in tis if ti.state == 'failed']
        # first_fail = sorted(failed_tis, key=lambda x: x.end_date)[0] if failed_tis else []
    
        task = last_ti
    else:
        task = ti
        
    task_id = task.task_id
    map_index = task.map_index
    map_ind_str = getattr(task, 'rendered_map_index', '')
    try_number = task.try_number
    state = task.state
        
    msg = context.get('exception') # or getattr(ti, 'error', None) or "No exception trace available"

    # if state == TaskInstanceState.FAILED:
    # elif state == TaskInstanceState.SUCCESS:
    # elif state == TaskInstanceState.UP_FOR_RETRY:
        
    # if not msg:
    #     xcom = ti.xcom_pull(task_ids=ti.task_id)
    #     msg = str(xcom) if xcom else ""

        
    map_str = f"   *Map Index*: ({map_index}) **{map_ind_str}**" if str(map_index) != '-1' else ""
    try_str = f"   *Try number*: **{try_number}**" if try_number > 1 else ""
    msg_str = f"```\n{str(msg)[:1000]}\n```\n" if msg else ""
    
    task_msg = f'❌ FAILED' if state.lower() == 'failed' else f'✅ SUCCESS' if state.lower() == 'success' else state.upper()
    
    message = (
        f"*{pendulum.now().format('DD.MM.YYYY HH:mm:ss zz')}*\n\n"
    )
    
    if level == 'DAG':
        dag_msg = f'❌ FAILED' if dag_state.lower() == 'failed' else f'✅ SUCCESS'
        message += f"*DAG:* **{dag_id}**: **{dag_msg}**\n\n" 
    else:
        message += (
            f"*Task:* **{task_id}**: **{task_msg}**\n\n"
            f"{try_str}{map_str}\n\n"
            f"{msg_str}" 
        )            
    
    add_note(message, context, level, add=True)
    
    if state == TaskInstanceState.SUCCESS:
        logger.info(message)
    elif state == TaskInstanceState.FAILED:
        logger.error(message)
        if level != 'DAG':
            add_note(message, context, level='DAG', add=True)
        if level != 'DAG':
            add_note(message, context, level='DAG', add=True)
    else:
        logger.warning(message)

def query_to_dict(gp_hook, sql, timeout=300):
    with gp_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            if timeout:
                sql = f"SET statement_timeout TO {timeout*1000}; " + sql
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
    return [dict(zip(cols, row)) for row in rows]

def get_conns_by_type(conn_type='aws'):
    from airflow.configuration import get_custom_secret_backend
    from airflow.models import Connection
    
    backend = get_custom_secret_backend()
    
    if not hasattr(backend, '_local_connections'):
        return []
    local_conn: dict[str, Connection] = backend._local_connections
    
    if conn_type:
        return [id for id,conn in local_conn.items() if conn.conn_type == conn_type.lower()]
    else:
        return [id for id,conn in local_conn.items()]

def get_conn(conn_id):
    from airflow.configuration import get_custom_secret_backend
    from airflow.models import Connection
    
    backend = get_custom_secret_backend()
    
    if not hasattr(backend, '_local_connections'):
        return []
    local_conn: dict[str, Connection] = backend._local_connections
    
    connection = local_conn.get(conn_id)
    return {
                'id': conn_id,
                'host': connection.host,
                'port': connection.port,
                'schema': connection.schema,
                'description': connection.description if connection.description else "No description",
                'extra_keys': connection.extra,
            }

def readable_size(size_bytes, base=1024):
    """
    Конвертирует размер в читаемую строку. Поддерживает отрицательные числа.
    """
    if base == 1024:
        units = ["B", "KB", "MB", "GB", "TB", "PB"]
    else:
        units = ["ед", "тыс", "млн", "млрд", "трлн", "птлн"]

    if not size_bytes or size_bytes == 0:
        return f"0 {units[0]}"

    import math
    
    # Запоминаем знак и работаем с модулем числа
    sign = "-" if size_bytes < 0 else ""
    size_bytes = abs(size_bytes)

    # Рассчитываем индекс юнита
    i = int(math.floor(math.log(size_bytes, base)))
    if i >= len(units): i = len(units) - 1
    if i < 0: i = 0

    size_value = round(size_bytes / (base ** i), 2)
    
    return f"{sign}{size_value} {units[i]}"

def readable(value, base=1024, indent=4, jsn=False)->str:
    if isinstance(value, str):
        return value
    elif isinstance(value, (int, float)): 
        return readable_size(value, base=base)
    elif isinstance(value, datetime):
        return pendulum.instance(value).format("DD.MM.YYYY HH:mm:ss zz")
    elif isinstance(value, timedelta):
        return str(value)
    else:
        if jsn:
            return json.dumps(value, indent=indent, default=str)
        else:
            return PrettyPrinter(indent=indent).pformat(value)
        

def str2timedelta(delta):
    from datetime import timedelta
    res = timedelta()
    aliases = {
        'd': 'days', 'day': 'days',
        'h': 'hours', 'hr': 'hours',
        'm': 'minutes', 'min': 'minutes',
        's': 'seconds', 'sec': 'seconds'
    }
    for s in delta.split(','):
        if '=' not in s: continue
        
        k = s.split('=')[0].strip().lower()
        v = s.split('=')[1].strip()
        
        k = aliases.get(k, k)
        
        if k not in [
            'days', 'hours', 'minutes', 'seconds', 
            'microseconds', 'milliseconds', 'weeks'
        ]: continue
        
        try: v = int(v)
        except: continue
        res += timedelta(**{k:v})
    
    return res

@provide_session
def update_dag_pause(dag_id, paused=True, session=None):
    session.query(DagModel).filter(DagModel.dag_id == dag_id).update({"is_paused": paused})

# rand = int(safe_eval(test_mode_str) * 45*60)
def safe_eval(expr):
    """Безопасный аналог eval() для математических выражений.
    Поддерживает: +, -, *, /, **, (), числа, строки.
    Не выполняет вызовы функций или доступ к переменным.
    
    Пример: safe_eval("2 ** 3 + 1") → 9
    
    :param expr: строка с выражением
    :return: результат вычисления
    :raises ValueError: если выражение содержит запрещённые операции
    """    
    import ast
    import operator as op

    tree = ast.parse(expr.strip(), mode='eval')
    
    # Разрешённые операции
    ALLOWED_OPS = {
        ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow,
        ast.USub, ast.UAdd, ast.Constant, ast.Num, ast.Str
    }
    
    op_map = {
        ast.Add: op.add, ast.Sub: op.sub, ast.Mult: op.mul,
        ast.Div: op.truediv, ast.Pow: op.pow, ast.USub: op.neg
    }

    def _eval(node):
        if isinstance(node, ast.Constant):  # Python 3.8+
            return node.value
        elif isinstance(node, ast.Num):     # Python < 3.8
            return node.n
        elif isinstance(node, ast.Str):     # строка
            return node.s
        elif isinstance(node, ast.BinOp) and type(node.op) in ALLOWED_OPS:
            return op_map[type(node.op)](_eval(node.left), _eval(node.right))
        elif isinstance(node, ast.UnaryOp) and type(node.op) in ALLOWED_OPS:
            return op_map[type(node.op)](_eval(node.operand))
        else:
            raise ValueError(f"Недопустимое выражение: {ast.dump(node)}")
    
    return _eval(tree.body)



