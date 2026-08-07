"""### 🔌 DAG: Список Airflow Connections
*2026-08-07 13:45 MSK · v1.2 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Выводит список всех подключений из secret backend, сгруппированных по их типу.
Используется для аудита доступных соединений и верификации конфигурации backend'а.

Запускается ежедневно в 23:00 MSK — за 15 минут до `tools_test_connections`, который
берёт список соединений из Variable `local_connections`, обновляемой здесь.

| Функция | Описание |
|---|---|
| **Группировка** | Все соединения распределяются по `conn_type` |
| **Отчёт** | В заметку DAG'а — сводка **по типам** (сколько соединений каждого); полная таблица с `conn_id`, `host`, `port`, `schema` и `description` — в заметке таска |
| **Кэширование** | Сохраняет результат в Airflow Variable `local_connections` для ускорения работы `test_connections` |
| **ClickHouse** | Автоматически подменяет `sqlite` на `clickhouse` для корректного отображения |
"""

from datetime import datetime, timedelta, timezone
from logging import getLogger

from airflow.decorators import dag, task

try:
    from plugins.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore
except ImportError:
    from CI06932748.tools.utils import TOOLS_POOL, ensure_pool, on_callback  # type: ignore

logger = getLogger("airflow.task")

# Москва живёт на постоянном UTC+3 с 2014 года, переходов на летнее время нет,
# поэтому фиксированное смещение точно описывает пояс и не зависит от tz-базы
MSK = timezone(timedelta(hours=3))

# Пул заводим при парсинге: к планированию первого таска он уже есть
ensure_pool(TOOLS_POOL)


@dag(
    doc_md=__doc__,
    default_args={
        'owner': 'DataLab (CI02420667)',
        'pool': TOOLS_POOL,
        'retries': 2,
        'on_failure_callback': on_callback,
    },
    # Часовой пояс DAG'а берётся из start_date.tzinfo (models/dag.py:614-628), поэтому
    # [core] default_timezone = utc не мешает: 23:00 — московские
    start_date=datetime(2026, 1, 1, tzinfo=MSK),
    # Ежедневно в 23:00 MSK: срез соединений обновляется перед ночным tools_test_connections
    # (23:15), который берёт список из Variable local_connections
    schedule='0 23 * * *',
    tags=['DataLab', 'tools', 'conn', 'AutoQA'],
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1,
    on_failure_callback=on_callback,
)
def tools_show_connections():

    @task
    def show_connections(**context):
        from collections import defaultdict

        from airflow.configuration import get_custom_secret_backend
        from airflow.models import Connection, Variable

        try:
            from plugins.utils import add_note  # type: ignore
        except ImportError:
            from CI06932748.tools.utils import add_note  # type: ignore

        backend = get_custom_secret_backend()
        if not hasattr(backend, '_local_connections'):
            msg = f"{backend} has no attr `_local_connections`"
            add_note(msg, context, level='DAG,task')
            return msg

        local_connections: dict[str, Connection] = backend._local_connections
        logger.info("Loaded %d connections from backend", len(local_connections))
        
        by_type = defaultdict(list)
        for conn_id, conn in local_connections.items():
            by_type[conn.conn_type].append({
                'conn_id': conn_id,
                'host': conn.host,
                'port': conn.port,
                'schema': conn.schema,
                'description': conn.description or 'No description',
            })
        if 'sqlite' in by_type:
            by_type['clickhouse'] = by_type.pop('sqlite')

        rows = []
        for conn_type, conns in by_type.items():
            for c in conns:
                logger.info("  [%s] %s: %s", conn_type, c['conn_id'], c['description'])
                rows.append({'conn_type': conn_type, **c})

        # Формируем Markdown таблицу без pandas
        headers = ['conn_type', 'conn_id', 'host', 'port', 'schema', 'description']
        table_lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
        for row in rows:
            table_lines.append("| " + " | ".join(str(row.get(h, '')) for h in headers) + " |")
        
        table_str = "\n".join(table_lines)

        # В заметку DAG'а идёт сводка по типам: соединений под сотню, и поимённая таблица
        # там всё равно резалась по MAX_NOTE_LEN. Полная таблица остаётся в заметке таска.
        summary_lines = ["| Тип | Соединений |", "|---|---:|"]
        for conn_type in sorted(by_type):
            summary_lines.append(f"| **{conn_type}** | {len(by_type[conn_type])} |")
        summary_lines.append(f"| **итого** | **{len(rows)}** |")

        title = f"Connections: {len(rows)} в {len(by_type)} типах"
        add_note("\n".join(summary_lines), context, level='DAG', title=title)
        add_note(table_str, context, level='task', title=title)


        # Сохраняем в Variable для дальнейшего использования
        Variable.set('local_connections', dict(by_type), serialize_json=True)
        
        return dict(by_type)

    show_connections()


tools_show_connections()
