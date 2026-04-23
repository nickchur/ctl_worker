"""### Список Airflow Connections

Показывает все подключения из secret backend, сгруппированные по типу.
"""

from collections import defaultdict

import pendulum
from airflow.configuration import get_custom_secret_backend
from airflow.models import Connection
from airflow.decorators import task, dag

from plugins.utils import add_note

from logging import getLogger
logger = getLogger("airflow.task")


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
        'retry_delay': pendulum.duration(seconds=30),
    },
    start_date=pendulum.datetime(2026, 1, 21, tz=pendulum.UTC),
    schedule_interval=None,
    tags=['EDP_ETL', 'tools'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
)
def tools_show_connections():

    @task
    def show_connections(**context):
        backend = get_custom_secret_backend()
        if not hasattr(backend, '_local_connections'):
            msg = f"{backend} has no attr `_local_connections`"
            add_note(msg, context)
            return msg

        local_connections: dict[str, Connection] = backend._local_connections
        by_type = defaultdict(list)
        for conn_id, conn in local_connections.items():
            by_type[conn.conn_type].append({
                'conn_id': conn_id,
                'host': conn.host,
                'port': conn.port,
                'schema': conn.schema,
                'description': conn.description or 'No description',
                'extra': conn.extra,
            })
        if 'sqlite' in by_type:
            by_type['clickhouse'] = by_type.pop('sqlite')

        rows = []
        for conn_type, conns in by_type.items():
            for c in conns:
                logger.info("  [%s] %s: %s", conn_type, c['conn_id'], c['description'])
                rows.append({'conn_type': conn_type, **c})

        import pandas as pd
        df = pd.DataFrame(rows)[['conn_type', 'conn_id', 'host', 'port', 'schema', 'description']]
        add_note(f"```\n{df.to_string(index=False)}\n```", context, title=f"Connections ({len(rows)})")
        return dict(by_type)

    show_connections()


tools_show_connections()
