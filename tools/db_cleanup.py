"""### 🧹 Очистка метадаты Airflow

Удаляет устаревшие записи из метабазы Airflow.

| Параметр           | Описание                                                          |
|--------------------|-------------------------------------------------------------------|
| 📅 `retention_days` | Хранить записи не старше N дней *(default: `180` = 6 мес)*       |
| 🔍 `dry_run`        | Только подсчёт без удаления *(default: `True`)*                   |
| 📋 `tables`         | Список таблиц для очистки *(default: все)*                        |

---

#### Таблицы очистки

`dag_run`, `task_instance`, `xcom`, `log`, `job`,
`sla_miss`, `rendered_task_instance_fields`, `import_error`

> **dry_run=True** по умолчанию — первый запуск всегда безопасен.
> Минимальный порог `retention_days` — 30 дней.
"""

from airflow.decorators import task, dag
from airflow.models import Param
import pendulum
import os

from logging import getLogger
logger = getLogger("airflow.task")

CLEANABLE_TABLES = [
    'dag_run',
    'task_instance',
    'xcom',
    'log',
    'job',
    'sla_miss',
    'rendered_task_instance_fields',
    'import_error',
]


@dag(
    doc_md=__doc__,
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args={
        'owner': 'DataLab (CI02420667)',
        'retries': 0,
    },
    start_date=pendulum.datetime(2025, 8, 7, tz=pendulum.UTC),
    tags=['DataLab', 'tools', 'maintenance'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
    schedule_interval='@weekly',
    params={
        'retention_days': Param(
            180,
            type='integer',
            minimum=30,
            description='Хранить записи не старше N дней (минимум 30)',
        ),
        'dry_run': Param(
            True,
            type='boolean',
            description='True — только подсчёт, False — реальное удаление',
        ),
        'tables': Param(
            CLEANABLE_TABLES,
            type='array',
            description='Таблицы для очистки',
            examples=[CLEANABLE_TABLES],
        ),
    },
)
def tools_db_cleanup():

    @task
    def check_retention(**context):
        from airflow.utils.db import provide_session
        from airflow.utils.session import create_session
        from sqlalchemy import text

        params = context['params']
        retention_days = params['retention_days']
        tables = params['tables']

        if retention_days < 30:
            raise ValueError(f"retention_days={retention_days} меньше минимума (30)")

        cutoff = pendulum.now('UTC').subtract(days=retention_days)
        logger.info(f"Граница очистки: {cutoff} (retention_days={retention_days})")

        # Колонки с датой для каждой таблицы
        date_columns = {
            'dag_run':                          'execution_date',
            'task_instance':                    'execution_date',
            'xcom':                             'timestamp',
            'log':                              'dttm',
            'job':                              'latest_heartbeat',
            'sla_miss':                         'timestamp',
            'rendered_task_instance_fields':    None,
            'import_error':                     'timestamp',
        }

        with create_session() as session:
            for table in tables:
                date_col = date_columns.get(table)
                if date_col is None:
                    logger.info(f"  {table}: подсчёт не поддерживается (нет колонки даты)")
                    continue
                try:
                    result = session.execute(
                        text(f"SELECT COUNT(*) FROM {table} WHERE {date_col} < :cutoff"),
                        {'cutoff': cutoff},
                    )
                    count = result.scalar()
                    logger.info(f"  {table}: {count} записей старше {cutoff}")
                except Exception as e:
                    logger.warning(f"  {table}: ошибка подсчёта — {e}")

    @task
    def clean_metadata(**context):
        import subprocess

        params = context['params']
        retention_days = params['retention_days']
        dry_run = params['dry_run']
        tables = params['tables']

        if retention_days < 30:
            raise ValueError(f"retention_days={retention_days} меньше минимума (30)")

        cutoff = pendulum.now('UTC').subtract(days=retention_days)
        cutoff_str = cutoff.strftime('%Y-%m-%dT%H:%M:%S+00:00')

        for table in tables:
            cmd = [
                'airflow', 'db', 'clean',
                '--clean-before-timestamp', cutoff_str,
                '--tables', table,
                '--yes',
            ]
            if dry_run:
                cmd.append('--dry-run')

            mode = 'DRY-RUN' if dry_run else 'УДАЛЕНИЕ'
            logger.info(f"[{mode}] {table}: {' '.join(cmd)}")

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.stdout:
                logger.info(result.stdout)
            if result.stderr:
                logger.warning(result.stderr)
            if result.returncode != 0:
                logger.error(f"Ошибка при обработке таблицы {table} (код {result.returncode})")

    check_retention() >> clean_metadata()


ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

if ENV_STAND == "prom":
    logger.warning("DAG tools_db_cleanup is disabled on 'prom' stand. Skipping DAG registration.")
else:
    tools_db_cleanup()
