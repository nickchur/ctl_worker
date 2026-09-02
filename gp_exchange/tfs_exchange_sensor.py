import pendulum
from airflow import DAG, Dataset
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor # pyright: ignore[reportMissingImports]

# Двойной импорт — см. пояснение в tfs_exchange_import.py.
try:
    from CI06932748.analytics.datalab.gp_exchange.tfs_exchange_common import ( # type: ignore
        TFS_IN_DATASET,
        TFS_IN_BUCKET,
        TFS_IN_PREFIX,
        default_args,
    )
except ImportError:
    from gp_exchange.tfs_exchange_common import ( # type: ignore
        TFS_IN_DATASET,
        TFS_IN_BUCKET,
        TFS_IN_PREFIX,
        default_args,
    )


with DAG(
    dag_id='import_gp_ue_exchange_sensor',
    description="Отслеживатель для загрузки универсального обмена из ПКАП",
    owner_links={'DataLab (CI02420667)': 'https://confluence.sberbank.ru/display/HRTECH/DataLab'},
    default_args=default_args,
    start_date=pendulum.datetime(2025, 12, 20),
    schedule_interval='*/30 * * * *',
    tags=['DataLab', 'import', 'TFS', 'CI02420667', 'PKAP', 'exchange'],
    catchup=False,
    is_paused_upon_creation=True,
    max_active_runs=1,
    max_active_tasks=1,
) as dag_sensor:

    wait_for_file = S3KeySensor(
        task_id='wait_for_file',
        # базовые параметры
        outlets=[TFS_IN_DATASET],
        mode='reschedule',
        wildcard_match=True,
        poke_interval=pendulum.duration(minutes=5),
        soft_fail=True,
        timeout=pendulum.duration(minutes=25),
        # s3 параметры
        bucket_name=TFS_IN_BUCKET,
        bucket_key=TFS_IN_PREFIX + 'ue_exchange_*.csv',
        retries=3,
        retry_delay=pendulum.duration(minutes=2),
    )
