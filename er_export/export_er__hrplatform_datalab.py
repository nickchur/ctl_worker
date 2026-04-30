import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator

from er_export.er_common import (
    DEFAULT_ARGS,
    TFS_OUT_BUCKET,
    get_export_time_placeholder,
    get_delta_condition_placeholder,
    make_er_export_task_group,
)

tables = {
    "evolution.lc_items_opened": {
        "replica":  "hrplatform_datalab",
        "schema":   "learning",
        "format":   "TSVWithNames",
        "strategy": "FULL_UK",
        "PK":       [],
        "UK":       ["person_uuid", "item_id"],
        "extra_columns": [
            {
                "column_name": "export_time",
                "source_type": "TIMESTAMP",
                "length":      None,
                "notnull":     False,
                "precision":   None,
                "scale":       None,
            },
            {
                "column_name": "ctl_action",
                "source_type": "VARCHAR",
                "length":      10,
                "notnull":     False,
                "precision":   None,
                "scale":       None,
            },
            {
                "column_name": "ctl_validfrom",
                "source_type": "TIMESTAMP",
                "length":      None,
                "notnull":     False,
                "precision":   None,
                "scale":       None,
            },
        ],
        "sql_stmt_export_delta": f"""
            select
                {get_export_time_placeholder('lc_items_opened')} as export_time,
                insert_time,
                header_id,
                header_person_id,
                header_event_type,
                header_created_dt,
                item_id,
                person_uuid,
                tenant,
                company,
                'I'   as ctl_action,
                now() as ctl_validfrom
            from evolution.lc_items_opened
            where {get_delta_condition_placeholder('lc_items_opened')}
        """,
    },
}


def create_er_dag(dag_id: str, table_key: str, params: dict) -> DAG:
    database_name, table_name = table_key.split(".", maxsplit=1)

    dag = DAG(
        dag_id=dag_id,
        description=f"ER-выгрузка {table_key} → S3 ZIP → TFS Kafka",
        default_args=DEFAULT_ARGS,
        start_date=pendulum.datetime(2024, 12, 18, tz=pendulum.timezone('UTC')),
        schedule_interval='55 0 * * *',
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False,
        tags=['DataLab', 'CI02420667', 'ClickHouse', 'xStream', 'ER'],
        is_paused_upon_creation=True,
        render_template_as_native_obj=True,
    )

    with dag:
        create_bucket = S3CreateBucketOperator(
            task_id='create_bucket',
            bucket_name=TFS_OUT_BUCKET,
        )
        tg = make_er_export_task_group(dag, database_name, table_name, params)
        create_bucket >> tg

    return dag


for _table_key, _params in tables.items():
    _, _table_name = _table_key.split(".", maxsplit=1)
    _dag_id = f"export_er__{_params['replica']}__{_table_name}"
    globals()[_dag_id] = create_er_dag(_dag_id, _table_key, _params)
