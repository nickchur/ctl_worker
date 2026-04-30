from __future__ import annotations

import base64
import json
import os
from datetime import timedelta

VAULT_PATH = '/vault/secrets/application'
CH_BD      = 'evolution'

if os.getenv("AIRFLOW__CTL_PIN"):
    CH_ID = 'dlab-click-test'
    with open(VAULT_PATH) as f:
        secrets = json.load(f)

    def _b64(s: str) -> str:
        return base64.b64decode(s).decode()

    conn_json = {
        "conn_type": "clickhouse",
        "host":      "tvlds-hrplt0429.cloud.delta.sbrf.ru",
        "port":      9440,
        "login":     _b64(secrets['SCSP_CLICKHOUSE_USERNAME']),
        "password":  _b64(secrets['SCSP_CLICKHOUSE_PASSWORD']),
        "schema":    CH_BD,
        "extra":     {"verify": False, "secure": True},
    }
    os.environ[f'AIRFLOW_CONN_{CH_ID.upper()}'] = json.dumps(conn_json)
else:
    CH_ID = 'dlab-click'

TFS_OUT_CONN_ID = 's3-tfs-hrplt'
TFS_OUT_BUCKET  = 'tfshrplt'
TFS_OUT_TOPIC   = 'TFS.HRPLT.IN'

ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

TFS_OUT_CONFIG_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er", "tfs_HRPLATFORM-2100"),
}

DEFAULT_ARGS = {
    "owner":               "DataLab (CI02420667)",
    "retries":             3,
    "retry_delay":         timedelta(minutes=5),
    "aws_conn_id":         TFS_OUT_CONN_ID,
    "clickhouse_conn_id":  CH_ID,
    "conn_id":             CH_ID,
    "kafka_config_id":     "tfs-kafka-out",
    "topic":               TFS_OUT_TOPIC,
}

ROW_COUNT_LIMIT_MAP = {
    "prom": 0,
    "uat":  100,
    "qa":   100,
    "ift":  100,
    "dev":  100,
}

CH_TYPE_MAP: dict[str, str] = {
    "DateTime":    "TIMESTAMP",
    "DateTime64":  "TIMESTAMP",
    "Date":        "DATE",
    "Date32":      "DATE",
    "String":      "STRING",
    "FixedString": "STRING",
    "UUID":        "STRING",
    "Int8":        "INT",
    "Int16":       "INT",
    "Int32":       "INT",
    "Int64":       "BIGINT",
    "UInt8":       "INT",
    "UInt16":      "INT",
    "UInt32":      "INT",
    "UInt64":      "BIGINT",
    "Float32":     "FLOAT",
    "Float64":     "DOUBLE",
    "Decimal":     "NUMERIC",
    "Array":       "STRING",
}


def get_export_time_placeholder(table_name: str) -> str:
    """Возвращает Jinja-шаблон, который подставляет метку времени среза (extract_time)
    из XCom задачи get_delta_params в SQL выгрузки."""
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.get_delta_params')[0][1] }}}}"


def get_delta_condition_placeholder(table_name: str) -> str:
    """Возвращает Jinja-шаблон, который подставляет условие WHERE для инкремента
    (например: '2024-01-01' < insert_time and insert_time <= '2024-01-02')
    из XCom задачи get_delta_params в SQL выгрузки."""
    return f"{{{{ ti.xcom_pull(task_ids='{table_name}.get_delta_params')[0][11] }}}}"


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
