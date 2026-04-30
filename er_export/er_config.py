from __future__ import annotations

import base64
import json
import os
from datetime import timedelta

VAULT_PATH = '/vault/secrets/application'
CH_BD      = 'export'

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
    TFS_OUT_CONN_ID = 's3-archive'
    MODE = 'test'
else:
    CH_ID           = 'dlab-click'
    TFS_OUT_CONN_ID = 's3-tfs-hrplt'
    MODE = 'prod'
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

ER_MANDATORY_FIELDS_PREFIX = ["{export_time} as export_time"]
ER_MANDATORY_FIELDS_SUFFIX = ["'I' as ctl_action", "now() as ctl_validfrom"]

ER_EXTRA_COLUMNS = [
    {"column_name": "export_time",   "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None},
    {"column_name": "ctl_action",    "source_type": "VARCHAR",   "length": 10,   "notnull": False, "precision": None, "scale": None},
    {"column_name": "ctl_validfrom", "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None},
]
