"""
Configuration and constants for the ER export framework.
Includes environment-specific settings, connection IDs, and data type mappings.
"""
from __future__ import annotations

import base64
import json
import os
from datetime import timedelta

VAULT_PATH = '/vault/secrets/application'
CH_BD      = 'export'
VAR_NAME = "datalab_er_wfs"

# ER_MODE=test включает тестовый CH-коннект из vault; по умолчанию — prod
MODE = os.getenv("ER_MODE", "prod" if not os.getenv("AIRFLOW__CTL_PIN") else "test")

if MODE == 'test':
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
    S3_CONN = 's3-archive'
else:
    CH_ID   = 'dlab-click'
    S3_CONN = 's3-tfs-hrplt'

BUCKET = 'tfshrplt'
TOPIC  = 'TFS.HRPLT.IN'

ENV_STAND = os.getenv("ENV_STAND", "").strip().lower()

TFS_MAP = {
    "hrplatform_datalab": ("HRPLATFORM-4000", "from/KAP802/hrpl_lm_er"),
}

DEF_ARGS = {
    "owner":               "DataLab (CI02420667)",
    "retries":             3,
    "retry_delay":         timedelta(minutes=5),
    "aws_conn_id":         S3_CONN,
    "clickhouse_conn_id":  CH_ID,
    "conn_id":             CH_ID,
    "kafka_config_id":     "tfs-kafka-out",
    "kafka_in_conn":"tfs-kafka-in",
    "kafka_in_topic":  "TFS.HRPLT.OUT",
    "topic":               TOPIC,
}

LIMITS = {
    "prom": 0,
    "uat":  100,
    "qa":   100,
    "ift":  100,
    "dev":  100,
}

TYPE_MAP: dict[str, str] = {
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

MANDATORY_PRE = ["{export_time} as export_time"]
MANDATORY_SUF = ["'I' as ctl_action", "now() as ctl_validfrom"]

EXTRA_COLS_PRE = [
    {"column_name": "export_time",   "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None, "description": None},
]
EXTRA_COLS_SUF = [
    {"column_name": "ctl_action",    "source_type": "VARCHAR",   "length": 10,   "notnull": False, "precision": None, "scale": None, "description": None},
    {"column_name": "ctl_validfrom", "source_type": "TIMESTAMP", "length": None, "notnull": False, "precision": None, "scale": None, "description": None},
]
EXTRA_COLS = EXTRA_COLS_PRE + EXTRA_COLS_SUF

POOL_NAME   = 'datalab_export_er'
POOL_SLOTS  = 20

def get_dict(ch_hook, sql: str) -> list[dict]:
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    return []


def get_config() -> dict:
    return {
        'CH_ID':         CH_ID,
        'TYPE_MAP':      TYPE_MAP,
        'DEF_ARGS':      DEF_ARGS,
        'ENV_STAND':     ENV_STAND,
        'EXTRA_COLS':     EXTRA_COLS,
        'EXTRA_COLS_PRE': EXTRA_COLS_PRE,
        'EXTRA_COLS_SUF': EXTRA_COLS_SUF,
        'MANDATORY_PRE': MANDATORY_PRE,
        'MANDATORY_SUF': MANDATORY_SUF,
        'MODE':          MODE,
        'LIMITS':        LIMITS,
        'BUCKET':        BUCKET,
        'TFS_MAP':       TFS_MAP,
        'S3_CONN':       S3_CONN,
        'VAR_NAME':      VAR_NAME,
        'POOL_NAME':     POOL_NAME,
        'POOL_SLOTS':    POOL_SLOTS,
    }
