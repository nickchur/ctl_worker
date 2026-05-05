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

# ER_MODE включает тестовый CH-коннект из vault; по умолчанию — SIGMA/ALPHA
MODE = os.getenv("ER_MODE", "SIGMA" if not os.getenv("AIRFLOW__CTL_PIN") else "ALPHA")

if MODE == 'ALPHA':
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

def obj_load(key: str, default: any = None) -> any:
    """Loads an object from Airflow Variable (JSON)."""
    from airflow.models import Variable
    return Variable.get(key, default_var=default if default is not None else {}, deserialize_json=True)


def obj_save(key: str, data: any) -> None:
    """
    Saves an object to Airflow Variable (JSON).
    Compares with existing data to skip redundant writes.
    Updates Variable description with metadata: {'ts': ..., 'len': ..., 'size': ...}.
    """
    from airflow.models import Variable
    import json
    import pendulum

    # 1. Read and Compare
    try:
        old_val = Variable.get(key, default_var=None, deserialize_json=True)
    except Exception:
        old_val = None

    new_json = json.dumps(data, sort_keys=True, ensure_ascii=False)
    old_json = json.dumps(old_val, sort_keys=True, ensure_ascii=False) if old_val is not None else None

    if new_json == old_json:
        return

    # 2. Calculate Metadata
    size_bytes = len(new_json.encode('utf-8'))
    size_val = float(size_bytes)
    unit = 'B'
    for u in ['B', 'KB', 'MB', 'GB']:
        if size_val < 1024.0:
            unit = u
            break
        size_val /= 1024.0
    
    size_str = f"{size_val:.1f} {unit}"
    length   = len(data) if isinstance(data, (dict, list)) else 1
    ts       = pendulum.now().format('YYYY-MM-DD HH:mm:ss')
    
    desc = f"{{'ts': '{ts}', 'len': {length}, 'size': '{size_str}'}}"

    # 3. Save
    Variable.set(key, data, description=desc, serialize_json=True)


def add_note(msg, context=None, level='task', add=True, title='', compact=False):
    """
    Structured notes in Airflow UI (DAG/Task).
    Copied from plugins.utils to remove external dependency.
    """
    from airflow.utils.session import create_session
    from airflow.operators.python import get_current_context
    from pprint import PrettyPrinter
    import logging

    logger = logging.getLogger("airflow.task")
    MAX_NOTE_LEN = 1000

    if not context:
        try:
            context = get_current_context()
        except Exception:
            logger.warning("Could not get Airflow context for add_note")
            return
        
    if isinstance(msg, dict) and len(msg) == 1:
        t, msg = next(iter(msg.items()))
        title += str(t) + (f' ({len(msg)})' if isinstance(msg, (dict, list, tuple, set)) else '')
                    
    if not isinstance(msg, str):
        msg = PrettyPrinter(indent=4, compact=compact).pformat(msg).replace("'", '')
        msg = '```\n' + msg + '\n```'

    logger.info(f"📝 Note added to {level} {title}:\n{msg}")
    
    with create_session() as session:
        for l in list(set(level.upper().split(',')))[:2]:
            new_note = msg.strip()
            if l == 'DAG':
                obj = session.merge(context['dag_run'])
            else:
                obj = session.merge(context['task_instance'])
            session.expire(obj)
            
            if title:
                import unicodedata
                if not (title and unicodedata.category(title[0]) == 'So'):
                    title = "📝 " + title
                new_note = f"{title}\n---\n{new_note}"

            if obj.note and obj.note.startswith(new_note[:MAX_NOTE_LEN]):
                continue
                
            if add:
                new_note = f"{new_note}\n\n---\n{obj.note if obj.note else ''}"
                
            obj.note = new_note[:MAX_NOTE_LEN]


def get_dict(ch_hook, sql: str) -> list[dict]:
    res, cols = ch_hook.execute(sql, with_column_types=True)
    if res:
        cols = [col[0] for col in cols]
        return [dict(zip(cols, row)) for row in res]
    return []


DEFAULT_PARAMS: dict = {
    'increment':         60,
    'selfrun_timeout':   10,
    'strategy':          'FULL_UK',
    'auto_confirm':       1,
    'auto_confirm_delta': 0,
    'confirm_timeout':    3600,
    'lower_bound':       '',
    'time_field':        'extract_time',
    'overlap':           0,
    'recent_interval':   3600,
    'compression_type':  'none',
    'compression_ext':   '',
    'max_file_size':     '',
    'pg_array_format':   0,
    'csv_format_params': '',
    'xstream_sanitize':  0,
    'sanitize_array':    0,
    'sanitize_list':     '',
}


def get_params(row: dict) -> dict:
    """Merges er_wf_meta 'params' JSON with DEFAULT_PARAMS. Row-level values win."""
    overrides = json.loads(row.get('params') or '{}')
    return {**DEFAULT_PARAMS, **overrides}


def get_config() -> dict:
    return {
        'CH_ID':          CH_ID,
        'TYPE_MAP':       TYPE_MAP,
        'DEF_ARGS':       DEF_ARGS,
        'ENV_STAND':      ENV_STAND,
        'EXTRA_COLS':     EXTRA_COLS,
        'EXTRA_COLS_PRE': EXTRA_COLS_PRE,
        'EXTRA_COLS_SUF': EXTRA_COLS_SUF,
        'MANDATORY_PRE':  MANDATORY_PRE,
        'MANDATORY_SUF':  MANDATORY_SUF,
        'MODE':           MODE,
        'LIMITS':         LIMITS,
        'BUCKET':         BUCKET,
        'TFS_MAP':        TFS_MAP,
        'S3_CONN':        S3_CONN,
        'VAR_NAME':       VAR_NAME,
        'POOL_NAME':      POOL_NAME,
        'POOL_SLOTS':     POOL_SLOTS,
        'DEFAULT_PARAMS': DEFAULT_PARAMS,
    }
