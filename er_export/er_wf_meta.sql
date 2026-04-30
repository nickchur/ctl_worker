-- DDL for export.er_wf_meta
-- Flat config table for ER export workflows (no JSON columns)
-- Synced to Airflow Variable "datalab_er_wfs" every 5 min by er_sync__datalab_er_wfs DAG

CREATE TABLE IF NOT EXISTS export.er_wf_meta
(
    extract_name  String,
    db_name       String,
    replica       String,
    schema_name   String,
    format        String        DEFAULT 'TSVWithNames',
    strategy      String        DEFAULT 'FULL_UK',
    pk            Array(String) DEFAULT [],
    uk            Array(String) DEFAULT [],
    fields        Array(String) DEFAULT [],
    sql_from      String        DEFAULT '',
    sql_where     String        DEFAULT '',
    is_recent     UInt8         DEFAULT 0,
    is_active     UInt8         DEFAULT 1,
    updated_at    DateTime      DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (db_name, extract_name);

-- Example row for evolution.lc_items_opened
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, strategy, uk, fields, sql_from, sql_where)
VALUES (
    'lc_items_opened',
    'evolution',
    'hrplatform_datalab',
    'learning',
    'FULL_UK',
    ['person_uuid', 'item_id'],
    [
        '{export_time} as export_time',
        'insert_time',
        'header_id',
        'header_person_id',
        'header_event_type',
        'header_created_dt',
        'item_id',
        'person_uuid',
        'tenant',
        'company',
        '''I'' as ctl_action',
        'now() as ctl_validfrom'
    ],
    'evolution.lc_items_opened',
    '{condition}'
);
