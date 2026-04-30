-- DDL for export.er_wf_meta
-- Flat config table for ER export workflows (no JSON columns)
-- Synced to Airflow Variable "datalab_er_wfs" every 5 min by er_sync__datalab_er_wfs DAG

CREATE TABLE IF NOT EXISTS export.er_wf_meta ON CLUSTER datalab
(
    extract_name  String                    COMMENT 'Имя выгрузки (table name без схемы), соответствует extract_name в extract_registry_vw',
    db_name       String                    COMMENT 'База данных источника в ClickHouse (левая часть "db.table")',
    replica       String                    COMMENT 'Реплика-маршрутизатор TFS (ключ в TFS_OUT_CONFIG_MAP)',
    schema_name   String                    COMMENT 'Целевая схема в .meta-файле для xStream',
    format        String        DEFAULT 'TSVWithNames' COMMENT 'Формат выгрузки ClickHouse',
    strategy      String        DEFAULT 'FULL_UK'      COMMENT 'Стратегия merge в .meta: FULL_UK, FULL_PK, DELTA_UK и др.',
    pk            Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа',
    uk            Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа',
    fields        Array(String) DEFAULT []             COMMENT 'SELECT-выражения таблицы-источника (export_time, ctl_action, ctl_validfrom добавляются автоматически)',
    sql_from      String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос',
    sql_where     String        DEFAULT ''             COMMENT 'WHERE-условие; пустая строка — без фильтра; плейсхолдер {condition} подставляется рантаймом',
    description   String        DEFAULT ''             COMMENT 'Произвольное описание DAG-а (отображается в Airflow UI)',
    is_recent     UInt8         DEFAULT 0             COMMENT '0 = дельта-выгрузка (sql_stmt_export_delta), 1 = recent (sql_stmt_export_recent)',
    is_active     UInt8         DEFAULT 1             COMMENT '0 = запись игнорируется при синхронизации в Variable',
    updated_at    DateTime      DEFAULT now()         COMMENT 'Версия строки для ReplacingMergeTree'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/export/er_wf_meta', '{replica}', updated_at)
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
        'insert_time',
        'header_id',
        'header_person_id',
        'header_event_type',
        'header_created_dt',
        'item_id',
        'person_uuid',
        'tenant',
        'company'
    ],
    'evolution.lc_items_opened',
    '{condition}'
);
