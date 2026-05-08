-- DDL для export.er_wf_meta
-- Управляющая таблица ER-выгрузок. Синхронизируется в Airflow Variable "datalab_er_wfs"
-- каждые 5 минут DAG-ом export_er_sync.
--
-- ВАЖНО: выполнять через clickhouse-client или HTTP-интерфейс, НЕ через JDBC.
-- JDBC-драйвер интерпретирует {shard} и {replica} как именованные параметры → ошибка.
-- Пример: clickhouse-client --multiquery < er_wf_meta.sql
--
-- Чтение без дублей (ReplacingMergeTree гарантирует дедупликацию только после MERGE):
--   SELECT * FROM export.er_wf_meta FINAL WHERE is_active = 1
--
-- Поля extract_name, db_name, sql_from, replica — обязательны (пустая строка → запись
-- пропускается при синхронизации в Variable и не порождает DAG).


CREATE TABLE IF NOT EXISTS export.er_wf_meta ON CLUSTER datalab
(
    extract_name  String                    COMMENT 'Имя выгрузки (table name без схемы); обязательное, непустое',
    db_name       String                    COMMENT 'База данных источника в ClickHouse (левая часть "db.table"); обязательное, непустое',
    replica       String                    COMMENT 'Реплика-маршрутизатор TFS (ключ в TFS_MAP er_config.py); обязательное — запись пропускается если не в TFS_MAP',
    schema_name   String                    COMMENT 'Целевая схема в .meta-файле для TFS',
    pk            Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа',
    uk            Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа',
    fields        Array(String) DEFAULT []             COMMENT 'SELECT-выражения таблицы-источника; [] = все колонки (DESCRIBE TABLE)',
    sql_from      String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос; обязательное — пустая строка → запись пропускается',
    sql_where     String        DEFAULT ''             COMMENT 'WHERE-условие; пустая строка — без фильтра; {condition} подставляется рантаймом',
    sql_join      String        DEFAULT ''             COMMENT 'JOIN-clause (полное выражение: JOIN t ON ...); вставляется между FROM и WHERE',
    sql_with      String        DEFAULT ''             COMMENT 'WITH-блок (CTE); вставляется перед SELECT',
    sql_settings  String        DEFAULT ''             COMMENT 'SETTINGS-блок ClickHouse; вставляется в конец запроса',
    params        String        DEFAULT '{}'           COMMENT 'JSON с переопределёнными параметрами выгрузки (см. DEFAULT_PARAMS в er_config.py)',
    description   String        DEFAULT ''             COMMENT 'Описание DAG-а (отображается в Airflow UI)',
    schedule      String        DEFAULT '55 0 * * *'   COMMENT 'Cron-расписание первичного запуска DAG (самозапуск через schedule_next)',
    is_recent     UInt8         DEFAULT 0              COMMENT '0 = delta-выгрузка, 1 = recent (скользящее окно)',
    is_active     UInt8         DEFAULT 1              COMMENT '0 = запись игнорируется при синхронизации в Variable',
    updated_at    DateTime64(3) DEFAULT now64(3)        COMMENT 'Версия строки для ReplacingMergeTree (мс-точность исключает коллизии при быстрых обновлениях)'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/export/er_wf_meta', '{replica}', updated_at)
ORDER BY (db_name, extract_name);


-- Пример: delta-выгрузка с JOIN и переопределённым selfrun_timeout.
-- sql_join содержит полное JOIN-выражение (включая ключевое слово JOIN/LEFT JOIN/INNER JOIN и т.п.).
-- В params указываются только отличия от DEFAULT_PARAMS (er_config.py); остальные берутся по умолчанию.
-- Повторный INSERT той же (db_name, extract_name) не заменяет строку мгновенно —
-- дедупликация происходит при фоновом MERGE; для немедленного чтения актуальной версии использовать FINAL.
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk, sql_from, sql_join, sql_where, params)
VALUES (
    'lc_items_opened',
    'evolution',
    'hrplatform_datalab',
    'learning',
    ['person_uuid', 'item_id'],
    'evolution.lc_items_opened t1',
    'LEFT JOIN evolution_export.lc_items_opened_exp t2 ON t1.person_uuid = t2.person_uuid AND t1.item_id = t2.item_id',
    '{condition}',
    '{"selfrun_timeout": 10}'
);
