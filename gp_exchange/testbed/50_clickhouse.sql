-- Приёмная сторона на стенде: одиночный ClickHouse, поэтому без Replicated и
-- ON CLUSTER — на боевом контуре эти таблицы реплицированные, здесь реплики нет.
-- Партиционирование gp_ue_exchange по _gp_name: именно его ждёт код обмена,
-- удаляя партицию потока после переноса (drop_src в process_any).

CREATE DATABASE IF NOT EXISTS support;

DROP TABLE IF EXISTS support.gp_ue_exchange_load;
CREATE TABLE support.gp_ue_exchange_load (
    _gp_id   Int64,
    _gp_name String,
    _gp_key  String,
    _gp_data String
) ENGINE = MergeTree
PARTITION BY _gp_id
ORDER BY (_gp_name, _gp_key, _gp_id);

DROP TABLE IF EXISTS support.gp_ue_exchange;
CREATE TABLE support.gp_ue_exchange (
    _gp_ts   DateTime,
    _gp_id   Int64,
    _gp_name String,
    _gp_key  String,
    _gp_data String,
    _gp_hash UUID
) ENGINE = ReplacingMergeTree(_gp_id)
PARTITION BY _gp_name
ORDER BY (_gp_name, _gp_key, _gp_hash);

DROP TABLE IF EXISTS support.gp__exchange_log;
CREATE TABLE support.gp__exchange_log (
    _gp_ts   DateTime,
    wf_type  String,
    wf_name  String,
    _gp_data String,
    _gp_key  String,
    _gp_id   Int64,
    _gp_hash UUID
) ENGINE = ReplacingMergeTree(_gp_id)
ORDER BY (wf_name, _gp_id, _gp_key, _gp_hash);
