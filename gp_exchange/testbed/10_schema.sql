-- Тестовый стенд Greenplum на PostgreSQL: схемы и таблицы-источники.
-- Полной копии витрин нет и не нужно: пакет обмена собирается из семи вьюх,
-- и нам важны их имена, ключи инкремента и набор колонок — ровно то, что
-- разбирает tfs_exchange_import.py на стороне ClickHouse.

DROP SCHEMA IF EXISTS s_grnplm_vd_hr_edp_srv_wf CASCADE;
DROP SCHEMA IF EXISTS s_grnplm_vd_hr_edp_srv_dq CASCADE;
DROP SCHEMA IF EXISTS s_grnplm_vd_hr_edp_vda CASCADE;

CREATE SCHEMA s_grnplm_vd_hr_edp_srv_wf;
CREATE SCHEMA s_grnplm_vd_hr_edp_srv_dq;
CREATE SCHEMA s_grnplm_vd_hr_edp_vda;

-- ─── Источники ───────────────────────────────────────────────────────────────
-- Имена и колонки — как в целевых таблицах ClickHouse (tasks в tfs_exchange_import).

CREATE TABLE s_grnplm_vd_hr_edp_srv_dq.vw_ztest (
    ts          timestamp,
    object      text,
    ztest_ok    boolean,
    is_except   boolean,
    is_error    boolean,
    confidence  real,
    stable      boolean,
    zscore      real,
    key_date    text,
    rows_count  bigint,
    key_diff    bigint,
    value       text,
    avg         real,
    std         real,
    cnt         bigint,
    min         real,
    max         real,
    log_id      bigint,
    notes       text,
    error       text,
    z_cfg       text,
    z_except    text,
    z_error     text
);

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity (
    id       bigint,
    ts       timestamp,
    url      text,
    name     text,
    path     text,
    storage  text,
    parentid bigint,
    msg      text
);

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading (
    id          bigint,
    ts          timestamp,
    url         text,
    alive       text,
    auto        boolean,
    start_dttm  timestamp,
    end_dttm    timestamp,
    profile     text,
    wf_id       bigint,
    status_dttm timestamp,
    status      text,
    status_log  text,
    msg         text
);

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf (
    id                  integer,
    ts                  timestamp,
    url                 text,
    profile             text,
    category            text,
    name                text,
    scheduled           boolean,
    deleted             boolean,
    singleloading       boolean,
    engine              text,
    type                text,
    connected           text,
    eventawaitstrategy  text,
    wf_sched            text,
    param               text,
    statusnotifications text,
    msg                 text,
    wf_interval         text
);

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow (
    start_id     integer,
    start_ts     timestamp,
    start_action text,
    workflow     text,
    end_id       integer,
    end_ts       timestamp,
    end_action   text,
    duration     text,
    message      text,
    rows_count   bigint,
    period_name  text,
    period_from  text,
    period_to    text,
    load_name    text,
    load_min     text,
    load_max     text,
    key_name     text,
    key_min      text,
    key_max      text,
    zscore       real,
    ztest_ok     boolean,
    confidence   real,
    key_date     text
);

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.vw_swf_ctl_log (
    beg_id     integer,
    beg_ts     timestamp,
    beg_action text,
    duration   text,
    end_id     integer,
    end_ts     timestamp,
    end_action text,
    beg_msg    text,
    end_msg    text
);

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log (
    beg_id      integer,
    beg_ts      timestamp,
    beg_action  text,
    obj         text,
    sch         text,
    end_id      integer,
    end_ts      timestamp,
    duration    text,
    end_action  text,
    res         integer,
    msg         text,
    value       text,
    beg_message text,
    end_message text
);

-- ─── Журнал обмена ───────────────────────────────────────────────────────────
-- Из HR_Data (sql/create/s_grnplm_vd_hr_edp_srv_wf/tables/tb_exchange_log.sql),
-- без греенпламовских WITH (appendonly…) и DISTRIBUTED.

CREATE SEQUENCE s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log_id_seq START 1;

CREATE TABLE s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log (
    id      bigint not null DEFAULT nextval('s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log_id_seq'::regclass),
    ts      timestamp without time zone not null DEFAULT clock_timestamp(),
    wf_name text null,
    wf_key  text null,
    wf_data json null
);
