CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_growth_stats AS
 WITH log AS (
         SELECT tb_size_log.load_date,
            tb_size_log.table_schema,
            tb_size_log.table_name,
            tb_size_log.table_size,
            tb_size_log.relstorage,
            tb_size_log.reloptions,
            tb_size_log.distributedby,
            tb_size_log.partition_def,
            tb_size_log.prt_end,
            tb_size_log.n_live_tup,
            tb_size_log.n_dead_tup,
            tb_size_log.prt_cnt,
            tb_size_log.use_cnt,
            tb_size_log.prt_analyze,
            tb_size_log.last_analyze,
            tb_size_log.last_vacuum,
            tb_size_log.tableowner
           FROM s_grnplm_vd_hr_edp_srv_wf.tb_size_log
          WHERE ((tb_size_log.load_date >= (('now'::text)::date - 7)) AND (tb_size_log.load_date <= (('now'::text)::date + 1)))
        )
 SELECT ((a.load_date)::timestamp(0) without time zone)::text AS load_date,
    s_grnplm_vd_hr_edp_srv_wf.readable(a.total) AS total,
    replace(a.table_schema, 's_grnplm_vd_hr_edp_'::text, ''::text) AS table_schema,
    a.table_name,
    s_grnplm_vd_hr_edp_srv_wf.readable(((a.size_delta / a.days))::numeric) AS speed,
    round(((((a.size_delta)::numeric * 100.0) / (a.days)::numeric) / a.total), 4) AS speed_percent,
    s_grnplm_vd_hr_edp_srv_wf.readable((a.size_delta)::numeric) AS size_delta,
    a.days,
    s_grnplm_vd_hr_edp_srv_wf.readable((a.table_size)::numeric) AS table_size,
    round((((a.table_size)::numeric * 100.0) / a.total), 4) AS size_percent,
        CASE a.relstorage
            WHEN 'a'::bpchar THEN 'row'::bpchar
            WHEN 'c'::bpchar THEN 'column'::bpchar
            WHEN 'h'::bpchar THEN 'uncompress'::bpchar
            ELSE a.relstorage
        END AS storage,
    a.skew,
    s_grnplm_vd_hr_edp_srv_wf.readable((a.n_live_tup)::numeric, 1000) AS n_live_tup,
    s_grnplm_vd_hr_edp_srv_wf.readable(((a.n_delta / a.days))::numeric, 1000) AS n_speed,
    s_grnplm_vd_hr_edp_srv_wf.readable((a.n_delta)::numeric, 1000) AS n_delta,
    s_grnplm_vd_hr_edp_srv_wf.readable((a.n_dead_tup)::numeric, 1000) AS n_dead_tup,
    a.last_analyze,
    a.last_vacuum
   FROM ( SELECT a_1.load_date,
            a_1.table_schema,
            a_1.table_name,
            a_1.table_size,
            a_1.relstorage,
            a_1.reloptions,
            a_1.distributedby,
            a_1.partition_def,
            a_1.prt_end,
            a_1.n_live_tup,
            a_1.n_dead_tup,
            a_1.prt_cnt,
            a_1.use_cnt,
            a_1.prt_analyze,
            a_1.last_analyze,
            a_1.last_vacuum,
            a_1.tableowner,
            (a_1.table_size - b.table_size) AS size_delta,
            NULLIF(((a_1.load_date)::date - (b.load_date)::date), 0) AS days,
            sum(a_1.table_size) OVER () AS total,
            (a_1.n_live_tup - b.n_live_tup) AS n_delta,
            sum(a_1.n_live_tup) OVER () AS n_total,
            round((c.skew)::numeric, 2) AS skew
           FROM ((( SELECT DISTINCT ON (log.table_schema, log.table_name) log.load_date,
                    log.table_schema,
                    log.table_name,
                    log.table_size,
                    log.relstorage,
                    log.reloptions,
                    log.distributedby,
                    log.partition_def,
                    log.prt_end,
                    log.n_live_tup,
                    log.n_dead_tup,
                    log.prt_cnt,
                    log.use_cnt,
                    log.prt_analyze,
                    log.last_analyze,
                    log.last_vacuum,
                    log.tableowner
                   FROM log
                  ORDER BY log.table_schema, log.table_name, log.load_date DESC) a_1
             JOIN ( SELECT DISTINCT ON (log.table_schema, log.table_name) log.load_date,
                    log.table_schema,
                    log.table_name,
                    log.table_size,
                    log.relstorage,
                    log.reloptions,
                    log.distributedby,
                    log.partition_def,
                    log.prt_end,
                    log.n_live_tup,
                    log.n_dead_tup,
                    log.prt_cnt,
                    log.use_cnt,
                    log.prt_analyze,
                    log.last_analyze,
                    log.last_vacuum,
                    log.tableowner
                   FROM log
                  ORDER BY log.table_schema, log.table_name, log.load_date) b ON (((a_1.table_schema = b.table_schema) AND (a_1.table_name = b.table_name))))
             LEFT JOIN ( SELECT DISTINCT ON (tb_log_skew.tbl) tb_log_skew.ts,
                    tb_log_skew.tbl,
                    tb_log_skew.skew,
                    tb_log_skew.segments,
                    tb_log_skew.sum,
                    tb_log_skew.min,
                    tb_log_skew.max,
                    tb_log_skew.avg,
                    tb_log_skew.std,
                    tb_log_skew.data_size,
                    tb_log_skew.distributedby,
                    tb_log_skew.tbl_size,
                    tb_log_skew.duration,
                    tb_log_skew.options,
                    tb_log_skew.storage
                   FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_skew
                  WHERE (tb_log_skew.tbl !~~ '%_1_prt_%'::text)) c ON ((format('%s.%s'::text, a_1.table_schema, a_1.table_name) = c.tbl)))
          ORDER BY (a_1.table_size - b.table_size) DESC NULLS LAST) a
  ORDER BY a.size_delta DESC NULLS LAST;

