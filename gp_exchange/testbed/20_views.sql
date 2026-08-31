-- Вьюхи журнала обмена: перенесены из HR_Data как есть.
-- Единственная правка — vw_exchange_log_keys ссылается на vw_log_ctl_loading,
-- которая здесь таблица-заглушка, а не витрина.

CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log AS
 SELECT a.wf_name,
    a.id,
    a.wf_key,
    min(a.cnt) AS cnt,
    min(a.sum_len) AS sum_len,
    min(a.min_len) AS min_len,
    min(a.max_len) AS max_len,
    sum(a."time") AS "time",
    min(a.ts) AS ts,
    ((((((count(1) = 2) AND (max(a.cnt) = min(a.cnt))) AND (NOT (max(a.sum_len) IS DISTINCT FROM min(a.sum_len)))) AND (NOT (max(a.min_len) IS DISTINCT FROM min(a.min_len)))) AND (NOT (max(a.max_len) IS DISTINCT FROM min(a.max_len)))) AND (NOT (max(a.type) IS DISTINCT FROM min(a.type)))) AS completed,
    (concat((('{"ts":"'::text || max(a.ts)) || '"'::text), ((',"type":"'::text || min(a.type)) || '"'::text),
        CASE WHEN (count(1) <> 2) THEN (',"count":'::text || count(1)) ELSE ''::text END,
        CASE WHEN (max(a.cnt) <> min(a.cnt)) THEN (',"cnt":'::text || max(a.cnt)) ELSE ''::text END,
        CASE WHEN (max(a.sum_len) IS DISTINCT FROM min(a.sum_len)) THEN (',"sum_len":'::text || max(a.sum_len)) ELSE ''::text END,
        CASE WHEN (max(a.min_len) IS DISTINCT FROM min(a.min_len)) THEN (',"min_len":'::text || max(a.min_len)) ELSE ''::text END,
        CASE WHEN (max(a.max_len) IS DISTINCT FROM min(a.max_len)) THEN (',"max_len":'::text || max(a.max_len)) ELSE ''::text END,
        '}'))::json AS wf_data
   FROM ( SELECT b.wf_name, b.id, b.wf_key,
            ((b.wf_data ->> 'cnt'::text))::bigint AS cnt,
            ((b.wf_data ->> 'sum_len'::text))::bigint AS sum_len,
            ((b.wf_data ->> 'min_len'::text))::bigint AS min_len,
            ((b.wf_data ->> 'max_len'::text))::bigint AS max_len,
            ((b.wf_data ->> 'time'::text))::interval AS "time",
            (b.wf_data ->> 'type'::text) AS type,
            b.ts
           FROM s_grnplm_vd_hr_edp_srv_wf.tb_exchange_log b) a
  GROUP BY a.wf_name, a.id, a.wf_key, a.type;

CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys AS
 WITH log AS (
         SELECT DISTINCT ON (a_1.wf_name, a_1.id, a_1.wf_key) a_1.wf_name, a_1.id, a_1.wf_key,
            a_1.cnt, a_1.sum_len, a_1.min_len, a_1.max_len, a_1."time", a_1.ts, a_1.completed, a_1.wf_data,
            b_1.status_dttm, b_1.auto, b_1.alive, b_1.status, b_1.status_log,
            (((NULLIF(b_1.alive, 'ACTIVE'::text) = ANY (ARRAY['COMPLETED'::text, 'ABORTED'::text])) AND ((b_1.status <> 'SUCCESS'::text) OR (NOT a_1.completed))) OR ((NOT a_1.completed) AND (a_1.ts < (now() - '00:30:00'::interval)))) AS retry
           FROM (s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log a_1
             LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading b_1 ON ((b_1.id = a_1.id)))
          ORDER BY a_1.wf_name DESC, a_1.id DESC, a_1.wf_key DESC, a_1.ts DESC
        )
 SELECT DISTINCT ON (a.wf_name, a.wf_key) a.wf_name, a.wf_key, a.completed, a.retry,
    a.ts AS last_ts, a.id AS last_id, b.count AS ids_cnt, b.ids,
    a.cnt, a.sum_len, a."time", a.status_dttm, (a.auto)::boolean AS auto, a.alive, a.status, a.status_log
   FROM (log a
     JOIN ( SELECT log.wf_name, log.wf_key, count(1) AS count, json_agg(log.id ORDER BY log.id DESC) AS ids
           FROM log GROUP BY log.wf_name, log.wf_key) b ON (((a.wf_name = b.wf_name) AND (a.wf_key = b.wf_key))))
  WHERE (a.completed OR (a.retry IS NOT NULL))
  ORDER BY a.wf_name, a.wf_key DESC, a.id DESC;

CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_exchange AS
 WITH log AS (
         SELECT a.wf_name, max(a.wf_key) AS max
           FROM s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log a
          WHERE ((((a.wf_data ->> 'type'::text) = 'OUT'::text) AND a.completed) AND (a.wf_name = 'vw_ztest'::text))
          GROUP BY a.wf_name
        ), retry AS (
         SELECT a.wf_name, a.wf_key
           FROM s_grnplm_vd_hr_edp_srv_wf.vw_exchange_log_keys a
          WHERE (a.retry AND (a.wf_name = 'vw_ztest'::text))
          GROUP BY a.wf_name, a.wf_key
        )
 SELECT a.wf_name, a.wf_key, a.wf_data
   FROM ( SELECT NULL::text AS wf_name, NULL::text AS wf_key, NULL::text AS wf_data LIMIT 0) a
UNION ALL
 SELECT 'vw_ztest'::text AS wf_name,
    ((a.ts)::date)::text AS wf_key,
    (row_to_json(a.*))::text AS wf_data
   FROM ( SELECT a_1.ts, a_1.object, a_1.ztest_ok, a_1.is_except, a_1.is_error, a_1.confidence,
            a_1.stable, a_1.zscore, a_1.key_date, a_1.rows_count, a_1.key_diff, a_1.value,
            a_1.avg, a_1.std, a_1.cnt, a_1.min, a_1.max, a_1.log_id, a_1.notes, a_1.error,
            a_1.z_cfg, a_1.z_except, a_1.z_error
           FROM (s_grnplm_vd_hr_edp_srv_dq.vw_ztest a_1
             JOIN ( SELECT (a_2.ts)::date AS ts
                   FROM s_grnplm_vd_hr_edp_srv_dq.vw_ztest a_2
                  WHERE ((a_2.ts > COALESCE(( SELECT (b_1.max)::date AS max
                           FROM log b_1 WHERE (b_1.wf_name = 'vw_ztest'::text) LIMIT 1), '1900-01-01'::date))
                         OR (a_2.ts > (('now'::text)::date - 1)))
                  GROUP BY (a_2.ts)::date
                UNION
                 SELECT (f.wf_key)::date AS wf_key
                   FROM retry f WHERE (f.wf_name = 'vw_ztest'::text)
                  GROUP BY (f.wf_key)::date
          ORDER BY 1 LIMIT 100) b ON (((a_1.ts)::date = b.ts)))
          WHERE true) a;
