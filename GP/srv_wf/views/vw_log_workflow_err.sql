CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow_err AS
 SELECT a.log_id,
    a.dt,
    a.message,
    b.last_call,
    b.calls,
    a.context,
    a.hint,
    a.detail,
    a.usename
   FROM (s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_err a
     LEFT JOIN ( SELECT b_1.log_id,
            max(
                CASE
                    WHEN (b_1.rn = 1) THEN b_1.name
                    ELSE NULL::text
                END) AS last_call,
            string_agg(b_1.name, '->'::text ORDER BY b_1.rn DESC) AS calls
           FROM ( SELECT a_1.log_id,
                    err_str.ordinality AS rn,
                    "substring"(err_str.err_str, '(\w+ \w+) '::text) AS type,
                    "substring"(err_str.err_str, '\w+ \w+ "?([\.\w]+)'::text) AS name,
                    "substring"(err_str.err_str, ' line (\d+) at '::text) AS line,
                    "substring"(err_str.err_str, ' line \d+ at (.+)'::text) AS at,
                    err_str.err_str,
                    a_1.context
                   FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_err a_1,
                    LATERAL unnest(string_to_array(a_1.context, chr(10))) WITH ORDINALITY err_str(err_str, ordinality)) b_1
          WHERE (b_1.type = ANY (ARRAY['pgSQL function'::text, 'SQL statement'::text, 'External table'::text]))
          GROUP BY b_1.log_id) b ON ((a.log_id = b.log_id)))
  ORDER BY a.log_id DESC;

