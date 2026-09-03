CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_swf_ctl_log AS
 SELECT a.id AS beg_id,
    a.ts AS beg_ts,
    a.wf_action AS beg_action,
    (a.wf_message ->> 'wf'::text) AS wf,
    ((a.wf_message ->> 'lid'::text))::integer AS loading_id,
    (b.ts - a.ts) AS duration,
    b.id AS end_id,
    b.ts AS end_ts,
    b.wf_action AS end_action,
    (COALESCE((b.wf_message ->> 'res'::text), (b.wf_message ->> 'reselt'::text)))::integer AS res,
    ((b.wf_message ->> 'wf_id'::text))::integer AS wf_id,
    (b.wf_message ->> 'msg'::text) AS msg,
    a.wf_message AS beg_msg,
    b.wf_message AS end_msg
   FROM (s_grnplm_vd_hr_edp_srv_wf.tb_swf_ctl_log a
     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.tb_swf_ctl_log b ON ((a.id = b.parent)))
  WHERE (a.parent IS NULL);

