CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl AS
 SELECT DISTINCT ON (a.id, a.obj, a.url) a.ts,
    a.id,
    a.obj,
    a.url,
    a.msg
   FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a
  ORDER BY a.id DESC, a.obj, a.url, a.ts DESC;

