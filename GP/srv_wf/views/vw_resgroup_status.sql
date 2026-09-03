CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_resgroup_status AS
 SELECT r.rsgname,
    s.groupid,
    s.num_running,
    s.num_queueing,
    s.num_queued,
    s.num_executed,
    s.total_queue_duration,
    s.cpu_usage,
    s.memory_usage
   FROM pg_resgroup_get_status(NULL::oid) s(groupid, num_running, num_queueing, num_queued, num_executed, total_queue_duration, cpu_usage, memory_usage),
    pg_resgroup r
  WHERE (s.groupid = r.oid);

