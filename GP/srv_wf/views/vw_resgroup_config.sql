CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_resgroup_config AS
 SELECT g.oid AS groupid,
    g.rsgname AS groupname,
    t1.value AS concurrency,
    t2.value AS cpu_rate_limit,
    t3.value AS memory_limit,
    t4.value AS memory_shared_quota,
    t5.value AS memory_spill_ratio,
        CASE
            WHEN (t6.value IS NULL) THEN 'vmtracker'::text
            WHEN (t6.value = '0'::text) THEN 'vmtracker'::text
            WHEN (t6.value = '1'::text) THEN 'cgroup'::text
            ELSE 'unknown'::text
        END AS memory_auditor,
    t7.value AS cpuset
   FROM (((((((pg_resgroup g
     JOIN pg_resgroupcapability t1 ON (((g.oid = t1.resgroupid) AND (t1.reslimittype = 1))))
     JOIN pg_resgroupcapability t2 ON (((g.oid = t2.resgroupid) AND (t2.reslimittype = 2))))
     JOIN pg_resgroupcapability t3 ON (((g.oid = t3.resgroupid) AND (t3.reslimittype = 3))))
     JOIN pg_resgroupcapability t4 ON (((g.oid = t4.resgroupid) AND (t4.reslimittype = 4))))
     JOIN pg_resgroupcapability t5 ON (((g.oid = t5.resgroupid) AND (t5.reslimittype = 5))))
     LEFT JOIN pg_resgroupcapability t6 ON (((g.oid = t6.resgroupid) AND (t6.reslimittype = 6))))
     LEFT JOIN pg_resgroupcapability t7 ON (((g.oid = t7.resgroupid) AND (t7.reslimittype = 7))));

