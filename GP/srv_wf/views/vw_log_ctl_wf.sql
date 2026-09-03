CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf AS
 WITH wf AS (
         SELECT DISTINCT ON (a_1.id) a_1.ts,
            a_1.id,
            a_1.url,
            a_1.connectedentities,
            a_1.connectedstats,
            a_1.msg
           FROM ( SELECT a_2.ts,
                    a_2.id,
                    a_2.url,
                    a_2.connectedentities,
                    a_2.connectedstats,
                    a_2.msg
                   FROM ( SELECT DISTINCT ON (a_3.id) a_3.ts,
                            a_3.id,
                            a_3.url,
                            (a_3.msg -> 'connectedEntities'::text) AS connectedentities,
                            (a_3.msg -> 'connectedStats'::text) AS connectedstats,
                            (a_3.msg -> 'wf'::text) AS msg
                           FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a_3
                          WHERE ((a_3.url = '/wf/extended'::text) AND (((a_3.msg -> 'wf'::text) ->> 'category'::text) ~~ 'p1080%'::text))
                          ORDER BY a_3.id, a_3.ts DESC) a_2
                UNION ALL
                 SELECT a_2.ts,
                    a_2.id,
                    a_2.url,
                    a_2.connectedentities,
                    a_2.connectedstats,
                    a_2.msg
                   FROM ( SELECT DISTINCT ON ((((a_3.msg -> 'workflow'::text) ->> 'id'::text))::bigint) a_3.ts,
                            (((a_3.msg -> 'workflow'::text) ->> 'id'::text))::bigint AS id,
                            a_3.url,
                            (b_1.connectedentities)::jsonb AS connectedentities,
                            '[]'::jsonb AS connectedstats,
                            (a_3.msg -> 'workflow'::text) AS msg
                           FROM (s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a_3
                             LEFT JOIN ( SELECT DISTINCT ON (("substring"(a_4.url, '/entity/wf/(\d+)'::text))::bigint) a_4.ts,
                                    ("substring"(a_4.url, '/entity/wf/(\d+)'::text))::bigint AS id,
                                    a_4.url,
                                    json_agg(a_4.id) AS connectedentities
                                   FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a_4
                                  WHERE (a_4.url ~ '/entity/wf/\d+'::text)
                                  GROUP BY a_4.ts, a_4.url
                                  ORDER BY ("substring"(a_4.url, '/entity/wf/(\d+)'::text))::bigint, a_4.ts DESC) b_1 ON (((((a_3.msg -> 'workflow'::text) ->> 'id'::text))::bigint = b_1.id)))
                          WHERE ((a_3.url ~ '/loading/\d+'::text) AND (((a_3.msg -> 'workflow'::text) ->> 'category'::text) ~~ 'p1080%'::text))
                          ORDER BY (((a_3.msg -> 'workflow'::text) ->> 'id'::text))::bigint, a_3.ts DESC) a_2) a_1
          ORDER BY a_1.id, a_1.ts DESC
        ), statval AS (
         SELECT DISTINCT ON ((a_1.msg ->> 'entity_id'::text), (a_1.msg ->> 'stat_id'::text), (a_1.msg ->> 'profile'::text)) (a_1.msg ->> 'entity_id'::text) AS entity_id,
            (a_1.msg ->> 'stat_id'::text) AS stat_id,
            (a_1.msg ->> 'profile'::text) AS profile,
            a_1.id AS loading_id
           FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a_1
          WHERE (a_1.obj = 'statval'::text)
          ORDER BY (a_1.msg ->> 'entity_id'::text), (a_1.msg ->> 'stat_id'::text), (a_1.msg ->> 'profile'::text), a_1.ts DESC
        ), active AS (
         SELECT wf.id,
            (a_1.value ->> 'profile'::text) AS profile,
            (a_1.value ->> 'stat_id'::text) AS stat_id,
            (a_1.value ->> 'entity_id'::text) AS entity_id
           FROM wf,
            LATERAL json_array_elements(((wf.msg ->> 'wf_event_sched'::text))::json) a_1(value)
          WHERE ((a_1.value ->> 'active'::text))::boolean
        )
 SELECT a.id,
    a.ts,
    a.url,
    (a.msg ->> 'profile'::text) AS profile,
    (a.msg ->> 'category'::text) AS category,
    (a.msg ->> 'name'::text) AS name,
    ((a.msg ->> 'scheduled'::text))::boolean AS scheduled,
    ((a.msg ->> 'deleted'::text))::boolean AS deleted,
    ((a.msg ->> 'singleLoading'::text))::boolean AS singleloading,
    (a.msg ->> 'engine'::text) AS engine,
    (a.msg ->> 'type'::text) AS type,
    ( SELECT string_agg(concat((a_1.value)::text, ': ', b_1.name), ', '::text) AS string_agg
           FROM (json_array_elements((a.connectedentities)::json) a_1(value)
             LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity b_1 ON ((((a_1.value)::text)::bigint = b_1.id)))) AS connected,
    (a.msg ->> 'eventAwaitStrategy'::text) AS eventawaitstrategy,
    COALESCE((((a.msg ->> 'eventAwaitStrategy'::text) || ': '::text) || b.wf_sch), concat('sched: ', ((a.msg -> 'wf_time_sched'::text) ->> 'sched'::text))) AS wf_sched,
    ( SELECT string_agg(concat((jsn.value ->> 'param'::text), ': ', replace((jsn.value ->> 'prior_value'::text), '$'::text, '%'::text)), '; '::text) AS string_agg
           FROM json_array_elements(((a.msg ->> 'param'::text))::json) jsn(value)) AS param,
    (a.msg ->> 'statusNotifications'::text) AS statusnotifications,
    (json_build_object('wf', a.msg, 'connectedEntities', a.connectedentities, 'connectedStats', a.connectedstats))::jsonb AS msg,
    (COALESCE(( SELECT (jsn.value ->> 'prior_value'::text)
           FROM json_array_elements(((a.msg ->> 'param'::text))::json) jsn(value)
          WHERE ((jsn.value ->> 'param'::text) = 'wf_interval'::text)
         LIMIT 1), '1 day'::text))::interval AS wf_interval
   FROM (wf a
     LEFT JOIN ( SELECT a_1.id,
            string_agg(a_1.wf_sch, ', '::text) AS wf_sch
           FROM ( SELECT a_2.id,
                    concat(a_2.profile, ' [', a_2.stat_id, '] {', string_agg(concat(a_2.entity_id, ' (', COALESCE(((c.end_dttm)::timestamp(0) without time zone)::text, (b_1.loading_id)::text)), '), '::text ORDER BY c.end_dttm NULLS FIRST, b_1.loading_id NULLS FIRST), ')}') AS wf_sch
                   FROM ((active a_2
                     LEFT JOIN statval b_1 ON ((((a_2.entity_id = b_1.entity_id) AND (a_2.stat_id = b_1.stat_id)) AND (a_2.profile = b_1.profile))))
                     LEFT JOIN s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading c ON ((b_1.loading_id = c.id)))
                  GROUP BY a_2.id, a_2.profile, a_2.stat_id) a_1
          GROUP BY a_1.id) b ON ((a.id = b.id)));

