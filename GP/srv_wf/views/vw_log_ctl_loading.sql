CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading AS
 SELECT DISTINCT ON (a.id) a.id,
    a.ts,
    a.url,
    (a.msg ->> 'alive'::text) AS alive,
    (a.msg ->> 'auto'::text) AS auto,
    ((a.msg ->> 'start_dttm'::text))::timestamp without time zone AS start_dttm,
    ((a.msg ->> 'end_dttm'::text))::timestamp without time zone AS end_dttm,
    (a.msg ->> 'profile'::text) AS profile,
    ((a.msg ->> 'wf_id'::text))::bigint AS wf_id,
        CASE
            WHEN ((a.msg ->> 'alive'::text) = 'ABORTED'::text) THEN ((a.msg ->> 'end_dttm'::text))::timestamp without time zone
            ELSE (( SELECT max((a_1.value ->> 'effective_from'::text)) AS max
               FROM json_array_elements(((a.msg -> 'loading_status'::text))::json) a_1(value)))::timestamp without time zone
        END AS status_dttm,
        CASE
            WHEN ((a.msg ->> 'alive'::text) = 'ABORTED'::text) THEN 'ABORTED'::text
            ELSE (a.msg ->> 'status'::text)
        END AS status,
    (a.msg ->> 'status_log'::text) AS status_log,
    a.msg
   FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a
  WHERE (a.obj = 'loading'::text)
  ORDER BY a.id DESC, a.ts DESC;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading is '
Состояние процессов загрузки данных.
Содержит статус, временные метки, профиль и результат.
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.id is 'ID загрузки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.ts is 'Время события';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.url is 'API-путь';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.alive is 'Текущий статус (ACTIVE, COMPLETED и т.д.)';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.auto is 'Автоматический запуск';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.start_dttm is 'Время старта';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.end_dttm is 'Время завершения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.profile is 'Профиль загрузки';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.wf_id is 'ID связанного workflow';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.status_dttm is 'Время последнего прогресса';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.status is 'Статус выполнения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.status_log is 'Лог процесса';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading.msg is 'Полные данные (JSON)';