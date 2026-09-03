CREATE VIEW s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity AS
 SELECT DISTINCT ON (a.id) a.id,
    a.ts,
    a.url,
    (a.msg ->> 'name'::text) AS name,
    (a.msg ->> 'path'::text) AS path,
    (a.msg ->> 'storage'::text) AS storage,
    ((a.msg ->> 'parentId'::text))::bigint AS parentid,
    a.msg
   FROM s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl a
  WHERE (a.obj = 'entity'::text)
  ORDER BY a.id DESC, a.ts DESC;

comment on view s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity is '
Метаданные сущностей системы (объекты, справочники).
Фильтр по obj = 'entity'. Последняя версия каждой сущности.
';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.id is 'ID сущности';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.ts is 'Время изменения';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.url is 'API-путь';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.name is 'Имя сущности';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.path is 'Путь в иерархии';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.storage is 'Хранилище';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.parentid is 'Родительская сущность';
comment on column s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity.msg is 'Полные данные (JSON)';