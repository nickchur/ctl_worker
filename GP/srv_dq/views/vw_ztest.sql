CREATE VIEW s_grnplm_vd_hr_edp_srv_dq.vw_ztest AS
 SELECT z.ts,
    z.object,
    COALESCE(((z.notes ->> 'ztest'::text))::boolean, ((z.zscore >= COALESCE(((c.z_cfg ->> 'zfr'::text))::double precision, ('-5.0'::numeric)::double precision)) AND (z.zscore <= COALESCE(((c.z_cfg ->> 'zto'::text))::double precision, ((+ 5.0))::double precision)))) AS ztest_ok,
    (z.key_date = ANY (c.z_except)) AS is_except,
    (z.key_date = ANY (c.z_error)) AS is_error,
    COALESCE((round(((100)::numeric - (abs((z.zscore)::numeric) * (2)::numeric)), 0))::integer, 0) AS confidence,
    ((z.std)::double precision / (NULLIF(z.avg, 0))::double precision) AS stable,
    z.zscore,
    z.key_date,
    z.rows_count,
    z.key_diff,
    z.value,
    z.avg,
    z.std,
    z.cnt,
    z.min,
    z.max,
    z.log_id,
    z.notes,
    c.rollback AS error,
    c.z_cfg,
    c.z_except,
    c.z_error
   FROM (s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data z
     LEFT JOIN s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config c ON (((z.object = c.object) AND c.active)))
  ORDER BY z.ts DESC, z.key_date DESC;

comment on view s_grnplm_vd_hr_edp_srv_dq.vw_ztest is '
Представление результатов Z-теста для мониторинга качества данных. 
Содержит оценку отклонений метрик от нормы, уровень доверия, стабильность и флаги исключений/ошибок.
';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.ts is 'Время записи';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.object is 'Объект контроля';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.ztest_ok is 'Результат Z-теста (в пределах нормы)';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.is_except is 'Дата в списке исключений';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.is_error is 'Дата в списке ошибок';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.confidence is 'Уровень доверия, %';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.stable is 'Стабильность (std/avg)';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.zscore is 'Z-оценка отклонения';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.key_date is 'Ключевая дата';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.rows_count is 'Число строк в данных';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.key_diff is 'Разница по ключу';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.value is 'Текущее значение';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.avg is 'Среднее историческое';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.std is 'Среднеквадратичное отклонение';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.cnt is 'Число исторических значений';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.min is 'Минимальное значение';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.max is 'Максимальное значение';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.log_id is 'ID лога обработки';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.notes is 'Доп. информация (JSON)';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.error is 'Описание ошибки';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.z_cfg is 'Конфиг Z-теста (JSON)';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.z_except is 'Список исключений по датам';
comment on column s_grnplm_vd_hr_edp_srv_dq.vw_ztest.z_error is 'Список ошибок по датам';