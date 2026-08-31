-- Тестовые данные: по несколько строк на каждый поток, за два дня.
-- Даты — от текущей, чтобы условия вида "or ts >= current_date - 1" срабатывали.

INSERT INTO s_grnplm_vd_hr_edp_srv_dq.vw_ztest
SELECT (current_date - d)::timestamp + (i || ' hour')::interval,
       'vw_object_' || i, (i % 2 = 0), false, false, 0.95, true, 1.5 + i,
       (current_date - d)::text, 1000 + i, i, 'значение ' || i, 10.5, 1.2, 100 + i, 1, 99,
       i, 'заметка со спецсимволами: кавычка '' и таб →', null, '{}', '[]', null
FROM generate_series(0, 1) d, generate_series(1, 5) i;

INSERT INTO s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_entity
SELECT i, now() - (i || ' hour')::interval, 'https://ctl/entity/' || i,
       'сущность ' || i, '/path/' || i, 'GP', 100, 'ok'
FROM generate_series(1, 4) i;

INSERT INTO s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading
SELECT i, now() - (i || ' hour')::interval, 'https://ctl/loading/' || i,
       'COMPLETED', true, now() - (i || ' hour')::interval, now(), 'p1080', 500 + i,
       now(), 'SUCCESS', 'END', 'ok'
FROM generate_series(1, 4) i;

INSERT INTO s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf
SELECT i, now() - (i || ' hour')::interval, 'https://ctl/wf/' || i, 'p1080', 'p1080.CDM',
       'wf_' || i, true, false, true, 'GP', 'SQL', '', 'or', '0 2 * * *', '{}', '[]', 'ok', 'days=1'
FROM generate_series(1, 4) i;

INSERT INTO s_grnplm_vd_hr_edp_srv_wf.vw_log_workflow
SELECT i, now() - (i || ' hour')::interval, 'START', 'wf_' || i, i, now(), 'END',
       '00:05:00', 'сообщение ' || i, 1000 * i, 'month', '2026-08-01', '2026-08-31',
       'load', '1', '100', 'key', '1', '999', 1.1, true, 0.9, current_date::text
FROM generate_series(1, 4) i;

INSERT INTO s_grnplm_vd_hr_edp_srv_wf.vw_swf_ctl_log
SELECT i, now() - (i || ' hour')::interval, 'BEG', '00:01:00', i, now(), 'END',
       'начало ' || i, 'конец ' || i
FROM generate_series(1, 4) i;

INSERT INTO s_grnplm_vd_hr_edp_srv_wf.vw_swf_chk_log
SELECT i, now() - (i || ' hour')::interval, 'CHK', 'tb_object_' || i, 'stg', i, now(),
       '00:00:30', 'END', 1, 'проверка пройдена', '42', 'beg', 'end'
FROM generate_series(1, 4) i;
