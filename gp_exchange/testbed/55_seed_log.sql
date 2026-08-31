-- Затравка журнала загрузки.
--
-- do_branch отбирает потоки запросом с INNER JOIN на gp__exchange_log:
--   join (… where wf_type = 'IN_log' …) b on a._gp_name = b.wf where a._gp_id > b.max
-- Поток без единой записи IN_log join отсекает, а запись появляется только после
-- его обработки — на пустой базе круг замкнут, и ветвление возвращает пустой список.
-- Поэтому при заведении стенда (и, судя по всему, нового потока в бою) журнал нужно
-- засеять нулевым ключом: он ниже любого настоящего _gp_id.

INSERT INTO support.gp__exchange_log (_gp_ts, wf_type, wf_name, _gp_data, _gp_key, _gp_id, _gp_hash)
SELECT now(), 'IN_log', name,
       toJSONString(map('bd', 'support', 'table', 'gp_' || name, 'wf_key', '0', 'cnt', '0')),
       '0000000000', 0, generateUUIDv4()
FROM (
    SELECT arrayJoin([
        'vw_ztest', 'vw_exchange_log', 'vw_log_ctl_entity', 'vw_log_ctl_loading',
        'vw_log_ctl_wf', 'vw_log_workflow', 'vw_swf_ctl_log', 'vw_swf_chk_log'
    ]) AS name
);
