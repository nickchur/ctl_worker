CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_ztest_set(_object text, _active boolean DEFAULT NULL::boolean, _rollback boolean DEFAULT NULL::boolean, _z_cfg json DEFAULT NULL::json, _z_except date[] DEFAULT NULL::date[], _z_error date[] DEFAULT NULL::date[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    _old json;
    _new json;
    txt text = '';
    log_id int4;
begin
    log_id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action('start', concat('ztest_set ', _object));
    begin
        _object = _object::regclass::text;

        select  row_to_json(a.*) into _old from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config a where object = _object ;
        delete from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config where object = _object;
        _old = (select json_object_agg(key, value) from json_each(_old) where key not in ('object') );

        _active   = coalesce(_active,  (_old->>'active')::bool, true);
        _rollback = coalesce(_rollback,(_old->>'rollback')::bool, false);
        _z_cfg    = coalesce(_z_cfg,   (_old->>'z_cfg')::json, null);
        
        _z_except = coalesce(_z_except,translate(_old->>'z_except', '[]', '{}')::date[], '{}'::date[]);
        _z_error  = coalesce(_z_error, translate(_old->>'z_error', '[]', '{}')::date[], '{}'::date[]);

        with ins as (
            insert into s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config 
            (object, active, rollback, z_cfg, z_except, z_error)
            values (_object, _active, _rollback, _z_cfg, _z_except, _z_error)
            returning *
        )
        select row_to_json(ins.*) into _new from ins limit 1;

        _new = (select json_object_agg(key, value) from json_each(_new) where key not in ('object') );
        txt = format('Ok  %s %s ~ Old %s', _new::text, _object, _old::text);
        log_id = s_grnplm_vd_hr_edp_srv_wf.pr_log_action('end', txt, log_id);
        return txt;

    exception when OTHERS then
        declare 
            e_txt text;
            e_detail text;
            e_hint text;
            e_context text;
        begin
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

            perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(log_id, e_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ

            return concat('Error: ', e_txt);
        end;
    end;
end; 
$body$
EXECUTE ON ANY;
	