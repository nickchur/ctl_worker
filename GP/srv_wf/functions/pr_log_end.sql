CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_end(log_id integer, log_tb text DEFAULT NULL::text, period_date text DEFAULT NULL::text, load_date text DEFAULT NULL::text, key_date text DEFAULT NULL::text) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    new_id int4;
    txt text;
begin
    new_id = nextval('s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_id_seq'::regclass);

    if log_tb is not null then
        txt = s_grnplm_vd_hr_edp_srv_wf.pr_log_stat(new_id, log_tb, period_date, load_date, key_date);
    end if;

    insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow 
        (id, parent, wf_action, wf_message) 
        values (new_id, nullif(log_id,0), 'end', log_tb);  

    return new_id;

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

        if sqlstate = 'XX001' then
            raise exception using ERRCODE = sqlstate, MESSAGE = e_txt, DETAIL = e_detail, HINT = e_hint;
        end if;
        
        perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(log_id,e_txt,e_detail,e_hint,e_context) ; --ЛОГИРОВАНИЕ
        return 0;
    end;
end;
$body$
EXECUTE ON ANY;
	