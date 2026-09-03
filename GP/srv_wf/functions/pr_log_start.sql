CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_start(v_workflow text) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    new_id int4;
    m_txt text;
    e_detail text;
    e_hint text;
    e_context text;
begin
    new_id = (
        select id 
        from (
            select max(id) as id from s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow 
            where wf_action = 'start' and ts>=now() - '1 hour'::interval and wf_message = v_workflow 
        ) a
        where id not in (select distinct parent from s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow where parent is not null)
    );   

    if new_id is null then
        insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow (wf_action, wf_message) values ('start', v_workflow) 
        returning id into new_id ;  
    end if;

    return new_id; 

exception when OTHERS then
    get stacked diagnostics m_txt = MESSAGE_TEXT;
    get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
    get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
    get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
    
    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0,m_txt,e_detail,e_hint,e_context) ; 
    return 0;
end;
$body$
EXECUTE ON ANY;
	