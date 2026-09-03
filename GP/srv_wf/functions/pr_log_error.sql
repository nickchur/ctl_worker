CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_error(log_id integer, m_txt text DEFAULT ''::text, e_detail text DEFAULT ''::text, e_hint text DEFAULT ''::text, e_context text DEFAULT ''::text) 
	RETURNS integer
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    new_id int4;
    context text;
begin
    insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow 
    (parent, wf_action , wf_message) values (nullif(log_id,0), 'error', coalesce(m_txt,''))
    returning id into new_id ;  

    if nullif(e_context,'') is null then
        GET diagnostics context = PG_CONTEXT;
--        e_context = substring(context from 'PL/pgSQL function %PL/pgSQL function #"%#" line%' for '#');
        e_context = context;
--        e_context = substring(context from '.*(SQL statement .*)' );
    end if;

    insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_err (log_id, message, detail, hint, context)
    values (coalesce(nullif(log_id,0),new_id), coalesce(m_txt,''), coalesce(e_detail,''), coalesce(e_hint,''), coalesce(e_context,''));

    return new_id; 
end;
$body$
EXECUTE ON ANY;
	