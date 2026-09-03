CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_action(v_action text, v_swf text DEFAULT ''::text, v_message json DEFAULT NULL::json, log_id integer DEFAULT 0, ts timestamp without time zone DEFAULT clock_timestamp()) 
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
    sql text;   
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

    sql = format($sql$
        INSERT INTO tb_swf_%s_log (ts, parent, wf_action, wf_message) 
        values (%L, nullif(%s,0), %s, %s)
        returning id
    $sql$, coalesce(lower(v_swf),''), coalesce(ts, clock_timestamp()), coalesce(log_id::text,'Null')
        , coalesce(quote_literal(v_action),'Null'), coalesce(quote_literal(v_message),'Null'));

    raise log '%', sql;
    
    execute sql into new_id;

    return new_id; 

exception when OTHERS then
    get stacked diagnostics m_txt = MESSAGE_TEXT;
    get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
    get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
    get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
    
    perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(log_id,m_txt,e_detail,e_hint,e_context) ; 
    return 0;
end;

$body$
EXECUTE ON ANY;
	