CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_tbl2html_style(sql text, jsn json DEFAULT NULL::json, key text DEFAULT ''::text, value text DEFAULT ''::text, type text DEFAULT ''::text) 
	RETURNS text
	LANGUAGE plpgsql
	IMMUTABLE
as $body$

declare 
    txt text;
begin
    if sql is null then
        return null;
    end if;
    sql = replace(sql, '%row%', '%1$L');
    sql = replace(sql, '%key%', '%2$L');
    sql = replace(sql, '%value%', '%3$L');
    sql = replace(sql, '%type%', '%4$L');
    -- sql = replace(sql, '%last%', '%5$L');
    sql = format(sql, jsn, key, value, type);
    
    execute format($sql$
        select string_agg(a.v, ' ') v from (
            select a.k||'="'||string_agg(a.v, '; ')||'"' v from (
                select null::text k, null::text v
                union %1$s
            ) a
            where a.k is not null and a.v is not null
            group by a.k
        ) a 
   $sql$, sql) into txt;
   
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

        -- perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, sql, e_hint, e_context) ; 
        raise exception using MESSAGE = e_txt, DETAIL = sql, HINT = e_context;
        
        return null;
    end;
end;

$body$
EXECUTE ON ANY;
	