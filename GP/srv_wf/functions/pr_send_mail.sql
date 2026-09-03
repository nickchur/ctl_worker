CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_send_mail(report text DEFAULT ''::text, error integer DEFAULT 0, mark boolean DEFAULT true) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    html text[];
    jsn json;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    -- lock tb_swf_mail_log in EXCLUSIVE mode;
    
    if try_cast2int(report) is null then
        jsn = (
            select row_to_json(a.*) from (
                select id, ts, wf_action
                from tb_swf_mail_log a
                where nullif(parent, 0) is null 
                    and wf_action ~ report
                    and id not in (select parent from tb_swf_mail_log where wf_action = 'send')
                    and id in (select parent from tb_swf_mail_log where wf_action = 'end')
                order by id 
                limit 1
            ) a
        );
    else
        jsn = (
            select row_to_json(a.*) from (
                select id, ts, wf_action
                from tb_swf_mail_log a
                where nullif(parent, 0) is null 
                    and id = try_cast2int(report)
                order by id 
                limit 1
            ) a
        );
    end if;
    
    if jsn is null then
        return json_build_object('res', error, 'msg', format('No %s', report), 'html', html)::text;
    end if;
    
    mail_id = coalesce((jsn->>'id')::int4, -1);
    
    html = (
        select array_agg(a.html order by id) from (
            select id, a.wf_message->>'html' html
            from tb_swf_mail_log a
            where parent = mail_id or id = mail_id
            order by id 
        ) a
        where nullif(a.html, '') is not Null
        limit 1
    );
    
    if mark then
        mail_id = pr_swf_log_action('send', 'mail', jsn, mail_id);
    end if;
    
    return json_build_object('res', 1, 'id', mail_id,'ts', jsn->>'ts', 'report', jsn->>'wf_action', 'html', html)::text; 

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

        perform pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ; 
        return e_txt;
    end;
end;

$body$
EXECUTE ON ANY;
	