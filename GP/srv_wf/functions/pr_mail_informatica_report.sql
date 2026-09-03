CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_informatica_report(reports text[] DEFAULT NULL::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    sql text;
    txt text = 'Informatica Report';
    html text = '';
    style json;
    max_ts timestamp;
    stl text;
    res int4;
    log_id int4;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('REPORT_%1$s', replace(txt, ' ', '_')));
    begin
        max_ts = (select max(start_ts) from vw_log_workflow where start_action = 'log_inf');
        res = case 
            when max_ts < now() - '2 hours'::interval then -3
            when max_ts < now() - '1 hour'::interval then 0
            else 1 end;

        -- html = '<style>table,th,tr,td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        html = html || '<style>#table,#th,#tr,#td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        -- html = html || '<style>#tr:nth-child(odd) {background:#fff} #tr:nth-child(even) {background:#f7f7а7}</style>';
        html = html || '<style>#np {font-size:0px}</style>';
        html = html || '<style>#num {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; font-weight:500; text-align: right}</style>';

        -- html = html || format('<h2> %s </h2><h4> %s </h4>', txt, left(now()::text,16));
        stl = 'color:' || (case res when -3 then 'red' when 0 then 'darkred' else 'darkgreen' end);
        html = html || format('<table><h2 style="%3$s"> %1$s </h2><h4 style="%3$s"> Last %2$s </h4></table>', txt, left(max_ts::text, 16), stl);

        mail_id = pr_swf_log_action( txt, 'mail', json_build_object( 'html', html));
    
        style = json_build_object(
            'h', format($$ select 'style', %L $$, stl)
            ,
            'th', $$
                select 'id' , 'th'
                union
                select 'style' ,'background:silver' union select 'style' ,'text-align:center' 
            $$
            ,
            '_tr', $$ select 'style' , 'background:' || case  when rn % 2 = 0 then 'snow' else 'white' end $$
            ,
            'td', $$ 
                select 'id' , case when %type% = 'number' or %key% in ('rows_count') then 'num' else 'td' end 
                union
                -- select 'style' , 'text-align:' || case when %type% = 'number' or %key% in ('rows_count') then 'right' else null end
                -- union
                select 'style' , 'color:' || 
                    case 
                    when %type% = 'boolean' and %value% = 'true' then 'red' 
                    when %type% = 'boolean' and %value% = 'false' then 'green' 
                    else null end
                union
                select 'style' , 'background:'||
                    case 
                    when %key% in ('start_ts') then 
                        case 
                        when (%value%)::timestamp < now() - '48 hours'::interval then 'red'
                        when (%value%)::timestamp < now() - '12 hours'::interval then 'pink'
                        when (%value%)::timestamp < now() - '6 hours'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('period_to', 'load_max') then 
                        case 
                        when (%value%)::timestamp::date < current_date - 5 then 'red'
                        when (%value%)::timestamp::date < current_date - 3 then 'pink'
                        when (%value%)::timestamp::date < current_date - 2 then 'LemonChiffon'
                        else null end
                    when %key% in ('duration') then 
                        case 
                        when (%value%) >= '02:30' then 'red'
                        when (%value%) >= '02:00' then 'salmon'
                        when (%value%) >= '01:30' then 'pink'
                        when (%value%) >= '01:00' then 'yellow'
                        when (%value%) >= '00:30' then 'LemonChiffon'
                        else null end
                    else null end
            $$
        );
        
        txt = 'Informatica Last';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select distinct on (workflow, start_action, end_action)
                -- select distinct on (workflow, start_action)
                    left(start_ts::text, 16) start_ts
                    --, left(end_ts::text, 16) end_ts
                    , workflow
                    -- , start_action
                    -- , end_action 
                    , concat(start_action,' - ' || end_action) action
                    , left(duration::text, 8) duration
                    , to_char(rows_count, 'FM999 999 999 999 999 999') rows_count
                    , period_name
                    , left(period_from::text, 16) period_from
                    , left(period_to::text, 16) period_to
                    , message txt
                from vw_log_workflow 
                where start_ts > current_date - '1 month'::interval
                    and workflow like 'FILE_TO_GP_%' 
                    -- and start_action = 'start'
                    -- and workflow in (select distinct workflow from vw_log_workflow where start_action = 'log_inf' and start_ts > current_date - 3)
                order by workflow, start_action, end_action, start_ts desc
                -- order by workflow, start_action, start_ts desc
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by start_ts desc', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
        
        txt = 'Informatica Log';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select -- distinct on (workflow, start_action, end_action)
                    left(start_ts::text, 16) start_ts
                    , workflow, start_action
                    --, left(end_ts::text, 16) end_ts
                    , left(duration::text, 8) duration
                    --, end_action 
                    , to_char(rows_count, 'FM999 999 999 999 999 999') rows_count
                    , period_name
                    , left(period_from::text, 16) period_from
                    , left(period_to::text, 16) period_to
                    , message txt
                from vw_log_workflow 
                where start_ts > current_date - '7 days'::interval
                    and workflow like 'FILE_TO_GP_%' 
                    -- and start_action = 'start'
                    -- and workflow in (select distinct workflow from vw_log_workflow where start_action = 'log_inf' and start_ts > current_date - 3)
                order by workflow, start_action, end_action, start_ts desc
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by start_ts desc', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;

        txt = pr_swf_log_action('end', 'mail', null, mail_id)::text;
        txt = pr_send_mail(mail_id::text);
        
        txt = (select json_object_agg(key, value) from (select 'res' key, res::text::json value union all select * from json_each(txt::json) where key not in ('res')) a)::text;

        log_id = pr_Log_end(log_id); 
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

            log_id = pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context); 
            return e_txt;
        end;
    end;
end;

$body$
EXECUTE ON ANY;
	