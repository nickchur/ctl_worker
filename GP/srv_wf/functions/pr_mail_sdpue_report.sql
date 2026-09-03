CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_sdpue_report(reports text[] DEFAULT NULL::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    sql text;
    txt text = 'SDPUE Report';
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
        max_ts = (
            select max(a.ts)::timestamp(0) 
            from vw_exchange_log_ids a
            left join vw_log_ctl_wf b on (a.msg ->> 'wf_id')::int4 = b.id
            where a.ts >= current_date - 7 and b.scheduled
            group by a.wf_name
            order by 1
            limit 1
        );
        res = case 
            when max_ts < now() - '24 hours'::interval then -3
            -- when max_ts < now() - '24 hour'::interval then 0
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
                    when %key% in ('retry') then 
                        case 
                        when %type% = 'boolean' and %value% = 'true' then 'red' 
                        when %type% = 'boolean' and %value% = 'false' then 'green' 
                        else null end
                    when %key% in ('alive') then 
                        case 
                        when %value% = 'ABORTED' then 'grey' 
                        when %value% = 'COMPLETED' then 'green' 
                        when %value% = 'ACTIVE' then 'gold' 
                        else null end
                    when %key% in ('status') then 
                        case 
                        when %value% = 'ERROR' then 'red' 
                        when %value% = 'SUCCESS' then 'green' 
                        when %value% = 'ABORTED' then 'grey' 
                        else null end
                    else
                        case 
                        when %type% = 'boolean' and %value% = 'true' then 'green' 
                        when %type% = 'boolean' and %value% = 'false' then 'red' 
                        else null end
                    end
                union
                select 'style' , 'background:'||
                    case 
                    when %key% in ('sum_len') then 
                        case 
                        when (%value%) ~ 'T' then 'salmon'
                        when (%value%) ~ 'G' then 'pink'
                        when (%value%) ~ 'MB' then 'yellow'
                        when (%value%) ~ 'kB' then 'LemonChiffon'
                        else null end
                    when %key% in ('status_dttm') then 
                        case 
                        when (%value%)::timestamp < now() - '7 day'::interval then 'red'
                        when (%value%)::timestamp < now() - '5 day'::interval then 'salmon'
                        when (%value%)::timestamp < now() - '3 day'::interval then 'pink'
                        when (%value%)::timestamp < now() - '2 day'::interval then 'yellow'
                        when (%value%)::timestamp < now() - '1 day'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('run_time','sql_time') then 
                        case 
                        when (%value%)::interval >= '04:00'::interval then 'red'
                        when (%value%)::interval >= '02:00'::interval then 'salmon'
                        when (%value%)::interval >= '01:00'::interval then 'pink'
                        when (%value%)::interval >= '00:30'::interval then 'yellow'
                        when (%value%)::interval >= '00:15'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('wait_time') then 
                        case 
                        when (%value%)::interval >= '7 day'::interval then 'red'
                        when (%value%)::interval >= '5 day'::interval then 'salmon'
                        when (%value%)::interval >= '3 day'::interval then 'pink'
                        when (%value%)::interval >= '2 day'::interval then 'yellow'
                        when (%value%)::interval >= '1 day'::interval then 'LemonChiffon'
                        else null end
                    else null end
            $$
        );
        
        txt = 'SDPUE Last';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
            select row_number() over(order by a.status_dttm) as n, a.*
                , b.keys_cnt
                , to_char(b.cnt, 'FM999,999,999,999,999,999') rows_count
                -- , round(b.sum_len/1024/1024,1)::text || ' Mb' sum_len
                , pg_size_pretty(b.sum_len) sum_len
                , b.time sql_time
                , b.completed
                , b.retry
            from (
                select distinct on (a.wf_id)
                    -- , concat(a.id, '<br>', profile) id
                    a.id
                    , a.auto::bool
                    , alive
                    -- , start_dttm, end_dttm, profile
                    , a.status_dttm::timestamp(0)::text
                    , -((a.msg ->> 'start_dttm')::timestamp - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) wait_time
                    , (a.status_dttm - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) run_time
                    --, (now()::timestamp(0) - a.status_dttm::timestamp(0))::interval(0) status_time
                    , status
                    , wf_id
                    -- , b.profile
                    -- , b.category
                    -- , split_part(category, '.', 1) category
                    , concat(split_part(category, '.', 2), ' ('||nullif(b.profile, 'HR_Data')||')')  category
                    -- , b.name 
                    , replace(name, 'pc1080.', '') name
                    -- , wf_interval
                    --, status_log
                    , b.scheduled
                    
--                    , concat(status, ': ', replace(status_log, '''', '')) status_info
                    --, replace(connected, ',', ',<br>') connected
                    --, replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    --, split_part(wf_sched, ',', 1) wf_sched
                    --, replace(param, ';', ';<br>') param

                from vw_log_ctl_loading a
                left join vw_log_ctl_wf b on a.wf_id=b.id
                where true
                    and status not in ('EVENT-WAIT', 'TIME-WAIT', 'ABORTED')
                    and alive <> 'ACTIVE' 
                    and b.category like 'p1080.sdpue'
                    and a.status_dttm between current_date - 7 and current_date + 1
                order by a.wf_id, a.id::int8 desc
            ) a
            left join vw_exchange_log_ids b on a.id = b.id
            order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
        

        txt = 'SDPUE logged Last';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select distinct on(a.wf_name)
                    a.ts::timestamp(0)::text
                    , a.wf_name
                    , a.id
                    , a.completed
                    , a.keys_cnt
                    , to_char(a.cnt, 'FM999,999,999,999,999,999') rows_count
                    -- , round(a.sum_len/1024/1024,1)::text || ' Mb' sum_len
                    , pg_size_pretty(a.sum_len) sum_len
                    , a.time sql_time
                    , a.retry
                    , a.status_dttm::timestamp(0)::text
                    , a.auto::bool
                    , a.alive
                    , a.status
                    , -((a.msg ->> 'start_dttm')::timestamp - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) wait_time
                    , (a.status_dttm - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) run_time
                    , b.scheduled
                    , left(replace(coalesce(a.status_log::text, a.wf_keys::text), '"', ''), 100) notes
                from vw_exchange_log_ids a
                left join vw_log_ctl_wf b on (a.msg ->> 'wf_id')::int4 = b.id
                where a.ts >= current_date - 7
                order by a.wf_name desc, a.ts desc
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by ts', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
        

        txt = 'SDPUE Errors';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
            select row_number() over(order by a.status_dttm) as n, a.*
                , b.keys_cnt
                , to_char(b.cnt, 'FM999,999,999,999,999,999') rows_count
                -- , round(b.sum_len/1024/1024,1)::text || ' Mb' sum_len
                , pg_size_pretty(b.sum_len) sum_len
                , b.time sql_time
                , b.completed
                , b.retry
            from (
                select distinct on (a.wf_id)
                    a.id
                    , a.auto::bool
                    , alive
                    , a.status_dttm::timestamp(0)::text
                    , -((a.msg ->> 'start_dttm')::timestamp - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) wait_time
                    , (a.status_dttm - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) run_time
                    , status
                    , wf_id
                    , concat(split_part(category, '.', 2), ' ('||nullif(b.profile, 'HR_Data')||')')  category
                    , replace(name, 'pc1080.', '') name
                    , b.scheduled
                    
                from vw_log_ctl_loading a
                left join vw_log_ctl_wf b on a.wf_id=b.id
                where true
                    and (status in ('ABORTED', 'ERROR', 'ERRORCHECK') or alive in ('ABORTED'))
                    and b.category like 'p1080.sdpue'
                    and a.status_dttm between current_date - 1 and current_date + 1
                order by a.wf_id, a.id::int8 desc
            ) a
            left join vw_exchange_log_ids b on a.id = b.id
            order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
        

        txt = 'SDPUE logged Errors';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select 
                    a.ts::timestamp(0)::text as err_ts
                    , a.wf_name
                    , a.id
                    , a.completed
                    , a.keys_cnt
                    , to_char(a.cnt, 'FM999,999,999,999,999,999') rows_count
                    -- , round(a.sum_len/1024/1024,1)::text || ' Mb' sum_len
                    , pg_size_pretty(a.sum_len) sum_len
                    , a.time sql_time
                    , a.retry
                    , a.status_dttm::timestamp(0)::text
                    , a.auto::bool
                    , a.alive
                    , a.status
                    , -((a.msg ->> 'start_dttm')::timestamp - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) wait_time
                    , (a.status_dttm - (
                            SELECT min(a.value ->> 'effective_from'::text) AS max
                            FROM json_array_elements((a.msg -> 'loading_status'::text)::json) a(value)
                            where value->>'status' = 'RUNNING'
                        )::timestamp)::interval(0) run_time
                    , b.scheduled
                    , left(replace(coalesce(a.status_log::text, a.wf_keys::text), '"', ''), 100) notes
                from vw_exchange_log_ids a
                left join vw_log_ctl_wf b on (a.msg ->> 'wf_id')::int4 = b.id
                where a.ts >= current_date - 1
                    and (not a.completed or a.retry)
                order by a.ts desc
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by err_ts desc', style);
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
	