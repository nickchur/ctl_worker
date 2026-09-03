CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_status(reports text[] DEFAULT NULL::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    sql text;
    txt text = 'CTL Status';
    html text = '';
    style json;
    max_ts timestamp;
    stl text;
    log_id int4;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('REPORT_%1$s', replace(txt, ' ', '_')));
    begin
        max_ts = (select max(ts) from tb_log_ctl);
        -- html = '<style>table,th,tr,td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        html = '<style>#table,#th,#tr,#td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        -- html = html || '<style>#tr:nth-child(odd) {background:#fff} #tr:nth-child(even) {background:#f7f7а7}</style>';
        html = html || '<style>#np {font-size:0px}</style>';
        html = html || '<style>#num {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; font-weight:500; text-align: right}</style>';

        stl = 'style="background:' || (
            case 
            when max_ts < now() - '30 minutes'::interval then 'salmon'
            when max_ts < now() - '10 minutes'::interval then 'pink'
            when max_ts < now() -  '2 minutes'::interval then 'LemonChiffon'
            else null end || '"'
        );
        html = html || format('<table><h2> %1$s </h2><h4 %3$s> %2$s </h4></table>', txt, left(max_ts::text, 16), stl);

        mail_id = pr_swf_log_action( txt, 'mail', json_build_object('len', length(html), 'html', html));
        style = json_build_object(
            '_h', format($$ select 'style', %L $$, stl)
            ,
            'th', $$ select 'id' , 'th' union select 'style' ,'background:silver' union select 'style' ,'text-align:center' $$
            ,
            'td', $$ 
                select 'id' , case when %type% = 'number' or %key% in ('rows_count','value','avg','std','min','max','cnt')  then 'num' else 'td' end 
                -- union
                -- select 'style' , 'text-align:' || 
                --     case 
                --     -- when %type% = 'number' then 'right' 
                --     when %key% in ('rows_count', 'value', 'avg', 'std', 'min', 'max', 'cnt') then 'right' 
                --     else null end
                union
                select 'style' , 'color:' || 
                    case 
                    when %type% = 'boolean' and %value% = 'true' then 'green' 
                    when %type% = 'boolean' and %value% = 'false' then 'red' 
                    else null end
                union
                select 'style' , 'font-weight:' || 
                    case 
                    when %type% = 'boolean' and %value% = 'true' then '600' 
                    when %type% = 'boolean' and %value% = 'false' then '600' 
                    -- when %type% = 'number' then '500'
                    else null end
                union
                select 'style' , 'background:' ||
                    case 
                    when %key% in ('_min_dttm', '_max_dttm') then 
                        case 
                        when (%value%)::timestamp < now() - '48 hours'::interval then 'salmon'
                        when (%value%)::timestamp < now() - '24 hours'::interval then 'gold'
                        when (%value%)::timestamp < now() - '18 hours'::interval then 'yellow'
                        when (%value%)::timestamp < now() - '12 hours'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('max_duration', 'duration') then 
                        case 
                        when (%value%) >= '02:30' then 'red'
                        when (%value%) >= '02:00' then 'salmon'
                        when (%value%) >= '01:30' then 'pink'
                        when (%value%) >= '01:00' then 'yellow'
                        when (%value%) >= '00:30' then 'LemonChiffon'
                        else null end
                    when %key% in ('res', 'res_msg') then 
                        case 
                        when (%row%->>'res')::int =  1 then 'lime'         -- Ok
                        when (%row%->>'res')::int =  0 then 'LemonChiffon' -- No
                        when (%row%->>'res')::int = -1 then 'yellow'       -- Empty
                        when (%row%->>'res')::int = -2 then 'pink'         -- Cancel
                        when (%row%->>'res')::int = -3 then 'fuchsia'      -- Expired
                        when (%row%->>'res')::int = -4 then 'skyblue'       -- Uniq
                        when (%row%->>'res')::int = -5 then 'violet'       -- Ztest
                        when (%row%->>'res')::int = -6 then 'gray'         -- 
                        when (%row%->>'res')::int = -7 then 'silver'       -- CTL_err
                        when (%row%->>'res')::int = -8 then 'orange'       -- PXF_err
                        when (%row%->>'res')::int = -9 then 'salmon'       -- Error
                        else null end
                    when %key% in ('_status_dttm') then 
                        case 
                        when (%value%)::timestamp < now() - '48 hours'::interval then 'gold'
                        when (%value%)::timestamp < now() - '12 hours'::interval then 'yellow'
                        when (%value%)::timestamp < now() -  '6 hours'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('status_time') then 
                        case 
                        when (%value%)::interval > '2 days'::interval then 'salmon'
                        when (%value%)::interval > '1 days'::interval then 'pink'
                        when (%value%)::interval > '12 hours'::interval then 'gold'
                        when (%value%)::interval > '6 hours'::interval then 'yellow'
                        when (%value%)::interval > '3 hours'::interval then 'LemonChiffon'
                        else null end
                    else 
                        case 
                        when %value% is not null and %key% = 'cnt'          then 'silver'
                        when %value% is not null and %key% = 'active'       then 'LemonChiffon'
                        when %value% is not null and %key% = 'completed'    then 'palegreen' 
                        when %value% is not null and %key% = 'aborted'      then 'gold'          

                        when %value% is not null and %key% = 'cnt_ok'       then 'lime'         -- Ok
                        when %value% is not null and %key% = 'cnt_no'       then 'LemonChiffon' -- No
                        when %value% is not null and %key% = 'cnt_err'      then 'salmon'       -- Error
            
                        when %value% is not null and %key% = 'waited'       then 'yellow'
                        when %value% is not null and %key% = 'expired'      then 'salmon'       -- 2 day
                        when %value% is not null and %key% = 'delayed'      then 'pink'         -- 1 day

                        when %value% is not null and %key% = 'cnt_ok'       then 'lime'         -- Ok
                        when %value% is not null and %key% = 'cnt_no_data'  then 'LemonChiffon' -- No
                        when %value% is not null and %key% = 'cnt_empty'    then 'yellow'       -- Empty
                        when %value% is not null and %key% = 'cnt_canceled' then 'pink'         -- Cancel
                        when %value% is not null and %key% = 'cnt_expired'  then 'fuchsia'      -- Expired
                        when %value% is not null and %key% = 'cnt_uniq'     then 'skyblue'       -- Uniq
                        when %value% is not null and %key% = 'cnt_ztest'    then 'violet'       -- Ztest
--                        when %key% = '' then 'gray'         -- 
                        when %value% is not null and %key% = 'cnt_ctl_err'  then 'silver'       -- CTL_err
                        when %value% is not null and %key% = 'cnt_pxf_err'  then 'orange'       -- PXF_err
                        when %value% is not null and %key% = 'cnt_error'    then 'salmon'       -- Error
                        else null end
                    end
            $$
        );

        txt = 'CTL Today';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select res::int4
                    , (beg_msg->>'cat') category
                    , substring(lower(wf) from '([a-zA-Z0-9]+)_') wf_prefix
                    , count(1) wf_cnt
                    , left(sum(end_ts - beg_ts)::text,8) sum_time
                    , left(min(end_ts - beg_ts)::text,8) min_dur
                    , left(max(end_ts - beg_ts)::text,8) max_dur
                    , left(avg(end_ts - beg_ts)::text,8) avg_dur
                    , nullif(sum(case res::text when  '1' then 1 else 0 end),0) cnt_ok
                    , nullif(sum(case res::text when  '0' then 1 else 0 end),0) cnt_no_data
                    , nullif(sum(case res::text when '-1' then 1 else 0 end),0) cnt_empty
                    , nullif(sum(case res::text when '-2' then 1 else 0 end),0) cnt_canceled
                    , nullif(sum(case res::text when '-3' then 1 else 0 end),0) cnt_expired
                    , nullif(sum(case res::text when '-4' then 1 else 0 end),0) cnt_uniq
                    , nullif(sum(case res::text when '-5' then 1 else 0 end),0) cnt_ztest
                    , nullif(sum(case res::text when '-7' then 1 else 0 end),0) cnt_ctl_err
                    , nullif(sum(case res::text when '-8' then 1 else 0 end),0) cnt_pxf_err
                    , nullif(sum(case res::text when '-9' then 1 else 0 end),0) cnt_error
                from vw_swf_ctl_log l 
                where l.wf is not null
                    and l.beg_ts >= current_date - 0
                    and l.beg_ts <  current_date + 1
                    and substring(lower(wf) from '([a-zA-Z0-9]+)_') is not null
                group by 1,2,3
                order by 1,2,3
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by res,category,wf_prefix', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL All Active';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select 
                    row_number() over(order by status_dttm, a.id) as n
    --                , concat(a.id, '<br>', profile) id
                    , a.id
                --    , alive
                --    , start_dttm, end_dttm, profile
                    , status_dttm::timestamp(0)::text
                    , (now() - status_dttm)::interval(0) status_time
                --    , status, status_log
                    , concat(status, ': ', status_log) status_info
                    , wf_id
                    , b.profile
    --                , b.category
                    , split_part(category, '.', 1) category
                    , split_part(category, '.', 2) subcat
    --                , b.name 
                    , replace(name, 'pc1080.', '') name
                    , replace(connected, ',', ',<br>') connected
                    , replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    , replace(param, ';', ';<br>') param
                from vw_log_ctl_loading a
                left join vw_log_ctl_wf b on a.wf_id=b.id
                where alive = 'ACTIVE' and b.category like 'p1080%'
                order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Not scheduled';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by category, name) as n
                    , ts::timestamp(0)::text
                    , id
                    , profile
    --                , category
                    -- , split_part(category, '.', 1) category
                    , split_part(category, '.', 2) category
    --                , name
                    , replace(name, 'pc1080.', '') name
                    , scheduled, deleted
                    , replace(connected, ',', ',<br>') connected
                    , replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    , replace(param, ';', ';<br>') param
                from vw_log_ctl_wf a
                where not scheduled and not deleted
                    and category like 'p1080%'
                    and category not in ('p1080.ARCHIVE')
                order by n
            )$sql$;
            -- html = pr_tbl2html(sql, txt, 'order by n', style);
            -- txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL All WF';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by ts desc, id) as n 
                    , ts, id
                    , profile
                    , split_part(category, '.', 2) category
                    , replace(name, 'pc1080.', '') name
                    , scheduled, deleted, singleloading
                    , replace(connected, ',', ',<br>') connected
                    , replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    , replace(replace(replace(param, ';', ';<br>'), '''', ''), '"', '') param
                from vw_log_ctl_wf a
                where category like 'p1080%'
                order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'Ztest Errors';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by ts desc) n, *
                from (
                    select distinct on (z.object)
                        left(z.ts::text, 16) ts
                        , z.object, z.ztest_ok, z.is_except, z.error is_error
                        --, z.confidence
                        , round(z.zscore::numeric, 2) zscore
                        , z.key_date
                        , z.key_diff::text
                        , round(z.stable::numeric, 2) stable
                        , to_char(z.rows_count, 'FM999 999 999 999 999 999') rows_count
                        , to_char(z.value, 'FM999 999 999 999 999 999') value
                        , to_char(z.avg, 'FM999 999 999 999 999 999') avg
                        , to_char(z.std, 'FM999 999 999 999 999 999') std
                        , to_char(z.cnt, 'FM999 999 999 999 999 999') cnt
                        , to_char(z.min, 'FM999 999 999 999 999 999') min
                        , to_char(z.max, 'FM999 999 999 999 999 999') max
                        , z.log_id
                        --, z.notes 
                    from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z
                    order by z.object, ts desc
                ) a
                where not ztest_ok 
                    --and is_error is not null
                    and now() - ts::timestamp <= '1 day'::interval
                order by 1  
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL All Today';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by end_ts desc) n 
    --            , left(beg_msg->>'sdt',16) event_ts
                , left(beg_ts::text,16) beg_ts
                , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
        --        , wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , wf, res
                , replace(beg_msg->'rtr'->>'try', '"', '') try
                , coalesce(replace(replace(end_msg->>'msg', chr(10), '<br>'), '''', ''), end_msg::text) msg
    --            , replace(beg_msg->>'rtr', '"', '') retry
    --            , replace(end_msg->>'exe', '''', '') exe
                FROM vw_swf_ctl_log
                where true and loading_id IS NOT NULL
                    and end_ts >= (now() - '1 hour'::interval)::date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = pr_swf_log_action('end', 'mail', null, mail_id)::text;
        return pr_send_mail(mail_id::text);

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

            perform pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context) ; 
            return e_txt;
        end;
    end;
end;

$body$
EXECUTE ON ANY;
	