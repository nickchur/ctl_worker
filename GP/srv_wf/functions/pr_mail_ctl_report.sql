CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_report(reports text[] DEFAULT NULL::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    sql text;
    txt text = 'CTL Today Report';
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
        max_ts = (select max(ts) from tb_log_ctl);
        res = case 
            when now() - max_ts <  '2 minutes'::interval then 1
            when now() - max_ts < '10 minutes'::interval then 0
            else -1 end;

        -- html = '<style>table,th,tr,td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        html = html || '<style>#table,#th,#tr,#td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        -- html = html || '<style>#tr:nth-child(odd) {background:#fff} #tr:nth-child(even) {background:#f7f7а7}</style>';
        html = html || '<style>#np {font-size:0px}</style>';
        html = html || '<style>#num {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; font-weight:500; text-align: right}</style>';

        stl = 'color:' || (case res when -1 then 'red' when 0 then 'darkred' else 'darkgreen' end);
        html = html || format('<div style="%3$s"><h2> %1$s </h2><h4> Last %2$s </h4></div>', txt, left(max_ts::text, 16), stl);

        mail_id = pr_swf_log_action( txt, 'mail', json_build_object('len', length(html), 'html', html));
    
        style = json_build_object(
            '_h', format($$ select 'style', %L $$, stl)
            ,
            'th', $$ select 'id' , 'th' union select 'style' ,'background:silver' union select 'style' ,'text-align:center' $$
            ,
            'td', $$ 
                select 'id' , case when %type% = 'number' or %key% in ('rows_count') then 'num' else 'td' end 
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
                    when %key% in ('alive') then 
                        case 
                        when (%value%) in ('ACTIVE') then 'rgba(99,255,71,0.1)'
                        when (%value%) in ('COMPLETED') then 'rgba(99,255,71,0.5)'
                        else 'rgba(255,99,71,0.5)' end
                    when %key% in ('status') then 
                        case 
                        when (%value%) in ('TIME-WAIT', 'EVENT-WAIT') then 'rgba(99,255,71,0.1)'
                        when (%value%) in ('PREREQ', 'LOCK-WAIT', 'PARAM') then 'rgba(99,255,71,0.3)'
                        when (%value%) in ('START') then 'rgba(99,255,71,0.5)'
                        when (%value%) in ('RUNNING') then 'rgba(99,255,71,0.7)'
                        when (%value%) in ('SUCCESS') then 'rgba(99,255,71,1.0)'
                        when (%value%) in ('ERRORCHECK') then 'rgba(255,99,71,0.5)'
                        when (%value%) in ('ERROR') then 'rgba(255,99,71,1.0)'
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
                        else null end
                    end
            $$
        );

        txt = 'CTL Today';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                with rep as (

                    select 
                        --, split_part(a.category, '.', 1) category
                        split_part(a.category, '.', 2) category
                        --, profile
                        -- , substring(lower(name) from '([a-zA-Z0-9]+)_') wf_prefix
                        , '' as wf_prefix

                        -- , min(status_dttm)::timestamp(0)::text min_dttm
                        -- , split_part(string_agg(format('%s', id), ';' order by status_dttm), ';', 1) min_lid
                        -- , max(status_dttm)::timestamp(0)::text max_dttm
                        -- , split_part(string_agg(format('%s', id), ';' order by status_dttm desc), ';', 1) max_lid


                        -- , string_agg(distinct 
                        --              case 
                        --              when alive = 'ACTIVE' and status in('EVENT-WAIT', 'TIME-WAIT') then lower(status)
                        --              when alive = 'ACTIVE' and status not in('EVENT-WAIT', 'TIME-WAIT') then status 
                        --              else null end, ', ') ACTIVE_status
                    
                        --, min(case when alive = 'ACTIVE' then status_dttm else null end)::timestamp(0)::text ACTIVE_min
                        --, max(case when alive = 'ACTIVE' then status_dttm else null end)::timestamp(0)::text ACTIVE_max
                        --, max(case when alive = 'ACTIVE' then status_time else null end)::interval(0) ACTIVE_time

                        -- , nullif(count(distinct wf_id), 0) cnt
                        , nullif(count(distinct case when alive = 'ACTIVE' then wf_id else null end), 0) ACTIVE
                        , nullif(count(distinct case when alive = 'COMPLETED' then wf_id else null end), 0) COMPLETED
                        -- , nullif(count(distinct case when alive = 'ABORTED' then 1 else null end), 0) ABORTED
                    
                        , nullif(count(distinct case when (jsn->>'res')::int4 > 0 then wf_id else null end), 0) cnt_ok
                        , nullif(count(distinct case when (jsn->>'res')::int4 = 0 then wf_id else null end), 0) cnt_no
                        , nullif(count(distinct case when (jsn->>'res')::int4 < 0 then wf_id else null end), 0) cnt_err

                        , nullif(count(distinct case when alive in ('ACTIVE', 'COMPLETED') then wf_id else null end) 
                            - count(distinct case when alive = 'COMPLETED' then wf_id else null end), 0) waited
                    
                        , nullif(count(distinct case when alive = 'ACTIVE' 
                            and status_time between wf_interval and wf_interval * 2 then wf_id else null end), 0) delayed
                        , nullif(count(distinct case when alive = 'ACTIVE' 
                            and status_time > wf_interval * 2 then wf_id else null end), 0) expired

                        -- , string_agg(distinct 
                        --              case 
                        --              when alive != 'ACTIVE' and status in('SUCCESS') then null --lower(status)
                        --              when alive != 'ACTIVE' and status not in('SUCCESS') then status
                        --              else null end, ', ') COMPLETED_status

                        --, min(case when alive != 'ACTIVE' then status_dttm else null end)::timestamp(0)::text COMPLETED_min
                        --, max(case when alive != 'ACTIVE' then status_dttm else null end)::timestamp(0)::text COMPLETED_max
                        -- , max(case when alive != 'ACTIVE' then status_time else null end)::interval(0) COMPLETED_time

                        --, string_agg(distinct case when alive = 'ABORTED' then status else null end, ', ') ABORTED_status
                        --, min(case when alive = 'ABORTED' then status_time else null end)::interval(0) ABORTED_time

                        --, split_part(string_agg(format('%s', name_id), ';' order by status_dttm), ';', 1) first_wf
                        --, split_part(string_agg(name_id, ';' order by status_dttm desc), ';', 1) last_name

                    from (
                        select a.id
                            , alive
                            , start_dttm, end_dttm
                            , a.profile
                            , status_dttm
                            , (now() - status_dttm) status_time
                            , status
                            , status_log
                            , wf_id
                            , coalesce(a.msg->'workflow'->>'category', b.category) category
                            , coalesce(a.msg->'workflow'->>'name', b.name) name
                            , format('%s (%s)', replace(coalesce(a.msg->'workflow'->>'name', b.name), 'pc1080.', ''), b.id) name_id 
                            , coalesce((a.msg->'workflow'->>'singleLoading')::bool, b.singleLoading) singleLoading
                            , coalesce((a.msg->'workflow'->>'scheduled')::bool, b.scheduled) scheduled
                            , coalesce((a.msg->'workflow'->>'deleted')::bool, b.deleted) deleted
                            , replace(connected, ',', ',<br>') connected
                            , replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                            , replace(param, ';', ';<br>') param
                            , coalesce((
                                select value->>'prior_value'
                                from json_array_elements((b.msg->'wf'->>'param')::json) jsn 
                                where value->>'param' = 'wf_interval'
                                limit 1
                            ), '1 day')::interval wf_interval
                            , jsn
                        from (
                --            select distinct on (wf_id) *
                            select *, try_cast2jsonb(status_log) jsn
                            from vw_log_ctl_loading
                            where status_dttm >= (now() - '1 hour'::interval)::date or alive = 'ACTIVE' 
                            order by wf_id, id desc
                        ) a 
                        left join vw_log_ctl_wf b on a.wf_id=b.id
                        where not b.deleted
                    ) a
                    where category like 'p1080%'
                    group by a.category, wf_prefix --, profile
                )
                select row_number() over(order by category, wf_prefix) as n, * from rep a
                union all
                select null::int as n
                    , null category
                    --, null profile
                    , null wf_prefix
                    -- , min(min_dttm)
                    -- , null
                    -- , max(max_dttm)
                    -- , null
                    -- , null active_status
                    -- , sum(cnt)
                    , sum(active)
                    , sum(completed)
                    -- , sum(aborted)
                    , sum(cnt_ok)
                    , sum(cnt_no)
                    , sum(cnt_err)
                    , sum(waited)
                    , sum(delayed)
                    , sum(expired)
                    --, sum(cnt_not - expired - delayed)
                    -- , max(COMPLETED_time)
                from rep a
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;

        txt = 'CTL SDPUE Errors';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select 
                    row_number() over(order by status_dttm desc, a.id) as n
                    -- , concat(a.id, '<br>', profile) id
                    , a.id
                    , a.auto
                    , alive
                    -- , start_dttm, end_dttm, profile
                    , status_dttm::timestamp(0)::text
                    --, (now()::timestamp(0) - status_dttm::timestamp(0))::interval(0) status_time
                    , status
                    , wf_id
                    -- , b.profile
                    -- , b.category
                    -- , split_part(category, '.', 1) category
                    , concat(split_part(category, '.', 2), ' ('||nullif(b.profile, 'HR_Data')||')')  category
                    -- , b.name 
                    , replace(name, 'pc1080.', '') name
                    -- , wf_interval
                    --, replace(status_log, '''', '') status_log
                    , concat(status, ': ', replace(status_log, '''', '')) status_info
                    --, replace(connected, ',', ',<br>') connected
                    --, replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    --, split_part(wf_sched, ',', 1) wf_sched
                    --, replace(param, ';', ';<br>') param

                from s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading a
                left join s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf b on a.wf_id=b.id
                where true
                    and status='ERROR'
                    and b.category = 'p1080.sdpue'
                    -- and a.profile = 'arnsdpue'
                    and ((now() - end_dttm) < '1 day'::interval 
                         or (alive = 'ACTIVE' and (now() - status_dttm) < '1 month'::interval ))
                order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Old and Working';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select 
                    row_number() over(order by status_dttm, a.id) as n
                    -- , concat(a.id, '<br>', profile) id
                    , a.id
                    , a.auto
                    -- , alive
                    -- , start_dttm, end_dttm, profile
                    , status_dttm::timestamp(0)::text
                    , (now()::timestamp(0) - status_dttm::timestamp(0))::interval(0) status_time
                    -- , status, status_log
                    , concat(status, ': ', replace(status_log, '''', '')) status_info
                    , wf_id
                    -- , b.profile
                    -- , b.category
                    -- , split_part(category, '.', 1) category
                    , concat(split_part(category, '.', 2), ' ('||nullif(b.profile, 'HR_Data')||')')  category
                    -- , b.name 
                    , replace(name, 'pc1080.', '') name
                    -- , wf_interval
                    , replace(connected, ',', ',<br>') connected
                    --, replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    , split_part(wf_sched, ',', 1) wf_sched
                    --, replace(param, ';', ';<br>') param

                from s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading a
                left join s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf b on a.wf_id=b.id
                where alive = 'ACTIVE' 
                    and b.category like 'p1080%'
                    and b.profile = 'HR_Data'
                    and (now() - start_dttm > wf_interval or status not in ('EVENT-WAIT', 'TIME-WAIT'))
                order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'GP Working';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by query_start,xact_start) as n
                    , query_start::timestamp(0)::text ts
                    --, left(xact_start::text,16) tr, usename
                    , (clock_timestamp()-query_start)::interval(0) duration
                    , pid, state
                    , coalesce(nullif(application_name,''),'...') app_name
                    , coalesce(nullif(waiting_reason,''),'.') waiting
                    , work->>'lid' lid
                    , work->>'cwf' cwf
                    , work->>'rtr' rtr
                    --, sess_id
                from (
                    select *, substring(query from $$select pr_swf_start_ctl\(\'(.+)\'::json\)$$)::json work  --'
                    from pg_stat_activity a
                    where query_start is not Null and state <> 'idle' 
                        and query like 'select pr_swf_start_ctl(%)'
                        --and pid <> pg_backend_pid()
                ) a
                order by query_start,xact_start
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'Lock';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                SELECT row_number() over(order by relation, blocked_pid, blocked_user) n, * from (
                    SELECT b.relname relation 
                         --, blocked_locks.pid         AS blocked_pid
                         , string_agg(distinct blocked_locks.pid::text, ', ') blocked_pid
                         --, blocked_locks.locktype AS blocked_locktype
                         --, blocked_locks.mode     AS blocked_mode
                         , string_agg(distinct blocked_locks.mode, ', ' order by blocked_locks.mode) blocked_mode
                         , blocked_activity.usename  AS blocked_user
                         , blocked_activity.application_name    AS blocked_app_name
                         , blocking_locks.pid        AS blocking_pid
                         --, blocking_locks.mode     AS blocking_mode
                         , string_agg(distinct blocking_locks.mode, ', ' order by blocking_locks.mode) blocking_mode
                         , blocking_activity.usename AS blocking_user
                         , blocking_activity.application_name   AS blocking_app_name
                         --, blocking_locks.granted blocking_granted
                    FROM pg_locks         blocked_locks
                    JOIN pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
                    JOIN pg_locks         blocking_locks 
                        ON blocking_locks.locktype = blocked_locks.locktype
                        AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
                        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
                        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
                        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
                        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
                        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
                        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
                        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
                        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
                        AND blocking_locks.pid != blocked_locks.pid
                    JOIN pg_stat_activity  blocking_activity ON blocking_activity.pid = blocking_locks.pid
                    left join pg_class b on b.oid = blocked_locks.relation
                    -- left join pg_class c on c.oid = blocking_locks.relation
                    WHERE NOT blocked_locks.granted and blocking_locks.granted
                        and (blocked_activity.usename like 'u_%_hr_%' or blocking_activity.usename like 'u_%_hr_%')
                    group by 1,4,5,6,8,9
                ) a
                order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Today Errors';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by end_ts desc) n 
    --            , left(beg_msg->>'sdt',16) event_ts
                , left(beg_ts::text,16) beg_ts
    --            , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
        --        , wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , (beg_msg->>'cat') category
                , wf , res
                , replace(beg_msg->'rtr'->>'try', '"', '') try
    --            , end_action
                , replace(replace(end_msg->>'msg', chr(10), '<br>'), '''', '') msg
    --            , replace(beg_msg->>'rtr', '"', '') retry
    --            , replace(end_msg->>'exe', '''', '') exe
                , ts_last::timestamp(0)::text last_ok
                FROM vw_swf_ctl_log a
                left join (
                    select distinct on (wf) wf wf_last, beg_ts ts_last, res res_last
                    from vw_swf_ctl_log
                    where res in ('1')
                        and (beg_msg->>'cat') <> 'fcts'
                    order by 1, 2 desc
                ) b on a.wf = b.wf_last
                where res not in ('1', '0')
                    and (beg_msg->>'cat') <> 'fcts'
                    -- and end_msg->>'wf_exec' not like 'pr_check_ctl(%'
                    --and now() - end_ts <= '24 hours'::interval
                    and end_ts >= (now() - '1 hour'::interval)::date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;

        txt = 'CTL Today No data';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by end_ts desc) n 
    --            , left(beg_msg->>'sdt',16) event_ts
                , left(beg_ts::text,16) beg_ts
    --            , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
        --        , wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , (beg_msg->>'cat') category
                , wf, res
                , replace(beg_msg->'rtr'->>'try', '"', '') try
                , replace(replace(end_msg->>'msg', chr(10), '<br>'), '''', '') msg
    --            , replace(beg_msg->>'rtr', '"', '') retry
    --            , replace(end_msg->>'exe', '''', '') exe
                , ts_last::timestamp(0)::text last_ok
                FROM vw_swf_ctl_log a
                left join (
                    select distinct on (wf) wf wf_last, beg_ts ts_last, res res_last
                    from vw_swf_ctl_log
                    where res in ('1')
                        and (beg_msg->>'cat') <> 'fcts'
                    order by 1, 2 desc
                ) b on a.wf = b.wf_last
                where res = '0'
                    and (beg_msg->>'cat') <> 'fcts'
                    -- and (end_msg->>'exe') not like 'pr_last_mail(%'
                    -- and now() - end_ts <= '24 hours'::interval
                    and end_ts >= (now() - '1 hour'::interval)::date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Today Long';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by duration desc) n 
    --            , left(beg_msg->>'sdt',16) event_ts
                , left(beg_ts::text,16) beg_ts
    --            , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
        --        , wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , (beg_msg->>'cat') category
                , wf, res
                , replace(beg_msg->'rtr'->>'try', '"', '') try
                , replace(replace(end_msg->>'msg', chr(10), '<br>'), '''', '') msg
    --            , replace(beg_msg->>'rtr', '"', '') retry
    --            , replace(end_msg->>'exe', '''', '') exe
                FROM vw_swf_ctl_log
                where res = '1'
                    and duration >= '30 min'::interval
                    and (beg_msg->>'cat') <> 'fcts'
                    and end_ts >= (now() - '1 hour'::interval)::date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Today Done';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                with ctl as (
                    select res::int
                        , (beg_msg->>'cat') category
                        , substring(lower(wf) from '([a-zA-Z0-9]+)_') wf_prefix
                        , case res::int
                            when  1 then 'Ok'
                            when  0 then 'No new'
                            when -1 then 'Empty'
                            when -2 then 'Canceled'
                            when -3 then 'Expire'
                            when -4 then 'Uniq'
                            when -5 then 'Ztest'
                            when -7 then 'CTL error'
                            when -8 then 'PXF error'
                            else 'Error' end res_msg 
                        , count(1)
                        , min(beg_ts) min_beg
                        , max(end_ts) max_end
                        , max(duration) max_duration
                        , sum(duration) sum_duration
                    from s_grnplm_vd_hr_edp_srv_wf.vw_swf_ctl_log
                    where beg_ts >= current_date and wf is not null
                        and res is not null
                    group by 1,2,3
                )
                select row_number() over(order by res,category,wf_prefix) n
                    , res
                    , category
                    , wf_prefix
                    , res_msg
                    , count
                    , left(min_beg::text,16) min_beg
                    , left(max_end::text,16) max_end
                    , left(max_duration::text,8) max_duration
                    , left(sum_duration::text,8) sum_duration
                from (
                    select ctl.* from ctl
                    union all
                    select null::int4 res
                        , '' category
                        , '' wf_prefix
                        , '' res_msg
                        , sum(count) 
                        , min(min_beg)
                        , max(max_end)
                        , max(max_duration)
                        , sum(sum_duration)
                    from ctl
                    group by 1,2,3,4
                ) a
                order by n
            )$sql$;
            html = s_grnplm_vd_hr_edp_srv_wf.pr_tbl2html(sql, txt, 'order by res,category,wf_prefix', style);
            txt = s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Not scheduled';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select row_number() over(order by category, name) as n
                    , ts::timestamp(0)::text
                    , id
                    , profile
                    --, category
                    --, split_part(category, '.', 1) category
                    , split_part(category, '.', 2) category
                    -- , name
                    , replace(name, 'pc1080.', '') name
                    --, scheduled, deleted
                    , replace(connected, ',', ',<br>') connected
                    -- , replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    -- , replace(param, ';', ';<br>') param
                from s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf a
                where not scheduled and not deleted
                    and category like 'p1080%'
                    and category not in ('p1080.ARCHIVE')
                order by n
            )$sql$;
            html = s_grnplm_vd_hr_edp_srv_wf.pr_tbl2html(sql, txt, 'order by n', style);
            txt = s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Today Ok';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select row_number() over(order by end_ts desc) n 
    --            , left(beg_msg->>'sdt',16) event_ts
                , left(beg_ts::text,16) beg_ts
    --            , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
        --        , wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , (beg_msg->>'cat') category
                , wf, res
                , replace(beg_msg->'rtr'->>'try', '"', '') try
                , replace(replace(end_msg->>'msg', chr(10), '<br>'), '''', '') msg
    --            , replace(beg_msg->>'rtr', '"', '') retry
    --            , replace(end_msg->>'exe', '''', '') exe
                FROM vw_swf_ctl_log
                where res = '1'
                    and (beg_msg->>'cat') <> 'fcts'
                    and end_ts >= (now() - '1 hour'::interval)::date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Today Fcts';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select row_number() over(order by end_ts desc) n 
    --            , left(beg_msg->>'sdt',16) event_ts
                , left(beg_ts::text,16) beg_ts
    --            , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
        --        , wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , (beg_msg->>'cat') category
                , wf, res
                , replace(beg_msg->'rtr'->>'try', '"', '') try
                , replace(replace(end_msg->>'msg', chr(10), '<br>'), '''', '') msg
    --            , replace(beg_msg->>'rtr', '"', '') retry
    --            , replace(end_msg->>'exe', '''', '') exe
                FROM vw_swf_ctl_log
                where (beg_msg->>'cat') = 'fcts'
                    and end_ts >= (now() - '1 hour'::interval)::date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
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
	