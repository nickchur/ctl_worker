CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ctl_work_load_report(reports text[] DEFAULT NULL::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    sql text;
    txt text = 'CTL Yesterday';
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
            '_table', $$ select 'border', '1' $$
            ,
            '_th', $$ select 'style' ,'background:silver' union select 'style' ,'text-align:center' $$
            ,
            -- 'tr', $$ select 'style' , 'background:' || case  when rn % 2 = 0 then 'snow' else 'white' end union select 'style' , 'text-align:left' $$
            -- ,
            'td', $$ 
                select 'id' , case when %type% = 'number' or %key% in ('rows_count') then 'num' else 'td' end 
                union
                -- select 'style' , 'text-align:' || 
                --     case 
                --     when %type% = 'number' then 'right' 
                --     -- when %key% in ('rows_count', 'value', 'avg', 'std', 'min', 'max', 'cnt') then 'right' 
                --     -- else 'left' end
                --     else null end
                -- union
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
                    when %key% in ('max_duration', 'max_dur') then 
                        case 
                        when (%value%) >= '02:00' then 'red'
                        when (%value%) >= '01:00' then 'pink'
                        when (%value%) >= '00:30' then 'LemonChiffon'
                        else null end
                    when %key% in ('key_date') then 
                        case 
                        when (%value%)::timestamp::date < current_date - 5 then 'salmon'
                        when (%value%)::timestamp::date < current_date - 3 then 'pink'
                        when (%value%)::timestamp::date < current_date - 1 then 'LemonChiffon'
                        else null end
                    when %key% in ('ts', 'event_ts') then 
                        case 
                        when (%value%)::timestamp < now() - '48 hours'::interval then 'salmon'
                        when (%value%)::timestamp < now() - '12 hours'::interval then 'pink'
                        when (%value%)::timestamp < now() -  '6 hours'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('status_dttm') then 
                        case 
                        when (%value%)::timestamp < now() - '48 hours'::interval then 'salmon'
                        when (%value%)::timestamp < now() - '24 hours'::interval then 'pink'
                        when (%value%)::timestamp < now() - '12 hours'::interval then 'gold'
                        when (%value%)::timestamp < now() - '6 hours'::interval then 'yellow'
                        when (%value%)::timestamp < now() - '3 hours'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('status_time') then 
                        case 
                        when (%value%)::interval > '2 days'::interval then 'salmon'
                        when (%value%)::interval > '1 days'::interval then 'pink'
                        when (%value%)::interval > '12 hours'::interval then 'gold'
                        when (%value%)::interval > '6 hours'::interval then 'yellow'
                        when (%value%)::interval > '3 hours'::interval then 'LemonChiffon'
                        else null end
                    when %key% in ('res') then 
                        case 
                        when (%value%)::int =  1 then 'lime'         -- Ok
                        when (%value%)::int =  0 then 'LemonChiffon' -- No
                        when (%value%)::int = -1 then 'yellow'       -- Empty
                        when (%value%)::int = -2 then 'pink'         -- Cancel
                        when (%value%)::int = -3 then 'fuchsia'      -- Expired
                        when (%value%)::int = -4 then 'skyblue'       -- Uniq
                        when (%value%)::int = -5 then 'violet'       -- Ztest
                        when (%value%)::int = -6 then 'gray'         -- 
                        when (%value%)::int = -7 then 'silver'       -- CTL_err
                        when (%value%)::int = -8 then 'orange'       -- PXF_err
                        when (%value%)::int = -9 then 'salmon'       -- Error
                        else null end
                    when %key% in ('rate_00', 'rate_10', 'rate_20', 'rate_30', 'rate_40', 'rate_50') then 
                        case 
                        when (%value%)::numeric >= 0.75 then 'salmon'
                        when (%value%)::numeric >= 0.60 then 'orange'
                        when (%value%)::numeric >= 0.45 then 'gold'
                        when (%value%)::numeric >= 0.30 then 'yellow'
                        when (%value%)::numeric >= 0.15 then 'LemonChiffon'
                        else null end
                    when %key% in ('size_percent') then 
                        case 
                        when (%value%)::numeric >= 20.0 then 'salmon'
                        when (%value%)::numeric >= 10.0 then 'orange'
                        when (%value%)::numeric >=  5.0 then 'gold'
                        when (%value%)::numeric >=  2.5 then 'yellow'
                        when (%value%)::numeric >=  1.0 then 'LemonChiffon'
                        else null end
                    when %key% in ('speed_percent') then 
                        case 
                        when (%value%)::numeric >= 0.0500 then 'salmon'
                        when (%value%)::numeric >= 0.0250 then 'orange'
                        when (%value%)::numeric >= 0.0100 then 'gold'
                        when (%value%)::numeric >= 0.0050 then 'yellow'
                        when (%value%)::numeric >= 0.0025 then 'LemonChiffon'
                        else null end
                    when %key% in ('size_ratio') then 
                        case 
                        when (%value%)::numeric >= 5.00 then 'salmon'
                        when (%value%)::numeric >= 2.50 then 'orange'
                        when (%value%)::numeric >= 1.00 then 'gold'
                        when (%value%)::numeric >= 0.50 then 'yellow'
                        when (%value%)::numeric >= 0.25 then 'LemonChiffon'
                        else null end
                    when %key% in ('skew') then 
                        case 
                        when (%value%)::numeric >= 0.500 then 'salmon'
                        when (%value%)::numeric >= 0.250 then 'orange'
                        when (%value%)::numeric >= 0.100 then 'gold'
                        when (%value%)::numeric >= 0.050 then 'yellow'
                        when (%value%)::numeric >= 0.025 then 'LemonChiffon'
                        else null end
                    when %key% in ('segments') then 
                        case 
                        when (%value%)::int < 192 then 'salmon'
                        else null end
                    when %key% in ('storage', 'relstorage') then 
                        case 
                        when (%value%) = 'uncompress' then 'salmon'
                        else null end
                    when %key% in ('distributedby') then 
                        case 
                        when (%value%) = 'REPLICATED' then 'salmon'
                        else null end
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
                    when %key% in ('alive') then 
                        case 
                        when (%value%) in ('ACTIVE') then 'rgba(99,255,71,0.1)'
                        when (%value%) in ('COMPLETED') then 'rgba(99,255,71,0.5)'
                        else 'rgba(255,99,71,0.5)' end
                    else 
                        case 
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


        txt = 'HR_Data Lake Speed';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over() n, a.* 
                from vw_growth_stats a
                limit 25
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;

        txt = 'HR_Data Lake Skew';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql=$sql$(
                select row_number() over(order by (tbl_size * coalesce(skew^2, 1)) desc) n
                , ts::timestamp(0)::text
                , replace(tbl, 's_grnplm_vd_hr_edp_', '') tbl
                , segments
                , case storage
                        when 'a' then 'row'
                        when 'c' then 'column'
                        when 'h' then 'uncompress'
                        else storage end storage
                , readable(tbl_size) tbl_size
                , readable(data_size) data_size
                , round(size_ratio, 2) as size_ratio
                , (100.0 - 100.0 * avg/max)::int::text || ' %' avg_max
                , round(skew::numeric, 2) skew
                , readable(sum, 1000) sum
                , readable(min, 1000) min
                , readable(max, 1000) max
                , readable(avg, 1000) avg
                , readable(std, 1000) std
                , duration::interval(0)
                , replace(distributedby, 'DISTRIBUTED ', '') distributedby
                --, tbl_size
                --, options
                --, tbl_size * coalesce(skew, 10) ord
                --, tbl_size * coalesce(100.0 - 100.0 * min/max, 1) ord
                from (
                    select distinct on (tbl) * 
                        , 1.0 * tbl_size / nullif(data_size, 0) as size_ratio
                    from tb_log_skew 
                    where tbl not like '%_1_prt_%' 
                        and ts between current_date-30 and current_date+1
                    order by tbl, 1 desc
                ) a
                order by n
                limit 25
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
                    
        txt = 'HR_Data Lake Ratio';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql=$sql$(
                select row_number() over(order by (tbl_size - data_size) desc) n
                , ts::timestamp(0)::text
                , replace(tbl, 's_grnplm_vd_hr_edp_', '') tbl
                , segments
                , case storage
                        when 'a' then 'row'
                        when 'c' then 'column'
                        when 'h' then 'uncompress'
                        else storage end storage
                , readable(tbl_size) tbl_size
                , readable(data_size) data_size
                , round(size_ratio, 2) as size_ratio
                , (100.0 - 100.0 * avg/max)::int::text || ' %' avg_max
                , round(skew::numeric, 2) skew
                , readable(sum, 1000) sum
                , readable(min, 1000) min
                , readable(max, 1000) max
                , readable(avg, 1000) avg
                , readable(std, 1000) std
                , duration::interval(0)
                , replace(distributedby, 'DISTRIBUTED ', '') distributedby
                --, tbl_size
                --, options
                --, tbl_size * coalesce(skew, 10) ord
                --, tbl_size * coalesce(100.0 - 100.0 * min/max, 1) ord
                from (
                    select distinct on (tbl) * 
                        , 1.0 * tbl_size / nullif(data_size, 0) as size_ratio
                    from tb_log_skew 
                    where tbl not like '%_1_prt_%' 
                        and ts between current_date-30 and current_date+1
                    order by tbl, 1 desc
                ) a
                where data_size > 0
                order by n
                limit 25
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
                    
        txt = 'HR_Data Lake Uncompress';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql=$sql$(
                select row_number() over(order by (tbl_size) desc) n
                , ts::timestamp(0)::text
                , replace(tbl, 's_grnplm_vd_hr_edp_', '') tbl
                , segments
                , case storage
                        when 'a' then 'row'
                        when 'c' then 'column'
                        when 'h' then 'uncompress'
                        else storage end storage
                , readable(tbl_size) tbl_size
                , readable(data_size) data_size
                , round(size_ratio, 2) as size_ratio
                , (100.0 - 100.0 * avg/max)::int::text || ' %' avg_max
                , round(skew::numeric, 2) skew
                , readable(sum, 1000) sum
                , readable(min, 1000) min
                , readable(max, 1000) max
                , readable(avg, 1000) avg
                , readable(std, 1000) std
                , duration::interval(0)
                , replace(distributedby, 'DISTRIBUTED ', '') distributedby
                --, tbl_size
                --, options
                --, tbl_size * coalesce(skew, 10) ord
                --, tbl_size * coalesce(100.0 - 100.0 * min/max, 1) ord
                from (
                    select distinct on (tbl) * 
                        , 1.0 * tbl_size / nullif(data_size, 0) as size_ratio
                    from tb_log_skew 
                    where tbl not like '%_1_prt_%' 
                        and ts between current_date-30 and current_date+1
                    order by tbl, 1 desc
                ) a
                where storage = 'h'
                order by n
                limit 25
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
                    
                    
        txt = 'CTL Yesterday Summary';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                with ctl as (
                    select (beg_msg->>'cat') category
                        -- , substring(lower(wf) from '([a-zA-Z0-9]+)_') wf_prefix
                        , '' as wf_prefix
                        , count(1) wf_cnt
                        , left(sum(end_ts - beg_ts)::text,8) sum_time
        --                , left(min(end_ts - beg_ts)::text,8) min_dur
                        , left(max(end_ts - beg_ts)::text,8) max_dur
        --                , left(avg(end_ts - beg_ts)::text,8) avg_dur
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
                        and l.beg_ts >= current_date -1
                        and l.beg_ts <  current_date
                        and substring(lower(wf) from '([a-zA-Z0-9]+)_') is not null
                    group by 1,2
                )
                select row_number() over(order by category, wf_prefix) n, * from ctl
                union all
                    select null::int8
                        , '' category
                        , 'All' wf_prefix
                        , sum(wf_cnt) wf_cnt
                        , left(sum(sum_time::interval)::text, 8) sum_time
        --                , min(min_dur) min_dur
                        , left(max(max_dur::interval)::text, 8) max_dur
        --                , avg(avg_dur) avg_dur
                        , sum(cnt_ok) cnt_ok
                        , sum(cnt_no_data) cnt_no_data
                        , sum(cnt_empty) cnt_empty
                        , sum(cnt_canceled) cnt_canceled
                        , sum(cnt_expired) cnt_expired
                        , sum(cnt_uniq) cnt_uniq
                        , sum(cnt_ztest) cnt_ztest
                        , sum(cnt_ctl_err) cnt_ctl_err
                        , sum(cnt_pxf_err) cnt_pxf_err
                        , sum(cnt_error) cnt_error
                    from ctl
                    group by 1,2
                order by 1,2
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('html', html), mail_id)::text;
        end if;
        

        txt = 'CTL and GP Yesterday Errors';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by first_dt desc) n
                    , src, first_dt, last_dt, cnt, obj
                    , replace(msg, 'psql:sql/', '<br>psql:sql/') msg
                    -- , replace(msg, chr(10), '<br>') msg
                from (
                    select 'log' as src
                        , coalesce(wf_message->>'first', ts::text)::timestamp(0)::text first_dt
                        , (wf_message->>'last')::timestamp(0)::text last_dt
                        , (wf_message->>'cnt') cnt
                        , (wf_message->>'obj') obj
                        , (wf_message->>'msg') msg
                    from tb_swf_ctl_log
                    where wf_action = 'error' 
                        and (wf_message->>'first') is not null
                        and ts between current_date - 1 and current_date + 1
                    union all
                    select 'tbl' as src
                        , min(ts::text)::timestamp(0)::text first_dt
                        , max(ts::text)::timestamp(0)::text last_dt
                        , count(1)::text cnt
                        , (wf_message->>'obj') obj
                        , (wf_message->>'msg') msg
                    from tb_swf_ctl_log
                    where wf_action = 'error' 
                        and (wf_message->>'first') is null
                        and ts between current_date - 1 and current_date + 1
                    group by obj, msg
                ) a
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;
        

        txt = 'CTL Yesterday Work Load';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                with ll as (
                    select b.dt
                    , date_part('hour', b.dt::time) h, date_part('minute', b.dt::time) m
                    , b.wf wf
                    --, left(b.sum_time::text,8) sum_time
                    , (sum_time/(ti * m.max)) rate
                    --, (sum_time/ti)::int+1 need_swf
                    , b.cwf
                    from (
                        select left(dt::time::text,5) dt
                        , '10 minutes'::interval ti
                        , count(distinct wf) wf
                        , sum(dtt-dtf) sum_time
                        , min(dtf) min_beg
                        , max(dtt) max_end
                        --, array_agg(distinct wf order by wf) as wfs
                        , array_agg(distinct cwf) as cwf
                        from (
                            select dt
                            , l.wf
                            , greatest(beg_ts, dt) dtf
                            , least(coalesce(end_ts, dt), dt + '10 minutes'::interval) dtt
                            , (beg_msg->>'cat') cwf
                            --, substring(lower(wf) from '([a-zA-Z0-9]+)_') cwf
                            from generate_series(current_date - 1, current_date - '1 minutes'::interval, '10 minutes'::interval) dt
                            left join vw_swf_ctl_log l 
                                on l.loading_id is not null
                                and l.beg_ts < dt + '10 minutes'::interval 
                                and l.end_ts > dt
                        ) a
                        group by dt
                    )b
                    left join (
                        select max((beg_msg->>'processes')::int) max 
                        from vw_swf_ctl_log a 
                        where loading_id is null
                        and beg_ts between current_date - 1 and current_date
                    ) m on true
                )
                select a.h as n
                    , max(case when m=0 then wf else 0 end) cnt_00
                    , to_char(max(case when m=0 then rate else 0 end), '0.00') rate_00
                    , max(case when m=10 then wf else 0 end) cnt_10
                    , to_char(max(case when m=10 then rate else 0 end), '0.00') rate_10
                    , max(case when m=20 then wf else 0 end) cnt_20
                    , to_char(max(case when m=20 then rate else 0 end), '0.00') rate_20
                    , max(case when m=30 then wf else 0 end) cnt_30
                    , to_char(max(case when m=30 then rate else 0 end), '0.00') rate_30
                    , max(case when m=40 then wf else 0 end) cnt_40
                    , to_char(max(case when m=40 then rate else 0 end), '0.00') rate_40
                    , max(case when m=50 then wf else 0 end) cnt_50
                    , to_char(max(case when m=50 then rate else 0 end), '0.00') rate_50
                    --, array_agg(distinct wfs) msg
                    -- , left(string_agg(distinct ucwf, ', '), 150) msg
                from ll a
                left join (select ll.h, unnest(ll.cwf) ucwf from ll) ar on a.h = ar.h
                group by 1
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;

        txt = 'CTL Yesterday SDPUE';
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
                    --, status_log
                    , concat(status, ': ', replace(status_log, '''', '')) status_info
                    --, replace(connected, ',', ',<br>') connected
                    --, replace(replace(wf_sched, ',', ',<br>'), '{', '{<br>') wf_sched
                    --, split_part(wf_sched, ',', 1) wf_sched
                    --, replace(param, ';', ';<br>') param

                from s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_loading a
                left join s_grnplm_vd_hr_edp_srv_wf.vw_log_ctl_wf b on a.wf_id=b.id
                where true
                    --and status='ERROR'
                    and status not in ('EVENT-WAIT', 'TIME-WAIT')
                    --and alive = 'ACTIVE' 
                    and b.category like 'p1080.sdpue'
                    and status_dttm between current_date-1 and current_date
                order by n
            )$sql$;
            html = s_grnplm_vd_hr_edp_srv_wf.pr_tbl2html(sql, txt, 'order by n', style);
            txt = s_grnplm_vd_hr_edp_srv_wf.pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Not scheduled';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by ts desc, id) as n
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
                from vw_log_ctl_wf a
                where not scheduled and not deleted
                    and category like 'p1080%'
                    and category not in ('p1080.ARCHIVE')
                order by n
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;

        txt = 'CTL Yesterday Errors';
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
                    --and (beg_msg->>'cat') <> 'fcts'
                    and end_ts between current_date-1 and current_date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;

        txt = 'Yesterday Errors';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select log_id, left(dt::text, 16) dt, message as msg, last_call, calls, usename
                from vw_log_workflow_err 
                where last_call <> 'pr_log_error' 
                    and dt >= current_date - 1
                    and dt < current_date 
                order by 1 desc
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by log_id desc', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Yesterday Long';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by duration desc) n 
                , left(beg_ts::text,16) beg_ts
                , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
                --, wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , wf , res
                , replace(end_msg->>'msg', '''', '') msg
                FROM vw_swf_ctl_log
                where loading_id is not null
                    and duration >= '30 min'::interval
                    and end_ts between current_date -1 and current_date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Yesterday All';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select row_number() over(order by end_ts desc) n 
                , left(beg_ts::text,16) beg_ts
                , left(end_ts::text,16) end_ts
                , left(duration::text,8) duration
                , loading_id
                --, wf_id swf_id
                , beg_msg->>'cwf' cwf_id
                , wf , res
                , replace(end_msg->>'msg', '''', '') msg
                FROM vw_swf_ctl_log
                where loading_id is not null
                    and end_ts between current_date -1 and current_date
                order by 1
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'CTL Yesterday Events';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select 
                    row_number() over(order by status_dttm, a.id, (c.value->>'effective_from')::timestamp) as n
                    , a.id
                    , alive
                --    , start_dttm
                --    , end_dttm
                    , profile
                --    , status_dttm::timestamp(0)
                --    , (now() - status_dttm)::interval(0) status_time
                --    , status
                --    , status_log
                --    , concat(status, ': ', status_log) status_info
                    , wf_id
                    , name
                    , row_number() over(partition by id order by status_dttm, a.id, (c.value->>'effective_from')::timestamp) as nn
                    , (c.value->>'effective_from')::timestamp(0) status_dttm
                --    , ((c.value->>'status') = a.status) is_last
                    , (c.value->>'status') status
                    , replace(replace(c.value->>'log', '''', ''), '"', '') log
                --    , b.name 
                --    , (select string_agg(format('%s (%s)', key, length(a.msg->>key)), ' ' order by key) keys from jsonb_object_keys(a.msg) key) keys 
                --    , a.msg->>'loading_status' loading_status
                from (
                    select a.*, b.name, b.category
                    from vw_log_ctl_loading a
                    left join vw_log_ctl_wf b on a.wf_id=b.id
                ) a, json_array_elements((a.msg->'loading_status')::json) c
                where true
                    and a.alive != 'ACTIVE' and category like 'p1080%'
                    and a.status_dttm::date = current_date - 1
                order by n, nn
            )$sql$;
            html = pr_tbl2html(sql, txt, 'order by n,nn', style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = pr_swf_log_action('end', 'mail', null, mail_id)::text;
        txt = pr_send_mail(mail_id::text);
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

            perform pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context) ; 
            return e_txt;
        end;
    end;
end;

$body$
EXECUTE ON ANY;
	