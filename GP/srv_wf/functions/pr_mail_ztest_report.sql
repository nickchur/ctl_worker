CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_mail_ztest_report(reports text[] DEFAULT NULL::text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    mail_id int4;
    sql text;
    sql2 text;
    txt text = 'Ztest Report';
    html text = '';
    style json;
    log_id int4;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('REPORT_%1$s', replace(txt, ' ', '_')));
    begin
        -- html = '<style>table,th,tr,td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        html = '<style>#table,#th,#tr,#td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        -- html = html || '<style>#tr:nth-child(odd) {background:#fff} #tr:nth-child(even) {background:#f7f7а7}</style>';
        html = html || '<style>#np {font-size:0px}</style>';
        html = html || '<style>#num {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; font-weight:500; text-align: right}</style>';

        html = html || format('<h2> %s </h2><h4> %s </h4>', txt, left(now()::text,16));

        mail_id = pr_swf_log_action( txt, 'mail', json_build_object('len', length(html), 'html', html));
        
        style = pr_mail_style();

        txt = 'Ztest Summary';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select a.n, '' as rel_key, row_to_json(a.*) row 
                from (
                    select row_number() over(order by split_part(z.object, '.', 1) desc, ts desc) n 
                        , left(z.ts::text, 16) dttm
                        , replace(split_part(z.object, '.', 1), 's_grnplm_vd_hr_edp_', '') sch
                        , split_part(z.object, '.', 2) object
                        , z.ztest_ok
                        --, z.is_except, z.is_error
                        --, z.confidence
                        , greatest(round(100 - abs(z.zscore::numeric), 1), 0.0) hub_val
                        , round(z.zscore::numeric, 2) zscore
                        , z.key_date key
                        , z.key_diff::text
                        , round(z.stable::numeric, 2) stable
                        , to_char(z.rows_count, 'FM999 999 999 999 999 999') rows_count
                        , to_char(z.value, 'FM999 999 999 999 999 999') value
                        , to_char(z.avg, 'FM999 999 999 999 999 999') avg
                        , to_char(z.std, 'FM999 999 999 999 999 999') std
                        , to_char(z.min, 'FM999 999 999 999 999 999') min
                        , to_char(z.max, 'FM999 999 999 999 999 999') max
                        , to_char(z.cnt, 'FM999 999 999 999 999 999') cnt
                        --, z.mail_id
                        --, z.notes 
                        --, (z.notes->>'msg') msg
                        , (z.notes->>'cfg') cfg
                    from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z
                    where true
                        --and ts >= current_date -1 --and ts < current_date
                        and ts >= (now() - '6 hours'::interval)::date
        --                and (not ztest_ok or split_part(z.object, '.', 1) ~ 's_grnplm_vd_hr_edp_vd')
                        and not ztest_ok 
                        and split_part(z.object, '.', 1) in ('s_grnplm_vd_hr_edp_vd','s_grnplm_vd_hr_edp_stg')
                        --and (error is not null or split_part(z.object, '.', 1) ~ 's_grnplm_vd_hr_edp_vd')
                ) a
                order by 1  
            )$sql$;
            -- html = pr_tbl2html(sql, txt, 'order by n', style);
            html = pr_tbl2html_loop(sql, sql2, txt, style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;

        txt = 'Actual_date repeat';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select a.n, '' as rel_key, row_to_json(a.*) row 
                from (
                    select row_number() over(order by days, ts desc) n
                        , ts::timestamp(0)::text
                        , replace(object, 's_grnplm_vd_hr_edp_', '') object
                        , key_date as actual_date
                        --, rn
                        , rows_count
                        , days
                        , round(days / ((cnt-days)/(cnt_uniq-1)), 1) ratio
                        , format('%s : %s', max, key_from) as load_date
                        --, key_from as load_from
                        --, min, keys, next
                        --, keys_len
                        , cnt, cnt_uniq, cnt - cnt_uniq as not_uniq
                        , round((cnt-days)/(cnt_uniq-1), 1) avg_uniq
                        , max_days
                        --, replace(key_dates::text, 'NULL', '...') key_dates
                    from (
                        select a.*
                            , array_length(keys, 1) keys_len
                            , (
                                select count(*)
                                from unnest(a.keys) with ordinality keys(key,n)
                                where key > coalesce(a.next, a.min - 1)
                            ) days
                            , (
                                select min(key)
                                from unnest(a.keys) with ordinality keys(key,n)
                                where key > coalesce(a.next, a.min - 1)
                            ) key_from
                            , (
                                select array_agg(distinct case when n <= 10 then key else null end order by  case when n <= 10 then key else null end desc nulls last)
                                from unnest(a.keys) with ordinality keys(key,n)
                                where key > coalesce(a.next, a.min - 1)
                            ) key_dates
                        from (
                            select *
                                , lead(ts::date) over(partition by object order by ts::date desc) next
                                , sum(row_cnt) over(partition by object) cnt
                                , max(array_length(a.keys, 1)) over(partition by object) max_days
                            from (
                                select object, key_date
                                    , min(rn) rn
                                    , max(ts::date)
                                    , min(ts::date)
                                    , array_agg(ts::date order by ts::date desc) keys
                                    , max(ts) ts
                                    , count(distinct key_date) over(partition by object) cnt_uniq
                                    , count(1) row_cnt
                                    , string_agg(distinct to_char(rows_count, 'FM999 999 999 999 999 999'), '; ') rows_count
                                    -- , string_agg(distinct rows_count::text, '; ') rows_count

                                from (
                                    select distinct on (object, ts::date) *
                                        , row_number() over(partition by object order by ts::date desc, key_date desc, ts desc ) rn
                                    from (
                                        select start_ts as ts
                                            , message as object
                                            , rows_count
                                            , case 
                                                when key_name = 'actual_date' then key_max::timestamp::date
                                                when load_name = 'actual_date' then load_max::date
                                                when period_name = 'actual_date' then period_to::date
                                                else null::date end key_date
                                        from vw_log_workflow
                                        where message like 's_grnplm_vd_hr_edp_stg.%'
                                            and message not like 's_grnplm_vd_hr_edp_stg.tb_ref_%'
                                            and start_ts > current_date - 100
                                            --and message ~ 'tb_ucp_orgunit_properties'
                                    ) a
                                    order by object, ts::date desc, key_date desc, ts desc
                                ) a
                                where rn <= 1000
                                group by 1,2
                            ) a
                        ) a
                        where array_length(a.keys, 1) > 1
                            and rn = 1
                            and ts >= (now() - '6 hour'::interval)::date - 1
                    ) a
                    where days > 1 and cnt_uniq > 1
                        --and days > (cnt-days)/(cnt_uniq-1)
                ) a
                order by n 
            )$sql$;
            -- html = pr_tbl2html(sql, txt, 'order by n', style);
            html = pr_tbl2html_loop(sql, sql2, txt, style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        txt = 'Row_count repeat';
        if txt = any(reports) or reports = '{All}' or nullif(reports, '{}') is null then
            sql = $sql$(
                select a.n, '' as rel_key, row_to_json(a.*) row 
                from (
                    select row_number() over(order by days, ts desc) n
                        , ts::timestamp(0)::text
                        , replace(object, 's_grnplm_vd_hr_edp_', '') object
                        , to_char(rows_count, 'FM999 999 999 999 999 999') rows_count
                        --, rn
                        , days
                        , round(days / ((cnt-days)/(cnt_uniq-1)), 1) ratio
                        , format('%s : %s',key_date,key_from) as actual_date
                        -- , key_from as actual_from
                        --, min, keys, next
                        --, keys_len
                        , cnt, cnt_uniq, cnt - cnt_uniq as not_uniq
                        , round((cnt-days)/(cnt_uniq-1), 1) avg_uniq
                        , max_days
                        --, replace(key_dates::text, 'NULL', '...') key_dates
                    from (
                        select a.*
                            , array_length(keys, 1) keys_len
                            , (
                                select count(*)
                                from unnest(a.keys) with ordinality keys(key,n)
                                where key > coalesce(a.next, a.min - 1)
                            ) days
                            , (
                                select min(key)
                                from unnest(a.keys) with ordinality keys(key,n)
                                where key > coalesce(a.next, a.min - 1)
                            ) key_from
                            , (
                                select array_agg(distinct case when n <= 10 then key else null end order by  case when n <= 10 then key else null end desc nulls last)
                                from unnest(a.keys) with ordinality keys(key,n)
                                where key > coalesce(a.next, a.min - 1)
                            ) key_dates
                        from (
                            select *
                                , lead(key_date) over(partition by object order by key_date desc) next
                                , sum(row_cnt) over(partition by object) cnt
                                , max(array_length(a.keys, 1)) over(partition by object) max_days
                            from (
                                select object, rows_count
                                    , min(rn) rn
                                    , max(key_date) key_date
                                    , min(key_date)
                                    , array_agg(key_date order by key_date desc) keys
                                    , max(ts) ts
                                    , count(distinct rows_count) over(partition by object) cnt_uniq
                                    , count(1) row_cnt
                                from (
                                    -- select distinct on (object, key_date) z.*
                                    -- , row_number() over(partition by object order by key_date desc, ts desc ) rn
                                    -- from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z 
                                    -- where object like 's_grnplm_vd_hr_edp_stg.%'
                                    --     and object not like 's_grnplm_vd_hr_edp_stg.tb_ref_%'
                                    --     and ts > current_date - 100
                                    -- order by object, key_date desc, ts desc
                                    select distinct on (object, key_date) *
                                        , row_number() over(partition by object order by key_date desc, ts desc ) rn
                                    from (
                                        select start_ts as ts
                                            , message as object
                                            , rows_count
                                            , case 
                                                when key_name = 'actual_date' then key_max::timestamp::date
                                                when load_name = 'actual_date' then load_max::date
                                                when period_name = 'actual_date' then period_to::date
                                                else null::date end key_date
                                        from vw_log_workflow
                                        where message like 's_grnplm_vd_hr_edp_stg.%'
                                            and message not like 's_grnplm_vd_hr_edp_stg.tb_ref_%'
                                            and start_ts > current_date - 100
                                    ) a
                                    order by object, key_date desc, ts desc

                                ) a
                                where rn <= 1000
                                group by 1,2
                            ) a
                        ) a
                        where array_length(a.keys, 1) > 1
                            and rn = 1
                            and ts >= (now() - '6 hour'::interval)::date - 1
                    ) a
                    where days > 1 and cnt_uniq > 1
                        and days > (cnt-days)/(cnt_uniq-1)
                ) a
                order by n 
            )$sql$;
            -- html = pr_tbl2html(sql, txt, 'order by n', style);
            html = pr_tbl2html_loop(sql, sql2, txt, style);
            txt = pr_swf_log_action(txt, 'mail', json_build_object('len', length(html), 'html', html), mail_id)::text;
        end if;


        style = pr_mail_style($${ "th": { "colspan": {"": { "key = 'cfg'": "15"} } }, "td": { "colspan": {"": { "key = 'cfg'": "15"} } } }$$::json);
        
        txt = 'Ztest Details';
        if txt = any(reports) or reports = '{All}' then
            sql = $sql$(
                select a.n, md5(a.object)::uuid as rel_key, row_to_json(a.*) row 
                from (
                    select row_number() over(order by last desc) n, 0 nn
                        , replace(z.object, 's_grnplm_vd_hr_edp_', '') object
                        , last::timestamp(0)::text
                        , (z.notes->>'cfg') cfg
                    from (
                        select distinct on (object) object, ts last, notes
                        from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z
                        where true
                            --and ts >= current_date -1 --and ts < current_date
                            and ts >= (now() - '6 hours'::interval)::date -- 10
                            and not ztest_ok
                            and (error is not null or split_part(z.object, '.', 1) ~ 's_grnplm_vd_hr_edp_stg')
                        order by object, ts desc
                    ) z
                    order by n
                    limit 10
                ) a
            )$sql$;
            
            sql2 = $sql$(
                select a.n, a.nn, row_to_json(a.*) row 
                from (
                    select distinct b.n
                        , dense_rank() over(partition by b.n order by ts desc) nn
                        , (z.notes->>'msg') msg
                        , ts::timestamp(0)::text
                        --, replace(z.object, 's_grnplm_vd_hr_edp_', '') object
                        -- , replace(split_part(z.object, '.', 1), 's_grnplm_vd_hr_edp_', '') sch
                        -- , split_part(z.object, '.', 2) object
                        --, '</td></tr><tr><td>' as "</th></tr><tr><th>"
                        --, z.object
                        , z.ztest_ok
                        , z.is_except, z.is_error
                        --, z.confidence
                        , round(z.zscore::numeric, 2) zscore
                        , z.key_date
                        , z.key_diff::text
                        , round(z.stable::numeric, 2) stable
                        , to_char(z.rows_count, 'FM999 999 999 999 999 999') rows_count
                        , to_char(z.value, 'FM999 999 999 999 999 999') value
                        , to_char(z.avg, 'FM999 999 999 999 999 999') avg
                        , to_char(z.std, 'FM999 999 999 999 999 999') std
                        , to_char(z.min, 'FM999 999 999 999 999 999') min
                        , to_char(z.max, 'FM999 999 999 999 999 999') max
                        , to_char(z.cnt, 'FM999 999 999 999 999 999') cnt
                        , z.log_id
                        --, z.notes 
                    from s_grnplm_vd_hr_edp_srv_dq.vw_ztest z
                    inner join tbody b on b.rel_key = md5(replace(z.object, 's_grnplm_vd_hr_edp_', ''))::uuid
                    -- where z.ts >= current_date - 50
                ) a
                where nn <= 30
            )$sql$;
            html = pr_tbl2html_loop(sql, sql2, txt, style);
            -- html = pr_tbl2html(sql, txt, 'order by n', style);
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
	