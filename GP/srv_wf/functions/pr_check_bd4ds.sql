CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_bd4ds(obj text) 
	RETURNS text
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare 
    m_txt text;
    e_detail text;
    e_hint text;
    e_context text;
    
    sql text;
    swf text = 'chk';
    sch text = 'bd4ds';
    -- obj text = '';

    log_id int4;
    chk_id int4;
    m_res int4;
    
    lst record;

    m_jsn json;
    r_jsn json;
    
    style json;
    html text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('CHECK_%s_%s', sch, obj));
    begin
        m_jsn = json_build_object('obj', obj, 'sch', sch);
        chk_id = pr_swf_log_action('chk', swf, m_jsn);
        
        sql = (
            select string_agg(a.sql , ' union all ') as sql
            from (
                select format($sql$
                    select 
                        ((%4$s) %5$s (%6$s)) as res
--                        , %1$L as grp
                        , %2$L as sch
                        , %3$L as tbl
                        , %4$L as bd_exp
                        , %4$s as bdate
                        , %5$L as operation
                        , %6$s as condition
                    from s_grnplm_vd_hr_edp_%2$s.%3$s %7$s
                $sql$, a.grp, a.sch, a.tbl, a.bdate, a.operation, a.condition, a.whr) as sql
                from tb_bd4ds a
                where grp like obj
            ) a
        );

        drop table if exists tmp_vw;
        
        execute format($sql$
            create temp table tmp_vw
            WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
            on commit drop 
            as %s
            order by 1,2
        $sql$, sql);

        r_jsn = (
            select row_to_json(b) from (
                select a.res, a.bdate as min_bd
                , concat(a.operation, ' ', a.condition) msg
                -- , string_agg(distinct concat(a.operation, ' ', a.condition), ', ') msg
                -- , string_agg(concat(a.sch, '.', a.tbl, ' ' ,a.operation, ' ', a.condition), ', ') msg
                -- , string_agg(concat(a.sch, '.', a.tbl), ', ') msg
                from tmp_vw a
                -- group by 1, 2, 3
                order by 1, 2, 3
                limit 1
            ) b
        );
        -- alter table tmp_vw drop column sch;
        alter table tmp_vw drop column operation;
        alter table tmp_vw drop column condition;

        select distinct on (a.wf_message::jsonb::text) (b.wf_message->>'value')::jsonb jsn, a.ts
        into lst
        from tb_swf_chk_log a
        join tb_swf_chk_log b on b.parent = a.id and b.wf_action='end' and (b.wf_message->>'res')='1'
        where true and a.parent is null and a.wf_action='chk'
            and a.wf_message::jsonb::text = m_jsn::jsonb::text
        order by a.wf_message::jsonb::text, a.id desc;
        
        if not (r_jsn->>'res')::bool then 
            m_txt = 'Error';
            m_res = -3 ;
        -- elsif lst.jsn::jsonb::text = r_jsn::jsonb::text then
        elsif lst.jsn::jsonb->>'min_bd' = r_jsn::jsonb->>'min_bd' then
            m_txt = 'No new';
            m_res = 0 ;
        else
            m_txt = 'Ok';
            m_res = 1 ;
        end if;
        m_jsn = json_build_object('res', m_res, 'msg', m_txt, 'last', left(lst.ts::text, 19), 'value', r_jsn, 'stat', json_build_object('5', r_jsn->>'min_bd'));

        chk_id = pr_swf_log_action('end', swf, m_jsn, chk_id);

        style = json_build_object(
            '_table', $css$ select 'border', '1' $css$
            ,
            '_th', $css$ select 'style' ,'background:silver' union select 'style' ,'text-align:center' $css$
            ,
            'td', $css$ 
                select 'id' , 'td'
                union
                select 'style' , 'text-align:'|| 
                    case 
                    when %type% = 'number' then 'right' 
                    when %key% = 'operation' then 'center' 
                    else null end
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
                    else null end
            $css$
        );
        
        html = '<style>table,th,tr,td {border:1px solid grey; border-collapse:collapse; padding:5px; font-size:13px; text-align: left}</style>';
        html = concat(html, format('<div style="color:%1$s"><h2> %2$s </h2><h3> Grp %3$s Min Bisiness date %4$s </h3><h4> %5$s </h4></div>'
            , case when m_txt = 'Error' then 'red' when m_txt = 'Ok' then 'green' else 'black' end, m_txt, obj, r_jsn->>'min_bd',  r_jsn->>'msg'));
        html = concat(html, pr_tbl2html('tmp_vw', format('CHECK_%s_%s', sch, obj), 'order by res, bdate', style));
        
        m_jsn = (
            select json_object_agg(key, value order by rn) from (
                select row_number() over() as rn, * from jsonb_each(m_jsn::jsonb)
                union select 999, 'html', to_json(array[html])::jsonb
            ) a
        );

        log_id = pr_log_action('end', format('%s last %s %s', m_txt, left(lst.ts::text, 19), r_jsn::text), log_id);
        return m_jsn::text;

    exception when OTHERS then
        get stacked diagnostics m_txt = MESSAGE_TEXT;
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
        get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
        get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;

        perform pr_Log_error(log_id, m_txt, e_detail, sql, e_context) ; 
        return format('Error: %s', m_txt);
    end;
end;

$body$
EXECUTE ON ANY;
	