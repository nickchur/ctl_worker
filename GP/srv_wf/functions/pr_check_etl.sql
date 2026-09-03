CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_etl(obj text, sch text, rdt text, ldt text DEFAULT NULL::text, kdt text DEFAULT NULL::text, etype boolean DEFAULT false, expire timestamp without time zone DEFAULT NULL::timestamp without time zone) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    return s_grnplm_vd_hr_edp_srv_wf.pr_check_etl(obj, sch, json_build_object('rdt', rdt, 'ldt', ldt, 'kdt', kdt, 'etype', etype, 'expire', expire));
end;

$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_check_etl(obj text, sch text, prm json DEFAULT NULL::json) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    swf text = 'chk';
    m_jsn json;
    r_jsn json;
    s_jsn json;
    lst record;
    sql text = '';
    sql1 text = '';
    sql2 text = '';
    sql3 text = '';
    chk_id int4;
    log_id int4;
    new_id int4;
    m_txt text;
    ztest json;
    log_tb text;
    rc int8;
    keys date[];
    
    rdt text = (prm->>'rdt')::text;
    ldt text = (prm->>'ldt')::text;
    kdt text = (prm->>'kdt')::text;
    etype bool = (prm->>'etype')::bool;
    expire timestamp = (prm->>'expire')::timestamp;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    log_id = pr_Log_start(format('CHECK_%s_%s', sch, obj));
    begin
        m_jsn = json_build_object('obj', obj, 'sch', sch);
        chk_id = pr_swf_log_action('chk', swf, m_jsn);
        -- expire = coalesce(expire, now() - '1 month'::interval);
        expire = coalesce(expire, '1900-01-01' );
        
        if (prm->>'sql')::text is not null then
            sql = format($sql$
                select row_to_json(b) from (%s) b
            $sql$, prm->>'sql');
            execute sql into r_jsn;
        elsif (prm->>'select')::text is not null then
            sql = format($sql$
                select row_to_json(b) from ( select %3$s from "s_grnplm_vd_hr_edp_%1$s".%2$I a ) b
            $sql$, sch, obj, prm->>'select');
            execute sql into r_jsn;
        else
            if kdt is null and (
                select true from information_schema.columns 
                where table_schema = format('s_grnplm_vd_hr_edp_%s', sch) 
                    and table_name = obj 
                    and column_name = 'actual_date'
                    and split_part(data_type, ' ', 1) in ('date', 'timestamp')
            ) then
                kdt = 'actual_date';
            end if;

            if ldt is null and (
                select true from information_schema.columns 
                where table_schema = format('s_grnplm_vd_hr_edp_%s', sch) 
                    and table_name = obj 
                    and column_name = 'load_date'
                    and split_part(data_type, ' ', 1) in ('date', 'timestamp')
            ) then
                ldt = 'load_date';
            end if;

            if rdt is null and (
                select true from information_schema.columns 
                where table_schema = format('s_grnplm_vd_hr_edp_%s', sch) 
                    and table_name = obj 
                    and column_name = 'report_date'
                    and split_part(data_type, ' ', 1) in ('date', 'timestamp')
            ) then
                rdt = 'report_date';
            end if;

            if kdt is not null then
                sql1 = concat(sql1, ', min(kmin)::text as kmin, max(kmax)::text as kmax');
                sql2 = concat(sql2, ', min(kdt) as kmin, max(kdt) as kmax');
                sql3 = concat(sql3, format(', %1$s as kdt', kdt));
            end if;

            if ldt is not null then
                sql1 = concat(sql1, ', min(lmin)::text as lmin, max(lmax)::text as lmax');
                sql2 = concat(sql2, ', min(ldt) as lmin, max(ldt) as lmax');
                sql3 = concat(sql3, format(', %1$s as ldt', ldt));
            end if;

            if rdt is not null then
                sql1 = concat(sql1, ', min(rmin)::text as rmin, max(rmax)::text as rmax');
                sql2 = concat(sql2, ', min(rdt) as rmin, max(rdt) as rmax');
                sql3 = concat(sql3, format(', %1$s as rdt', rdt));
            end if;

            begin
                sql = format($sql$
                    select row_to_json(b) from (
                        -- select md5(string_agg(md5(a.*::text), '' order by md5(a.*::text))) hash 
                        select md5(string_agg(md5(a.*::text), '' order by sid, rid)) hash 
                            , to_char(coalesce(sum(cnt), 0), 'FM999,999,999,999,999,999') cnt, count(sid) as x %3$s
                        from (
                            select sid, (rn/10000000)::int4 as rid , count(1) cnt
                                , md5(string_agg(hash::text, '' order by rn)) hash %4$s
                            from (
                                select row_number() over (partition by sid order by hash) as rn, a.*
                                from (
                                    select md5(a.*::text)::uuid hash, gp_segment_id sid %5$s
                                    from "s_grnplm_vd_hr_edp_%1$s".%2$I a 
                                ) a
                            ) a
                            group by 1, 2
                        ) a
                    ) b
                $sql$, sch, obj, sql1, sql2, sql3);
                execute sql into r_jsn;
            exception when OTHERS then
                sql = format($sql$
                    select row_to_json(b) 
                    from (
                        -- select md5(string_agg(md5(a.*::text), '' order by md5(a.*::text))::text) hash
                        select md5(string_agg(md5(a.*::text), '' order by rid)) hash
                            , to_char(coalesce(sum(cnt), 0), 'FM999,999,999,999,999,999') cnt %3$s
                        from (
                            select (rn/10000000)::int4 as rid , count(1) cnt
                                , md5(string_agg(hash::text, '' order by rn)) hash %4$s
                            from (
                                select row_number() over (order by hash) as rn, a.*
                                from (
                                    select md5(a.*::text)::uuid hash %5$s
                                    from "s_grnplm_vd_hr_edp_%1$s".%2$I a 
                                ) a
                            ) a
                            group by 1
                        ) a 
                        limit 1
                    ) b
                $sql$, sch, obj, sql1, sql2, sql3);
                execute sql into r_jsn;
            end;
        end if;

        select distinct on (a.wf_message::jsonb::text) (b.wf_message->>'value')::jsonb jsn, a.ts
        into lst
        from tb_swf_chk_log a
        join tb_swf_chk_log b on b.parent = a.id and b.wf_action='end' and (b.wf_message->>'res')='1'
        where true and a.parent is null and a.wf_action='chk'
            and a.wf_message::jsonb::text = m_jsn::jsonb::text
        order by a.wf_message::jsonb::text, a.id desc;
        
        rc = replace(coalesce(r_jsn->>'cnt', r_jsn->>'count'), ',', '')::int8;
        
        if (not etype) and (rc is null or rc = 0) then
            m_txt = 'Empty';
            m_jsn = json_build_object('res', -1, 'msg', m_txt, 'last', left(lst.ts::text, 19), 'value', r_jsn);
        elsif lst.jsn::jsonb::text = r_jsn::jsonb::text then
            if lst.ts >= expire then
                m_txt = 'No new';
                m_jsn = json_build_object('res', 0, 'msg', m_txt, 'last', left(lst.ts::text, 19), 'value', r_jsn);
             else
                m_txt = 'Expire';
                m_jsn = json_build_object('res',-2, 'msg', m_txt, 'last', left(lst.ts::text, 19), 'value', r_jsn);
             end if;
        else
            m_txt = 'Ok';
            m_jsn = json_build_object('res', 1, 'msg', m_txt, 'last', left(lst.ts::text, 19), 'value', r_jsn);
        end if;

        chk_id = pr_swf_log_action('end', swf, m_jsn, chk_id);
        
        new_id = nextval('s_grnplm_vd_hr_edp_srv_wf.tb_log_workflow_id_seq'::regclass);
        
        log_tb = format('s_grnplm_vd_hr_edp_%s.%s', sch, obj);
        
        with add as (
            insert into tb_log_workflow_stat (log_id, wf_obj, rw_cnt, key_name, key_min, key_max, load_name, load_min, load_max, data_name, data_min, data_max) 
            values (new_id, log_tb, rc, kdt, (r_jsn->>'kmin'), (r_jsn->>'kmax')
                    , ldt, try_cast2timestamp(r_jsn->>'lmin'), try_cast2timestamp(r_jsn->>'lmax')
                    , rdt, try_cast2timestamp(r_jsn->>'rmin'), try_cast2timestamp(r_jsn->>'rmax'))
            returning *
        )
        select row_to_json(add.*) into s_jsn from add
        ;
        
        keys = array [s_grnplm_vd_hr_edp_srv_wf.try_cast2timestamp((r_jsn->>'kmax'))::date, (r_jsn->>'lmax')::timestamp::date, current_date];
        ztest = s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(log_tb, keys, rc, new_id, now()::timestamp);
--        ztest = s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(log_tb, s_jsn, rc, new_id, now()::timestamp);

        insert into tb_log_workflow (id, parent, wf_action, wf_message) 
        values (new_id, nullif(log_id,0), 'end', log_tb);
        
        return format('%s last %s %s', m_txt, left(lst.ts::text, 19), r_jsn::text);
        
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
            
            -- m_jsn = json_build_object('res', -3, 'msg', translate(e_txt,'"',''''));
            -- chk_id = pr_swf_log_action('error', swf, m_jsn, chk_id);
            perform pr_Log_error(log_id, e_txt, e_detail, e_hint, e_context) ; 
            return 'Error: '||e_txt;
        end;
    end;
end;

$body$
EXECUTE ON ANY;
	