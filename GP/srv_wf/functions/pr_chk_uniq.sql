CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_chk_uniq(srs text, keys text, fdate text, tdate text, raise_exc boolean, lmt integer DEFAULT 10) 
	RETURNS json
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    exe text;
    ext text;
    err int8;
    chk text = '';
    msg text = '';
    rc int8;
begin
    if split_part(srs, '.', 1) in ('stg','dia','vd','vda','dm','fcts','srv_dq','srv_wf','udlprod','udlapprove','dac','dds','md','res') then
        srs = concat('s_grnplm_vd_hr_edp_', srs);
    end if;
    
    if fdate is null then
        ext = 'sum(cnt) cnt';
    elsif fdate = fdate then
        ext = 'sum(cnt) cnt, min(cd) cd';
    else
        ext = 'count(distinct cd) cnt, min(cd), max(cd)';
    end if;
        
    fdate = coalesce(fdate, 'current_date');
    tdate = coalesce(tdate, 'current_date');
    
    exe = format($exe$
        -- select count(distinct concat(%2$s)), string_agg(row_to_json(a.*)::text, ' ' order by count desc, %2$s) 
        select count(distinct concat(%2$s)), json_agg(row_to_json(a.*)::json order by cnt desc, %2$s) 
        from (
            select %2$s, %5$s
            from (
                select cd, %2$s, count(1) cnt
                from (
                    select distinct %3$s cd from %1$s a
                    union 
                    select distinct %4$s cd from %1$s a
                ) cd
                inner join %1$s a on cd between %3$s and %4$s
                group by cd, %2$s
                having count(1) > 1 
                limit 1000000
            ) a 
            group by %2$s
            order by cnt desc
            limit %6$s
        ) a
    $exe$, srs, keys, fdate, tdate, ext, lmt);
    execute exe into err, chk;
    -- get diagnostics rc = ROW_COUNT;
    
    if err > 0 then
        msg = format('Uniq check errors %s %s', case err when lmt then '>=' else '=' end, err);
        
        if raise_exc then 
            raise exception using ERRCODE = 'XX001', MESSAGE = msg, DETAIL = chk; 
        else
            return json_build_object('err', err, 'msg', msg, 'chk', chk);
        end if;
    else
        msg = '';
        return json_build_object('msg', 'Uniq Ok');
    end if;
    
exception when OTHERS then
    raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = exe;            
    -- raise exception using ERRCODE = sqlstate, MESSAGE = exe, DETAIL = exe, HINT = exe;            
end;

$body$
EXECUTE ON ANY;
	