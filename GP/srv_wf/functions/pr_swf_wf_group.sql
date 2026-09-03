CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_wf_group(fnc text[], rel text[]) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$


declare
--    fnc text[] default '{smdtodia_ref_pos,smdtodia_ref_pos_hist,diatostg_ref_pos,diatostg_ref_pos_hist}';
--    rel text[] default '{"{}","{1}","{1,2}","{3,4}"}';
    res  int[];
    mif  bool default true;
    err  bool default true;
    rif int[];
    k int;
    i int;
    ret text default '';
    log_id int4 default null;
    m_txt text;
    sql text;
    app text;
    func text;
    try int;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    execute 'show application_name' into app;
    begin
        for k  in 1..array_length(fnc,1) loop
            if (mif) then
            
                func = trim(fnc[k]);

                sql = format('set application_name = %L', coalesce(app||'/', '')||func);
                execute sql;
                
                if left(lower(func),3) <> 'pr_' then
                    func = 'pr_' || func;
                end if;
                
                if right(func, 1) <> ')' then
                    func = func || '()';
                end if;
            
                try = 1;
                for i  in 1..3 loop
                    execute format('select %s', func) into m_txt;  
                    exit when m_txt not like '%transfer error (18)%' and m_txt not like '%PXF server error%';
                    try = try + 1;
                    perform pg_sleep(60);
                end loop;
                
                if try > 1 then
                    m_txt = concat(m_txt, ' (try=', try, ')');
                end if;

                if (lower(left(coalesce(trim(m_txt),'')||' ',3)) in ('ok ', ' ')) then
                    res[k] =  1;
                elsif (lower(left(coalesce(trim(m_txt),'')||' ',3)) in ('no ')) then
                    res[k] =  0;
                else 
                    res[k] = -1;
                end if;
            else 
                m_txt = 'WF skipped';
                -- res[k] = 0;
                res[k] = - err::int;
            end if;
            -- ret = concat(chr(10),'< ', fnc[k], ': ', m_txt, ret);
            ret = concat(ret, chr(10),'> ', fnc[k], ': ', m_txt);
       
            rif = rel[k]::int[];
            if array_length(rif,1)>=1 then
                mif = false;
                err = false;
                for i  in 1..array_length(rif,1) loop
                    mif = (mif or (res[rif[i]] =  1));
                    err = (err or (res[rif[i]] = -1));
                end loop;
            else 
                mif = true;
            end if;
        end loop;

--        if coalesce(array_length(rif,1), 0) = 0 then 
        if coalesce(array_length(rel[array_length(fnc, 1)]::int[], 1), 0) = 0 then 
            mif = true;
            err = false;
            for i  in 1..array_length(fnc,1) loop
                mif = (mif and (res[i] =  1));
                err = (err or  (res[i] = -1));
            end loop;
        end if;
        
        if (err) then
            return format('Error %s %s', res, ret);
        elsif (mif) then
            return format('Ok %s %s', res, ret);
        else 
            return format('No %s %s', res, ret);
        end if;

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
            
            log_id = pr_Log_error(log_id,e_txt,e_detail,e_hint,e_context) ;
            return concat('Error ', e_txt, ret);
        end;
    end;
end; 
$body$
EXECUTE ON ANY;
	