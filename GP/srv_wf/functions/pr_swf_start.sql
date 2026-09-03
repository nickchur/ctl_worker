CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start(swf text, wf text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    m_txt text;
    m_jsn jsonb;
    ret text;
    wf_rec record;
    log_id int4 default null;
    sql text;
    e_txt text;
    e_detail text;
    e_hint text;
    e_context text;
    res int8;
begin
    sql = format('set application_name = %L', wf);
    execute sql;
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

--  begin
--      set lock_timeout = 10;
--  --  execute format('update tb_swf_%s_log set ts = now() where id=0', lower(swf));
--      execute format('truncate tb_swf_%s_log', lower(swf));
--      execute format('insert into tb_swf_%s_log (id, ts) values(0, now())', lower(swf));
--      set lock_timeout = 0;
--  exception when lock_not_available then
--      m_txt = 'SWF busy (lock_timeout)';
--      raise log '%', m_txt;
--      m_txt = json_build_object('reselt', -1,'swf', swf, 'wf', wf, 'msg', m_txt)::text;
--      set lock_timeout = 0;
--      return m_txt;
--  end;

    log_id = pr_swf_log_action('start', swf);
    begin
        perform pg_sleep(10);
    
        select count(1) into wf_rec from pg_stat_activity a
        where application_name = wf or application_name like wf||'/%'
            and pid <> pg_backend_pid() and state <> 'idle';
        
        if wf_rec.count is null then
            m_txt = json_build_object('reselt', -12, 'swf', swf, 'wf', wf, 'msg', 'WF is  allready started')::text;
            log_id = pr_swf_log_action('error', swf, m_txt::json, log_id);
            return m_txt;
        end if;
        

        select * into wf_rec from vw_swf where wf_name = wf;

        raise info '%', wf_rec;

        if wf_rec.wf_id is null then
            m_txt = json_build_object('reselt', -11, 'swf', swf, 'wf', wf, 'msg', 'No WF')::text;
            log_id = pr_swf_log_action('error', swf, m_txt::json, log_id);
            return m_txt;
        end if;

        if wf_rec.todo is not true then
            m_txt = json_build_object('reselt', -10, 'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg', 'WF TODO status is not True')::text;
            log_id = pr_swf_log_action('todo', swf, m_txt::json, log_id);
            return m_txt;
        end if;
        
        if wf_rec.rel_ok is not true then
            m_txt = json_build_object('reselt', -9, 'swf', swf, 'wf', wf, 'msg', format('WF skipped %s status is not OK', wf_rec.not_ok_note))::text;
            log_id = pr_swf_log_action('skip', swf, m_txt::json, log_id);
        elseif (now() - wf_rec.wf_next) > wf_rec.wf_expire then 
            m_txt = json_build_object('reselt', -8, 'swf', swf, 'wf', wf, 'msg',  'Too late. More then '||wf_rec.wf_expire||' passed. '||wf_rec.wf_next);
            log_id = pr_swf_log_action('late', swf, m_txt::json, log_id);
        else
            sql = format('select %s',wf_rec.wf_exec);
            raise info '%', sql ;
            execute sql  into ret;
            raise info '%', ret ;
            set search_path to s_grnplm_vd_hr_edp_srv_wf;

            m_jsn = try_cast2jsonb(ret);
            res = try_cast2int(m_jsn->>'reselt');
            if res is null then
                if (lower(ret||' ') like 'no %') then
                    m_txt = json_build_object('reselt', -1,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                elseif (lower(ret||' ') like 'ok %') then
                    m_txt = json_build_object('reselt', 1, 'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                elseif (lower(ret) like 'dedlock detected%') then
                    m_txt = json_build_object('reselt', -5,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                else 
--                elseif (lower(ret) like '%error%') then
                    m_txt = json_build_object('reselt', -4,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
                end if;
            else 
                m_txt = json_build_object('reselt', res, 'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',translate(ret,'"',''''))::text;
            end if;
        
            log_id = pr_swf_log_action('end', swf, m_txt::json, log_id);
        end if;
        raise log '%', m_txt;
        update tb_swf set wf_last = now(), wf_duration = clock_timestamp() - now(), wf_reselt = m_txt::json, wf_swf = swf where wf_name = wf;
        return m_txt;
    
    exception 
        when query_canceled then
            m_txt = 'query_canceled or statement_timeout';
            raise log '%', m_txt;
            m_txt = json_build_object('reselt', -2,'swf', swf, 'wf', wf, 'wf_id', wf_rec.wf_id, 'msg',m_txt)::text;
            log_id = pr_swf_log_action('cancel', swf, m_txt::json, log_id);
            update tb_swf set wf_last = now(), wf_duration = clock_timestamp() - now(), wf_reselt = m_txt::json, wf_swf = swf where wf_name = wf;
            set lock_timeout = 0;
            return m_txt;
        when OTHERS then
            get stacked diagnostics e_txt = MESSAGE_TEXT;
            get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL;
            get stacked diagnostics e_hint = PG_EXCEPTION_HINT;
            get stacked diagnostics e_context = PG_EXCEPTION_CONTEXT;
            perform pr_Log_error(0, e_txt, e_detail, e_hint, e_context) ; 
            raise log '%', e_txt;
            m_txt = json_build_object('reselt', -3,'swf', swf, 'wf', wf, 'msg', translate(e_txt,'"',''''))::text;
            log_id = pr_swf_log_action('error', swf, m_txt::json, log_id);
            update tb_swf set wf_last =  now(), wf_duration = clock_timestamp() - now(), wf_reselt = m_txt::json, wf_swf = swf where wf_name = wf;
            set lock_timeout = 0;
            return m_txt;
    end;
end; 
$body$
EXECUTE ON ANY;
	