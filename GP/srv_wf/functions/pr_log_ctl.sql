CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_ctl(_url text, _msg text, _data text DEFAULT NULL::text) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    txt text;
    jsn jsonb;
    _obj text;
    _sub text;
    rc int8;
begin 
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

    _url = split_part(_url, '?', 1);
    _obj = substring(_url from '^/([a-z]+)/?') ;
    _sub = substring(_url from '^/[a-z]+/([a-z]+)/?');

--    insert into tb_log_ctl_all(url, msg, ts, data) values (_url, _msg, now()::timestamp, json_build_object('url', _url, 'obj', _obj, 'sub', _sub)::text);

    if _obj = 'tmpl' then 
        return 'No';
    end if;
    
--    jsn = try_cast2jsonb(_msg);
    begin 
        jsn = _msg::jsonb;
    exception when OTHERS then
        jsn = null::jsonb;
    end;
        
    
    if jsn is null then 
        return 'No json';
    end if;
    
    if jsonb_typeof(jsn) = 'object' then 
        jsn = try_cast2jsonb('['||_msg||']');
    end if;

    if jsonb_typeof(jsn) != 'array' then
        return 'Error';
    end if;
    
    -- drop table if exists tmp_log_ctl;
    -- create temp table tmp_log_ctl 
    -- WITH (appendonly=true, orientation=row, compresstype=zstd, compresslevel=3)
    -- on commit drop as
    insert into tb_log_ctl(id, obj, url, msg, ts)    
        select a.*
        from (
            select id, a.obj, a.url, a.value::jsonb, now()::timestamp
            from (        
                select _url as url, value, _obj as obj, _sub as sub, id
                -- from jsonb_array_elements(jsn)
                from (
                    select a.id,
                        json_object_agg(j.key
                            , case when j.key = 'params' then --j.value
                                (
                                    select json_agg(json_build_object(value->>'param', coalesce(value->>'value', value->>'prior_value')))::jsonb
                                    from json_array_elements(j.value::json) jsn
                                    where left(value->>'param', 2) = 'wf' 
                                        and (value->>'param') <> 'wf_id'
                                ) 
                            else j.value::jsonb end order by nn)::jsonb value
                    from (
                        select *,
                        case _obj
                        when 'info' then 0::int8
                        when 'tmpl' then null::int8
        --                when 'category' then (value->>'cat_id')::int8
                        when 'statval' then (value->>'loading_id')::int8
                        when 'wf' then
                            case _sub
                            when 'extended' then (value->'wf'->>'id')::int8 
                            else (value->>'id')::int8 end
                        else (value->>'id')::int8
                        end id
                        from jsonb_array_elements(jsn) --with ordinality 
                        where jsonb_typeof(value) = 'object'
                    ) a, jsonb_each(a.value) with ordinality as j (key, value, nn)
                    group by id
                    --order by id
                ) a
            ) a
        ) a
        left join (
            select distinct on (a.id) * 
            from tb_log_ctl a
            where a.url = _url  --and a.id = _id
            order by a.id, a.ts desc
        ) b on a.id = b.id and a.value = b.msg
        where (b.id is null and a.id is not null) or _obj = 'info'
    -- distributed by (id)
    ;
    get diagnostics rc = ROW_COUNT;
    
    if rc = 0 then
        return 'No data';
    end if;

    -- insert into tb_log_ctl(id, obj, url, msg, ts) select * from tmp_log_ctl;

    return 'Ok '||rc;

--exception when OTHERS then
--    return sqlerrm;
end; 

$body$
EXECUTE ON ANY;
	