-- Сборка пакета обмена: перенесено из HR_Data
-- (sql/create/s_grnplm_vd_hr_edp_vda/functions/pr_exchange.sql) с тремя правками
-- под PostgreSQL, все — про физическое хранение, не про логику:
--   * убраны WITH (appendonly=true, orientation=column, compresstype=zstd) у temp-таблиц;
--   * убраны DISTRIBUTED BY / DISTRIBUTED REPLICATED;
--   * убран EXECUTE ON ANY.
-- Тело функции, конфигурация потоков и SQL сборки не тронуты — иначе стенд проверял бы
-- не то, что работает в бою.

CREATE FUNCTION s_grnplm_vd_hr_edp_vda.pr_exchange(_id integer DEFAULT NULL::integer) 
	RETURNS TABLE(wf_id text, wf_name text, wf_key text, wf_data text)
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

declare
    sql text;
    sql_cnt text;
    rc int8;
    rec record;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

    -- Если _id = 0, генерируем новый ID
    if _id = 0 then
        _id = nextval('tb_exchange_log_id_seq'::regclass);
    end if;


    drop table if exists tmp_exchange_config;
    create temp table tmp_exchange_config
    on commit drop as
        select distinct *
        from (
            select 0 wf_type, '' wf_sch, '' wf_name, '' wf_key, '' wf_key_type, '' wf_where, 0 wf_key_limit, '' wf_where_all
            union select 0, 'srv_dq', 'vw_ztest', 'ts' , 'date', 'or ts >= current_date - 1', 100, 'true'
            union select 1, 'srv_wf', 'vw_exchange_log', 'ts' , 'date', ' or ts >= current_date - 1', 100
                , format($$ (ts >= date_trunc('year', current_date - 30) and id <> %s) $$, _id)
            union select 1, 'srv_wf', 'vw_swf_chk_log', 'beg_ts' , 'date', 'or beg_ts >= current_date - 1', 100, 'true'
            union select 1, 'srv_wf', 'vw_swf_ctl_log', 'beg_ts' , 'date', 'or beg_ts >= current_date - 1', 100, 'true'
            union select 1, 'srv_wf', 'vw_log_workflow', 'start_ts' , 'date', 'or start_ts >= current_date - 1', 100, $$start_ts > '2025-01-01'$$
            union select 2, 'srv_wf', 'vw_log_ctl_loading', 'ts' , 'date', $$or ts > now() - '6h'::interval$$, 100, 'true'
            union select 2, 'srv_wf', 'vw_log_ctl_entity', 'ts' , 'date', $$or ts > now() - '6h'::interval$$, 100, 'true'
            union select 2, 'srv_wf', 'vw_log_ctl_wf', 'ts' , 'date', $$or ts > now() - '6h'::interval$$, 100, 'true'
        ) a
        where wf_key_limit > 0
    ;

    -- Создание временной таблицы с пакетом данных
    -- Инициализация tmp_exchange через vw_exchange, который уже содержит:
    -- vw_ztest (с инкрементом и retry)
    -- Это позволяет использовать готовую логику выгрузки без дублирования кода.
    -- Дальнейшие insert'ы добавят данные из других источников (логи, мета и т.п.)

    drop table if exists tmp_exchange;
    create temp table tmp_exchange
    on commit drop as
--         select null::text as wf_name, null::text as wf_key, null::text as wf_data limit 0
        select * from vw_exchange
    ;

    drop table if exists log;
    create temp table log
    on commit drop as
        -- Определение последнего обработанного ключа по каждому потоку
        -- select a.wf_name, max(a.wf_key) as max
        select distinct on (a.wf_name) a.wf_name, a.wf_key as max
        from vw_exchange_log a
        where a.wf_data->>'type' = 'OUT' and a.completed
        -- group by 1, 2
        order by a.wf_name, a.id desc, a.wf_key desc
    ;

    drop table if exists retry;
    create temp table retry
    on commit drop as
        -- Список ключей для повторной выгрузки
        SELECT distinct a.wf_name, a.wf_key
        FROM vw_exchange_log_keys a
        WHERE a.retry 
            and a.wf_name = 'tb_exchange_log'
    ;

    sql_cnt = $sql$
        select 'count' as type
            , 'tmp_exchange' as wf_name
            , 'wf_key' as wf_key
            , 'text' as wf_key_type
            , count(1) as cnt
--             , count(distinct wf_key) as key_cnt
            , min(wf_key)::text as key_min
            , max(wf_key)::text as key_max
            , 'true' as wf_where_all
        from tmp_exchange
    $sql$;
--     -- count
--     raise info '%', sql_cnt;
--     execute format($sql$
--         insert into tmp_exchange
--         select '_exchange_log', %2$L, row_to_json(a.*)::text
--         from (%1$s) a
--     $sql$, sql_cnt, lpad(_id::text, 10, '0'));

    for rec in (select * from tmp_exchange_config where wf_type >= 0) loop

        raise info '%', rec;

--         sql_cnt = format($sql$
        sql_cnt = sql_cnt || format($sql$ union all
            select 'count' as type
                , '%1$s' as wf_name
                , '%2$s' as wf_key
                , '%3$s' as wf_key_type
                , count(1) as cnt
--                 , count(distinct %2$s::%3$s) as key_cnt
                , min(%2$s::%3$s)::text as key_min
                , max(%2$s::%3$s)::text as key_max
                , $$%7$s$$ as wf_where_all
            from %6$s.%1$s a where %7$s
            $sql$
                , rec.wf_name   -- имя пакета 1
                , rec.wf_key    -- ключ 2
                , rec.wf_key_type   -- тип ключа 3
                , rec.wf_where  -- условия 4
                , rec.wf_key_limit -- лимит 5
                , 's_grnplm_vd_hr_edp_'||rec.wf_sch   -- схема 6
                , rec.wf_where_all -- все условия 7
        );
--         -- count
--         raise info '%', sql_cnt;
--         execute format($sql$
--             insert into tmp_exchange
--             select '_exchange_log', %2$L, row_to_json(a.*)::text
--             from (%1$s) a
--         $sql$, sql_cnt, lpad(_id::text, 10, '0'));

        case rec.wf_type
            when 1 then
                sql=$sql$
                    select distinct '%1$s', %2$s::%3$s::text, row_to_json(a.*)::text
                    from (
                        select a.* from %6$s.%1$s a
                        where %2$s::%3$s in (
                            select distinct %2$s::%3$s from %6$s.%1$s a
                            where ( %2$s::%3$s > coalesce((select b.max::%3$s from log b where b.wf_name = '%1$s' limit 1), '1900-01-01')
                                    %4$s ) and %7$s
                            union select distinct f.wf_key::%3$s from retry as f where f.wf_name = '%1$s'
                            order by 1 limit %5$s
                        ) and %7$s
                    ) a
                $sql$;
            when 2 then
                sql=$sql$
                    select distinct '%1$s', %2$s::%3$s::text, row_to_json(a.*)::text
                    from (
                        select a.* from %6$s.%1$s a
                        join (
                            select distinct %2$s::%3$s from %6$s.%1$s a
                            where ( %2$s::%3$s > coalesce((select b.max::%3$s from log b where b.wf_name = '%1$s' limit 1), '1900-01-01')
                                    %4$s
                                ) and %7$s
                            union select distinct f.wf_key::%3$s from retry as f where f.wf_name = '%1$s'
                            order by 1 limit %5$s
                        ) b on a.%2$s::%3$s = b.%2$s::%3$s
                        where %7$s
                    ) a
                $sql$;
            else continue;
        end case;

        sql = format(sql
            , rec.wf_name   -- имя пакета 1
            , rec.wf_key    -- ключ 2
            , rec.wf_key_type   -- тип ключа 3
            , rec.wf_where  -- условия 4
            , rec.wf_key_limit -- лимит 5
            , 's_grnplm_vd_hr_edp_'||rec.wf_sch   -- схема 6
            , rec.wf_where_all -- все условия 7
        );

        raise info '%', sql;
        execute 'insert into tmp_exchange ' || sql;
        get diagnostics rc = ROW_COUNT;

    end loop;

    -- count
    raise info '%', sql_cnt;
    execute format($sql$
        insert into tmp_exchange
        select '_exchange_log', %2$L, row_to_json(a.*)::text
        from (%1$s) a
    $sql$, sql_cnt, lpad(_id::text, 10, '0'));

    -- config
    insert into tmp_exchange
    select '_exchange_log', lpad(_id::text, 10, '0'), row_to_json(a.*)
    from (
        select 'config' type, * from tmp_exchange_config
    ) a
    ;

--     begin
--         lock table pg_catalog.pg_attribute IN ACCESS SHARE MODE NOWAIT;
--         lock table pg_catalog.pg_type IN ACCESS SHARE MODE NOWAIT;
--
--         -- Список полей пакета (fields)
--         insert into tmp_exchange
--         select '_exchange_log', lpad(_id::text, 10, '0'), row_to_json(a.*)
--         from (
--             select 'fields' type
--                 , a.wf_sch
--                 , a.wf_name
--                 , pa.attnum fld_num, pa.attname fld_name
--             --    , format_type(pa.atttypid, pa.atttypmod) ftype
--                 , tp.typname as fld_type
--                 , pa.atttypmod as fld_mod
--             --    , pa.attstorage, pa.attalign, pa.attbyval
--                 , col_description(format('%I.%I', 's_grnplm_vd_hr_edp_'||a.wf_sch, a.wf_name)::regclass::oid, pa.attnum) description
--             from tmp_exchange_config a
--             join pg_catalog.pg_attribute pa on pa.attrelid = format('%I.%I', 's_grnplm_vd_hr_edp_'||a.wf_sch, a.wf_name)::regclass::oid
--             join pg_catalog.pg_type tp on pa.atttypid = tp.typname::regtype::oid
--             where pa.attnum > 0 and tp.typrelid=0 and tp.typnamespace=11 and tp.typarray>0
--             order by 1, 2, pa.attnum
--         ) a
--         ;
--     exception when others then raise notice '%', sqlerrm; end;

   
    with meta as (
        -- Агрегация метаданных по пакету: количество, размеры, время
        select
            a.wf_name,
            a.wf_key,
            count(*) as cnt,
            sum(length(a.wf_data::text)) as sum_len,
            min(length(a.wf_data::text)) as min_len,
            max(length(a.wf_data::text)) as max_len,
            (clock_timestamp() - now())::interval(0) as time  -- Время формирования
        from tmp_exchange a
        group by a.wf_name, a.wf_key
    )
    insert into tmp_exchange
    select
        '_exchange_log' as wf_name
--         , now()::timestamp::text as wf_key
        , lpad(_id::text, 10, '0') as wf_key
        , row_to_json(c.*) as wf_data
    from (
        -- Метаданные по каждому wf_name/wf_key (OUT)
        select 'OUT' as "type", a.*  from meta a
        union all
        -- Общая статистика по всему пакету (SUM)
        select
            'SUM' as "type"
            , '_exchange_log'
--             , now()::timestamp::text
            , lpad(_id::text, 10, '0') as wf_key
            , count(1) as cnt
            , sum(sum_len) as sum_len
            , min(min_len) as min_len
            , max(max_len) as max_len
            , (clock_timestamp() - now())::interval(0) as time
        from meta b
    ) c
    ;

    -- Запись лога, если передан идентификатор
    if _id is not null then

        -- Запись в лог обмена
        insert into tb_exchange_log (id, ts, wf_name, wf_key, wf_data)
        select
            _id as id,
--             clock_timestamp() as ts,
            now() as ts,
            a.wf_name,
            a.wf_key,
            a.wf_data
        from (
            -- Формирование записи для лога: только OUT-записи из _exchange_log
            select
                a.wf_data::json->>'wf_name' as wf_name
                , a.wf_data::json->>'wf_key' as wf_key
                , a.wf_data::json
--                , case a.wf_data->>'wf_name'
--                    when '_exchange_log' then right(repeat('0', 10) || _id, 10)  -- Формат ID: 10-значное число
--                    else a.wf_data->>'wf_key' end as wf_key
--                , json_build_object(
--                    'type', 'OUT',
--                    'cnt', (a.wf_data->>'cnt')::int8,
--                    'sum_len', (a.wf_data->>'sum_len')::int8,
--                    'min_len', (a.wf_data->>'min_len')::int8,
--                    'max_len', (a.wf_data->>'max_len')::int8,
--                    'time', (a.wf_data->>'time')::interval
--                ) as wf_data
            from  tmp_exchange a
            where (a.wf_data::json->>'type')::text in ('OUT','SUM')  -- Только meta по исходящим пакетам
            order by 1, 2
        ) a
        order by a.wf_name, a.wf_key;
    end if;

    -- Возврат содержимого пакета
    return query
    select _id::text as wf_id, a.wf_name, a.wf_key, a.wf_data::text
    from tmp_exchange a;
end;

$body$;
	