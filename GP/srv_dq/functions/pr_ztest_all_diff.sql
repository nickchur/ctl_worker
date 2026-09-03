CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(_object text, _key_date date, _rows_count bigint, _log_id integer DEFAULT NULL::integer, _ts timestamp without time zone DEFAULT now()) 
	RETURNS json
	LANGUAGE plpgsql
	VOLATILE
as $body$

begin
    return s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(_object, array[_key_date], _rows_count, _log_id, _ts);
end; 

$body$
EXECUTE ON ANY;
	
CREATE FUNCTION s_grnplm_vd_hr_edp_srv_dq.pr_ztest_all_diff(_object text, _key_dates date[], _rows_count bigint, _log_id integer DEFAULT NULL::integer, _ts timestamp without time zone DEFAULT now()) 
	RETURNS json
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare
    app text;
    exe text;
    
    _lst json;        -- Последняя запись из истории
    _nxt json;        -- Новая запись (после вставки)
    _cfg json;        -- Конфигурация Z-теста
    _jsn json;        -- Промежуточные агрегаты (avg, std)
    
    _zsc numeric = 0; -- Z-оценка (число сигм отклонения)
    
    _dif int4;        -- Разница в днях между текущей и предыдущей датой
    _val numeric;     -- Прирост строк в день
    _avg numeric;     -- Средний прирост
    _std numeric;     -- Стандартное отклонение
    _stb numeric;     -- Коэффициент стабильности (std/avg)
    _cnt int8;        -- Количество исторических точек
    _min int8;        -- Мин. значение прироста
    _max int8;        -- Макс. значение прироста
    
    _bck bool;        -- Флаг "выполнять откат при ошибке"
    _z_except date[]; -- Даты, исключённые из контроля
    _z_error date[];  -- Даты с известными ошибками
    
    _ret json;        -- Результат функции
    _tst bool;        -- Результат теста (true = норма, false = аномалия)
    _msg text;        -- Сообщение о результате
    _key_date date;   -- Ключевая дата для анализа
    _row_min int8 = 0;
    _row_max int8 = 9223372036854775807; -- Диапазон строк для анализа
    _val_min int8;
    _val_max int8;
begin
    -- Автодополнение схемы
    if split_part(_object, '.', 1) in ('stg','dia','vd','vda','dm','fcts','srv_dq','srv_wf','udlprod','udlapprove','dac','dds','md','res') then
        _object = concat('s_grnplm_vd_hr_edp_', _object);
    end if;
    
    -- Загрузка конфигурации Z-теста
    _cfg = (select row_to_json(a.*) from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config a where object = _object);
    _bck = (_cfg->>'rollback')::bool;
    
    -- Проверка активности теста
    if not (_cfg->>'active')::bool then
        _ret = json_build_object('ztest', Null, 'back', _bck, 'msg', 'Active False', 'cfg', _cfg);
        return _ret;
    end if;
    
    -- Загрузка исключений и ошибок из конфига
    _z_except = coalesce(_z_except, translate(_cfg->>'z_except', '[]', '{}')::date[], '{}'::date[]);
    _z_error = coalesce(_z_error, translate(_cfg->>'z_error', '[]', '{}')::date[], '{}'::date[]);

    -- Извлечение параметров теста
    _cfg = _cfg->'z_cfg';

    -- Определение ключевой даты (обычно data_max)
    if (_cfg->>'key')::int is null or array_length(_key_dates, 1) < (_cfg->>'key')::int then
        foreach _key_date in array _key_dates loop
            exit when _key_date is not null and _key_date <= current_date;
        end loop;
    else
        _key_date = _key_dates[(_cfg->>'key')::int];
    end if;
    
    _key_date = least(_key_date, current_date);

    -- Учёт исторических данных (например, только первые N строк)
    if (_cfg->>'hist')::int8 is not null then
        if _rows_count <= (_cfg->>'hist')::int8 then
            _row_max = (_cfg->>'hist')::int8;
        else
            _row_min = (_cfg->>'hist')::int8 + 1;
        end if;
    end if;

    _val_min = coalesce((_cfg->>'min')::int8, -9223372036854775807);
    _val_max = coalesce((_cfg->>'max')::int8, +9223372036854775807);

    -- Получение последней записи из истории
    select row_to_json(a.*)
    into _lst
    from (
        select distinct on (object) zscore, ts, key_date, rows_count, key_diff, value, notes , avg, std, cnt, min, max
        from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data a
        where object = _object 
            and key_date <= _key_date - coalesce((_cfg->>'interval')::interval, '1 day'::interval)
            and (not (coalesce((_cfg->>'skip')::bool, true) and value = 0) or value is null)
            and not (key_date = any(_z_error))
            and key_date >= coalesce((_cfg->>'beg')::date, '1900-01-01')
            and rows_count between _row_min and _row_max
            and (value between _val_min and _val_max or value is null)
        order by object, key_date desc, (notes->>'ztest') desc, ts desc, log_id desc
    ) a;
              
    if _lst is not null then
        _dif = (_key_date - (_lst->>'key_date')::date)::int4;
        _val = (_rows_count - (_lst->>'rows_count')::int8) / _dif;

        -- Расчёт среднего и стандартного отклонения
        select row_to_json(a.*)
        into _jsn
        from (
            select avg(value), stddev_pop(value) std, count(value) cnt, min(value), max(value)
            from (
                select row_number() over(order by key_date desc) n, value 
                from (
                    select distinct on (a.object, a.key_date) a.value, a.key_date, a.notes
                    from s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data a
                    left join s_grnplm_vd_hr_edp_srv_dq.tb_ztest_config c on c.active and c.object = a.object
                    where a.object = _object and key_date < _key_date::date
                        and not (coalesce((_cfg->>'skip')::bool, true) and value = 0)
                        and (coalesce((a.notes->>'ztest')::bool, True) or key_date = any(_z_except))
                        and not (key_date = any(_z_error))
                        and key_date >= coalesce((_cfg->>'beg')::date, '1900-01-01')
                        and rows_count between _row_min and _row_max
                        and value between _val_min and _val_max 
                    order by a.object, a.key_date desc, (a.notes->>'ztest') desc, a.ts desc, a.log_id desc
                ) a
                union all select 0, _val::int8
            ) a
            where n <= coalesce((_cfg->>'dots')::int4, 90)
        ) a
        limit 1;

        _avg = (_jsn->>'avg')::numeric;
        _std = (_jsn->>'std')::numeric;
        _cnt = (_jsn->>'cnt')::int8;
        _min = (_jsn->>'min')::int8;
        _max = (_jsn->>'max')::int8;
        _zsc = (_val - _avg) / coalesce(nullif(_std, 0), 1);
        _stb = _std / nullif(_avg, 0);
    end if;

    -- Логика принятия решения
    if _key_date = any(_z_except) then 
        _msg = 'Ok z_except';
        _tst = true;
    elsif _key_date = any(_z_error) then 
        _msg = 'No z_error';
        _tst = false;
    elsif _key_date > (_cfg->>'end')::date then
        _msg = format('Key_date (%s) > end (%s)', _key_date, (_cfg->>'end') );
        _tst = null;
    elsif _key_date < (_cfg->>'beg')::date then
        _msg = format('Key_date (%s) < beg (%s)', _key_date, (_cfg->>'beg') );
        _tst = null;
    elsif _lst is null then
        _msg = 'Ok first';
        _tst = true;
    elsif _val < (_cfg->>'min')::int8 then
        _msg = format('No < min (%s)', (_cfg->>'min') );
        _tst = false;
    elsif _val > (_cfg->>'max')::int8 then
        _msg = format('No > max (%s)', (_cfg->>'max') );
        _tst = false;
    elsif abs(_stb) < 0.01 and _zsc between coalesce((_cfg->>'zfr')::numeric, -10.0) and coalesce((_cfg->>'zto')::numeric, +10.0) then
        _msg = 'Ok';
        _tst = true;
    elsif _zsc between coalesce((_cfg->>'zfr')::numeric, -5.0) and coalesce((_cfg->>'zto')::numeric, +5.0) then
        _msg = 'Ok';
        _tst = true;
    else 
        _msg = 'No';
        _tst = false;
    end if;

    -- Формирование итогового сообщения
    _msg = concat(_msg, format(' %s %s (%s) value %s * %s rows %s %s', _key_date, round(_zsc, 2), round(_stb, 2)
                               , to_char(_val::int8, 'SGFM999,999,999,999,999,999'), _dif, to_char(_rows_count, 'FM999,999,999,999,999,999')
                               , case when (_cfg->>'hist')::int8 is not null then 'hist' else '' end ));  

    _ret = json_build_object('ztest', _tst, 'back', _bck, 'msg', _msg, 'cfg', _cfg);
    
    -- Восстановление старых значений avg/std при ошибке
    if not _tst and _lst is not null then
        _avg = (_lst->>'avg')::numeric;
        _std = (_lst->>'std')::numeric;
        _cnt = (_lst->>'cnt')::int8;
        _min = (_lst->>'min')::int8;
        _max = (_lst->>'max')::int8;
    end if;

    -- Запись результата в таблицу
    with add as (
        insert into s_grnplm_vd_hr_edp_srv_dq.tb_ztest_data
        (zscore, ts, object, key_date, rows_count, key_diff, value, avg, std, cnt, min, max, log_id, notes)
        values 
            ( _zsc::float
            , _ts::timestamp
            , _object::text
            , _key_date::date
            , _rows_count::int8
            , _dif::int4 
            , _val::int8 
            , _avg::int8
            , _std::int8
            , _cnt::int8
            , _min::int8
            , _max::int8
            , abs(_log_id)::int4
            , _ret::json
        ) returning *
    )
    select row_to_json(add.*) into _nxt from add limit 1;

    -- Вызов отката при ошибке (если включено в конфиге)
    if not (_ret->>'ztest')::bool and (_ret->>'back')::bool and _log_id >= 0 then
        raise exception using ERRCODE = 'XX001', MESSAGE = format('Ztest error %s', _ret::text), DETAIL = _nxt::text, HINT = '';
    end if;

    return _ret;

exception when OTHERS then
    declare 
        e_detail text;
        e_hint text;
        e_context text;
    begin
        get stacked diagnostics e_detail = PG_EXCEPTION_DETAIL, e_hint = PG_EXCEPTION_HINT, e_context = PG_EXCEPTION_CONTEXT;

        -- Переброс исключения с логированием
        if sqlstate = 'XX001' then
            raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = e_detail, HINT = e_hint;
        end if;

        if split_part(e_context, 'statement', 1) like '% at EXECUTE %' then
            e_detail = exe;
            e_context = replace(e_context, exe, '...');
        end if;

        perform s_grnplm_vd_hr_edp_srv_wf.pr_log_error(0, sqlerrm, e_detail, e_hint, e_context) ;

        return json_build_object('error', sqlerrm, 'detail', e_detail, 'hint', e_hint, 'context', e_context, 'code', sqlstate)::json;
    end;
end; 

$body$
EXECUTE ON ANY;
	