CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_chk_cnt_delta(srs text, keys text, dcol text, pct numeric, raise_exc boolean, fdate text DEFAULT NULL::text, lcol text DEFAULT NULL::text, lmt integer DEFAULT 10)
	RETURNS json
	LANGUAGE plpgsql
	SECURITY DEFINER
	VOLATILE
as $body$

-- pr_chk_cnt_delta(srs, keys, dcol, pct, raise_exc, fdate, lcol, lmt) -> json
-- 2026-08-04 17:53 MSK, v1.0, Чуркин Николай
--
-- ККД: на каждую дату среза (dcol) кол-во значений ключа (keys) не должно отличаться
-- от предыдущей даты среза более чем на pct процентов в любую сторону.
-- Переиспользуемая блокирующая проверка по образцу pr_chk_uniq.
--   srs           - таблица: 'stg.tb_...' или полное имя со схемой
--   keys          - поле, кол-во значений которого сравниваем: count(distinct keys)
--                   если null - сравнивается кол-во строк, count(1)
--   dcol          - поле даты среза (например calc_date)
--   pct           - допустимое отклонение в процентах, по умолчанию 10
--   raise_exc     - true -> raise XX001 -> откат загрузки в вызывающей процедуре
--   fdate         - проверять только срезы >= этой даты (null - все)
--   lcol          - поле даты загрузки: если задано, берутся только строки текущей
--                   загрузки (lcol = now()). Вызывать только внутри транзакции загрузки,
--                   иначе выборка будет пустой - см. поле dates в ответе
--   lmt           - сколько нарушений вернуть в DETAIL
--
-- Возврат при успехе: {"msg": "Cnt delta Ok", "dates": <кол-во проверенных срезов>}.
-- При нарушении и raise_exc = false: {"err", "msg", "dates", "chk"}, где chk - массив
-- нарушений (cd, prev_cd, cnt, prev_cnt, delta_pct, limit_pct).
-- При нарушении и raise_exc = true: raise XX001, MESSAGE = 'Ошибка качества данных.
-- Проверка не пройдена', DETAIL = chk. Исключение откатывает подтранзакцию вызывающего
-- загрузчика, а pr_swf_start_ctl отдаёт CTL код результата -5.
--
-- Поле dates важно контролировать: при заданном lcol вне транзакции загрузки выборка
-- пуста, dates = 0 и проверка формально проходит, ничего не проверив.
--
-- История изменений:
--   v1.0  2026-08-04  E360-6371: первая версия

declare
    exe text;
    agg text;
    flt text = '';
    err int8;
    cdt int8;
    chk text = '';
    msg text = '';
begin
    if split_part(srs, '.', 1) in ('stg','dia','vd','vda','dm','fcts','srv_dq','srv_wf','udlprod','udlapprove','dac','dds','md','res') then
        srs = concat('s_grnplm_vd_hr_edp_', srs);
    end if;

    pct = coalesce(pct, 10);
    agg = case when keys is null then 'count(1)' else format('count(distinct %s)', keys) end;

    if fdate is not null then
        flt = format('%s and %s >= %L::date', flt, dcol, fdate);
    end if;

    if lcol is not null then
        flt = format('%s and %s = now()', flt, lcol);
    end if;

    exe = format($exe$
        with c as (
            select %3$s::date cd, %2$s::int8 cnt
            from %1$s a
            where %3$s is not null %4$s
            group by 1
        ), d as (
            select cd, cnt
                 , lag(cd)  over (order by cd) prev_cd
                 , lag(cnt) over (order by cd) prev_cnt
            from c
        ), e as (
            select cd, prev_cd, cnt, prev_cnt
                 , round((cnt - prev_cnt)::numeric * 100 / nullif(prev_cnt, 0), 2) as delta_pct
                 , %5$s::numeric as limit_pct
            from d
            where prev_cnt is not null
              and (prev_cnt = 0 and cnt <> 0
                   or prev_cnt <> 0 and abs(cnt - prev_cnt)::numeric * 100 / prev_cnt > %5$s::numeric)
            order by cd
            limit %6$s
        )
        select (select count(1) from e)
             , (select json_agg(row_to_json(e.*)::json order by e.cd) from e)
             , (select count(1) from c)
    $exe$, srs, agg, dcol, flt, pct, lmt);
    execute exe into err, chk, cdt;

    if coalesce(err, 0) > 0 then
        msg = 'Ошибка качества данных. Проверка не пройдена';

        if raise_exc then
            raise exception using ERRCODE = 'XX001', MESSAGE = msg, DETAIL = coalesce(chk, '');
        else
            return json_build_object('err', err, 'msg', msg, 'dates', cdt, 'chk', chk);
        end if;
    else
        return json_build_object('msg', 'Cnt delta Ok', 'dates', cdt);
    end if;

exception when OTHERS then
    if sqlstate = 'XX001' then
        raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = coalesce(chk, '');
    end if;
    raise exception using ERRCODE = sqlstate, MESSAGE = sqlerrm, DETAIL = exe;
end;

$body$
EXECUTE ON ANY;
	