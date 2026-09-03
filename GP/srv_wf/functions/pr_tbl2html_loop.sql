CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_tbl2html_loop(tbl text, tbl2 text DEFAULT NULL::text, subj text DEFAULT ''::text, styles json DEFAULT NULL::json) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    html text = '';
    html1 text;
    html2 text;
    sql text;
    rec record;
    
    style text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;
    styles = pr_mail_style(styles, true);
    
    sql = format($sql$
        with tbody as (
            select *, (
                select string_agg(a.v, ' ') v from (
                    select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                    from ( 
                        select null::text k, null::text v union %3$s 
                    ) a
                    where a.k is not null and a.v is not null
                    group by a.k
                ) a 
            ) style
            from (
                select a.n, a.rel_key, b.fn, b.key, b.value#>>'{}' value, json_typeof(b.value) type, a.row_style, a.row
                from (
                    select *, (
                        select string_agg(a.v, ' ') v from (
                            select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                            from ( 
                                select null::text k, null::text v union %2$s 
                            ) a
                            where a.k is not null and a.v is not null
                            group by a.k
                        ) a 
                    ) row_style
                    from %1$s as a
                ) a, json_each(row) with ordinality b (key, value, fn)
            ) a
        )
        select * from tbody
        order by 1, 2
    $sql$
        , tbl   -- %1
        , coalesce(styles->>'tr', $$ select 'id', 'tr' $$)  -- %2
        , coalesce(styles->>'td', $$ select 'id', 'td' $$)  -- %3
    );
    sql = replace(sql, '%row%',   'row');
    sql = replace(sql, '%key%',   'key');
    sql = replace(sql, '%value%', 'value');
    sql = replace(sql, '%type%',  'type');
    sql = replace(sql, '%subj%',  quote_literal(subj));

    drop table if exists tbody;
    execute format($sql$
        create temp table tbody
        WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
        on commit drop 
        as %s
    $sql$, sql);
    
    if tbl2 is not null then
        sql = format($sql$
            with tbody as (
                select *, (
                    select string_agg(a.v, ' ') v from (
                        select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                        from ( 
                            select null::text k, null::text v union %3$s 
                        ) a
                        where a.k is not null and a.v is not null
                        group by a.k
                    ) a 
                ) style
                from (
                    select a.n, a.nn, b.fn, b.key, b.value#>>'{}' value, json_typeof(b.value) type, a.row_style, a.row
                    from (
                        select *, (
                            select string_agg(a.v, ' ') v from (
                                select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                                from ( 
                                    select null::text k, null::text v union %2$s 
                                ) a
                                where a.k is not null and a.v is not null
                                group by a.k
                            ) a 
                        ) row_style
                        from %1$s as a
                    ) a, json_each(row) with ordinality b (key, value, fn)
                ) a
            )
            select * from tbody
            order by 1,2
        $sql$
            , tbl2   -- %1
            , coalesce(styles->>'tr', $$ select 'id', 'tr' $$)  -- %2
            , coalesce(styles->>'td', $$ select 'id', 'td' $$)  -- %3
        );
        sql = replace(sql, '%row%',   'row');
        sql = replace(sql, '%key%',   'key');
        sql = replace(sql, '%value%', 'value');
        sql = replace(sql, '%type%',  'type');
        sql = replace(sql, '%subj%',  quote_literal(subj));
        
        drop table if exists tbody2;
        execute format($sql$
            create temp table tbody2
            WITH (appendonly=true, orientation=column, compresstype=zstd, compresslevel=3)
            on commit drop 
            as %1$s
        $sql$, sql);
    end if;

    style = coalesce(pr_tbl2html_style(styles->>'table'), 'id="table"');
    html = concat(html, '<table ', style, '>');

    sql = format($sql$
        select  
            ('<tr id="tr">' || string_agg(concat('<th ', (
                select string_agg(a.v, ' ') v from (
                    select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                    from ( 
                        select null::text k, null::text v union %s 
                    ) a
                    where a.k is not null and a.v is not null
                    group by a.k
                ) a 
            ), '>', key, '</th>'), '' order by fn) || '</tr>') html
        from tbody 
        where n=(select min(n) from tbody)
        limit 1
    $sql$
        , coalesce(styles->>'th', $$ select 'id', 'th' $$)
    );
    sql = replace(sql, '%row%',   'row');
    sql = replace(sql, '%key%',   'key');
    sql = replace(sql, '%value%', 'value');
    sql = replace(sql, '%type%',  'type');
    sql = replace(sql, '%subj%',  quote_literal(subj));
    execute sql into html1;

    if tbl2 is not null then
        sql = format($sql$
            select  
                ('<tr id="tr">' || string_agg(concat('<th ', (
                    select string_agg(a.v, ' ') v from (
                        select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                        from ( 
                            select null::text k, null::text v union %s 
                        ) a
                        where a.k is not null and a.v is not null
                        group by a.k
                    ) a 
                ), '>', key, '</th>'), '' order by fn) || '</tr>') html
            from tbody2 a
            inner join (select n, nn from tbody2 order by 1 limit 1) b on a.n=b.n and a.nn=b.nn
            limit 1
        $sql$
            , coalesce(styles->>'th', $$ select 'id', 'th' $$)
        );
        sql = replace(sql, '%row%',   'row');
        sql = replace(sql, '%key%',   'key');
        sql = replace(sql, '%value%', 'value');
        sql = replace(sql, '%type%',  'type');
        sql = replace(sql, '%subj%',  quote_literal(subj));
        execute sql into html2;
    end if;

    style = coalesce(pr_tbl2html_style(styles->>'thead'), 'id="thead"');
    html = concat(html, '<thead ', style, '>', html1, html2, '</thead>');
    
    html = concat(html, '<tbody ', coalesce(pr_tbl2html_style(styles->>'tbody'), 'id="tbody"'), '>');

    for rec in (
        select a.n, concat('<tr id="tr">', string_agg(concat('<td ', coalesce(a.style, ' ', a.row_style), '>', a.value, '</td>'), '' order by fn), '</tr>') html 
        from tbody a 
        group by a.n
        order by n
    ) loop
        html = concat(html, rec.html);
        
        if tbl2 is not null then
            html = concat(html, (
                select string_agg(concat('<tr id="tr">', a.html, '</tr>'), '' order by a.nn)
                from (
                    select a.nn, string_agg(concat('<td ', coalesce(a.style, ' ', a.row_style), '>', a.value, '</td>'), '' order by fn) html 
                    from tbody2 a 
                    where a.n = rec.n
                    group by 1
                ) a
            ));
        end if;
    end loop;
    
    html = concat(html, '</tbody>');
    html = concat(html, '</table>');

    style = coalesce(pr_tbl2html_style(styles->>'h'), 'id="h"');
    html = concat('<h3 ', style,'>', subj, '</h3>') || html;
    
    return html;

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

        perform s_grnplm_vd_hr_edp_srv_wf.pr_Log_error(0, e_txt, e_detail, sql, e_context) ; 
        return e_txt;
    end;
end;

$body$
EXECUTE ON ANY;
	