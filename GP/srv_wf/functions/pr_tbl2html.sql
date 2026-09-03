CREATE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_tbl2html(tbl text, subj text DEFAULT ''::text, over text DEFAULT ''::text, styles json DEFAULT NULL::json) 
	RETURNS text
	LANGUAGE plpgsql
	VOLATILE
as $body$

declare 
    html text = '';
    style text;
    sql text;
    tb text;
    th text;
    tr text;
    td text;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf;

    drop table if exists tmp_html;
    sql = format($sql$
        with tbl as (
            select row_number() over(%2$s) rn, row_to_json(a.*) row from %1$s as a
        ), tbody as (
            select *, (
                select string_agg(a.v, ' ') v from (
                    select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                    from ( select null::text k, null::text v union %6$s ) a
                    where a.k is not null and a.v is not null
                    group by a.k
                ) a 
            ) style
            from (
                select a.rn, b.fn, b.key, b.value#>>'{}' value, json_typeof(b.value) type, a.row_style, a.row
                from (
                    select *, (
                        select string_agg(a.v, ' ') v from (
                            select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                            from ( select null::text k, null::text v union %5$s ) a
                            where a.k is not null and a.v is not null
                            group by a.k
                        ) a 
                    ) row_style
                    from tbl a
                ) a, json_each(row) with ordinality b (key, value, fn)
            ) a
        )
        -- select concat('<table id="table" ', (
        select concat('<table ', (
            select string_agg(a.v, ' ') v from (
                select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                from ( select null::text k, null::text v union %3$s ) a
                where a.k is not null and a.v is not null
                group by a.k
            ) a 
        ) , '>') || string_agg(html, '' order by rn) || '</table>' html
        from (
            select 0 as rn, 
                -- ('<thead id="thead"><tr id="tr">' || string_agg(concat('<th id="th" ', (
                ('<thead id="thead"><tr id="tr">' || string_agg(concat('<th ', (
                    select string_agg(a.v, ' ') v from (
                        select concat(a.k, '="', string_agg(a.v, '; '), '"') v 
                        from ( select null::text k, null::text v union %4$s ) a
                        where a.k is not null and a.v is not null
                        group by a.k
                    ) a 
                ), '>', key, '</th>'), '' order by fn) || '</tr></thead>') html
            from tbody where rn = 1
            union all
            select 1 as rn, 
                ('<tbody id="tbody">' || string_agg(concat('<tr id="tr">', html, '</tr>'), '' order by rn) || '</tbody>')
                -- ('' || string_agg(concat('<tr id="tr">', html, '</tr>'), '' order by rn) || '')
            from ( 
                -- select a.rn, string_agg(concat('<td id="td" ', coalesce(a.style, ' ', row_style), '>', format(%7$L, a.value), '</td>'), '' order by fn) html 
                select a.rn, string_agg(concat('<td ', coalesce(a.style, ' ', row_style), '>', format(%7$L, a.value), '</td>'), '' order by fn) html 
                from tbody a group by rn 
            ) a
        ) a
        order by 1
    $sql$
        , tbl   -- %1
        , over  -- %2
        , coalesce(styles->>'table', $$ select 'id', 'table' $$)  -- %3
        , coalesce(styles->>'th', $$ select 'id', 'th' $$)  -- %4
        , coalesce(styles->>'tr', $$ select 'id', 'tr' $$)  -- %5
        , coalesce(styles->>'td', $$ select 'id', 'td' $$)  -- %6
        , trim(coalesce(styles->>subj,  $$ %s $$))          -- %7
    );
    
    sql = replace(sql, '%row%',   'row');
    sql = replace(sql, '%key%',   'key');
    sql = replace(sql, '%value%', 'value');
    sql = replace(sql, '%type%',  'type');
    sql = replace(sql, '%subj%',  quote_literal(subj));


    execute sql into html;
    
    style = pr_tbl2html_style(styles->>'h');
    -- html = concat('<h3 ', style,'>', subj, '</h3>', html);
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
	