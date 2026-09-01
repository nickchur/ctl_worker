-- Состояние эмулятора CTL и заглушки процедур Greenplum
-- 2026-09-01 12:20 MSK · v1.0 · Nick Churkin · NSChurkin@sber.ru
--
-- Разворачивается в стендовый postgres, база gp_test — ту же базу видит и conn 'gp'
-- из ctl_config: заглушки pr_swf_start_ctl и pr_log_ctl обязаны лежать там, куда ходит
-- gp_exe, иначе run_exe упадёт на первом же шаге.
--
--   docker exec -i aftest-postgres psql -U airflow -d gp_test < schema.sql
--
--
-- 📦 ЧТО ЗДЕСЬ
--
-- ctl_mock.*  — состояние эмулятора: загрузки, их статусы, параметры, statval'ы,
--               состояние воркфлоу (расписание, шаблон, параметры) и журнал запросов.
--               Справочники (воркфлоу, категории, сущности, профиль) состоянием НЕ
--               являются: они лежат фикстурами в JSON и только читаются.
--
-- s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_ctl / pr_log_ctl — заглушки боевых функций
--               (оригиналы в HR_Data/sql/create/.../functions/). Контракт сохранён,
--               начинка урезана до того, что нужно для прогона тракта.


CREATE SCHEMA IF NOT EXISTS ctl_mock;


-- 🔑 Загрузка. Горячие поля вынесены колонками (по ним фильтрует /loading/extended),
-- всё остальное лежит в raw как есть: снимок с боя несёт полтора десятка полей, которые
-- наш код не читает, но которые должны доехать до ответа неизменными.
CREATE TABLE IF NOT EXISTS ctl_mock.loading (
    id         bigint PRIMARY KEY,
    wf_id      bigint      NOT NULL,
    profile    text        NOT NULL,
    alive      text        NOT NULL DEFAULT 'ACTIVE',   -- ACTIVE | COMPLETED | ABORTED
    status     text        NOT NULL DEFAULT 'INIT',     -- INIT | RUNNING | SUCCESS | ERROR | …
    status_log text        NOT NULL DEFAULT '',         -- шаг и данные: 'RUN {}', 'END …'
    auto       boolean     NOT NULL DEFAULT true,
    start_dttm timestamp   NOT NULL DEFAULT clock_timestamp(),
    end_dttm   timestamp,
    raw        jsonb       NOT NULL DEFAULT '{}'::jsonb
);

-- Загрузки, созданные эмулятором, нумеруются с 90 000 000: снимок с боя занимает
-- диапазон 26–27 млн, и пересечься с ним нельзя — id уедет в имена dag_run.
CREATE SEQUENCE IF NOT EXISTS ctl_mock.loading_id_seq START 90000000;

-- История статусов. Именно её отдаёт поле loading_status и по ней же наш код считает
-- ld_status_last, поэтому effective_from обязан быть уникальным в пределах загрузки:
-- при равных отметках нормализация схлопнула бы записи в одну.
CREATE TABLE IF NOT EXISTS ctl_mock.loading_status (
    id             bigserial PRIMARY KEY,
    loading_id     bigint    NOT NULL REFERENCES ctl_mock.loading(id) ON DELETE CASCADE,
    status         text      NOT NULL,
    log            text      NOT NULL DEFAULT '',
    effective_from timestamp NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (loading_id, effective_from)
);

CREATE TABLE IF NOT EXISTS ctl_mock.loading_param (
    loading_id bigint NOT NULL REFERENCES ctl_mock.loading(id) ON DELETE CASCADE,
    param      text   NOT NULL,
    value      text,
    PRIMARY KEY (loading_id, param)
);

-- Значения статистик. На них держится EVENT-WAIT: ctl_chk_wait спрашивает последнее
-- значение по (profile, entity_id, stat_id) и сравнивает published_dttm с датой запуска.
CREATE TABLE IF NOT EXISTS ctl_mock.statval (
    id             bigserial PRIMARY KEY,
    profile        text      NOT NULL,
    entity_id      bigint    NOT NULL,
    stat_id        bigint    NOT NULL,
    loading_id     bigint,
    value          text,
    published_dttm timestamp NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS statval_last_idx
    ON ctl_mock.statval (profile, entity_id, stat_id, published_dttm DESC);

-- Изменяемая часть воркфлоу. Сам воркфлоу — фикстура, но расписание, шаблон и параметры
-- воркер правит на ходу (PUT/DELETE /wf/{id}/scheduled, /tmpl, /params).
CREATE TABLE IF NOT EXISTS ctl_mock.wf_state (
    wf_id     bigint PRIMARY KEY,
    scheduled boolean NOT NULL DEFAULT false,
    tmpl_id   integer,
    params    jsonb   NOT NULL DEFAULT '{}'::jsonb
);

-- Журнал запросов: единственный способ увидеть, что именно спросил Airflow и что
-- ответил эмулятор. Полезнее логов uvicorn — тело ответа тоже здесь.
CREATE TABLE IF NOT EXISTS ctl_mock.api_log (
    id     bigserial PRIMARY KEY,
    ts     timestamp NOT NULL DEFAULT clock_timestamp(),
    method text      NOT NULL,
    path   text      NOT NULL,
    query  text,
    status integer,
    body   jsonb
);
CREATE INDEX IF NOT EXISTS api_log_ts_idx ON ctl_mock.api_log (ts DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- Заглушки процедур Greenplum
-- ─────────────────────────────────────────────────────────────────────────────

-- Журнал GET-запросов к CTL: ctl_api после каждого GET зовёт эту функцию (кроме tmpl
-- и statval). Боевая раскладывает ответ по витринам, стендовой достаточно записать факт.
CREATE TABLE IF NOT EXISTS s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl (
    id   bigserial PRIMARY KEY,
    ts   timestamp NOT NULL DEFAULT clock_timestamp(),
    url  text,
    msg  text
);

CREATE OR REPLACE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_log_ctl(
    _url text, _msg text, _data text DEFAULT NULL::text
) RETURNS text
LANGUAGE plpgsql VOLATILE
AS $body$
begin
    -- Сигнатура совпадает с боевой (HR_Data/sql/create/.../pr_log_ctl.sql): три
    -- аргумента, третий по умолчанию. Боевая на 'tmpl' отвечает 'No' — повторяем,
    -- чтобы поведение вызывающего кода не отличалось.
    if substring(split_part(_url, '?', 1) from '^/([a-z]+)/?') = 'tmpl' then
        return 'No';
    end if;

    insert into s_grnplm_vd_hr_edp_srv_wf.tb_log_ctl(url, msg)
    values (split_part(_url, '?', 1), left(coalesce(_msg, ''), 4000));

    return 'Ok';
end;
$body$;


-- Единая точка входа воркера в Greenplum. Боевая версия собирает и выполняет код
-- воркфлоу, разбирает текстовый ответ и возвращает JSON с кодом результата; заглушка
-- делает ровно это — но выполняет ровно то, что пришло в exe, без сборки кода из
-- витрин. Коды результата те же, поэтому ветки ERROR/нет данных/-7 на стенде честные:
--
--   res  1  ответ начинается с 'ok ' либо пуст
--   res  0  ответ начинается с 'no '
--   res -2  query_canceled (таймаут оператора)
--   res -7  необработанное исключение
--   res -9  прочее (ответ не опознан)
--
-- Подстановки $lid$, $try$, $left$, $sdt$ те же, что в бою: без них тестовые выражения
-- вида "select 'ok ' || $lid$" вели бы себя иначе, чем боевые процедуры.
CREATE OR REPLACE FUNCTION s_grnplm_vd_hr_edp_srv_wf.pr_swf_start_ctl(wf_jsn json)
RETURNS json
LANGUAGE plpgsql VOLATILE
AS $body$
declare
    _wf   text = wf_jsn->>'wf';
    _exe  text = coalesce(wf_jsn->>'exe', 'pr_' || coalesce(wf_jsn->>'wf', 'unknown') || '()');
    _lid  text = coalesce(wf_jsn->>'lid', '0');
    _sdt  text = coalesce(wf_jsn->>'sdt', '');
    _rtr  json = wf_jsn->'rtr';
    _try  text = coalesce(_rtr->>'try', '0');
    _left text = coalesce(_rtr->>'left', '0');
    _msg  text;
    _res  int;
begin
    set search_path to s_grnplm_vd_hr_edp_srv_wf, public;

    _exe = replace(_exe, '$lid$',  _lid);
    _exe = replace(_exe, '$try$',  _try);
    _exe = replace(_exe, '$left$', _left);
    _exe = replace(_exe, '$sdt$',  quote_literal(_sdt));

    begin
        execute 'select ' || _exe into _msg;
    exception
        when query_canceled then
            return json_build_object('res', -2, 'swf', 'ctl', 'wf', _wf, 'exe', _exe,
                                     'msg', 'query_canceled');
        when others then
            return json_build_object('res', -7, 'swf', 'ctl', 'wf', _wf, 'exe', _exe,
                                     'msg', SQLERRM);
    end;

    _msg = coalesce(_msg, '');
    _res = case
        when _msg = '' or lower(_msg) like 'ok %' or lower(_msg) = 'ok' then 1
        when lower(_msg) like 'no %'                                    then 0
        else -9
    end;

    return json_build_object('res', _res, 'swf', 'ctl', 'wf', _wf, 'exe', _exe,
                             'msg', _msg, 'stat', json_build_object(), 'cdc', json_build_object());
end;
$body$;
