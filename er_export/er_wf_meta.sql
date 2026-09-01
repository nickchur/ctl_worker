-- DDL для export.er_wf_meta
-- 2026-09-01 09:14 MSK · v3.4 · Nick Churkin · NSChurkin@sber.ru
-- Управляющая таблица ER-выгрузок. Синхронизируется в Airflow Variable "datalab_er_wfs"
-- DAG-ом export_er_setup, который раскладывает записи по группам поставок.
--
-- ВАЖНО: выполнять через clickhouse-client или HTTP-интерфейс, НЕ через JDBC.
-- JDBC-драйвер интерпретирует {uuid} и {replica} как именованные параметры → ошибка.
-- Пример: clickhouse-client --multiquery < er_wf_meta.sql
--
-- Путь в ZooKeeper построен на макросе {uuid} — он раскрывается в UUID самой таблицы
-- (движок БД Atomic), поэтому уникален без участия {shard} и без имени таблицы в пути.
--
-- ⚠️ При ON CLUSTER UUID генерируется ОДИН на весь кластер и рассылается по нодам, поэтому
-- {uuid} шарды не разводит: если в datalab больше одного шарда, все они сядут на одну
-- ZK-ноду и станут репликами друг друга. Здесь записано боевое значение — таблица уже
-- создана именно так. Если шардов больше одного, это отдельный разговор про топологию
-- репликации, а не про этот файл.
--
-- Чтение без дублей (ReplacingMergeTree гарантирует дедупликацию только после MERGE):
--   SELECT * FROM export.er_wf_meta FINAL WHERE is_active = 1
--
--
-- 🔑 КЛЮЧ — (replica, dag_group, schema_name, extract_name)
--
-- До 28.08.2026 ключом было (db_name, extract_name), а группа кодировалась суффиксом
-- в самой реплике ('hrplatform_datalab__1'). Из этого росли три неудобства:
--   • строка-дефолт группы была обязана нести db_name = replica — иначе дефолты ВСЕХ
--     групп получали один ключ ('', '') и схлопывались в одну строку;
--   • одну и ту же таблицу нельзя было включить в две группы, то есть нельзя было
--     завести новую версию пакета, не убив старую;
--   • группу приходилось всюду отрезать от реплики.
--
-- Теперь группа — отдельная колонка dag_group, а признак строки-дефолта ровно один:
-- пустой extract_name (у неё пуст и schema_name, поэтому дефолт в группе физически один).
-- Ключ записи в форме export_er_setup выглядит так же:
-- 'hrplatform_datalab/1/learning/lc_items_opened'.
--
-- Колонки db_name больше нет: база источника берётся из sql_from, где имя и так пишут
-- квалифицированным ('evolution.lc_items_meta'). Если базы там нет вовсе, описания
-- колонок в .meta просто не найдутся — с предупреждением, но без ошибки.
--
-- ⚠️ Состояние дельты ключуется тройкой (replica, schema_name, extract_name) в своей
-- таблице export.er_extract_history (см. er_extract_history.sql). Группы в ключе нет,
-- поэтому одну поставку НЕЛЬЗЯ заводить в двух группах одной реплики. Раньше две группы с одной
-- таблицей ведут разные окна, а структуру истории — общую с xStream — трогать не пришлось.
--
--
-- 📦 ГРУППЫ ПОСТАВОК
--
-- Один пакет = одна группа = один DAG = один внешний тикет на все поставки группы.
-- Пакет задаётся парой (replica, dag_group), даг называется
-- 'export_er__<replica>__<dag_group>':
--     replica = 'hrplatform_datalab', dag_group = '1'  → export_er__hrplatform_datalab__1
--     dag_group пуста                                  → синхронизация подставит '0'
--     dag_group произвольна, не обязательно цифра      → 'lm' тоже годится
--
-- Группа есть ВСЕГДА: имя архива строится как '{реплика}__{ts}__{группа}__{table}__…',
-- и без неё разделителей '__' на один меньше, чем у остальных пакетов. Строку в таблице
-- под это править не нужно — пустая группа нормализуется при синхронизации.
--
-- Маршрут в TFS (scenario_id и префикс в S3) ищется по РЕПЛИКЕ, то есть в TFS_MAP
-- (er_config.py) нужна запись только на неё; новая группа заводится строкой в таблице,
-- без правки кода. В имя архива уходят обе части: реплика первой, группа за меткой
-- времени, — поэтому архивы разных групп не пересекаются именами. Тикет пакета группы
-- не несёт, его разводит сама метка времени (таск make_ts, пул на реплику с одним слотом).
--
--
-- 🧩 ДВА ТИПА СТРОК
--
--   extract_name и schema_name ПУСТЫ  → строка-дефолт группы (ОБЯЗАТЕЛЬНА)
--   extract_name и schema_name заданы → поставка (одна таблица)
--
-- Строка-дефолт задаёт значения для всей группы и обязана существовать: без неё группа
-- целиком уходит в ошибку и вместо пакета создаётся даг-заглушка. schema_name у неё
-- обязан быть пуст — он входит в ключ, и с ним в группе появился бы второй «дефолт»
-- с другой схемой.
--
-- Что откуда берётся:
--
--   params      — merge по ключам: умолчания er_config.py → группа → таблица;
--   description — своё, иначе комментарий таблицы в CH, иначе групповое (групповой текст
--                 последний, иначе он затрёт осмысленные комментарии всех таблиц пакета);
--   schema_name — НЕ наследуется: входит в ключ и обязателен у каждой поставки;
--   pk, uk, fields, sql_from, sql_join, sql_where, sql_with, sql_settings — НЕ наследуются,
--                 они всегда про конкретную таблицу.
--
--
-- ⏹️ is_active И ⏸️ is_paused
--
--   is_active = 0 у строки-дефолта  → пакета нет вовсе (DAG не создаётся)
--   is_active = 0 у поставки        → её нет в пакете (таск не создаётся)
--   is_paused = 1 у строки-дефолта  → DAG создаётся, но ставится на паузу. Синк дожимает
--                                     паузу и на уже созданном даге; обратно НЕ снимает —
--                                     паузу, поставленную руками в UI, настройка возвращать
--                                     не должна
--   is_paused = 1 у поставки        → таск создаётся, но штатно скипается: флаг в форме
--                                     запуска снят по умолчанию, и его можно включить
--                                     галкой на один ран
--
-- Обязательные поля поставки: replica, schema_name, extract_name, sql_from, fields
-- (пустые → запись ломает свою группу при синхронизации).
--
--
-- 🔤 ИМЕНА: ТОЛЬКО [A-Za-z0-9_-], И БЕЗ '__'
--
-- replica, dag_group, schema_name и extract_name проверяются синхронизацией по маске
-- «буквы, цифры, одиночное подчёркивание, дефис». Причины конкретные:
--
--   • '__' — РАЗДЕЛИТЕЛЬ в именах файлов ЕР:
--       {replica}__{ts}__{dag_group}__{table}__{часть}_{всего}_{строк}.zip
--       {schema}__{table}__{ts}__{часть}_{всего}_{строк}.csv|.json|.meta
--     лишнее '__' внутри любой части сдвигает разбор у принимающей стороны — и файл
--     уедет «не тем», молча;
--   • точка разделяет части в ключе выгрузки внутри пакета (схема.имя) и в имени
--     состояния дельты ('<dag_id>.<extract_name>');
--   • слэш разделяет части ключа записи в форме (replica/dag_group/schema/extract);
--   • кавычка ломает SQL-литерал, пробел — ключ объекта в S3.
--
-- Одиночное подчёркивание разрешено: в хвосте имени файла разделитель как раз одинарный.
--
--
-- ⚙️ ЧТО ПЕРЕЕХАЛО В params
--
-- Колонок schedule и is_recent больше нет, их значения живут ключами в params:
--
--   schedule  — ОБЯЗАТЕЛЬНОЕ, только в params СТРОКИ-ДЕФОЛТА, умолчания у него нет.
--               Колонка позволяла задать расписание ещё и поставке, и тогда «какое
--               победит» зависело от того, у кого поле заполнено. Теперь расписание
--               в params поставки игнорируется с предупреждением. Пусто или не cron →
--               группа уходит в ошибку: пакет, поехавший не в своё окно, хуже непоехавшего.
--   is_recent — 0 = дельта (окно из состояния), 1 = recent (скользящее окно). В колонке
--               UInt8 DEFAULT 0 «не задано» было неотличимо от «явно дельта», поэтому
--               режим не наследовался вовсе; в JSON ключ либо есть, либо нет — и
--               наследование от группы работает как у остальных параметров.
--
-- Полный список ключей params — GROUP_PARAMS и TABLE_PARAMS в er_config.py.


CREATE TABLE IF NOT EXISTS export.er_wf_meta ON CLUSTER datalab
(
    replica       String                    COMMENT 'Реплика: ищется в TFS_MAP (er_config.py). Обязательна, первая часть ключа',
    dag_group     String        DEFAULT ''  COMMENT 'Группа поставок = один пакет = один даг export_er__<replica>__<dag_group>; пусто → 0',
    schema_name   String        DEFAULT ''  COMMENT 'Целевая схема в .meta-файле для TFS. У поставки обязательна, у строки-дефолта ПУСТА',
    extract_name  String        DEFAULT ''  COMMENT 'Имя выгрузки (table name без схемы); ПУСТО = строка-дефолт группы',
    description   String        DEFAULT ''  COMMENT 'Описание (отображается в Airflow UI); наследуется от строки-дефолта группы',
    is_active     UInt8         DEFAULT 1   COMMENT '0 = записи нет в пакете; на строке-дефолте — нет и самого пакета',
    is_paused     UInt8         DEFAULT 0   COMMENT '1 = мягкое выключение: у группы даг на паузе, у поставки таск скипается',
    pk            Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа; не наследуется',
    uk            Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа; не наследуется',
    fields        Array(String) DEFAULT []             COMMENT 'SELECT-выражения таблицы-источника; ОБЯЗАТЕЛЬНО и явно — "*" и "t1.*" запрещены, чтобы новая колонка источника не уезжала в выгрузку сама',
    sql_from      String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос; у поставки обязательное. Отсюда же берётся база источника для описаний колонок',
    sql_where     String        DEFAULT ''             COMMENT 'WHERE-условие: только бизнес-фильтр, окно дельты дописывается само',
    sql_join      String        DEFAULT ''             COMMENT 'JOIN-clause (полное выражение: JOIN t ON ...); вставляется между FROM и WHERE',
    sql_with      String        DEFAULT ''             COMMENT 'WITH-блок (CTE); вставляется перед SELECT',
    sql_settings  String        DEFAULT ''             COMMENT 'SETTINGS-блок ClickHouse; вставляется в конец запроса',
    params        String        DEFAULT '{}'           COMMENT 'JSON с параметрами (GROUP_PARAMS/TABLE_PARAMS в er_config.py). Здесь же schedule (только у строки-дефолта) и is_recent',
    updated_at    DateTime64(3) DEFAULT now64(3)       COMMENT 'Версия строки для ReplacingMergeTree (мс-точность исключает коллизии при быстрых обновлениях)'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_wf_meta_{uuid}', '{replica}', updated_at)
ORDER BY (replica, dag_group, schema_name, extract_name);


-- ─────────────────────────────────────────────────────────────────────────────
-- 🔄 ПЕРЕЕЗД СО СТАРОЙ СТРУКТУРЫ
--    (ключ (db_name, extract_name), группа суффиксом в replica, колонки schedule/is_recent)
--
-- Порядок: новая таблица → переливка → сверка → пауза export_er_setup и пакетов ЕР →
-- RENAME → перенос состояния дельты (er_extract_history.sql) → выкладка кода →
-- синк с галкой «Синхронизировать принудительно».
-- Старую таблицу не удалять, пока новая не отработала: откат — обратный RENAME.
--
--   CREATE TABLE export.er_wf_meta_v2 ON CLUSTER datalab (…как выше…)
--       ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_wf_meta_v2_{uuid}', '{replica}', updated_at)
--       ORDER BY (replica, dag_group, schema_name, extract_name);
--
--   INSERT INTO export.er_wf_meta_v2
--       (replica, dag_group, schema_name, extract_name, description, is_active, is_paused,
--        pk, uk, fields, sql_from, sql_where, sql_join, sql_with, sql_settings,
--        params, updated_at)
--   SELECT
--       -- реплика без суффикса, суффикс → в свою колонку ('' → '0')
--       splitByString('__', t.replica)[1]                                  AS replica,
--       if(position(t.replica, '__') = 0, '0',
--          substring(t.replica, position(t.replica, '__') + 2))            AS dag_group,
--       -- у строки-дефолта схема обнуляется, поставке — своя, иначе ГРУППОВАЯ:
--       -- раньше schema_name наследовался, и у большинства поставок он пуст
--       if(t.extract_name = '', '',
--          if(t.schema_name != '', t.schema_name, g.schema_name))          AS schema_name,
--       t.extract_name, t.description, t.is_active, 0 AS is_paused,
--       t.pk, t.uk, t.fields, t.sql_from, t.sql_where, t.sql_join, t.sql_with, t.sql_settings,
--       -- schedule и is_recent вливаются в params; пустые значения не добавляем
--       jsonMergePatch(
--           if(t.params = '', '{}', t.params),
--           concat('{', arrayStringConcat(arrayFilter(x -> x != '', [
--               if(t.schedule != '', concat('"schedule":"', t.schedule, '"'), ''),
--               if(t.is_recent != 0, '"is_recent":1', '')
--           ]), ','), '}')
--       )                                                                  AS params,
--       t.updated_at
--   FROM export.er_wf_meta AS t FINAL
--   LEFT JOIN (
--       SELECT replica, schema_name FROM export.er_wf_meta FINAL WHERE extract_name = ''
--   ) AS g ON g.replica = t.replica;
--
-- ⚠️ jsonMergePatch есть не на всех сборках ClickHouse — сначала проверить
--    (SELECT jsonMergePatch('{}','{}')). Нет функции — перелить одноразовым скриптом
--    на питоне: собрать params в json.dumps и вставить готовые значения.
-- ⚠️ JOIN на строку-дефолт — не украшение. schema_name раньше НАСЛЕДОВАЛСЯ, поэтому
--    у большинства поставок он пуст, а теперь входит в ключ и обязателен: без раздачи
--    групповой схемы вниз синк отвергнет их все как «пустой schema_name». Проверено
--    на тестовом стенде 28.08.2026 — там пустой была схема у 8 поставок из 8.
--
--   -- сверка перед переключением
--   SELECT count() FROM export.er_wf_meta FINAL;
--   SELECT count() FROM export.er_wf_meta_v2 FINAL;
--   SELECT replica, dag_group,
--          countIf(extract_name = '')                                  AS defaults,
--          countIf(extract_name = '' AND JSONHas(params, 'schedule'))  AS with_cron,
--          countIf(extract_name = '' AND schema_name != '')            AS bad_defaults,
--          countIf(extract_name != '' AND schema_name = '')            AS no_schema,
--          countIf(extract_name != '')                                 AS tables
--   FROM export.er_wf_meta_v2 FINAL GROUP BY replica, dag_group ORDER BY 1, 2;
--   -- defaults = 1, with_cron = 1, bad_defaults = 0, no_schema = 0 у каждой группы
--
--   RENAME TABLE export.er_wf_meta    TO export.er_wf_meta_old,
--                export.er_wf_meta_v2 TO export.er_wf_meta
--   ON CLUSTER datalab;   -- одним запросом; в БД Atomic UUID таблицы переезд переживает,
--                         -- поэтому ZK-путь на {uuid} остаётся валидным
-- ─────────────────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────────────────
-- Пример: группа из двух поставок.
-- Первая строка — дефолты группы (расписание и групповые параметры в params),
-- остальные — поставки, у которых своё только SQL, ключи и состав полей.
--
-- В params указываются только отличия от DEFAULT_PARAMS (er_config.py).
-- Повторный INSERT того же ключа не заменяет строку мгновенно — дедупликация происходит
-- при фоновом MERGE; для немедленного чтения использовать FINAL.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO export.er_wf_meta (replica, dag_group, description, params)
VALUES (
    'hrplatform_datalab',
    '1',
    'Пакет 1: справочники обучения',
    -- schedule обязателен и живёт только здесь; schema_name и extract_name пусты
    '{"schedule": "30 2 * * *", "auto_confirm": 0, "confirm_timeout": 90, "max_active_tasks": 2}'
);

-- Поставка с JOIN. При непустом sql_join поле fields задаёт состав явно, иначе колонки
-- обеих таблиц уехали бы в CSV и в .meta: дубли справа ClickHouse назовёт 't2.col'.
INSERT INTO export.er_wf_meta
    (replica, dag_group, schema_name, extract_name, uk, fields, sql_from, sql_join, params)
VALUES (
    'hrplatform_datalab',
    '1',
    'learning',
    'lc_items_opened',
    ['person_uuid', 'item_id'],
    ['t1.person_uuid AS person_uuid', 't1.item_id AS item_id', 't1.opened_at AS opened_at'],
    'evolution.lc_items_opened t1',
    'LEFT JOIN evolution_export.lc_items_opened_exp t2 ON t1.person_uuid = t2.person_uuid AND t1.item_id = t2.item_id',
    '{"selfrun_timeout": 10}'
);

-- Вторая поставка того же пакета, выгрузка построчным JSON (файлы .json).
INSERT INTO export.er_wf_meta
    (replica, dag_group, schema_name, extract_name, uk, fields, sql_from, params)
VALUES (
    'hrplatform_datalab',
    '1',
    'learning',
    'lc_items_meta',
    ['item_id'],
    ['item_id', 'title', 'tags'],
    'evolution.lc_items_meta',
    '{"format": "JSONEachRow"}'
);

-- 🔀 ПЕРЕНОС ПОСТАВКИ ИЗ ГРУППЫ В ГРУППУ
--
-- Ключ состояния дельты — (replica, schema_name, extract_name), группы в нём нет
-- (er_extract_history.sql). Поэтому АКТИВНОЙ одна и та же поставка может быть только
-- в одной группе: иначе обе вели бы одну серию состояний, вторая читала бы окно первой
-- и выгружала пустоту. Синхронизация такую пару отвергает и называет обе группы.
--
-- Перенос от этого не страдает — состояние переезжает само, ключ-то один. Порядок:
-- сначала гасим старую строку, потом заводим новую.
--
--   ALTER TABLE export.er_wf_meta UPDATE is_active = 0
--   WHERE replica = 'hrplatform_datalab' AND dag_group = '1'
--         AND schema_name = 'learning' AND extract_name = 'lc_items_meta';
--   -- либо мягче, с возможностью вернуть: is_paused = 1
--
--   INSERT INTO export.er_wf_meta
--       (replica, dag_group, schema_name, extract_name, uk, fields, sql_from)
--   VALUES ('hrplatform_datalab', '2', 'learning', 'lc_items_meta', ['item_id'],
--           ['item_id', 'title', 'tags'], 'evolution.lc_items_meta');
--
-- Пока старая строка на паузе, синк о паре предупреждает: снятая галкой пауза вернёт
-- второго писателя состояния. Две версии пакета, работающие ОДНОВРЕМЕННО, разводятся
-- схемой (schema_name), именем выгрузки или репликой — но не группой.
