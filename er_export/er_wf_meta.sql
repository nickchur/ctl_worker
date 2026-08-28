-- DDL для export.er_wf_meta
-- 2026-08-28 12:50 MSK · v2.0 · Чуркин Николай · nschurkin@sber.ru
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
-- 🔑 КЛЮЧ — (replica, extract_name)
--
-- До 2026-08-28 ключом было (db_name, extract_name), и из этого росли два неудобства:
--   • строка-дефолт группы была обязана нести db_name = replica — иначе дефолты ВСЕХ
--     групп получали один ключ ('', '') и схлопывались в одну строку;
--   • одну и ту же таблицу нельзя было включить в две группы, то есть нельзя было
--     завести новую версию пакета, не убив старую.
-- С ключом (replica, extract_name) признак строки-дефолта ровно один — пустой
-- extract_name, а db_name стал обычной колонкой. Ключ записи в форме export_er_setup
-- выглядит так же: 'hrplatform_datalab__1/lc_items_opened'.
--
-- ⚠️ Состояние дельты ключуется той же парой (см. er_extract_history.sql): две группы
-- с одинаковой таблицей ведут РАЗНЫЕ окна и разную историю.
--
--
-- 📦 ГРУППЫ ПОСТАВОК
--
-- Один пакет = одна группа = один DAG = один внешний тикет на все поставки группы.
-- Группа кодируется суффиксом после '__' в поле replica:
--     hrplatform_datalab       — суффикса нет: синхронизация подставит __0
--     hrplatform_datalab__1    — группа 1 той же реплики
--     hrplatform_datalab__lm   — суффикс произвольный, не обязательно цифровой
--
-- Суффикс есть ВСЕГДА: имя архива строится как '{база}__{ts}__{группа}__{table}__...', и без
-- суффикса разделителей '__' на один меньше, чем у остальных пакетов. Строку в таблице
-- под это править не нужно — реплика нормализуется при синхронизации.
--
-- Маршрут в TFS (scenario_id и префикс в S3) ищется по БАЗОВОЙ реплике — части до первого
-- '__', то есть в TFS_MAP (er_config.py) нужна запись только на базу. В имя архива уходят
-- обе части: база первой, суффикс группы за меткой времени, — поэтому архивы разных групп
-- не пересекаются именами. Тикет пакета суффикса не несёт, его разводит сама метка времени
-- (таск make_ts, пул на базовую реплику с одним слотом).
--
--
-- 🧩 ДВА ТИПА СТРОК
--
--   replica заполнена, extract_name ПУСТ  → строка-дефолт группы (ОБЯЗАТЕЛЬНА)
--   replica и extract_name заполнены      → поставка (одна таблица)
--
-- Строка-дефолт задаёт значения для всей группы и обязана существовать: без неё группа
-- целиком уходит в ошибку и вместо пакета создаётся даг-заглушка. Каждое поле разрешается
-- по своему правилу:
--
--   schema_name — наследуется напрямую: своё непустое перебивает групповое;
--   params      — merge по ключам: умолчания er_config.py → группа → таблица;
--   description — своё, иначе комментарий таблицы в CH, иначе групповое (групповой текст
--                 последний, иначе он затрёт осмысленные комментарии всех таблиц пакета);
--   db_name     — НЕ наследуется: база источника у поставок пакета бывает разной,
--                 а строке-дефолту она не нужна вовсе;
--   pk, uk, fields, sql_from, sql_join, sql_where, sql_with, sql_settings — НЕ наследуются,
--                 они всегда про конкретную таблицу.
--
-- is_active = 0 на строке-дефолте выключает группу целиком.
--
-- Обязательные поля поставки: replica, extract_name, db_name, sql_from, fields
-- (пустые → запись пропускается при синхронизации и ломает свою группу).
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
    replica       String                    COMMENT 'Реплика с суффиксом группы: база до "__" ищется в TFS_MAP (er_config.py); обязательное, первая часть ключа',
    extract_name  String                    COMMENT 'Имя выгрузки (table name без схемы); ПУСТО = строка-дефолт группы',
    schema_name   String                    COMMENT 'Целевая схема в .meta-файле для TFS; наследуется от строки-дефолта группы',
    db_name       String                    COMMENT 'База данных источника в ClickHouse (левая часть "db.table"); у поставки обязательна, у строки-дефолта не нужна',
    pk            Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа; не наследуется',
    uk            Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа; не наследуется',
    fields        Array(String) DEFAULT []             COMMENT 'SELECT-выражения таблицы-источника; ОБЯЗАТЕЛЬНО и явно — "*" и "t1.*" запрещены, чтобы новая колонка источника не уезжала в выгрузку сама',
    sql_from      String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос; у поставки обязательное',
    sql_where     String        DEFAULT ''             COMMENT 'WHERE-условие: только бизнес-фильтр, окно дельты дописывается само',
    sql_join      String        DEFAULT ''             COMMENT 'JOIN-clause (полное выражение: JOIN t ON ...); вставляется между FROM и WHERE',
    sql_with      String        DEFAULT ''             COMMENT 'WITH-блок (CTE); вставляется перед SELECT',
    sql_settings  String        DEFAULT ''             COMMENT 'SETTINGS-блок ClickHouse; вставляется в конец запроса',
    params        String        DEFAULT '{}'           COMMENT 'JSON с параметрами (GROUP_PARAMS/TABLE_PARAMS в er_config.py). Здесь же schedule (только у строки-дефолта) и is_recent',
    description   String        DEFAULT ''             COMMENT 'Описание (отображается в Airflow UI); наследуется от строки-дефолта группы',
    is_active     UInt8         DEFAULT 1              COMMENT '0 = запись игнорируется при синхронизации; на строке-дефолте выключает всю группу',
    updated_at    DateTime64(3) DEFAULT now64(3)        COMMENT 'Версия строки для ReplacingMergeTree (мс-точность исключает коллизии при быстрых обновлениях)'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_wf_meta_{uuid}', '{replica}', updated_at)
ORDER BY (replica, extract_name);


-- ─────────────────────────────────────────────────────────────────────────────
-- 🔄 ПЕРЕЕЗД СО СТАРОЙ СТРУКТУРЫ (ключ (db_name, extract_name), колонки schedule/is_recent)
--
-- Порядок: новая таблица → переливка → сверка → пауза export_er_setup и пакетов ЕР →
-- RENAME → выкладка кода → синк с галкой «Синхронизировать принудительно».
-- Старую таблицу не удалять, пока новая не отработала: откат — обратный RENAME.
--
--   CREATE TABLE export.er_wf_meta_v2 ON CLUSTER datalab (…как выше…)
--       ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_wf_meta_v2_{uuid}', '{replica}', updated_at)
--       ORDER BY (replica, extract_name);
--
--   INSERT INTO export.er_wf_meta_v2
--       (replica, extract_name, schema_name, db_name, pk, uk, fields,
--        sql_from, sql_where, sql_join, sql_with, sql_settings,
--        params, description, is_active, updated_at)
--   SELECT
--       replica, extract_name, schema_name, db_name, pk, uk, fields,
--       sql_from, sql_where, sql_join, sql_with, sql_settings,
--       -- schedule и is_recent вливаются в params; пустые значения не добавляем
--       jsonMergePatch(
--           if(params = '', '{}', params),
--           concat('{', arrayStringConcat(arrayFilter(x -> x != '', [
--               if(schedule != '', concat('"schedule":"', schedule, '"'), ''),
--               if(is_recent != 0, '"is_recent":1', '')
--           ]), ','), '}')
--       ) AS params,
--       description, is_active, updated_at
--   FROM export.er_wf_meta FINAL;
--
-- ⚠️ jsonMergePatch есть не на всех сборках ClickHouse — сначала проверить
--    (SELECT jsonMergePatch('{}','{}')). Нет функции — перелить одноразовым скриптом
--    на питоне: собрать params в json.dumps и вставить готовые значения.
--
--   -- сверка перед переключением: строк столько же, у каждой группы есть дефолт с cron
--   SELECT count() FROM export.er_wf_meta FINAL;
--   SELECT count() FROM export.er_wf_meta_v2 FINAL;
--   SELECT replica,
--          countIf(extract_name = '')                                  AS defaults,
--          countIf(extract_name = '' AND JSONHas(params, 'schedule'))  AS with_cron,
--          countIf(extract_name != '')                                 AS tables
--   FROM export.er_wf_meta_v2 FINAL GROUP BY replica ORDER BY replica;
--
--   RENAME TABLE export.er_wf_meta    TO export.er_wf_meta_old,
--                export.er_wf_meta_v2 TO export.er_wf_meta
--   ON CLUSTER datalab;   -- одним запросом; в БД Atomic UUID таблицы переезд переживает,
--                         -- поэтому ZK-путь на {uuid} остаётся валидным
-- ─────────────────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────────────────
-- Пример: группа из двух поставок.
-- Первая строка — дефолты группы (расписание, схема, групповые параметры),
-- остальные — поставки, у которых своё только SQL, ключи и состав полей.
--
-- В params указываются только отличия от DEFAULT_PARAMS (er_config.py).
-- Повторный INSERT той же (replica, extract_name) не заменяет строку мгновенно —
-- дедупликация происходит при фоновом MERGE; для немедленного чтения использовать FINAL.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO export.er_wf_meta (replica, extract_name, schema_name, description, params)
VALUES (
    'hrplatform_datalab__1',
    '',                                     -- пусто → это строка-дефолт группы
    'learning',
    'Пакет 1: справочники обучения',
    -- schedule обязателен и живёт только здесь
    '{"schedule": "30 2 * * *", "auto_confirm": 0, "confirm_timeout": 90, "max_active_tasks": 2}'
);

-- Поставка с JOIN. При непустом sql_join поле fields задаёт состав явно, иначе колонки
-- обеих таблиц уехали бы в CSV и в .meta: дубли справа ClickHouse назовёт 't2.col'.
INSERT INTO export.er_wf_meta
    (replica, extract_name, db_name, uk, fields, sql_from, sql_join, sql_where, params)
VALUES (
    'hrplatform_datalab__1',
    'lc_items_opened',
    'evolution',
    ['person_uuid', 'item_id'],
    ['t1.person_uuid', 't1.item_id', 't1.opened_at', 't1.status'],
    'evolution.lc_items_opened t1',
    'LEFT JOIN evolution_export.lc_items_opened_exp t2 ON t1.person_uuid = t2.person_uuid AND t1.item_id = t2.item_id',
    '',
    '{"selfrun_timeout": 10}'
);

-- Вторая поставка того же пакета, выгрузка построчным JSON (файлы .json).
INSERT INTO export.er_wf_meta
    (replica, extract_name, db_name, uk, fields, sql_from, sql_where, params)
VALUES (
    'hrplatform_datalab__1',
    'lc_items_meta',
    'evolution',
    ['item_id'],
    ['item_id', 'title', 'tags'],
    'evolution.lc_items_meta',
    '',
    '{"format": "JSONEachRow"}'
);

-- Та же таблица во ВТОРОЙ группе — так заводится новая версия пакета рядом со старой.
-- Раньше это было невозможно: ключ (db_name, extract_name) схлопнул бы обе строки в одну.
-- Состояние дельты у групп разное — оно ключуется парой (replica, extract_name).
INSERT INTO export.er_wf_meta
    (replica, extract_name, db_name, uk, fields, sql_from, params)
VALUES (
    'hrplatform_datalab__2',
    'lc_items_meta',
    'evolution',
    ['item_id'],
    ['item_id', 'title', 'tags', 'updated_at'],
    'evolution.lc_items_meta',
    '{}'
);
