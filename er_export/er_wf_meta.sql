-- DDL для export.er_wf_meta
-- 2026-08-13 13:00 MSK · v1.1 · Чуркин Николай · nschurkin@sber.ru
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
-- 📦 ГРУППЫ ПОСТАВОК
--
-- Один пакет = одна группа = один DAG = один внешний тикет на все поставки группы.
-- Группа кодируется суффиксом после '__' в поле replica:
--     hrplatform_datalab       — суффикса нет: синхронизация подставит __0
--     hrplatform_datalab__1    — группа 1 той же реплики
--     hrplatform_datalab__lm   — суффикс произвольный, не обязательно цифровой
--
-- Суффикс есть ВСЕГДА: имена файлов строятся как '{replica}__{ts}__{table}__...', и без
-- суффикса разделителей '__' на один меньше, чем у остальных пакетов. Строку в таблице
-- под это править не нужно — реплика нормализуется при синхронизации.
--
-- Маршрут в TFS (scenario_id и префикс в S3) ищется по БАЗОВОЙ реплике — части до первого
-- '__', то есть в TFS_MAP (er_config.py) нужна запись только на базу. Полная replica уходит
-- в имена архива и тикета, поэтому имена пакетов разных групп не пересекаются.
--
--
-- 🧩 ДВА ТИПА СТРОК
--
--   replica заполнена, extract_name ПУСТ  → строка-дефолт группы
--   replica и extract_name заполнены      → поставка (одна таблица)
--
-- Строка-дефолт задаёт значения для всей группы. Каждое поле разрешается по своему правилу:
--
--   schema_name — наследуется напрямую: своё непустое перебивает групповое;
--   params      — merge по ключам: умолчания er_config.py → группа → таблица;
--   schedule    — берётся групповое (см. ниже);
--   description — своё, иначе комментарий таблицы в CH, иначе групповое (групповой текст
--                 последний, иначе он затрёт осмысленные комментарии всех таблиц пакета);
--   is_recent   — НЕ наследуется: колонка UInt8 DEFAULT 0, «не задано» от «явно delta» не
--                 отличить, и таблица в recent-группе не смогла бы вернуться к дельте;
--   pk, uk, fields, sql_from, sql_join, sql_where, sql_with, sql_settings — НЕ наследуются,
--                 они всегда про конкретную таблицу.
--
-- schedule: ОБЯЗАТЕЛЬНОЕ, умолчания у него нет. Расписание пакета берётся из строки-дефолта,
-- иначе — первое непустое среди поставок; если не задано нигде, группа целиком уходит
-- в ошибку и даг не создаётся. Пакет, поехавший не в своё окно, хуже непоехавшего.
-- Свой отличный cron у поставки даёт предупреждение, но поставку из пакета не выкидывает.
--
-- is_active = 0 на строке-дефолте выключает группу целиком.
--
-- ⚠️ У строки-дефолта db_name ОБЯЗАН быть равен replica. Ключ сортировки —
-- (db_name, extract_name), extract_name у дефолтов пуст, и с пустым db_name дефолты всех
-- групп получили бы один ключ ('', '') — фоновый MERGE схлопнул бы их в одну строку,
-- оставив последнюю по updated_at. С db_name = replica ключ уникален внутри группы.
-- Синхронизация проверяет это и отбрасывает строку-дефолт с пустым db_name.
--
-- Обязательные поля поставки: extract_name, db_name, replica, sql_from, fields
-- (пустые → запись пропускается при синхронизации и не порождает таск).


CREATE TABLE IF NOT EXISTS export.er_wf_meta ON CLUSTER datalab
(
    extract_name  String                    COMMENT 'Имя выгрузки (table name без схемы); ПУСТО = строка-дефолт группы',
    db_name       String                    COMMENT 'База данных источника в ClickHouse (левая часть "db.table"); у строки-дефолта группы = replica, иначе дефолты групп схлопнутся по ключу',
    replica       String                    COMMENT 'Реплика с суффиксом группы: база до "__" ищется в TFS_MAP (er_config.py); обязательное',
    schema_name   String                    COMMENT 'Целевая схема в .meta-файле для TFS; наследуется от строки-дефолта группы',
    pk            Array(String) DEFAULT []             COMMENT 'Список колонок первичного ключа; не наследуется',
    uk            Array(String) DEFAULT []             COMMENT 'Список колонок уникального ключа; не наследуется',
    fields        Array(String) DEFAULT []             COMMENT 'SELECT-выражения таблицы-источника; ОБЯЗАТЕЛЬНО и явно — "*" и "t1.*" запрещены, чтобы новая колонка источника не уезжала в выгрузку сама',
    sql_from      String        DEFAULT ''             COMMENT 'FROM-часть запроса: "db.table" или подзапрос; у поставки обязательное',
    sql_where     String        DEFAULT ''             COMMENT 'WHERE-условие: только бизнес-фильтр, окно дельты дописывается само',
    sql_join      String        DEFAULT ''             COMMENT 'JOIN-clause (полное выражение: JOIN t ON ...); вставляется между FROM и WHERE',
    sql_with      String        DEFAULT ''             COMMENT 'WITH-блок (CTE); вставляется перед SELECT',
    sql_settings  String        DEFAULT ''             COMMENT 'SETTINGS-блок ClickHouse; вставляется в конец запроса',
    params        String        DEFAULT '{}'           COMMENT 'JSON с параметрами выгрузки (см. GROUP_PARAMS/TABLE_PARAMS в er_config.py); групповые параметры читаются только из строки-дефолта',
    description   String        DEFAULT ''             COMMENT 'Описание (отображается в Airflow UI); наследуется от строки-дефолта группы',
    schedule      String        DEFAULT ''             COMMENT 'Cron-расписание DAG-а группы; задаётся в строке-дефолте. ОБЯЗАТЕЛЬНОЕ: пусто → группа не синхронизируется',
    is_recent     UInt8         DEFAULT 0              COMMENT '0 = delta-выгрузка, 1 = recent (скользящее окно); НЕ наследуется',
    is_active     UInt8         DEFAULT 1              COMMENT '0 = запись игнорируется при синхронизации; на строке-дефолте выключает всю группу',
    updated_at    DateTime64(3) DEFAULT now64(3)        COMMENT 'Версия строки для ReplacingMergeTree (мс-точность исключает коллизии при быстрых обновлениях)'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_wf_meta_{uuid}', '{replica}', updated_at)
ORDER BY (db_name, extract_name);

-- ─────────────────────────────────────────────────────────────────────────────
-- Строка ENGINE выше отличается от прежней версии этого файла: там был устаревший путь
-- '/clickhouse/tables/{shard}/export/er_wf_meta', а существующая таблица создана по
-- '/clickhouse/tables/er_wf_meta_{uuid}'. Приведено к фактическому значению — менялась
-- не таблица, а её описание.
-- ─────────────────────────────────────────────────────────────────────────────


-- ─────────────────────────────────────────────────────────────────────────────
-- Пример: группа из двух поставок.
-- Первая строка — дефолты группы (schedule, схема, групповые параметры),
-- остальные — поставки, у которых своё только SQL, ключи и состав полей.
--
-- В params указываются только отличия от DEFAULT_PARAMS (er_config.py).
-- Повторный INSERT той же (db_name, extract_name) не заменяет строку мгновенно —
-- дедупликация происходит при фоновом MERGE; для немедленного чтения использовать FINAL.
-- ─────────────────────────────────────────────────────────────────────────────

INSERT INTO export.er_wf_meta (extract_name, db_name, replica, schema_name, schedule, description, params)
VALUES (
    '',                                     -- пусто → это строка-дефолт группы
    'hrplatform_datalab__1',                -- = replica: держит ключ (db_name, extract_name) уникальным
    'hrplatform_datalab__1',
    'learning',
    '30 2 * * *',                           -- отличный от DDL-дефолта cron: только такой считается заданным
    'Пакет 1: справочники обучения',
    '{"auto_confirm": 0, "confirm_timeout": 90, "max_active_tasks": 2}'
);

-- Поставка с JOIN. При непустом sql_join поле fields задаёт состав явно, иначе колонки
-- обеих таблиц уехали бы в CSV и в .meta: дубли справа ClickHouse назовёт 't2.col'.
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, uk, fields, sql_from, sql_join, sql_where, params)
VALUES (
    'lc_items_opened',
    'evolution',
    'hrplatform_datalab__1',
    ['person_uuid', 'item_id'],
    ['t1.person_uuid', 't1.item_id', 't1.opened_at', 't1.status'],
    'evolution.lc_items_opened t1',
    'LEFT JOIN evolution_export.lc_items_opened_exp t2 ON t1.person_uuid = t2.person_uuid AND t1.item_id = t2.item_id',
    '',
    '{"selfrun_timeout": 10}'
);

-- Вторая поставка того же пакета, выгрузка построчным JSON (файлы .json).
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, uk, fields, sql_from, sql_where, params)
VALUES (
    'lc_items_meta',
    'evolution',
    'hrplatform_datalab__1',
    ['item_id'],
    ['item_id', 'title', 'tags'],
    'evolution.lc_items_meta',
    '',
    '{"format": "JSONEachRow"}'
);
