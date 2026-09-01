-- DDL для export.er_extract_history и export.er_extract_current_vw
-- 2026-09-01 09:14 MSK · v3.1 · Nick Churkin · NSChurkin@sber.ru
-- Состояние дельты ER-выгрузок: своя таблица вместо общей с xStream export.extract_history.
--
-- ВАЖНО: выполнять через clickhouse-client или HTTP-интерфейс, НЕ через JDBC.
-- JDBC-драйвер интерпретирует {uuid} и {replica} как именованные параметры → ошибка.
--
--
-- 📦 ЗАЧЕМ СВОЯ ТАБЛИЦА
--
-- До 01.09.2026 ЕР писал состояние в export.extract_history — таблицу, общую с xStream.
-- Разводить там поставки нечем: идентификация выгрузки это одно строковое поле, поэтому
-- ЕР складывал в него составное имя '<dag_id>.<extract_name>'. Приём работал, но:
--
--   • отобрать историю по реплике или схеме нельзя — только LIKE по строке;
--   • соединить с er_wf_meta нечем: ключ там из четырёх колонок, а здесь склейка;
--   • неизвестно, какой ран это записал, — ни dag_id отдельно, ни run_id;
--   • структура общая, и любое поле «под ЕР» пришлось бы согласовывать с xStream.
--
-- Своя таблица снимает всё это разом. Чужие объекты (extract_history,
-- extract_current_vw, extract_registry_vw) остаются нетронутыми — там живёт xStream,
-- и после перехода ЕР к ним не обращается.
--
-- Реестра для ЕР заводить не нужно: роль extract_registry_vw у нас играет
-- export.er_wf_meta.
--
--
-- 🔑 КЛЮЧ
--
--     ORDER BY (replica, schema_name, extract_name, extract_time)
--
-- Строка на КАЖДОЕ окно выгрузки: extract_time — верхняя граница отработанного окна,
-- она же версия состояния. Движок ReplacingMergeTree по этому ключу: строка одной и той
-- же выгрузки ДОПИСЫВАЕТСЯ по мере смены статуса (загружено → отправлено → подтверждено),
-- как это устроено в er_sent_files.
--
-- ⚠️ dag_group в ключ НЕ входит — он колонка. Следствие: две РАБОТАЮЩИЕ поставки с одной
-- тройкой (одна таблица в двух группах реплики) лягут в одну серию, и вторая группа
-- прочитает окно первой. Поэтому синхронизация (export_er_setup) считает ошибкой настройки
-- две АКТИВНЫЕ такие поставки и не раскладывает метаданные: state_conflicts в er_config.py.
--
-- 🔀 Перенос поставки из группы в группу этим не запрещён: приостановленная (is_paused)
-- поставка не выгружает и состояние не двигает, выключенная (is_active = 0) — тем более.
-- Флаги работают и на строке-дефолте группы: выключенная группа уносит с собой все свои
-- поставки, приостановленная — создаёт даг на паузе. Порядок: старую строку (или всю
-- старую группу) гасим либо ставим на паузу, новую заводим в другой группе, состояние
-- переезжает само — ключ-то один.
--
-- Про пару с паузой синхронизация предупреждает: паузу снимают галкой в форме запуска,
-- мимо синка, и тогда писать состояние начнут обе. Про выключенную молчит: её возвращают
-- правкой is_active и новым синком, а он эту проверку прогонит заново.
--
--
-- ⏱️ ЕДИНИЦЫ ИЗМЕРЕНИЯ — ЧИТАТЬ ПЕРЕД ПРАВКОЙ
--
--     increment — МИНУТЫ,   на столько сдвигается верхняя граница окна;
--     overlap   — СЕКУНДЫ,  на столько нижняя граница отодвигается назад (нахлёст).
--
-- Разные единицы у соседних колонок — наследство общей вью extract_current_vw, где так
-- было: toIntervalMinute(increment) и toIntervalSecond(overlap). Сохранены осознанно,
-- чтобы перенесённое состояние продолжило считаться теми же окнами. В er_wf_meta и в
-- форме запуска increment тоже минуты, overlap тоже секунды — расхождения нет нигде.
--
-- Значения в колонках увеличения окна — СПРАВОЧНЫЕ: окно считает код по настройке
-- поставки (er_wf_meta), а не по тому, что записано здесь. Прежде наоборот, и правка
-- increment в настройке ни на что не влияла: вью брала значение из истории, а у поставок,
-- заведённых bootstrap-ом, там лежали минуты, умноженные на 60.
--
-- 🕐 Границы окна округляются до секунды (миллисекунды режет формат export_time, общий
-- для всех режимов). На переходе от прежнего состояния это даёт ПЕРЕКРЫТИЕ в доли
-- секунды, а не пропуск: нижняя граница берётся от округлённого вниз значения.
--
--
-- ⚠️ ОБНОВЛЕНИЕ СТРОКИ
--
-- ClickHouse не про UPDATE: смена статуса ДОПИСЫВАЕТ ту же строку с большим updated_at,
-- а ReplacingMergeTree схлопывает по ключу. До фонового MERGE обе версии живы, поэтому
-- читать состояние ТОЛЬКО через export.er_extract_current_vw (там argMax) или с FINAL.
-- Всё, что во вставке не названо, у дописанной строки обнулится — со стороны кода это
-- подпёрто списком CH_HIST_COLS в er_export/er_config.py: вставки собираются из него,
-- а не перечисляют колонки руками.


CREATE TABLE IF NOT EXISTS export.er_extract_history ON CLUSTER datalab
(
    replica         String                     COMMENT 'Реплика-источник; первая часть ключа настройки er_wf_meta',
    schema_name     String                     COMMENT 'Схема источника в ClickHouse',
    extract_name    String                     COMMENT 'Имя таблицы-поставки, как в er_wf_meta',
    extract_time    DateTime64(3)              COMMENT 'Верхняя граница отработанного окна = достигнутое состояние',
    dag_group       String        DEFAULT ''   COMMENT 'Группа пакета; НЕ в ключе, см. шапку файла',
    extract_count   Nullable(Int64)            COMMENT 'Выгружено строк; null = состояние без выгрузки (bootstrap, full)',
    loaded          Nullable(DateTime64(3))    COMMENT 'Когда данные легли в S3',
    sent            Nullable(DateTime64(3))    COMMENT 'Когда файлы встали в очередь отправки в ТФС',
    confirmed       Nullable(DateTime64(3))    COMMENT 'Когда пришла квитанция ТФС; null = не ждали или не дождались',
    increment       Int32         DEFAULT 60   COMMENT 'Шаг окна, МИНУТЫ',
    overlap         Int32         DEFAULT 0    COMMENT 'Нахлёст назад, СЕКУНДЫ',
    recent_interval Int32         DEFAULT 0    COMMENT 'Окно recent-режима, минуты; 0 у delta и full',
    time_field      String        DEFAULT ''   COMMENT 'Поле времени источника, по которому режется окно',
    time_from       DateTime64(3)              COMMENT 'Нижняя граница отработанного окна (строгая)',
    time_to         DateTime64(3)              COMMENT 'Верхняя граница отработанного окна (включительно)',
    exported_files  Array(String) DEFAULT []   COMMENT 'Имена ZIP-архивов пакета',
    mode            String        DEFAULT ''   COMMENT 'delta | recent | full | ad_hoc',
    dag_id          String        DEFAULT ''   COMMENT 'DAG пакета: export_er__<replica>__<dag_group>',
    run_id          String        DEFAULT ''   COMMENT 'Ран, записавший состояние',
    package_ts      Nullable(DateTime64(3))    COMMENT 'Метка пакета; связывает историю с er_sent_files и квитанциями',
    created_at      DateTime64(3) DEFAULT now64(3) COMMENT 'Когда состояние записано впервые',
    updated_at      DateTime64(3) DEFAULT now64(3) COMMENT 'Версия строки для ReplacingMergeTree'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_extract_history_{uuid}', '{replica}', updated_at)
ORDER BY (replica, schema_name, extract_name, extract_time);


-- Последнее состояние каждой поставки. Окно СЛЕДУЮЩЕЙ выгрузки вью НЕ считает — это
-- делает код (er_export.next_window). Так арифметика окна живёт в одном месте, лежит
-- в репозитории и проверяется тестом, а не правится ALTER-ом вью на боевом кластере.
CREATE VIEW IF NOT EXISTS export.er_extract_current_vw ON CLUSTER datalab AS
SELECT
    replica,
    schema_name,
    extract_name,
    count()                                AS num_state,
    -- alias'ом здесь НЕ extract_time: он затенил бы колонку внутри argMax(..., extract_time),
    -- и ClickHouse отвергает такой запрос (ILLEGAL_AGGREGATION).
    max(extract_time)                      AS state_time,
    argMax(time_to,         extract_time)  AS reached,
    argMax(extract_count,   extract_time)  AS extract_count,
    argMax(loaded,          extract_time)  AS loaded,
    argMax(sent,            extract_time)  AS sent,
    argMax(confirmed,       extract_time)  AS confirmed,
    argMax(increment,       extract_time)  AS increment,
    argMax(overlap,         extract_time)  AS overlap,
    argMax(time_field,      extract_time)  AS time_field,
    argMax(dag_group,       extract_time)  AS dag_group,
    argMax(dag_id,          extract_time)  AS dag_id
FROM export.er_extract_history FINAL
GROUP BY replica, schema_name, extract_name;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1️⃣ ПЕРЕНОС НАКОПЛЕННОГО СОСТОЯНИЯ
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Без переноса каждая поставка стартует с bootstrap от lower_bound и переливает всё
-- заново. Делать ПОСЛЕ создания таблицы и ДО первого рана пакетов на новом коде.
--
-- Сначала — проверка на тройки, заведённые в двух группах: у них общая серия, и перенос
-- склеит две истории в одну. Пусто — переносим смело.
--
--   SELECT replica, schema_name, extract_name, groupArray(dag_group) AS groups
--   FROM export.er_wf_meta FINAL
--   WHERE extract_name != '' AND is_active = 1
--   GROUP BY replica, schema_name, extract_name HAVING count() > 1;
--
-- Сам перенос. Имя в старой таблице составное — 'export_er__<replica>__<group>.<table>';
-- реплику и группу достаём разбором dag_id, схему — из er_wf_meta.
--
--   INSERT INTO export.er_extract_history
--       (replica, schema_name, extract_name, extract_time, dag_group, extract_count,
--        loaded, sent, confirmed, increment, overlap, recent_interval, time_field,
--        time_from, time_to, exported_files, mode, dag_id, created_at, updated_at)
--   SELECT
--       m.replica, m.schema_name, m.extract_name, h.extract_time, m.dag_group,
--       h.extract_count, h.loaded, h.sent, h.confirmed, h.increment, h.overlap,
--       h.recent_interval, h.time_field, h.time_from, h.time_to,
--       -- exported_files в общей таблице String: пустое → [], иначе одна строка списком
--       if(empty(h.exported_files), [], [toString(h.exported_files)]) AS exported_files,
--       'delta' AS mode,
--       splitByString('.', h.extract_name)[1] AS dag_id,
--       h.extract_time AS created_at, now64(3) AS updated_at
--   FROM export.extract_history AS h
--   INNER JOIN (
--       SELECT replica, dag_group, schema_name, extract_name,
--              concat('export_er__', replica, '__', dag_group, '.', extract_name) AS state_name
--       FROM export.er_wf_meta FINAL
--       WHERE extract_name != ''
--   ) AS m ON m.state_name = h.extract_name
--   WHERE h.extract_name LIKE 'export_er\_\_%';
--
-- ⚠️ Если exported_files в боевой extract_history не String, а Array(String) — убрать
-- обёртку if(...) и переносить колонку как есть. Проверить одной строкой:
--   SELECT type FROM system.columns
--   WHERE database = 'export' AND table = 'extract_history' AND name = 'exported_files';
--
--
-- ─────────────────────────────────────────────────────────────────────────────
-- 2️⃣ ПРОВЕРКА ПЕРЕНОСА
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Сколько поставок переехало и все ли они есть в настройке:
--   SELECT count() AS moved FROM export.er_extract_current_vw;
--   SELECT count() AS configured FROM export.er_wf_meta FINAL
--   WHERE extract_name != '' AND is_active = 1;
--
-- Достигнутое состояние совпадает со старым (должно быть пусто):
--   SELECT n.replica, n.schema_name, n.extract_name, n.reached, o.time_to
--   FROM export.er_extract_current_vw AS n
--   INNER JOIN (
--       SELECT extract_name, argMax(time_to, extract_time) AS time_to
--       FROM export.extract_history WHERE extract_name LIKE 'export_er\_\_%'
--       GROUP BY extract_name
--   ) AS o ON o.extract_name = concat(n.dag_id, '.', n.extract_name)
--   WHERE n.reached != o.time_to;
--
-- Строки xStream не задеты — их имена по-прежнему без точки:
--   SELECT count() FROM export.extract_history WHERE position(extract_name, '.') = 0;
--
--
-- ─────────────────────────────────────────────────────────────────────────────
-- Полезные запросы
-- ─────────────────────────────────────────────────────────────────────────────

-- Где сейчас каждая поставка реплики и насколько отстала от текущего времени:
--   SELECT schema_name, extract_name, reached,
--          dateDiff('minute', reached, now64(3)) AS lag_min, num_state, extract_count
--   FROM export.er_extract_current_vw
--   WHERE replica = 'hrplatform_datalab'
--   ORDER BY lag_min DESC;

-- Что уехало и что подтверждено за сутки:
--   SELECT extract_name, extract_time, extract_count, sent, confirmed, exported_files
--   FROM export.er_extract_history FINAL
--   WHERE replica = 'hrplatform_datalab' AND extract_time > now64(3) - INTERVAL 1 DAY
--   ORDER BY extract_time DESC;

-- Отправленное без квитанции (кандидаты на разбор в тракте ТФС):
--   SELECT replica, extract_name, extract_time, sent, exported_files
--   FROM export.er_extract_history FINAL
--   WHERE sent IS NOT NULL AND confirmed IS NULL AND notEmpty(exported_files)
--   ORDER BY sent DESC;

-- История одной поставки целиком (видно, как двигалось окно):
--   SELECT extract_time, time_from, time_to, extract_count, run_id
--   FROM export.er_extract_history FINAL
--   WHERE replica = 'hrplatform_datalab' AND schema_name = 'learning'
--         AND extract_name = 'lc_items_opened'
--   ORDER BY extract_time DESC LIMIT 20;
