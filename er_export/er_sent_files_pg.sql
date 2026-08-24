-- DDL для export.er_sent_files — вариант PostgreSQL / Greenplum (STORAGE = 'pg')
-- 2026-08-12 19:16 MSK · v1.0 · Чуркин Николай · nschurkin@sber.ru
--
-- Для ClickHouse (STORAGE = 'ch') используйте er_sent_files.sql.
-- Для STORAGE = 's3' таблица не нужна: очередь лежит объектами под queue/pending
-- и queue/sent рядом с логами.
--
--
-- 🐘 GREENPLUM vs POSTGRESQL
--
-- Файл написан под Greenplum. Для обычного PostgreSQL уберите последнюю строку
-- DISTRIBUTED BY — он такого синтаксиса не знает. Больше отличий нет.
--
--
-- 📦 ЗАЧЕМ
--
-- Строка на ФАЙЛ, а не на выгрузку: в пакете ЕР несколько архивов плюс summary-тикет,
-- и у каждого свой RqUID. Именно RqUID связывает нашу отправку с обратной квитанцией
-- в export.tfs_receipts.
--
-- Таблица одновременно очередь и история:
--
--   notified_at IS NULL      → файл поставлен в очередь, но ещё не отправлен
--   notified_at IS NOT NULL  → уведомление ушло в Kafka, ждём квитанцию
--
--
-- ⚠️ APPEND-ONLY, БЕЗ UPDATE
--
-- Отметка отправки НЕ делает UPDATE, а дописывает строку с проставленным notified_at
-- и свежим updated_at. Причины две: в Greenplum построчный UPDATE медленный и раздувает
-- таблицу, и вторая — так модель совпадает с ClickHouse-вариантом, где UPDATE попросту
-- нет. Актуальная версия выбирается при чтении:
--
--   SELECT DISTINCT ON (rq_uid) * FROM export.er_sent_files
--   ORDER BY rq_uid, updated_at DESC
--
-- Уникального индекса на rq_uid поэтому НЕТ — он бы запретил вторую версию строки.


CREATE TABLE IF NOT EXISTS export.er_sent_files
(
    rq_uid       text        NOT NULL,   -- сгенерирован нами до отправки; ключ сопоставления с квитанцией
    file_name    text        NOT NULL,   -- имя ZIP-архива либо summary-тикета
    replica      text,                   -- группа поставок (replica целиком, с суффиксом)
    scenario_id  text,                   -- маршрут ТФС — по нему считаются лимиты отправки
    package_ts   timestamptz NOT NULL,   -- метка пакета; связывает файлы одного тикета
    dag_id       text        DEFAULT '',
    run_id       text        DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),  -- когда файл встал в очередь
    notified_at  timestamptz,            -- NULL = ещё в очереди; иначе время отправки в Kafka
    updated_at   timestamptz NOT NULL DEFAULT now()   -- версия строки: побеждает свежая
)
DISTRIBUTED BY (rq_uid);   -- ← убрать для PostgreSQL

-- Под DISTINCT ON (rq_uid) ... ORDER BY updated_at DESC — основной путь чтения очереди.
CREATE INDEX IF NOT EXISTS er_sent_files_rq_uid_idx
    ON export.er_sent_files (rq_uid, updated_at DESC);

-- Под счётчики лимитов: окна скользящие, отбор по маршруту и времени отправки.
CREATE INDEX IF NOT EXISTS er_sent_files_scenario_idx
    ON export.er_sent_files (scenario_id, notified_at DESC);


-- ─────────────────────────────────────────────────────────────────────────────
-- Полезные запросы
-- ─────────────────────────────────────────────────────────────────────────────

-- Очередь на отправку (что увидит tfs_kafka_snd):
--   SELECT * FROM (
--       SELECT DISTINCT ON (rq_uid) rq_uid, file_name, replica, scenario_id,
--              package_ts, created_at, notified_at
--       FROM export.er_sent_files ORDER BY rq_uid, updated_at DESC
--   ) q
--   WHERE notified_at IS NULL ORDER BY package_ts, created_at;

-- Расход лимитов по маршруту. Все окна СКОЛЬЗЯЩИЕ: полночь суточный бюджет не обнуляет.
--   SELECT
--       count(*) FILTER (WHERE notified_at > now() - interval '1 second') AS sec,
--       count(*) FILTER (WHERE notified_at > now() - interval '1 minute') AS min,
--       count(*) FILTER (WHERE notified_at > now() - interval '1 hour')   AS hour,
--       count(*) FILTER (WHERE notified_at > now() - interval '1 day')    AS day
--   FROM (
--       SELECT DISTINCT ON (rq_uid) scenario_id, notified_at
--       FROM export.er_sent_files ORDER BY rq_uid, updated_at DESC
--   ) q
--   WHERE scenario_id = 'HRPLATFORM-4000' AND notified_at IS NOT NULL;
