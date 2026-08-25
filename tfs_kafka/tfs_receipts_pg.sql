-- DDL для export.tfs_receipts — вариант PostgreSQL / Greenplum
-- 2026-08-14 11:25 MSK · v1.2 · Чуркин Николай · nschurkin@sber.ru
--
-- Нужна, только если включено зеркало в Postgres — непустой PG_CONN в plugins/tfs_utils.py.
-- Для зеркала в ClickHouse (непустой CH_ID) используйте tfs_receipts.sql.
-- Источник истины — S3, там квитанции лежат объектами рядом с логами и таблиц не требуют.
--
--
-- 🐘 GREENPLUM vs POSTGRESQL
--
-- Файл написан под Greenplum. Для обычного PostgreSQL уберите последнюю строку
-- DISTRIBUTED BY — он такого синтаксиса не знает. Больше отличий нет.
--
--
-- ⚠️ ДУБЛИ И ОТСУТСТВИЕ ON CONFLICT
--
-- Приём из Kafka идёт at-least-once: offset коммитится ПОСЛЕ записи, поэтому падение
-- между ними даёт повтор. ON CONFLICT здесь не используется — в Greenplum 6 (ядро
-- PG 9.4) его нет, а держать два разных запроса под GP и PG не хочется.
--
-- Поэтому дубли допускаются при записи и снимаются при чтении:
--
--   SELECT DISTINCT ON (rq_uid) * FROM export.tfs_receipts
--   WHERE rq_uid IN (...) ORDER BY rq_uid, received_at DESC
--
-- Тот же принцип, что FINAL в ClickHouse: побеждает свежая версия. Уникального индекса
-- на rq_uid поэтому НЕТ — он бы ронял вставку пачкой.


CREATE TABLE IF NOT EXISTS export.tfs_receipts
(
    rq_uid           text        NOT NULL,   -- RqUID из квитанции: ключ сопоставления с отправкой
    file_name        text        NOT NULL,   -- File/FileInfo/Name
    scenario_id      text,                   -- ScenarioInfo/ScenarioId: маршрут ТФС
    status_code      integer     NOT NULL,   -- File/Status/StatusCode: 0 = успех; -1 = сообщение не разобрано
    status_desc      text        DEFAULT '', -- File/Status/StatusDesc: текст причины от ТФС (до 1000 символов)
    rq_tm            timestamptz,            -- RqTm из квитанции (время на стороне ТФС)
    received_at      timestamptz NOT NULL DEFAULT now(),  -- когда вычитали; по нему выбирается свежая версия
    raw_xml          text,                   -- исходное сообщение целиком — для разбора инцидентов
    kafka_topic      text        DEFAULT '', -- топик, из которого прочитано (их может быть несколько)
    kafka_partition  integer     DEFAULT -1,
    kafka_offset     bigint      DEFAULT -1
)
DISTRIBUTED BY (rq_uid);   -- ← убрать для PostgreSQL

-- Поиск идёт по rq_uid со свежестью по received_at — покрывающий индекс под DISTINCT ON.
-- file_name в индексе потому, что под одним RqUID лежит по строке на файл: агрегат File
-- в квитанции ТФС идёт [1-N], и свежая версия выбирается для каждого файла отдельно.
CREATE INDEX IF NOT EXISTS tfs_receipts_rq_uid_idx
    ON export.tfs_receipts (rq_uid, file_name, received_at DESC);

-- 🔧 Миграция существующей таблицы (колонка status_desc добавлена 2026-08-14):
--
--   ALTER TABLE export.tfs_receipts ADD COLUMN IF NOT EXISTS status_desc text DEFAULT '';
--   DROP INDEX IF EXISTS export.tfs_receipts_rq_uid_idx;   -- пересоздать с file_name


-- ─────────────────────────────────────────────────────────────────────────────
-- Полезные запросы
-- ─────────────────────────────────────────────────────────────────────────────

-- Неуспешные передачи за сутки:
--   SELECT DISTINCT ON (rq_uid) rq_tm, scenario_id, file_name, status_code
--   FROM export.tfs_receipts
--   WHERE received_at >= now() - interval '1 day'
--   ORDER BY rq_uid, received_at DESC;
--   -- затем отфильтровать status_code <> 0

-- Сообщения, которые не разобрались:
--   SELECT received_at, kafka_topic, kafka_partition, kafka_offset, left(raw_xml, 500)
--   FROM export.tfs_receipts WHERE status_code = -1 ORDER BY received_at DESC;

-- Чистка старше 90 дней (аналог TTL в ClickHouse — здесь только вручную или по расписанию):
--   DELETE FROM export.tfs_receipts WHERE received_at < now() - interval '90 days';
