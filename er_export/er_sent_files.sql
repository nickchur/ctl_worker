-- DDL для export.er_sent_files
-- 2026-08-12 19:16 MSK · v1.0 · Чуркин Николай · nschurkin@sber.ru
-- Вариант ClickHouse (STORAGE = 'ch' в plugins/tfs_utils.py).
-- Для PostgreSQL/Greenplum — er_sent_files_pg.sql; при STORAGE = 's3' таблица не нужна.
-- Очередь отправки и реестр отправленных файлов ER.
--
-- ВАЖНО: выполнять через clickhouse-client или HTTP-интерфейс, НЕ через JDBC.
-- JDBC-драйвер интерпретирует {uuid} и {replica} как именованные параметры → ошибка.
--
--
-- 📦 ЗАЧЕМ
--
-- Строка на ФАЙЛ, а не на выгрузку: в пакете ЕР несколько архивов плюс summary-тикет,
-- и у каждого свой RqUID. Именно RqUID связывает нашу отправку с обратной квитанцией
-- в export.tfs_receipts. В export.extract_history этого нет: там строка на выгрузку,
-- файлы лежат массивом, и пофайловый RqUID туда не ложится.
--
-- Таблица одновременно очередь и история:
--
--   notified_at = 0  → файл поставлен в очередь, но ещё не отправлен
--   notified_at > 0  → уведомление ушло в Kafka, ждём квитанцию
--
-- Ставит в очередь таск make_summary пакета, разгребает даг tfs_kafka_snd —
-- он единственный, кто шлёт файлы ЕР, и потому единственное место, где соблюдается
-- темп, задекларированный ТФС (см. TFS_ROUTES в plugins/tfs_utils.py).
--
--
-- ⚠️ ОБНОВЛЕНИЕ СТРОКИ
--
-- ClickHouse не про UPDATE: отправитель ДОПИСЫВАЕТ ту же строку с проставленным
-- notified_at и большим updated_at, а ReplacingMergeTree схлопывает по rq_uid.
-- До фонового MERGE обе версии живы, поэтому очередь читать ТОЛЬКО с FINAL —
-- иначе уже отправленный файл уедет повторно.


CREATE TABLE IF NOT EXISTS export.er_sent_files ON CLUSTER datalab
(
    rq_uid       String                    COMMENT 'RqUID, сгенерирован нами до отправки; ключ сопоставления с квитанцией',
    file_name    String                    COMMENT 'Имя ZIP-архива либо summary-тикета',
    replica      String                    COMMENT 'Группа поставок (replica целиком, с суффиксом)',
    scenario_id  String                    COMMENT 'Маршрут ТФС — по нему считаются лимиты отправки',
    package_ts   DateTime64(3)             COMMENT 'Метка пакета (logical_date рана); связывает файлы одного тикета',
    dag_id       String        DEFAULT ''  COMMENT 'DAG, поставивший файл в очередь',
    run_id       String        DEFAULT ''  COMMENT 'Ран, поставивший файл в очередь',
    created_at   DateTime64(3) DEFAULT now64(3)        COMMENT 'Когда файл встал в очередь',
    notified_at  DateTime64(3) DEFAULT toDateTime64(0, 3) COMMENT '0 = ещё в очереди; иначе время отправки в Kafka',
    updated_at   DateTime64(3) DEFAULT now64(3)        COMMENT 'Версия строки для ReplacingMergeTree'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/er_sent_files_{uuid}', '{replica}', updated_at)
ORDER BY (rq_uid);

-- ⚠️ Обновление строки — это ДОПИСАННАЯ версия: отметка отправки вставляет строку заново
-- с бо́льшим updated_at, а ReplacingMergeTree по ключу rq_uid оставляет последнюю. Всё,
-- что во вставке не названо, у отправленного файла обнулится — так уже терялись dag_id
-- и run_id. Со стороны кода это подпёрто списком CH_SENT_COLS в plugins/tfs_utils.py:
-- обе вставки собираются из него, а не перечисляют колонки руками.


-- ─────────────────────────────────────────────────────────────────────────────
-- Полезные запросы
-- ─────────────────────────────────────────────────────────────────────────────

-- Очередь на отправку (что увидит tfs_kafka_snd):
--   SELECT package_ts, replica, file_name, created_at
--   FROM export.er_sent_files FINAL
--   WHERE notified_at = toDateTime64(0, 3)
--   ORDER BY package_ts, created_at;

-- Расход лимитов по маршруту. Все окна СКОЛЬЗЯЩИЕ: полночь суточный бюджет не обнуляет,
-- он освобождается постепенно, по мере ухода отправок за границу окна.
--   SELECT
--       countIf(notified_at > now64(3) - INTERVAL 1 SECOND) AS sec,
--       countIf(notified_at > now64(3) - INTERVAL 1 MINUTE) AS min,
--       countIf(notified_at > now64(3) - INTERVAL 1 HOUR)   AS hour,
--       countIf(notified_at > now64(3) - INTERVAL 1 DAY)    AS day
--   FROM export.er_sent_files FINAL
--   WHERE scenario_id = 'HRPLATFORM-4000' AND notified_at > toDateTime64(0, 3);

-- Судьба одного пакета: что отправлено и что подтверждено:
--   SELECT s.file_name, s.notified_at, r.status_code, r.rq_tm
--   FROM export.er_sent_files AS s FINAL
--   LEFT JOIN export.tfs_receipts AS r FINAL USING (rq_uid)
--   WHERE s.replica = 'hrplatform_datalab__1'
--   ORDER BY s.package_ts DESC, s.file_name;
