-- DDL для export.tfs_receipts
-- Обратные квитанции ТФС (TransferFileCephRs), как они пришли из Kafka.
--
-- ВАЖНО: выполнять через clickhouse-client или HTTP-интерфейс, НЕ через JDBC.
-- JDBC-драйвер интерпретирует {uuid} и {replica} как именованные параметры → ошибка.
--
--
-- 📨 ЗАЧЕМ
--
-- Квитанция приходит по ВСЕМ маршрутам ТФС (xStream и ЕР) в один топик TFS.HRPLT.OUT
-- и сопоставляется с отправкой по RqUID. Топик общий, поэтому читать его напрямую из
-- выгрузок нельзя: кто первым вычитал сообщение — тот его и забрал, остальные ждут
-- вечно. Единственный потребитель — даг tfs_receipts_sync, он складывает всё сюда,
-- а выгрузки ждут появления СВОЕЙ строки по своему RqUID.
--
-- Таблица намеренно ничего не знает про ER: сюда же лягут квитанции xStream, когда
-- и он переедет на эту схему.
--
--
-- ⚠️ ДУБЛИ
--
-- Чтение из Kafka идёт at-least-once: offset коммитится ПОСЛЕ вставки, поэтому падение
-- между вставкой и коммитом даст повтор. ReplacingMergeTree схлопнет его по (rq_uid,
-- file_name); до фонового MERGE читать с FINAL.
--
-- status_code = -1 — сообщение не разобралось (битый XML или нечисловой StatusCode).
-- Такие строки сохраняются с исходным текстом в raw_xml: потерять квитанцию хуже,
-- чем сохранить её неразобранной.


CREATE TABLE IF NOT EXISTS export.tfs_receipts ON CLUSTER datalab
(
    rq_uid           String                    COMMENT 'RqUID из квитанции — ключ сопоставления с отправкой',
    file_name        String                    COMMENT 'File/FileInfo/Name',
    scenario_id      String                    COMMENT 'ScenarioInfo/ScenarioId — маршрут ТФС',
    status_code      Int32                     COMMENT 'Status/StatusCode: 0 = успех; -1 = сообщение не разобрано',
    rq_tm            Nullable(DateTime64(3))   COMMENT 'RqTm из квитанции (время на стороне ТФС)',
    received_at      DateTime64(3) DEFAULT now64(3)  COMMENT 'Когда мы вычитали сообщение; версия строки для ReplacingMergeTree',
    raw_xml          String                    COMMENT 'Исходное сообщение целиком — для разбора инцидентов',
    kafka_partition  Int32         DEFAULT -1  COMMENT 'Партиция, из которой прочитано',
    kafka_offset     Int64         DEFAULT -1  COMMENT 'Offset сообщения'
)
ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/tfs_receipts_{uuid}', '{replica}', received_at)
ORDER BY (rq_uid, file_name)
TTL toDateTime(received_at) + INTERVAL 90 DAY;

-- Таблица операционная: она нужна, пока идёт ожидание подтверждения и разбор инцидентов.
-- 90 дней с запасом перекрывают оба сценария.


-- ─────────────────────────────────────────────────────────────────────────────
-- Полезные запросы
-- ─────────────────────────────────────────────────────────────────────────────

-- Неуспешные передачи за сутки:
--   SELECT rq_tm, scenario_id, file_name, status_code
--   FROM export.tfs_receipts FINAL
--   WHERE status_code != 0 AND received_at >= now() - INTERVAL 1 DAY
--   ORDER BY received_at DESC;

-- Отправлено, но квитанции нет (по данным ER):
--   SELECT s.file_name, s.notified_at
--   FROM export.er_sent_files FINAL AS s
--   LEFT ANTI JOIN export.tfs_receipts FINAL AS r USING (rq_uid)
--   WHERE s.notified_at > toDateTime64(0, 3)
--     AND s.notified_at < now() - INTERVAL 1 HOUR;

-- Сообщения, которые не разобрались:
--   SELECT received_at, kafka_partition, kafka_offset, substring(raw_xml, 1, 500)
--   FROM export.tfs_receipts FINAL
--   WHERE status_code = -1 ORDER BY received_at DESC;
