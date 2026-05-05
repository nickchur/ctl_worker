# ER Export Framework

Фреймворк на базе Airflow для автоматизированной выгрузки данных из **ClickHouse** в **S3** с последующей нотификацией системы **TFS** через **Kafka**.

## Обзор архитектуры

Система построена на принципах **Metadata-driven development**. Для добавления новой таблицы в поток выгрузки не требуется написание кода — достаточно добавить запись в управляющую таблицу в ClickHouse.

### Основной поток данных:
1. **ClickHouse** — источник данных.
2. **S3 (Ceph)** — промежуточное хранилище для CSV и ZIP архивов.
3. **Kafka** — канал уведомлений для TFS (отправка XML-сообщений).
4. **TFS** — целевая система потребления данных.

---

## Структура файлов

| Файл | Описание |
| :--- | :--- |
| `er_export.py` | **Фабрика DAG-ов**. Динамически создаёт DAG для каждой активной выгрузки. Содержит всю бизнес-логику: SQL-билдеры, хелперы, `export_tg` TaskGroup и `create_export_dag`. |
| `er_sync.py` | **DAG синхронизации**. Переносит настройки из `export.er_wf_meta` в Airflow Variable (`datalab_er_wfs`). Подтягивает комментарий таблицы из `system.tables`, если `description` в метаданных пуст. |
| `er_config.py` | **Конфигурация**. Константы подключений, маппинг типов, системные поля, пул `datalab_export_er`. При импорте автоматически создаёт пул в Airflow если его нет. |
| `er_wf_meta.sql` | **DDL**. Структура управляющей таблицы `export.er_wf_meta` и примеры заполнения. |

---

## Как добавить новую выгрузку

Все настройки хранятся в таблице `export.er_wf_meta`.

### Пример добавления:

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, strategy, uk, fields, sql_from, sql_where, increment, selfrun_timeout, auto_confirm)
VALUES (
    'my_table_name',           -- Имя выгрузки
    'my_database',             -- БД в ClickHouse
    'hrplatform_datalab',      -- Реплика (маршрут в TFS)
    'target_schema',           -- Схема в целевой системе
    'FULL_UK',                 -- Стратегия (FULL_UK/DELTA_UK/...)
    ['id'],                    -- Unique Key
    ['col1', 'col2', 'col3'],  -- Список полей (без export_time/ctl_*)
    'my_database.my_table',    -- Источник (FROM)
    '{condition}',             -- WHERE-условие
    60,                        -- Инкремент дельты (сек)
    10,                        -- Таймаут автозапуска (мин)
    1                          -- 1 = автоподтверждение, 0 = ждать Kafka
);
```

После вставки:
1. DAG `er_sync__datalab_er_wfs` подхватит изменения (запускается каждые 5 минут).
2. Новый DAG вида `export_er__hrplatform_datalab__my_table_name` автоматически появится в Airflow.

---

## Логика обработки данных (Task Group `er_export`)

Каждый сгенерированный DAG выполняет следующие шаги:

1. **init** — читает состояние дельты из реестра (`export.extract_current_vw`), применяет параметры DAG-запуска (extract_time, condition, strategy и др.).
2. **build_meta** — анализирует структуру таблицы через `DESCRIBE TABLE`, формирует `.meta` файл (JSON). Порядок колонок совпадает с порядком в CSV: `export_time` → поля таблицы → `ctl_action`, `ctl_validfrom`. Комментарии колонок ClickHouse переносятся в поле `description`.
3. **export_to_s3** — стримит данные из ClickHouse напрямую в S3 (`HrpClickNativeToS3ListOperator`).
4. **pack_zip** — потоково упаковывает CSV (из S3), `.meta` и `.tkt` в ZIP через `stream_zip`; загружает результат в S3 через `load_file_obj` (boto3 TransferManager, multipart). CSV не загружается в память целиком. Исходный CSV-файл удаляется.
5. **notify_tfs** — формирует XML и отправляет в Kafka (топик `TFS.HRPLT.IN`). В тестовом режиме (`ER_MODE=test`) пропускается.
6. **wait_confirm** *(опционально)* — если `auto_confirm = 0`, ждёт подтверждения из Kafka (топик `TFS.HRPLT.OUT`). Иначе — `EmptyOperator`.
7. **save_status** — записывает результат в историю (`export.extract_history`).
8. **schedule_next** — если дельта не догнала текущее время, планирует следующий запуск через `selfrun_timeout` минут.

---

## Системные поля

В каждую выгрузку автоматически добавляются (настраивается в `er_config.py`):

| Поле | Позиция в CSV | Описание |
| :--- | :--- | :--- |
| `export_time` | первая | Техническое время выгрузки (правая граница дельты) |
| `ctl_action` | последняя | Тип действия (`'I'` — Insert) |
| `ctl_validfrom` | последняя | Техническое время валидности записи |

---

## Пул

Все задачи выполняются в пуле **`datalab_export_er`** (20 слотов). Пул создаётся автоматически при первом импорте `er_config.py`.

---

## Конфигурация подключений

| Параметр | prod | test (`ER_MODE=test`) |
| :--- | :--- | :--- |
| ClickHouse conn | `dlab-click` | `dlab-click-test` (из Vault) |
| S3 conn | `s3-tfs-hrplt` | `s3-archive` |
| Kafka out | `tfs-kafka-out` / `TFS.HRPLT.IN` | (пропускается) |
| Kafka in | `tfs-kafka-in` / `TFS.HRPLT.OUT` | (пропускается) |

---

## Ограничения

- **Формат**: поддерживается только `TSVWithNames`.
- **Тестовые среды**: на IFT/UAT/QA/DEV автоматически применяется `LIMIT 100`.
- **Упаковка**: `pack_zip` работает потоково — `stream_zip` генерирует ZIP чанками без буферизации всего файла, `load_file_obj` передаёт их в S3 через multipart upload. Для разбивки очень больших таблиц на части использовать `max_file_size`.
