# ER Export Framework

Фреймворк на базе Airflow для автоматизированной выгрузки данных из **ClickHouse** в **S3** с последующей нотификацией системы **TFS** через **Kafka**.

## Обзор архитектуры

Система построена на принципах **Metadata-driven development**. Для добавления новой таблицы в поток выгрузки не требуется написание кода — достаточно добавить запись в управляющую таблицу в ClickHouse.

### Основной поток данных:
1. **ClickHouse** — источник данных.
2. **S3 (Ceph)** — промежуточное хранилище для CSV и ZIP-архивов.
3. **Kafka** — канал уведомлений для TFS (отправка XML-сообщений).
4. **TFS** — целевая система потребления данных.

---

## Режимы работы

| Режим | Описание | Признак |
| :--- | :--- | :--- |
| **SIGMA** | Продовый режим | `ER_MODE=SIGMA` (по умолчанию) |
| **ALPHA** | Тестовый режим | `ER_MODE=ALPHA` или наличие `AIRFLOW__CTL_PIN` |

В режиме **ALPHA** автоматически:
- Используются секреты из Vault (`/vault/secrets/application`), CH-коннект `dlab-click-test`.
- Применяется `LIMIT 100` для стендов `uat`, `qa`, `ift`, `dev`.
- Пропускается отправка уведомлений в Kafka и ожидание подтверждения.

---

## Структура файлов

| Файл | Описание |
| :--- | :--- |
| `er_export.py` | **Фабрика DAG-ов**. Динамически создаёт DAG для каждой активной выгрузки. Содержит всю бизнес-логику: SQL-билдеры, хелперы и таски. |
| `er_sync.py` | **DAG синхронизации** (`export_er_sync`). Синхронизирует `export.er_wf_meta` → Airflow Variable `datalab_er_wfs`. Создаёт пул `datalab_export_er`. |
| `er_config.py` | **Конфигурация и утилиты**. Константы, маппинг типов, `DEFAULT_PARAMS`, хелперы (`obj_load`, `obj_save`, `add_note`, `get_params`). |
| `er_wf_meta.sql` | **DDL**. Структура управляющей таблицы `export.er_wf_meta` и пример вставки. |

---

## Процесс выгрузки (Pipeline)

```
init → [build_meta, export_to_s3] → pack_zip → notify_tfs → wait_confirm → save_status → schedule_next
```

| Таск | Описание |
| :--- | :--- |
| **init** | Инициализирует состояние дельты из `export.extract_current_vw`. При первом запуске — bootstrap от `lower_bound`. |
| **build_meta** | Генерирует `.meta` JSON со схемой колонок (DESCRIBE TABLE или из `fields`). |
| **export_to_s3** | Нативная выгрузка ClickHouse → S3 (`HrpClickNativeToS3ListOperator`). |
| **pack_zip** | Потоковая упаковка CSV + `.meta` + `.tkt` в ZIP без буферизации в памяти (`stream_zip`). |
| **notify_tfs** | Отправка XML-уведомления в Kafka → TFS. Пропускается если нет данных или режим ALPHA. |
| **wait_confirm** | Ожидание подтверждения из Kafka (`AwaitMessageSensor`). Пропускается при `auto_confirm=True`, в режиме ALPHA или при отсутствии данных. |
| **save_status** | Фиксирует результат в `export.extract_history` (только при полном успехе). |
| **schedule_next** | Автоматически ставит следующий запуск если дельта не догнала текущее время. |

---

## Режимы выгрузки

**Delta** — инкрементальный, состояние хранится в `export.extract_history`:
- Окно `[time_from, time_to]` берётся из `export.extract_current_vw`.
- При первом запуске `time_from = lower_bound` (по умолчанию `1970-01-01`).
- После подтверждения автоматически запускается следующий цикл.

**Recent** — скользящее окно без хранения состояния:
- Окно вычисляется как `[now() - recent_interval, now()]`.
- Подходит для таблиц без нужды в сквозной полноте дельты.

---

## Управление параметрами (DEFAULT_PARAMS)

Все параметры выгрузки хранятся в `er_config.DEFAULT_PARAMS` и могут переопределяться на уровне записи через поле `params` (JSON).

| Параметр | По умолчанию | Описание |
| :--- | :--- | :--- |
| `increment` | `60` | Шаг дельты, сек |
| `selfrun_timeout` | `10` | Задержка до следующего автозапуска, мин |
| `overlap` | `0` | Перекрытие окна назад, сек |
| `lower_bound` | `''` | Нижняя граница первой дельты; `''` → `1970-01-01` |
| `time_field` | `'extract_time'` | Поле времени в таблице-источнике |
| `recent_interval` | `3600` | Окно режима recent, сек |
| `strategy` | `'FULL_UK'` | Стратегия слияния в TFS |
| `auto_confirm` | `1` | `1` = не ждать Kafka-подтверждения |
| `confirm_timeout` | `3600` | Таймаут ожидания подтверждения, сек |
| `compression_type` | `'none'` | Тип сжатия: `none` / `gzip` / `zstd` |
| `compression_ext` | `''` | Расширение сжатого файла |
| `max_file_size` | `''` | Ограничение размера CSV, байт; `''` = без ограничений |
| `format` | `'TSVWithNames'` | Формат выгрузки ClickHouse |
| `pg_array_format` | `0` | `1` = PostgreSQL-формат массивов в TSV |
| `csv_format_params` | `''` | Доп. параметры форматирования (dict-литерал) |
| `xstream_sanitize` | `0` | `1` = экранировать спецсимволы XStream |
| `sanitize_array` | `0` | `1` = санитизировать CH-массивы в строки |
| `sanitize_list` | `''` | Список колонок для санитизации (через запятую) |

---

## Как добавить новую выгрузку

Все настройки хранятся в таблице `export.er_wf_meta`. Параметры, отличные от дефолтных, указываются в поле `params` как JSON.

### Автоматическое определение полей

Если `fields = []` или `['*']`, система выполнит `DESCRIBE TABLE` и включит все колонки.

### Bootstrap (первый запуск)

Для новых таблиц запись в `extract_history` создаётся автоматически с `time_from = lower_bound`.

### Пример вставки (без JOIN)

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk, sql_from, sql_where, params)
VALUES (
    'my_table',                  -- имя выгрузки (= имя таблицы в CH без схемы)
    'my_database',               -- БД в ClickHouse
    'hrplatform_datalab',        -- реплика (маршрут в TFS)
    'target_schema',             -- схема в целевой системе TFS
    ['id'],                      -- Unique Key
    'my_database.my_table',      -- FROM-часть
    '{condition}',               -- WHERE (плейсхолдер подставляется рантаймом)
    '{"strategy": "FULL_UK", "increment": 300, "auto_confirm": 0}'
);
```

### Пример вставки (с JOIN)

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk, sql_from, sql_join, sql_where, params)
VALUES (
    'my_table',
    'my_database',
    'hrplatform_datalab',
    'target_schema',
    ['id'],
    'my_database.my_table t1',
    'LEFT JOIN my_database_export.my_table_exp t2 ON t1.id = t2.id',
    '{condition}',
    '{"strategy": "FULL_UK"}'
);
```

> `sql_join` содержит **полное** JOIN-выражение (включая ключевое слово: `JOIN`, `LEFT JOIN`, `INNER JOIN` и т.п.).
> Для автоматического включения всех полей оставьте `fields = []`.
> Для recent-режима установите `is_recent = 1` и добавьте `"recent_interval"` в `params`.

### Пример с CTE и SETTINGS

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk, sql_from, sql_with, sql_where, sql_settings, params)
VALUES (
    'my_heavy_table',
    'my_database',
    'hrplatform_datalab',
    'target_schema',
    ['id'],
    'cte',
    'WITH cte AS (SELECT id, max(updated_at) AS updated_at FROM my_database.my_heavy_table GROUP BY id)',
    '{condition}',
    'max_threads=2, max_memory_usage=''10000000000''',
    '{"strategy": "FULL_UK"}'
);
```

> `sql_with` — полный WITH-блок (включая ключевое слово `WITH`); вставляется перед `SELECT`.
> `sql_settings` — строка в формате ClickHouse SETTINGS: `key=value, key2=value2`.

---

## Системные поля (добавляются автоматически)

| Поле | Позиция | Описание |
| :--- | :--- | :--- |
| `export_time` | первая | Правая граница дельты (`time_to`) |
| `ctl_action` | последняя | Тип действия (`'I'`) |
| `ctl_validfrom` | последняя | Техническое время загрузки |

---

## Технические детали

- **Pool**: таски выполняются в пуле `datalab_export_er` (20 слотов), создаётся автоматически DAG-ом `export_er_sync`.
- **Стриминг**: ZIP-архивация потребляет минимум памяти — `stream_zip` + multipart-загрузка в S3 без буферизации всего файла.
- **Изоляция**: фреймворк минимизирует зависимости от общих `plugins`, используя собственные хелперы в `er_config.py`.
- **Идемпотентность синхронизации**: `export_er_sync` пишет Variable только если данные изменились.
