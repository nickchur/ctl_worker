# 🚀 ER Export Framework

Фреймворк на базе Airflow для автоматизированной выгрузки данных из **ClickHouse** в **S3** с последующей нотификацией системы **TFS** через **Kafka**.

Построен на принципах **Metadata-driven development**: для добавления новой таблицы не требуется написание кода — достаточно добавить запись в управляющую таблицу ClickHouse.

---

## 🏗️ Архитектура

### Основной поток данных

```
ClickHouse → S3 (CSV → ZIP) → Kafka (XML-уведомление) → TFS
```

### 📁 Файлы

| Файл | Описание |
| :--- | :--- |
| `er_export.py` | Фабрика DAG-ов. Динамически создаёт DAG для каждой активной выгрузки. Содержит бизнес-логику: SQL-билдеры, таски, Kafka-хелперы. |
| `er_sync.py` | DAG синхронизации `export_er_sync`. Синхронизирует `export.er_wf_meta` → Airflow Variable `datalab_er_wfs`. Создаёт пулы. |
| `er_config.py` | Конфигурация и утилиты. Константы, маппинг типов, `DEFAULT_PARAMS`, хелперы `obj_load`, `obj_save`, `add_note`, `get_params`, `on_callback`. |
| `er_wf_meta.sql` | DDL управляющей таблицы `export.er_wf_meta`. |
| `er_meta.json` | Пример формата Airflow Variable `datalab_er_wfs` (delta + recent). Поле `params` — JSON-строка внутри JSON: хранится в CH как `String`, десериализуется при чтении через `get_params`. |

---

## 🌍 Стенды и поведение

**`ENVIRONMENT`** — ограничения строк при выгрузке:

| Значение | Лимит строк |
| :--- | :--- |
| `PROM` | без ограничений |
| `UAT`, `QA`, `IFT` | 100 строк |
| `DEV` | 100 строк; `export_er_sync` создаёт `export.er_wf_meta` автоматически если таблица отсутствует |

Поведение Kafka-уведомлений и ожидания подтверждения управляется параметрами `notify_kafka` и `auto_confirm`, а не стендом.

---

## ⚡ Pipeline

```
init → [build_meta, export_to_s3] → pack_zip → notify_tfs → wait_confirm → save_status → schedule_next
```

| Таск | Описание |
| :--- | :--- |
| `init` | ⚙️ Инициализирует состояние дельты из `export.extract_current_vw`. При первом запуске — bootstrap от `lower_bound`. Применяет переопределения из DAG Params. |
| `build_meta` | 🗂️ Генерирует `.meta` JSON (`DESCRIBE TABLE`). Имена колонок, совпадающие с зарезервированными словами Hive, получают суффикс `_`. |
| `export_to_s3` | 📤 Нативная выгрузка ClickHouse → S3 (`HrpClickNativeToS3ListOperator`). |
| `pack_zip` | 📦 Потоковая упаковка CSV + `.meta` + `.tkt` в ZIP (`stream_zip`). При `send_empty=True` и нулевой дельте — пустой CSV с заголовком. |
| `notify_tfs` | 📨 Отправка XML-уведомления в Kafka → TFS. Пропускается при `notify_kafka=False` или если нет данных (и `send_empty=False`). |
| `wait_confirm` | ⏳ Ожидание подтверждения из Kafka (`AwaitMessageSensor`). Пропускается при `auto_confirm=True`. |
| `save_status` | 💾 Фиксирует результат в `export.extract_history` (trigger_rule=none_failed). |
| `schedule_next` | ⏭️ Автозапуск следующего цикла, если дельта не догнала текущее время. |

---

## 📊 Режимы выгрузки

**📈 Delta** — инкрементальный, состояние хранится в `export.extract_history`:
- Окно `[time_from, time_to]` читается из `export.extract_current_vw`.
- При первом запуске `time_from = lower_bound` (по умолчанию `1970-01-01`).
- После успешного завершения автоматически запускается следующий цикл.

**🔄 Recent** — скользящее окно без хранения состояния:
- Окно вычисляется как `[now() - recent_interval, now()]`.
- Подходит для таблиц без нужды в сквозной полноте дельты.

---

## 📦 Формат пакета

Все имена файлов — **строго нижний регистр**, только `[a-z0-9_-]`, длина ≤ 240 байт.

| Файл | Маска |
| :--- | :--- |
| ZIP-архив | `[replica]__[YYYYMMDDHHMISS]__[any].zip` |
| CSV с данными | `[schema]__[table]__[YYYYMMDDHHMISS]__[any].csv` |
| META-описание | то же имя, что у CSV, расширение `.meta` |
| TKT внутри архива | `[replica]__[YYYYMMDDHHMISS].tkt` (одна строка: `filename;rowcount`) |
| TKT снаружи архива | `[replica]__[YYYYMMDDHHMISS].tkt` (одна строка на ZIP-файл пакета) |

### CSV (TSVWithNames)

- Разделитель: `\t`; кодировка: UTF-8.
- Первая строка — заголовок с именами колонок.
- Обязательные технические поля: `export_time`, `ctl_action` (`I`), `ctl_validfrom`.

### META (JSON)

```json
{
  "mask_file":   null,
  "schema_name": "my_schema",
  "table_name":  "my_table",
  "description": "Описание или null",
  "strategy":    "FULL_UK",
  "PK":          ["id"],
  "UK":          [["id"]],
  "params":      {"separation": "\t"},
  "columns": [
    {
      "column_name": "id",
      "source_type": "BIGINT",
      "length":      null,
      "notnull":     true,
      "precision":   null,
      "scale":       null,
      "description": null
    }
  ]
}
```

> `UK` — массив массивов: `er_wf_meta.uk` (плоский список) оборачивается в `[uk]`.  
> `params.separation` указывается всегда (отклонение от дефолта `;`).  
> Для `FixedString(N)` → `length=N`; для `Decimal(P,S)` → `precision=P, scale=S, length=P`.

---

## 🗄️ Управляющая таблица `export.er_wf_meta`

| Колонка | Тип | Умолчание | Описание |
| :--- | :--- | :--- | :--- |
| `extract_name` | String | — | Имя выгрузки (table name без схемы) |
| `db_name` | String | — | База данных источника в ClickHouse |
| `replica` | String | — | Реплика-маршрутизатор TFS (ключ в `TFS_MAP` er_config.py) |
| `schema_name` | String | — | Целевая схема в `.meta`-файле для TFS |
| `pk` | Array(String) | `[]` | Первичный ключ |
| `uk` | Array(String) | `[]` | Уникальный ключ |
| `fields` | Array(String) | `[]` | SELECT-выражения; `[]` = все колонки (`DESCRIBE TABLE`) |
| `sql_from` | String | `''` | FROM-часть: `"db.table"` или псевдоним CTE |
| `sql_where` | String | `''` | WHERE-условие; `{condition}` подставляется рантаймом |
| `sql_join` | String | `''` | JOIN-clause (полное выражение, включая ключевое слово) |
| `sql_with` | String | `''` | WITH-блок (CTE); вставляется перед SELECT |
| `sql_settings` | String | `''` | SETTINGS-блок ClickHouse |
| `params` | String | `'{}'` | JSON с переопределёнными DEFAULT_PARAMS |
| `description` | String | `''` | Описание DAG-а |
| `schedule` | String | `'55 0 * * *'` | Cron-расписание первичного запуска DAG |
| `is_recent` | UInt8 | `0` | `0` = delta, `1` = recent |
| `is_active` | UInt8 | `1` | `0` = запись игнорируется синхронизацией |
| `updated_at` | DateTime64(3) | `now64(3)` | Версия строки для ReplacingMergeTree; читать с `FINAL` |

---

## ⚙️ Параметры (DEFAULT_PARAMS)

Хранятся в `er_config.DEFAULT_PARAMS`. Переопределяются в поле `params` (JSON) и через DAG Params UI при ручном запуске.

| Параметр | По умолчанию | Описание |
| :--- | :--- | :--- |
| **⏱️ Дельта / расписание** | | |
| `increment` | `60` мин | Шаг дельты; не чаще 1 пакета/час (требование TFS) |
| `selfrun_timeout` | `60` мин | Задержка до следующего автозапуска |
| `overlap` | `0` сек | Перекрытие окна назад (компенсация задержек CDC) |
| `lower_bound` | `''` | Нижняя граница первой дельты; `''` → `1970-01-01` |
| `time_field` | `'extract_time'` | Поле времени в таблице-источнике |
| `recent_interval` | `60` мин | Окно для режима recent |
| **🔀 Стратегия и подтверждение** | | |
| `strategy` | `'FULL_UK'` | Стратегия загрузки TFS: `FULL_UK`, `FULL_NO_UK`, `INC`, `APPEND` |
| `notify_kafka` | `1` | `1` = отправлять Kafka-уведомление; `0` = пропустить |
| `auto_confirm` | `1` | `1` = не ждать подтверждения от TFS |
| `confirm_timeout` | `60` мин | Таймаут ожидания подтверждения |
| `export_timeout` | `120` мин | Таймаут задачи `export_to_s3` |
| `notify_timeout` | `30` мин | Таймаут задачи `notify_tfs` |
| **📂 Файлы** | | |
| `max_file_size` | `''` | Ограничение размера CSV, байт; `''` = без ограничений |
| `send_empty` | `0` | `1` = слать пустой ZIP+Kafka при нулевой дельте (требование TFS) |
| **🔧 Формат и санитизация** | | |
| `format` | `'TSVWithNames'` | Формат выгрузки ClickHouse; **единственное поддерживаемое значение** — изменение вызывает ошибку при создании DAG |
| `pg_array_format` | `0` | `1` = PostgreSQL-формат массивов в TSV |
| `csv_format_params` | `''` | Доп. параметры форматирования (dict-литерал) |
| `xstream_sanitize` | `0` | `1` = экранировать спецсимволы XStream |
| `sanitize_array` | `0` | `1` = санитизировать CH-массивы в строки |
| `sanitize_list` | `''` | Список колонок для санитизации (через запятую) |

Все параметры доступны в DAG Params UI для переопределения при ручном запуске.

---

## 🔀 Стратегии загрузки TFS

| Стратегия | Тип | UK | `ctl_action` из источника | Операция TFS |
| :--- | :--- | :--- | :--- | :--- |
| `APPEND` | INC | нет | любое (игнорируется) | `I` |
| `FULL_UK` | FULL | UK | любое кроме `D` | `I` |
| `FULL_UK` | FULL | UK | `D` | отброс |
| `FULL_NO_UK` | FULL | нет | любое кроме `D` | `I` |
| `FULL_NO_UK` | FULL | нет | `D` | отброс |
| `INC` | INC | UK | любое кроме `D` | `U` (есть запись) / `I` (нет) |
| `INC` | INC | — | `D` | `D` (есть запись) / отброс (нет) |

> При `FULL*`-стратегиях TFS предварительно удаляет все записи snp, затем загружает новые. Строки с `ctl_action=D` отбрасываются.

---

## 🔢 Системные поля (добавляются автоматически)

| Поле | Позиция | SQL | Описание |
| :--- | :--- | :--- | :--- |
| `export_time` | первая | `{export_time}` | Правая граница дельты (`time_to`), подставляется рантаймом |
| `ctl_action` | последняя | `'I'` | Тип действия |
| `ctl_validfrom` | последняя | `now64(6)` | Время выгрузки (мкс) |

---

## ➕ Как добавить новую выгрузку

### Простая delta-выгрузка

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk, sql_from, sql_where, params)
VALUES (
    'my_table',
    'my_database',
    'hrplatform_datalab',
    'target_schema',
    ['id'],
    'my_database.my_table',
    '{condition}',
    '{"strategy": "FULL_UK", "auto_confirm": 0}'
);
```

### Delta с JOIN

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk,
     sql_from, sql_join, sql_where, params)
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

### Тяжёлый запрос с CTE и SETTINGS CH

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk,
     sql_with, sql_from, sql_where, sql_settings, params)
VALUES (
    'my_heavy_table',
    'my_database',
    'hrplatform_datalab',
    'target_schema',
    ['id'],
    'WITH cte AS (SELECT id, max(updated_at) AS updated_at FROM my_database.my_heavy_table GROUP BY id)',
    'cte',
    '{condition}',
    'max_threads=2, max_memory_usage=10000000000',
    '{"strategy": "FULL_UK"}'
);
```

### 🔄 Recent-режим

```sql
INSERT INTO export.er_wf_meta
    (extract_name, db_name, replica, schema_name, uk, sql_from, is_recent, params)
VALUES (
    'my_table',
    'my_database',
    'hrplatform_datalab',
    'target_schema',
    ['id'],
    'my_database.my_table',
    1,
    '{"recent_interval": 120}'  -- окно 120 мин
);
```

> Для recent-режима `sql_where` не нужен — условие формируется автоматически по `time_field`.

---

## 🛠️ Технические детали

- 🔤 **Hive reserved words**: `build_meta` автоматически добавляет суффикс `_` к именам колонок, совпадающим с зарезервированными словами Hive (все версии 1.2–4.0). Изменение отражается в `.meta`-файле и гарантирует корректную загрузку в KAP.
- 📭 **send_empty**: при `send_empty=1` и нулевой дельте `pack_zip` создаёт ZIP с пустым CSV (только строка заголовка) + TKT + META и отправляет Kafka-уведомление. Соответствует требованию TFS: "пустой пакет выгружается по расписанию даже при отсутствии изменений".
- 🏊 **Pool**: пул `datalab_export_er` (20 слотов) создаётся автоматически DAG-ом `export_er_sync`.
- 💧 **Стриминг**: ZIP-архивация потребляет минимум памяти — `stream_zip` + multipart-загрузка в S3 без буферизации всего файла.
- 🔒 **Изоляция**: фреймворк не зависит от общих `plugins`, используя собственные хелперы в `er_config.py`.
- ✅ **Идемпотентность**: `export_er_sync` записывает Variable только если данные изменились.
- 📝 **on_failure_callback**: при падении любого таска `on_callback` автоматически добавляет заметку с трейсом в Airflow UI (Task и DAG Run).
