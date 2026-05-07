# 🚀 ER Export Framework

Фреймворк на базе Airflow для автоматизированной выгрузки данных из **ClickHouse** в **S3** с последующей нотификацией системы **TFS** через **Kafka**.

## 🏗️ Обзор архитектуры

Система построена на принципах **Metadata-driven development**. Для добавления новой таблицы в поток выгрузки не требуется написание кода — достаточно добавить запись в управляющую таблицу в ClickHouse.

### Основной поток данных:
1. **ClickHouse** — источник данных.
2. **S3 (Ceph)** — промежуточное хранилище для CSV и ZIP-архивов.
3. **Kafka** — канал уведомлений для TFS (отправка XML-сообщений).
4. **TFS** — целевая система потребления данных.

---

## 🌍 Стенды и поведение

Поведение фреймворка управляется двумя переменными окружения.

**`ENV_SPACE`** — переключает CH-коннект и источник секретов:

| Значение | CH-коннект | S3 | Секреты |
| :--- | :--- | :--- | :--- |
| (не задана) | `dlab-click` | `s3-tfs-hrplt` | стандартные Airflow connections |
| `ALPHA` | `dlab-click-test` | `s3-archive` | Vault `/vault/secrets/application` |

**`ENVIRONMENT`** — определяет ограничения и поведение на стенде (значение приводится к uppercase):

| Значение | Лимит строк | Kafka / подтверждение |
| :--- | :--- | :--- |
| `PROM` | без ограничений | отправляется в штатном режиме |
| `UAT`, `QA`, `IFT` | 100 строк | отправляется в штатном режиме |
| `DEV` | 100 строк | **пропускается** (notify_tfs и wait_confirm — skip) |

---

## 📁 Структура файлов

| Файл | Описание |
| :--- | :--- |
| `er_export.py` | **Фабрика DAG-ов**. Динамически создаёт DAG для каждой активной выгрузки. Содержит всю бизнес-логику: SQL-билдеры, хелперы и таски. |
| `er_sync.py` | **DAG синхронизации** (`export_er_sync`). Синхронизирует `export.er_wf_meta` → Airflow Variable `datalab_er_wfs`. Создаёт пул `datalab_export_er`. |
| `er_config.py` | **Конфигурация и утилиты**. Константы, маппинг типов, `DEFAULT_PARAMS`, хелперы (`obj_load`, `obj_save`, `add_note`, `get_params`). |
| `er_wf_meta.sql` | **DDL**. Структура управляющей таблицы `export.er_wf_meta` и пример вставки. |

---

## 📦 Формат пакета ЕР

Каждый пакет — ZIP-архив с набором файлов. Требования ТФС (Полозов А.А., нояб. 2025; Коновалов М.В., апр. 2026).

### Именование

Все имена файлов — **строго нижний регистр**, только `[a-z0-9_-]`, длина ≤ 240 байт.

| Файл | Маска |
| :--- | :--- |
| ZIP-архив | `[replica]__[YYYYMMDDHHMISS]__[any].zip` |
| CSV с данными | `[schema]__[table]__[YYYYMMDDHHMISS]__[any].csv` |
| META-описание | имя совпадает с CSV, расширение `.meta` |
| TKT внутри архива | `[replica]__[YYYYMMDDHHMISS].tkt` |
| TKT снаружи архива | `[replica]__[YYYYMMDDHHMISS].tkt` |

### CSV (формат TSVWithNames)

Фреймворк выгружает данные в формате **TSVWithNames** (ClickHouse native). Разделитель `\t` (TAB) явно задаётся в поле `params.separation` META-файла. TFS принимает любой разделитель если он указан в META.

- Кодировка: UTF-8
- Первая строка — заголовок с именами колонок
- Обязательные технические поля: `ctl_action` (`I`/`U`/`D`), `ctl_validfrom` (микросекунды)

### META (JSON)

```json
{
  "mask_file":   null,
  "schema_name": "my_schema",
  "table_name":  "my_table",
  "description": "Описание таблицы или null",
  "strategy":    "FULL_UK",
  "PK":          ["id"],
  "UK":          [["id", "id2"]],
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

> `mask_file` — `null` когда имя META совпадает с именем CSV (всегда в нашем случае).  
> `UK` — массив массивов: каждый элемент — один уникальный индекс; `er_wf_meta.uk` (плоский список) автоматически оборачивается в `[uk]`.  
> `params.separation` указывается всегда (отклонение от дефолта `;`).  
> `columns` генерируется из `DESCRIBE TABLE`; для `FixedString(N)` → `length=N`, для `Decimal(P,S)` → `precision=P, scale=S, length=P`.

### TKT

- TKT **внутри** архива: одна строка на CSV-файл в формате `filename;rowcount` (строки без заголовка).
- TKT **снаружи** архива: одна строка на ZIP-файл пакета (передаётся в TFS через Kafka).
- В тикете НЕ должно быть мета-файлов.

---

## ⚡ Процесс выгрузки (Pipeline)

```
init → [build_meta, export_to_s3] → pack_zip → notify_tfs → wait_confirm → save_status → schedule_next
```

| Таск | Описание |
| :--- | :--- |
| **init** | Инициализирует состояние дельты из `export.extract_current_vw`. При первом запуске — bootstrap от `lower_bound`. |
| **build_meta** | Генерирует `.meta` JSON со схемой колонок (`DESCRIBE TABLE` или из `fields`). |
| **export_to_s3** | Нативная выгрузка ClickHouse → S3 (`HrpClickNativeToS3ListOperator`). |
| **pack_zip** | Потоковая упаковка CSV + `.meta` + `.tkt` в ZIP без буферизации в памяти (`stream_zip`). |
| **notify_tfs** | Отправка XML-уведомления в Kafka → TFS. Пропускается если нет данных или `ENV_STAND=dev`. |
| **wait_confirm** | Ожидание подтверждения из Kafka (`AwaitMessageSensor`). Пропускается при `auto_confirm=1`, `ENV_STAND=dev` или отсутствии данных. |
| **save_status** | Фиксирует результат в `export.extract_history` (только при полном успехе всех upstream-тасков). |
| **schedule_next** | Автоматически ставит следующий запуск если дельта не догнала текущее время. |

---

## 📊 Режимы выгрузки

**Delta** — инкрементальный, состояние хранится в `export.extract_history`:
- Окно `[time_from, time_to]` берётся из `export.extract_current_vw`.
- При первом запуске `time_from = lower_bound` (по умолчанию `1970-01-01`).
- После успешного завершения автоматически запускается следующий цикл.

**Recent** — скользящее окно без хранения состояния:
- Окно вычисляется как `[now() - recent_interval, now()]`.
- Подходит для таблиц без нужды в сквозной полноте дельты.

---

## 🗄️ Управляющая таблица `export.er_wf_meta`

| Колонка | Тип | Умолчание | Описание |
| :--- | :--- | :--- | :--- |
| `extract_name` | String | — | Имя выгрузки (table name без схемы) |
| `db_name` | String | — | База данных источника в ClickHouse |
| `replica` | String | — | Реплика-маршрутизатор TFS (ключ в `TFS_MAP` er_config.py) |
| `schema_name` | String | — | Целевая схема в `.meta`-файле для TFS |
| `pk` | Array(String) | `[]` | Список колонок первичного ключа |
| `uk` | Array(String) | `[]` | Список колонок уникального ключа |
| `fields` | Array(String) | `[]` | SELECT-выражения; `[]` = все колонки (`DESCRIBE TABLE`) |
| `sql_from` | String | `''` | FROM-часть запроса: `"db.table"` или псевдоним CTE |
| `sql_where` | String | `''` | WHERE-условие; `{condition}` подставляется рантаймом |
| `sql_join` | String | `''` | JOIN-clause (полное выражение, включая ключевое слово) |
| `sql_with` | String | `''` | WITH-блок (CTE); вставляется перед SELECT |
| `sql_settings` | String | `''` | SETTINGS-блок ClickHouse; вставляется в конец запроса |
| `params` | String | `'{}'` | JSON с переопределёнными DEFAULT_PARAMS |
| `description` | String | `''` | Описание DAG-а (отображается в Airflow UI) |
| `schedule` | String | `'55 0 * * *'` | Cron-расписание первичного запуска DAG |
| `is_recent` | UInt8 | `0` | `0` = delta-выгрузка, `1` = recent (скользящее окно) |
| `is_active` | UInt8 | `1` | `0` = запись игнорируется при синхронизации |

---

## ⚙️ Управление параметрами (DEFAULT_PARAMS)

Все параметры выгрузки хранятся в `er_config.DEFAULT_PARAMS` и могут переопределяться на уровне записи через поле `params` (JSON).

| Параметр | По умолчанию | Описание |
| :--- | :--- | :--- |
| **Дельта / расписание** | | |
| `increment` | `3600` | Шаг дельты, сек (не чаще 1 пакета/час — требование ТФС) |
| `selfrun_timeout` | `60` | Задержка до следующего автозапуска, мин (не чаще 1 пакета/час) |
| `overlap` | `0` | Перекрытие окна назад, сек |
| `lower_bound` | `''` | Нижняя граница первой дельты; `''` → `1970-01-01` |
| `time_field` | `'extract_time'` | Поле времени в таблице-источнике |
| `recent_interval` | `3600` | Окно режима recent, сек |
| **Стратегия и подтверждение** | | |
| `strategy` | `'FULL_UK'` | Стратегия загрузки TFS: `FULL_UK`, `FULL_NO_UK`, `INC`, `APPEND` (см. ниже) |
| `auto_confirm` | `1` | `1` = не ждать Kafka-подтверждения |
| `confirm_timeout` | `3600` | Таймаут ожидания подтверждения, сек |
| **Файлы и сжатие** | | |
| `compression_type` | `'none'` | Тип сжатия: `none` / `gzip` / `zstd` |
| `compression_ext` | `''` | Расширение сжатого файла |
| `max_file_size` | `''` | Ограничение размера CSV, байт; `''` = без ограничений |
| **Формат и санитизация** | | |
| `format` | `'TSVWithNames'` | Формат выгрузки ClickHouse |
| `pg_array_format` | `0` | `1` = PostgreSQL-формат массивов в TSV |
| `csv_format_params` | `''` | Доп. параметры форматирования (dict-литерал) |
| `xstream_sanitize` | `0` | `1` = экранировать спецсимволы XStream |
| `sanitize_array` | `0` | `1` = санитизировать CH-массивы в строки |
| `sanitize_list` | `''` | Список колонок для санитизации (через запятую) |

---

## 📋 Стратегии загрузки TFS и обработка CTL_ACTION

| Стратегия | Тип | UK | ctl_action источника | Операция TFS |
| :--- | :--- | :--- | :--- | :--- |
| `APPEND` | INC | NO UK | любое (игнорируется) | `I` |
| `FULL_UK` | FULL | UK | любое кроме `D` | `I` |
| `FULL_UK` | FULL | UK | `D` | **отброс** |
| `FULL_NO_UK` | FULL | NO UK | любое кроме `D` | `I` |
| `FULL_NO_UK` | FULL | NO UK | `D` | **отброс** |
| `INC` | INC | UK | любое кроме `D` | `U` (запись есть) / `I` (нет) |
| `INC` | INC | — | `D` | `D` (запись есть) / **отброс** (нет) |

> **Примечания:**
> - При всех `FULL%`-стратегиях TFS предварительно отправляет `op_type=D` для всех записей snp (очистка слоя), затем загружает новые данные. Строки с `ctl_action=D` из источника при этом отбрасываются.
> - При `APPEND` поле `ctl_action` игнорируется TFS; блок `before` должен отсутствовать, `op_type` и `ctl_action` всегда `I`.
> - При `INC` некорректное значение `ctl_action` из источника приводит к ошибке в модуле `pkg-snp` — важно передавать корректное значение.
> - Таблицы без первичного ключа не поддерживаются `pkg-diff/hist`, `pkg-snp`; при необходимости добавляется суррогатный ключ.

---

## ➕ Как добавить новую выгрузку

Все настройки хранятся в таблице `export.er_wf_meta`. Параметры, отличные от дефолтных, указываются в поле `params` как JSON.

### Автоматическое определение полей

Если `fields = []` или `['*']`, система выполнит `DESCRIBE TABLE` и включит все колонки.

### Bootstrap (первый запуск)

Для новых таблиц запись в `extract_history` создаётся автоматически с `time_from = lower_bound`. На стенде `dev` таблица `er_wf_meta` создаётся автоматически если её нет.

### Пример: простая delta-выгрузка

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
    '{"strategy": "FULL_UK", "auto_confirm": 0}'
);
```

### Пример: delta-выгрузка с JOIN

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

### Пример: тяжёлый запрос с CTE и настройками CH

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
    'cte',                                           -- FROM-часть = псевдоним CTE
    '{condition}',
    'max_threads=2, max_memory_usage=10000000000',
    '{"strategy": "FULL_UK"}'
);
```

> `sql_with` — полный WITH-блок (включая ключевое слово `WITH`); вставляется перед `SELECT`.
> `sql_settings` — строка в формате ClickHouse SETTINGS: `key=value, key2=value2`.

> Для автоматического включения всех полей оставьте `fields = []`.
> Для recent-режима установите `is_recent = 1` и добавьте `"recent_interval"` в `params`.

---

## 🔧 Системные поля (добавляются автоматически)

| Поле | Позиция | SQL | Описание |
| :--- | :--- | :--- | :--- |
| `export_time` | первая | `{export_time}` | Правая граница дельты (`time_to`), подставляется рантаймом |
| `ctl_action` | последняя | `'I'` | Тип действия (всегда `I`; для `D`-записей используйте INC-стратегию) |
| `ctl_validfrom` | последняя | `now64(6)` | Время выгрузки с точностью до микросекунды (`YYYY-MM-DD HH:mm:ss.SSSSSS`) |

---

## 🛠️ Технические детали

- **Pool**: таски выполняются в пуле `datalab_export_er` (20 слотов), создаётся автоматически DAG-ом `export_er_sync`.
- **Стриминг**: ZIP-архивация потребляет минимум памяти — `stream_zip` + multipart-загрузка в S3 без буферизации всего файла.
- **Изоляция**: фреймворк минимизирует зависимости от общих `plugins`, используя собственные хелперы в `er_config.py`.
- **Идемпотентность синхронизации**: `export_er_sync` пишет Variable только если данные изменились.
- **Соответствие стандарту ТФС**: имена файлов — нижний регистр; `ctl_validfrom` — `now64(6)` (мкс); META содержит `mask_file`, `description`, `length`/`precision`/`scale`; UK — массив массивов; частота — не чаще 1 пакета/час (`increment=3600`, `selfrun_timeout=60`).
