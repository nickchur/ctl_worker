# CTL (Change Tracking & Loading) — Система управления ETL-процессами в Airflow

---

## 📋 Обзор

Данный проект реализует интеграцию **Apache Airflow** с системой управления ETL-процессами **CTL (Change Tracking & Loading)**.  
Он обеспечивает автоматизированное управление загрузками данных из внешних источников в хранилище (Greenplum) через динамически генерируемые DAG'и.

Система поддерживает:
- Динамическую генерацию DAG'ов под каждый workflow
- Событийную оркестрацию через Dataset'ы
- Управление retry-логикой с автоматической задержкой между попытками
- Мониторинг SLA и автоматическую обработку ошибок
- Централизованное управление конфигурацией через Airflow Variables

---

## 📁 Структура компонентов

```
ctl_worker/
├── ctl_worker.py        # Основной исполнительный DAG
├── ctl_sensor.py        # Сенсор событий и триггеринг DAG'ов
├── ctl_loader.py        # Загрузчик метаданных из CTL
├── ctl_monitor.py       # Мониторинг и обработка SLA
├── ctl_events.py        # Обработка событий и Dataset'ы
├── ctl_config.py        # Управление конфигурацией
├── ctl_test.py          # Тестирование и симуляции
├── ctl_checker.py       # Диагностика API CTL
├── ctl_yml.py           # Экспорт конфигурации в YAML
├── ctl_tfs.py           # Управление TFS (Task Flow State)
├── ctl_test_conn.py     # Тестирование подключений
└── readme.md            # Документация
```

---

## 🏗️ Архитектура

Система состоит из нескольких модулей, отвечающих за различные аспекты жизненного цикла ETL:

| Компонент | Файл | Назначение |
|-----------|------|------------|
| **Worker** | `ctl_worker.py` | Динамически генерирует по одному DAG'у на каждый workflow: инициализация загрузки, выполнение SQL в GP, публикация статистики, финализация и retry. |
| **Sensor** | `ctl_sensor.py` | Каждую минуту опрашивает CTL API, фильтрует загрузки со статусом `RUNNING`/`TIME-WAIT`/`EVENT-WAIT` и запускает нужные DAG'и. |
| **Loader** | `ctl_loader.py` | Выгружает из CTL метаданные (workflows, сущности, события, категории) и кладёт в S3 + Airflow Variables. |
| **Monitor** | `ctl_monitor.py` | Анализирует активные загрузки: проверяет SLA, переводит зависшие в `Aborted`, инициирует retry и перезапуск. |
| **Events** | `ctl_events.py` | Опрашивает события CTL и публикует Airflow Dataset'ы `CTL/{profile}/{eid}/{ename}` для запуска зависимых DAG'ов. |
| **Config** | `ctl_config.py` | Инициализирует и сохраняет всю конфигурацию системы в Airflow Variable `ctl_config`; защищена PIN-кодом. |
| **Simulator** | `ctl_test.py` | Симулятор: генерирует тестовые события, Dataset-сигналы или случайные триггеры DAG'ов (активен только при `test_mode != False`). |
| **Checker** | `ctl_checker.py` | Ручная диагностика CTL API: произвольные HTTP-запросы (GET/POST/PUT/DELETE) с шаблонами URL (`{lid}`, `{wid}`, `{eid}`). |
| **YAML Export** | `ctl_yml.py` | Экспортирует конфигурацию CTL (категории, сущности, workflows) в YAML-файлы в S3 для бэкапа и IaC. |
| **TFS → S3** | `ctl_tfs.py` | Перемещение файлов из TFS в S3: по расписанию (`tfs_sensor`) и по Kafka-событию (`tfs_kafka`) с отправкой квитанции `TransferFileCephRs`. |
| **Test Conn** | `ctl_test_conn.py` | Непрерывный мониторинг доступности подключений (CTL, Greenplum, PostgreSQL, S3) с экспоненциальным backoff. |

---

## 🔧 Основные компоненты

### `ctl_worker.py` — Рабочий процесс загрузки

По одному DAG `CTL.{wf_name}` на каждый активный workflow.

**Пайплайн задач:**
1. `run_prm` — Инициализация: создание загрузки в CTL, извлечение параметров из триггера.
2. `run_tfs` *(опционально)* — Загрузка CSV-файлов из S3 в Greenplum (если `wf_tfs_in`).
3. `run_exe` — Выполнение SQL-процедуры `pr_swf_start_ctl()` в Greenplum, сбор метрик.
4. `run_out` *(опционально)* — Экспорт результата в TFS (если `wf_tfs_out`).
5. `run_end` — Публикация Dataset'ов, обновление статуса в CTL, retry при необходимости.

**Особенности:**
- Динамическая генерация: 1 DAG на каждый не-архивный workflow.
- Триггеры: Dataset-событие, cron, `DatasetOrTimeSchedule`, AND/OR-условия.
- Retry с настраиваемой задержкой (`delay` + `add` на каждый повтор).
- HTML-отчёт отправляется в CTL если результат содержит поле `html`.

---

### `ctl_sensor.py` — Сенсор событий

**Функции:**
- Проверка подключений (CTL, Greenplum, PostgreSQL, S3) в `chk_conn`.
- Опрос CTL API каждую минуту (`ctl_add_get`), лимит 50 загрузок.
- Фильтрация по статусу: `RUNNING`, `TIME-WAIT`, `EVENT-WAIT`; только `alive=ACTIVE`.
- Проверка готовности: учёт `expire`, задержки retry, дубликатов.
- Запуск DAG'а через `trigger_dag` или Dataset-публикацию (если `dag_run='dataset'`).
- Обновление статуса загрузки в CTL на `RUNNING` с пометкой `WAIT-AF`.

---

### `ctl_loader.py` — Загрузчик метаданных

Частота: по `loader_interval` (по умолчанию 15 мин).

**Сохраняет в S3 и Airflow Variables:**
- `ctl_profile` — метаданные профиля.
- `ctl_categories` — дерево категорий с именами родителей.
- `ctl_entities` — иерархия сущностей (ID → метаданные).
- `ctl_enames` — сопоставление ID → короткое имя сущности.
- `ctl_workflows` — нормализованные определения workflows (params, события, уведомления).
- `ctl_events` — расписание событий.
- `ctl_entity_events` — привязка событий к сущностям.
- `ctl_ue_events`, `ctl_prf_events` — история событий.

Изменения обнаруживаются по MD5-хэшу — лишние записи не создаются.

---

### `ctl_monitor.py` — Мониторинг

Частота: по `monitor_interval` (по умолчанию 15 мин), лимит 25 загрузок.

**Правила по статусу:**
- `ERRORCHECK` → `reRunned` 🔁
- `ERROR` + `abortOnFailure` → `Aborted` 🚫; иначе → `Completed` ✅
- `SUCCESS` → `Completed` ✅
- `TIME-WAIT` > 15 мин → `reStarted` ⚠️; иначе → `Skipped`
- `LOCK`/`LOCK-WAIT` > 5 ч → `reStarted` ⚠️
- `RUNNING` > 6 ч → `reRunned` (с логом SLA) 🚨

XCom из `ctl_monitor`: `{lid: {wid, wfn, sts, act, sch, ...}}` — передаётся в `ctl_action` для выполнения действия.

---

### `ctl_config.py` — Управление конфигурацией

Запуск `@once` (ручной). Сохраняет конфигурацию в Airflow Variable `ctl_config`.

**Управляемые параметры:**
- Профиль и иерархия: `profile`, `root_category`, `root_entity`, `ue_category`.
- Подключения: `ctl`, `gp`, `pg`, `s3` (основной), `files` (edpetl-files), `tfs` (источники файлов).
- Таймауты: `task_timeout`, `exe_timeout`, `sla_time`, `gp_timeout`.
- Пулы и лимиты: `ctl_pool_slots`, `ctl_limit`, `ctl_days`.
- PIN-защита: сохранение требует совпадения `AIRFLOW__CTL_PIN`.

---

### `ctl_events.py` — Обработка событий

Частота: по `events_interval` (по умолчанию 1–5 мин), лимит 25 событий.

**Логика:**
- Опрашивает CTL API на предмет изменившихся значений событий.
- Фильтрует по профилю и статусу `scheduled`.
- Публикует Dataset'ы `CTL/{profile}/{eid}/{ename}` для запуска зависимых DAG'ов.
- Не публикует дубликаты (проверяет уже отправленные Dataset-события).

---

### `ctl_test.py` — Симулятор

Активен только при `get_config()['test_mode'] != False`. Частота: `simulator_interval`.

**Режимы (`test_mode`):**
- `'event'` — создаёт синтетические события сущностей через POST в CTL API.
- `'dataset'` — публикует Dataset-сигналы напрямую в Airflow.
- иное — случайным образом запускает DAG'и через `trigger_dag`.

Учитывает загрузку системы (проверяет занятость пулов), игнорирует archive-категории.

---

### `ctl_checker.py` — Диагностика CTL API

Ручной запуск (schedule=None). DAG `tools_ctl_check_api`.

**Возможности:**
- Произвольные HTTP-запросы к CTL API: GET, POST, PUT, DELETE.
- URL-шаблоны с авто-подстановкой: `{lid}`, `{wid}`, `{eid}`, `{limit}`.
- Автодополнение `lid`/`wid`/`eid` из переменных `ctl_workflows` и `ctl_entities`.
- Результат логируется с `pprint`-форматированием JSON.

**Примечание по безопасности:** параметр `data` разбирается через `ast.literal_eval()` — произвольный код не выполняется.

---

### `ctl_yml.py` — Экспорт в YAML

Ручной запуск (schedule=None). DAG `CTL_{profile}.yml`.

**Функции:**
- Экспорт категорий, сущностей и workflows в отдельные YAML-файлы в S3 (`ctl_yaml/`).
- Фильтрация по диапазону ID сущностей и префиксам workflows.
- Режим `safe=True`: маскирует внешние ID сущностей (оставляет только `9410*`).
- Параметрическая подстановка: `{{ CTL_PROFILE_NAME }}` и аналоги.

**Файлы в S3:**
- `ctl_yaml/ctl_category_{root_category}.yml`
- `ctl_yaml/ctl_entity.yml`
- `ctl_yaml/ctl_wf_{wid}_{wf_name}.yml` (по одному на workflow)

---

### `ctl_tfs.py` — Перемещение файлов из TFS

Содержит два DAG'а:

- **`tfs_sensor`** — опрашивает все `tfs-in`-источники из конфига по расписанию (по умолчанию каждые 5 мин). При обнаружении файлов копирует их в S3 и публикует `DatasetAlias("TFS/<profile>")`.
- **`tfs_kafka`** — триггерный DAG: читает `TransferFileCephRq` из Kafka, копирует указанные в XML файлы, затем отправляет квитанцию `TransferFileCephRs` (StatusCode=0 при успехе, 104 при ошибке). `ScenarioId` из XML используется как `tfs_id`.

---

### `ctl_test_conn.py` — Мониторинг подключений

Частота: по `test_interval` (по умолчанию 1 мин). DAG `CTL.{profile}.test_conn`.

**Особенности:**
- По одному reschedule-сенсору `chk_{conn_id}` на каждое подключение из конфига.
- Поддерживаемые типы: PostgreSQL, S3, KerberosHttp.
- До 1000 повторов с экспоненциальным backoff (5–60 сек), таймаут 60 мин.
- `soft_fail=True` — не ронять DAG при недоступности.
- `priority_weight=999` — запускается раньше остальных задач в пуле.

---

## ⚙️ Интеграция с Airflow

### Триггеры DAG'ов

| Тип | Описание |
|-----|----------|
| **Dataset** | `CTL/wf/{wid}/...`, `CTL/entity/{eid}/...` |
| **Cron** | По расписанию (например, `0 0 * * *`) |
| **Ручной запуск** | Через UI Airflow |

### Пулы выполнения

| Пул | Назначение | Размер |
|-----|------------|--------|
| `ctl_pool` | Запросы к API CTL | 20 |
| `gp_pool` | Выполнение SQL в Greenplum | 20 |
| `pg_pool` | Операции с БД Airflow | 20 |

### XCom-ключи

| Ключ | Описание |
|------|----------|
| `params` | Параметры загрузки |
| `result` | Результат выполнения SQL (`res`) |
| `status` | Финальный статус потока |

---

## 📊 Статусы загрузок в CTL

### Жизненный цикл потока

```mermaid
stateDiagram-v2
    [*] --> INIT
    INIT --> RUNNING : run_prm()
    RUNNING --> SUCCESS : res ≥ 0
    RUNNING --> ERRORCHECK : res < 0 && retry.left > 0
    RUNNING --> ABORTED : res < 0 && retry.left = 0
    ERRORCHECK --> TIME-WAIT : set delay
    TIME-WAIT --> RUNNING : delay expired
    EVENT-WAIT --> RUNNING : event triggered
    SUCCESS --> COMPLETED : последний этап
    ABORTED --> [*]
    COMPLETED --> [*]
```

---

### Логика обработки результатов

| Код `res` | Сообщение | Иконка | Действие |
|----------|----------|--------|---------|
| `> 0` | `ok` | ✅ | Успех |
| `0` | `no` | ⚠️ | Нет данных (успешно) |
| `-7` | `retry` | ♻️ | Циклический повтор |
| `< 0` (иное) | `error` | ❌ | Ошибка → возможен retry |

---

### Конфигурация retry

```python
retry = {
    'left': N,      # Осталось попыток
    'try': 1,       # Текущая попытка
    'ok': 0,        # Успешные
    'no': 0,        # "Нет данных"
    'err': 0,       # Ошибки
    'on': ['error'],  # При каких статусах делать retry
    'delay': 'hours=+1',  # Начальная задержка
    'add': 'minutes=+5'   # Увеличение задержки при повторах
}
```

**Логика:**
- При `res < 0` и `retry['left'] > 0` → `ERRORCHECK` → `TIME-WAIT`.
- После истечения `delay` → новая попытка (`RUNNING`).
- `delay` увеличивается на `add` при каждом повторе.

---

### Особые случаи

- **Циклический retry (`res = -7`)** — используется при временных сбоях (например, недоступность источника).
- **EVENT-WAIT** — ожидание внешнего события (например, поступления файла).
- **SLA-нарушение** — мониторинг может перевести поток в `ABORTED`.

---

## 🔌 Подключения

| Коннектор | ID | Тип | Пул | Таймаут |
|----------|----|-----|-----|---------|
| CTL | `ctl` | KerberosHttp | `ctl_pool` | 10 мин |
| Greenplum | `alpha-adb_dev_comm-read` | Postgres | `gp_pool` | 4 часа |
| S3 | `s3` | S3 | `s3_pool` | — |
| Airflow DB | `airflowdb` | Postgres | `pg_pool` | — |

---

## ⚙️ Параметры CTL

### Конфигурационные параметры (из `ctl_config.py`)

| Параметр | Описание | Значение по умолчанию |
|----------|----------|----------------------|
| `profile` | Имя профиля CTL | `HR_Data` |
| `root_category` | Корневая категория workflow'ов | `p1080` |
| `root_entity` | Корневая сущность в иерархии | `941010000` |
| `ue_category` | Категория внешних событий (UE) | `p1080.sdpue` |
| `gp_conn_id` | ID подключения к Greenplum | `alpha-adb_dev_comm-read` |
| `gp_schema` | Схема GP для выполнения логики | `s_grnplm_vd_hr_edp_srv_wf` |
| `gp_timeout` | Таймаут выполнения SQL | `hours=4` |
| `gp_task_timeout` | Таймаут задачи GP | `hours=5` |
| `s3_conn_id` | Подключение к S3 | `s3` |
| `s3_bucket` | Бакет для хранения метаданных | `ctl` |
| `s3_ttl` | Время жизни объектов в S3 (дни) | `7` |
| `ctl_conn_id` | Подключение к API CTL | `ctl` |
| `ctl_timeout` | Таймаут запросов к API (сек) | `60` |
| `ctl_pool_slots` | Размер пула `ctl_pool` | `20` |
| `ctl_limit` | Лимит загрузки сущностей | `1000` |
| `ctl_days` | Глубина выгрузки событий (дней) | `5` |
| `ctl_url` | URL интерфейса CTL | `https://ctl-dev.dev.df.sbrf.ru:9080` |
| `tz` | Часовой пояс | `Europe/Moscow` |
| `expire` | Время ожидания события по умолчанию | `time=0:00` |
| `CTL_PIN` | Защитный PIN для сохранения | (скрыто) |

---

## 🚀 Запуск системы

1. Установите Airflow и зависимости.
2. Настройте подключения в Airflow UI.
3. Запустите `CTL.<profile>.config` — инициализация конфигурации.
4. Активируйте `CTL.<profile>.loader` — выгрузка метаданных.
5. Активируйте `CTL.<profile>.sensor` — отслеживание событий.

---

## 🔌 Интеграция с plugins

Все DAG'и импортируют конфигурацию через `get_config()` из `plugins.ctl_utils`:

```python
from plugins.ctl_utils import get_config, ctl_api, ...

profile = get_config()['profile']   # ленивая загрузка при первом обращении
```

`get_config()` — единственная точка входа к Airflow Variable `ctl_config`. Прямой импорт `config` не используется.

---

## 📚 Документация

- **Airflow**: 2.10.1
- **CTL API**: v4 / v5
- **Greenplum**: 6.x

---

**Автор:** EDP.ETL  
**Версия:** 1.1  
**Год:** 2026
