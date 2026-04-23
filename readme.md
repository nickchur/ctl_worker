# CTL — Change Tracking & Loading

Система автоматизированного управления ETL-процессами на базе **Apache Airflow** с интеграцией в **CTL API** и выполнением SQL-логики в **Greenplum**.

---

## Структура репозитория

```
ctl_worker/          # DAG'и Airflow
├── ctl_worker.py    # Основной исполнительный DAG (инициализация → выполнение → финализация)
├── ctl_sensor.py    # Сенсор: опрос CTL API и запуск DAG'ов по событиям
├── ctl_loader.py    # Выгрузка метаданных из CTL в S3 / Airflow Variables
├── ctl_monitor.py   # Мониторинг SLA, retry-логика, автоматические действия
├── ctl_events.py    # Публикация Airflow Dataset'ов при изменениях в CTL
├── ctl_config.py    # Управление конфигурацией через Airflow Variables (PIN-защита)
├── ctl_checker.py   # Диагностика доступности CTL API
├── ctl_yml.py       # Экспорт конфигурации CTL в YAML (бэкап / миграция)
├── ctl_tfs.py       # Перемещение файлов из TFS в S3, публикация Dataset
├── ctl_test.py      # Ручное тестирование и симуляции без влияния на продакшн
└── ctl_test_conn.py # Проверка доступности всех подключений

tools/                   # Служебные DAG'и (ручной запуск) → tools/readme.md
├── db_cleanup.py        # Очистка метадаты Airflow старше N дней
├── s3_from_content.py   # Загрузка текстового контента в S3
├── s3_to_s3.py          # Копирование объекта между S3-бакетами
├── s3_to_s3_test.py     # Поиск по маске и копирование/перемещение S3→S3
├── s3_checker.py        # Просмотр файлов S3: маска, сортировка, чтение содержимого
├── s3_set_ttl.py        # Управление TTL-правилами S3-бакета
├── s3_bucket_list.py    # Список всех бакетов по всем S3-подключениям
├── s3_bucket_viewer.py  # Список бакетов через HrpS3BucketViewerOperator
├── s3_viewer.py         # Список ключей и чтение файлов через HrpS3*Operator
├── show_connections.py  # Подключения из secret backend, сгруппированные по типу
├── maintenance.py       # Обслуживание S3-бакета: удаление старых объектов
└── dummy.py             # Шаблон DAG для проверки Markdown в Airflow UI

plugins/             # Переиспользуемые модули (импортируются DAG'ами)
├── ctl_core.py      # Ядро: retry, события (AND/OR), TIME-WAIT, нормализация данных
├── ctl_utils.py     # API-обёртки, SQL, S3, конфигурация (get_config), логирование
├── s3_utils.py      # Расширенные S3-утилиты: TTL, копирование, ZIP-распаковка
└── utils.py         # Общие хелперы Airflow: пулы, заметки, колбэки, timedelta
```

---

## Как работает система

1. **`ctl_loader`** (каждые 15 мин) — выгружает из CTL метаданные: workflows, сущности, события — кладёт в S3 и Airflow Variables.
2. **`ctl_sensor`** (каждую минуту) — опрашивает CTL, фильтрует активные загрузки (`RUNNING`, `TIME-WAIT`, `EVENT-WAIT`), запускает нужные DAG'и.
3. **`ctl_worker`** (per workflow) — выполняет цикл:
   - `run_prm` → инициализация загрузки в CTL
   - `run_exe` → выполнение SQL в Greenplum
   - `run_val` → публикация статистики
   - `run_sts` → решение: `success / retry / error`
   - `run_end` → финальный статус
4. **`ctl_monitor`** (периодически) — проверяет SLA, при нарушениях переводит загрузки в `ABORTED` или инициирует перезапуск.

### Жизненный цикл загрузки

```
INIT → RUNNING → SUCCESS → COMPLETED
              ↘ ERRORCHECK → TIME-WAIT → RUNNING (retry)
              ↘ ABORTED
EVENT-WAIT ──→ RUNNING
```

### Коды результата (`res`)

| Код | Значение | Действие |
|-----|----------|----------|
| `> 0` | Успех | `SUCCESS` |
| `0` | Нет данных | `SUCCESS` (no) |
| `-7` | Циклический retry | повтор |
| `< 0` | Ошибка | retry или `ABORTED` |

---

## Подключения

| Система | Connector ID | Тип |
|---------|-------------|-----|
| CTL API | `ctl` | KerberosHttp |
| Greenplum | `alpha-adb_dev_comm-read` | Postgres |
| S3 | `s3` | S3 |
| Airflow DB | `airflowdb` | Postgres |

---

## Запуск

```bash
# 1. Настроить подключения в Airflow UI

# 2. Запустить DAG инициализации конфигурации
CTL.<profile>.config

# 3. Активировать загрузчик метаданных
CTL.<profile>.loader

# 4. Активировать сенсор событий
CTL.<profile>.sensor
```

---

## Зависимости

- Apache Airflow 2.10.1
- Greenplum 6.x / psycopg2
- boto3 (S3)
- tenacity (retry)
- pendulum
- PyYAML
- hrp_operators (KerberosHttpHook)

---

**Автор:** EDP.ETL | **Версия:** 1.1 | **Год:** 2026
