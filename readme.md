# CTL — Change Tracking & Loading
*2026-08-31 18:22 MSK · v1.2 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Система автоматизированного управления ETL-процессами на базе **Apache Airflow** с интеграцией в **CTL API** и выполнением SQL-логики в **Greenplum**.

---

## Структура репозитория

```
ctl_worker/          # DAG'и Airflow
├── ctl_worker.py    # ⚙️ Динамическая генерация DAG'ов (1 на workflow): SQL в GP → публикация → retry
├── ctl_sensor.py    # 📡 Опрос CTL (1 мин): фильтрует активные загрузки и запускает DAG'и
├── ctl_loader.py    # 📥 Выгрузка метаданных CTL в S3 + Airflow Variables (workflows, сущности, события)
├── ctl_monitor.py   # 📊 Анализ загрузок (15 мин): SLA, retry, reStarted, Aborted
├── ctl_events.py    # 🔔 Публикация Dataset'ов CTL/{profile}/{eid}/{ename} для запуска зависимых DAG'ов
├── ctl_config.py    # 🔐 Инициализация конфигурации в Airflow Variable ctl_config (PIN-защита)
├── ctl_checker.py   # 🔍 Ручная диагностика CTL API: HTTP-запросы с шаблонами URL
├── ctl_yml.py       # 💾 Экспорт конфигурации CTL в YAML-файлы в S3 (бэкап / IaC)
├── ctl_tfs.py       # 📁 TFS → S3: по расписанию (tfs_sensor) и по Kafka-событию (tfs_kafka) с квитанцией
├── ctl_test.py      # 🧪 Симулятор: тестовые события / Dataset-сигналы / случайные триггеры
└── ctl_test_conn.py # 🔌 Мониторинг подключений (CTL, GP, PG, S3) с backoff

tools/                   # Служебные DAG'и (ручной запуск) → tools/readme.md
├── s3_from_content.py   # 📤 Загрузка текстового контента в S3
├── s3_to_s3.py          # 📦 Копирование объекта между S3-бакетами
├── s3_to_s3_test.py     # 🔍 Поиск по маске и копирование/перемещение S3→S3
├── s3_checker.py        # 👁️ Просмотр файлов S3: маска, сортировка, чтение содержимого
├── s3_set_ttl.py        # ⏱️ Управление TTL-правилами S3-бакета
├── s3_bucket_list.py    # 📋 Список всех бакетов по всем S3-подключениям
├── s3_bucket_viewer.py  # 🪣 Список бакетов через HrpS3BucketViewerOperator
├── s3_viewer.py         # 🗂️ Список ключей и чтение файлов через HrpS3*Operator
└── dummy.py             # 🎭 Шаблон DAG для проверки Markdown в Airflow UI

check/                   # DAG'и проверки и обслуживания → check/readme.md
├── show_connections.py  # 🔌 Подключения из secret backend, сгруппированные по типу
├── test_connections.py  # 🔎 Проверка доступности всех подключений + serialized_dag
├── test_hrp_operators.py # 🧪 Функциональный стенд для hrp_operators (pg↔s3↔ch)
├── test_kafka.py        # 📨 Проверка Kafka: продюсер и консьюмер тестовых сообщений
├── db_cleanup.py        # 🧹 Очистка метадаты Airflow старше N дней
└── log_cleanup.py       # 🪣 Обслуживание бакета логов задач: удаление старых объектов

plugins/             # Переиспользуемые модули (импортируются DAG'ами)
├── ctl_core.py      # 🧠 Ядро: retry, события (AND/OR), TIME-WAIT, нормализация данных
├── ctl_utils.py     # 🔧 API-обёртки, SQL, S3, конфигурация (get_config), логирование
├── s3_utils.py      # ☁️ Расширенные S3-утилиты: TTL, копирование, ZIP-распаковка
└── utils.py         # 🛠️ Общие хелперы Airflow: пулы, заметки, колбэки, timedelta
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

## Спецификации

Каждый каталог репозитория — отдельный проект со своей спецификацией в `openspec/specs/`:
что система обязана делать, требованиями и сценариями. Общий контекст (стек, контуры,
соглашения) — в `openspec/project.md`.

| Каталог | Спецификация | Состояние |
|---|---|---|
| `ctl_worker/` | `openspec/specs/ctl-worker/spec.md` | полная |
| `plugins/` | `openspec/specs/plugins/spec.md` | полная |
| `er_export/` | `openspec/specs/er-export/spec.md` | полная |
| `tfs_kafka/` | `openspec/specs/tfs-kafka/spec.md` | полная |
| `xs_export/` | `openspec/specs/xs-export/spec.md` | заготовка |
| `tools/` | `openspec/specs/tools/spec.md` | полная |
| `check/` | `openspec/specs/check/spec.md` | полная |

Заготовку `xs-export` дописывает тот, кто первым правит этот каталог: спека на код, который
никто не меняет, устаревает молча.

```bash
npm i -g --prefix ~/.local @fission-ai/openspec   # CLI (в /usr прав нет)
openspec list --specs                             # что описано
openspec show er-export                           # прочитать спеку
openspec validate --all --strict                  # проверить формат
```

Правка поведения начинается с предложения — `/opsx:propose "что меняем"`, — которое кладёт
в `openspec/changes/` дельту (`ADDED` / `MODIFIED` / `REMOVED`), задачи и обоснование.
После реализации `/opsx:archive` вливает дельту в основную спецификацию. Readme отвечает на
«как устроено», спека — на «что обязано работать»; дублировать одно в другом не нужно.

---

**Автор:** EDP.ETL | **Версия:** 1.2 | **Год:** 2026
