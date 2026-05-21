# Инструменты тестирования и проверки (ctl/check)

Данная директория содержит инструменты и DAG'и для верификации работоспособности кастомных операторов и системных компонентов платформы.

## Состав

### [test_connections.py](test_connections.py)
**Мониторинг и проверка всех Airflow Connections.**

DAG автоматически обнаруживает все подключения в secret backend и проверяет их доступность.
*   **Kafka**: проверка bootstrap-серверов и листинг топиков (`list_topics`).
*   **ClickHouse / Trino / Postgres**: выполнение тестовых запросов (`SELECT version()` и др.).
*   **S3**: проверка прав доступа (листинг бакетов).
*   **Таймауты**: все проверки ограничены 15 секундами.
*   **Self-healing**: при сбое проверки может динамически управлять слотами пулов (ставить в 0), чтобы предотвратить запуск процессов на битых коннектах.

### [test_hrp_operators.py](test_hrp_operators.py)
**Исчерпывающий тестовый стенд для кастомных операторов (HRP Operators).**

Этот DAG предназначен для функционального и регрессионного тестирования всех 22+ операторов из пакета `hrp_operators`.

#### Основные возможности:
*   **Интерактивность:** Параметры запуска позволяют динамически указывать `conn_id` для Postgres, ClickHouse и S3.
*   **Изоляция:** DAG сам создает временные тестовые таблицы (`test_hrp_*`) и удаляет их после завершения (даже в случае ошибки).
*   **Выборочный запуск:** Можно включить или выключить группы тестов (например, только S3-утилиты или только переливки между БД) через Boolean-флаги в параметрах.

#### Группы тестов:
1.  **pg_to_s3**: Выгрузка из Postgres в S3 (включая пакетную выгрузку списков таблиц).
2.  **s3_to_ch**: Загрузка данных из S3 в ClickHouse (включая трансформации).
3.  **ch_to_s3**: Выгрузка из ClickHouse в S3 (нативная и через SQL-запросы).
4.  **db_to_db**: Переливки между PG <-> CH, PG -> PG, а также работа с инкарнациями.
5.  **s3_utils**: Проверка утилит S3 (копирование, архивация, проверка хэш-сумм, чтение файлов).
6.  **db_utils**: Выполнение команд на кластере ClickHouse и DDL в Postgres.

## Поддерживаемые операторы

Ниже представлен список кастомных операторов, доступных в пакете `hrp_operators` и покрытых тестами в данном стенде:

### 📥 Загрузка в S3
*   `HrpPostgresToS3Operator` — выгрузка из Postgres в S3.
*   `HrpPostgresToS3ListOperator` — пакетная выгрузка списка таблиц из Postgres в S3.
*   `HrpClickhouseTableToS3Operator` — выгрузка таблицы ClickHouse в S3.
*   `HrpClickhouseQueryToS3Operator` — выгрузка результата SQL-запроса ClickHouse в S3.
*   `HrpClickNativeToS3Operator` — нативная выгрузка ClickHouse в S3.
*   `HrpClickNativeToS3ListOperator` — нативная пакетная выгрузка из ClickHouse в S3.

### 📤 Загрузка из S3
*   `HrpS3ToClickhouseTableOperator` — загрузка из S3 в таблицу ClickHouse.
*   `HrpS3ToClickhouseTransformedOperator` — загрузка из S3 с трансформацией в ClickHouse.

### 🔄 Межбазовое и внутреннее перемещение
*   `HrpS3ToS3Operator` — копирование данных внутри S3.
*   `HrpPostgresToPostgresOperator` — переливка данных между инстансами Postgres.
*   `HrpPostgresIncarnationInsertOperator` — вставка инкарнаций данных в Postgres.
*   `HrpClickhouseToPostgresOperator` — переливка из ClickHouse в Postgres.
*   `HrpClickhouseToPostgresIncarnationOperator` — переливка инкарнаций из ClickHouse в Postgres.
*   `HrpPostgresToClickhouseOperator` — переливка из Postgres в ClickHouse.

### 🛠️ Инструменты и утилиты
*   `HrpClickHouseClusterOperator` — выполнение команд на кластере ClickHouse.
*   `HrpPostgresDDL` — выполнение DDL-запросов в Postgres.
*   `HrpS3ArchiveOperator` — архивация файлов в S3.
*   `HrpCheckS3FileHash` — проверка хэш-суммы файлов в S3.
*   `ClickHouseDQExportOperator` — экспорт метрик качества данных (DQ) в DataCatalog.
*   `HrpS3ListKeysOperator` — получение списка ключей (файлов) в S3.
*   `HrpS3FileReadOperator` — чтение содержимого файла из S3.
*   `HrpS3BucketViewerOperator` — просмотр содержимого бакета S3.

---

## Как запустить тесты

1.  Перейдите в Airflow UI.
2.  Найдите DAG `test_hrp_operators`.
3.  Нажмите **Trigger DAG w/ config**.
4.  Настройте необходимые подключения и выберите группы тестов.
5.  Нажмите **Trigger**.

## Добавление новых тестов

При создании нового оператора в `hrp_operators`:
1.  Добавьте импорт нового класса в `test_hrp_operators.py`.
2.  Создайте соответствующий таск в подходящей `task_group` (или создайте новую).
3.  Если требуются новые тестовые данные, обновите секцию `setup`.
4.  Обновите секцию `cleanup` для удаления созданных объектов.
