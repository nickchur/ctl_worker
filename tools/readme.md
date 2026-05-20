# 🛠️ Tools — Служебные DAG'и

Инструменты для администрирования и отладки. Все DAG'и запускаются вручную (`schedule=None`).

---

## Файлы

| DAG | ID | Описание |
|---|---|---|
| `maintenance.py` | `maintenance` | Создаёт S3-бакет, удаляет старые объекты, показывает объём |
| `s3_checker.py` | `tools_s3_check_logs` | Находит файлы по маске, сортирует, читает содержимое (txt, gz, zip) |
| `s3_set_ttl.py` | `tools_s3_set_ttl` | Просматривает, устанавливает или удаляет TTL-правила S3-бакета |
| `s3_bucket_list.py` | `tools_s3_bucket_list` | Перечисляет все бакеты по всем S3-подключениям с размером и TTL |
| `s3_bucket_viewer.py` | `tools_s3_bucket_viewer` | Просматривает список бакетов через `HrpS3BucketViewerOperator` |
| `s3_viewer.py` | `tools_s3_viewer` | Выводит список ключей и читает содержимое файлов через `HrpS3*Operator` |
| `s3_from_content.py` | `tools_s3_from_content` | Загружает текстовый контент в S3 из параметров запуска |
| `s3_to_s3.py` | `tools_s3_to_s3` | Копирует один объект между S3-бакетами с опциональным сжатием |
| `s3_to_s3_test.py` | `tools_s3_to_s3_test` | Находит файлы по маске и копирует/перемещает их S3→S3 |
| `db_cleanup.py` | `tools_db_cleanup` | Очищает метадату Airflow старше N дней |
| `show_connections.py` | `tools_show_connections` | Показывает все подключения из secret backend, сгруппированные по типу |
| `test_connections.py` | `test_connections` | Проверяет каждое соединение из secret backend; таски сгруппированы по типу, TFS — отдельная группа |
| `dummy.py` | `dummy_dag` | Шаблон DAG для проверки отображения Markdown в Airflow UI |

---

## test_connections — проверки по типу

| Группа | conn_type | Проверка | Результат |
|---|---|---|---|
| `tfs` | `aws` + `tfs` в имени | `list_buckets()` (S3) | TFS S3-коннекты, отдельная группа |
| `s3` | `aws`, без `tfs` в имени | `list_buckets()` | список бакетов |
| `postgres` | `postgres` | `SELECT current_user, current_database(), inet_server_addr()` | пользователь, БД, адрес сервера |
| `ctl` | `http`, `ctl*` | `GET /v5/api/info` (KerberosHttp) | JSON-ответ API |
| `clickhouse` | `clickhouse`, `sqlite` | `SELECT version()` (ClickHouseHook) | версия сервера |
| `kafka` | `kafka` | `list_topics()` (KafkaAdminClientHook) | первые 10 топиков |
| `trino` | `trino` | `SELECT current_user, current_catalog, current_schema` (TrinoHook) | пользователь и каталог |
| `other` | прочие | — | `⏭ пропуск` |

**Поведение:** `soft_fail=True` — сбой одного таска не блокирует остальные.
Таск `summary` (trigger: `all_done`) пишет итоговую таблицу ✅/❌/⏭ в DAG note.
