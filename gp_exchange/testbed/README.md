# Тестовый стенд обмена: Greenplum на PostgreSQL

Проверяет тракт целиком — от сборки пакета в базе-источнике до целевых таблиц
ClickHouse: `pr_exchange` (PG) → TSV → S3 → `import_gp_ue_exchange` → `gp_vw_*`.

Полной копии витрин Greenplum здесь нет и не нужно: пакет собирается из семи вьюх,
и важны их имена, ключи инкремента и набор колонок — ровно то, что разбирает
`tfs_exchange_import.py` на приёмной стороне.

## Из чего состоит

| Файл | Что делает |
| :--- | :--- |
| `10_schema.sql` | Схемы `s_grnplm_vd_hr_edp_*` и таблицы-заглушки под семь потоков; журнал `tb_exchange_log` |
| `20_views.sql` | `vw_exchange`, `vw_exchange_log`, `vw_exchange_log_keys` — перенесены из HR_Data как есть |
| `30_pr_exchange.sql` | Сборщик пакета из HR_Data; убраны только `DISTRIBUTED`, `appendonly` и `EXECUTE ON ANY` |
| `40_data.sql` | Тестовые данные: два дня, кириллица, спецсимволы |
| `50_clickhouse.sql` | Приёмные таблицы: `gp_ue_exchange_load`, `gp_ue_exchange` (партиции по `_gp_name`), `gp__exchange_log` |
| `55_seed_log.sql` | Затравка журнала загрузки — без неё ветвление пустое, см. ниже |
| `60_export.sh` | Выгрузка пакета в файл формата ТФС и укладка в S3 |

## Как развернуть (тестовый стенд `testsrv`)

```bash
docker exec -i aftest-postgres psql -U airflow -d postgres -c "CREATE DATABASE gp_test;"
for f in 10_schema 20_views 30_pr_exchange 40_data; do
    docker exec -i aftest-postgres psql -U airflow -d gp_test -v ON_ERROR_STOP=1 < $f.sql
done
docker exec -i aftest-clickhouse clickhouse-client --multiquery < 50_clickhouse.sql
docker exec -i aftest-clickhouse clickhouse-client --multiquery < 55_seed_log.sql

set -a; . /opt/aftest/infra/.env; set +a
export MINIO_USER="$MINIO_ROOT_USER" MINIO_PASS="$MINIO_ROOT_PASSWORD"
bash 60_export.sh 0          # соберёт пакет и положит в s3://tfshrplt/to/CAPUE/pkap1080_to_hrplt/
```

Дальше — `airflow dags trigger import_gp_ue_exchange`. Целевые таблицы `gp_vw_*` создаются
руками: `process_any` намеренно не создаёт их сам, а печатает готовый `CREATE TABLE`
и падает с просьбой создать. DDL берётся из лога упавшей задачи.

## Чего на стенде нет и почему

- **Реплик и кластера.** ClickHouse одиночный, поэтому `ON_CLUSTER` и `REPLICATED`
  переопределены пустыми в мосте `/opt/aftest/lib/CI06932748/.../tfs_exchange_common.py`.
  Весь остальной код — SQL, ветвление, проверки — тот же, что в бою.
- **Боевого пакета `CI06932748`.** Мост подкладывает настоящий модуль из `dags_folder`;
  чтобы его видел планировщик, в `/opt/aftest/airflow.env` прописан
  `PYTHONPATH=/opt/aftest/lib`.

## Формат файла

Диктует задача `import_files`: `CustomSeparatedWithNames`, разделитель — таб,
экранирование CSV, `format_csv_allow_double_quotes=False`. Последнее и есть причина
трюка в `60_export.sh`: PostgreSQL получает в качестве символа кавычки `\x01`, которого
в данных не бывает, и потому не оборачивает поля — иначе кавычки уехали бы в JSON.
Табов и переводов строк внутри JSON нет: `row_to_json` экранирует их сам.

## Что стенд нашёл

1. **Первая загрузка потока не стартует без затравки.** `do_branch` соединяется с
   `gp__exchange_log` через INNER JOIN, поэтому поток без записи `IN_log` отсекается,
   а запись появляется только после обработки — круг замкнут. Отсюда `55_seed_log.sql`.
2. **Пустое ветвление оставляет DAG без сигнала.** Все `process_*` уходят в skip,
   `end_task` с `trigger_rule='one_success'` не выполняется, Dataset не публикуется,
   а сам DAG при этом зелёный.
3. **Автоопределение типов склеивало разные типы одного поля** (`'Double,Int64'`) —
   такого типа нет, `JSONExtract` падал. Исправлено в `sql_fields`.
4. **`CASE` над колонками не работает в ClickHouse 24.8** (`transform` требует
   константных веток) — поток с `fields='*'` не грузился при существующей целевой
   таблице. Переписано на `multiIf`.
