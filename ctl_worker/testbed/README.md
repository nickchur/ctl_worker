# 🎭 Эмулятор CTL API для тестового стенда

*2026-09-01 19:13 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

`ctl_worker/` — единственный каталог репозитория, который до сих пор проверялся только
выкладкой на alpha: все его даги ходят в CTL API, а он живёт на контуре и закрыт Kerberos.
Эмулятор отвечает так же, как CTL, и держит состояние загрузок — поэтому полный цикл
`run_prm → run_exe → run_end` гоняется на стенде, вместе с ретраями и ветками ошибок.

## Из чего состоит

| Файл | Что делает |
| :--- | :--- |
| `ctl_mock.py` | сам эмулятор: starlette + psycopg2, порт 9080 |
| `schema.sql` | схема `ctl_mock` в стендовом postgres и заглушки `pr_swf_start_ctl` / `pr_log_ctl` |
| `fixtures_from_cache.py` | снимок бакета `edpetl-ctl` → фикстуры эмулятора |
| `ctl-mock.service` | systemd-юнит |

Справочники (воркфлоу, категории, сущности, профиль) — фикстуры, только чтение.
Состояние (загрузки, статусы, параметры, statval'ы, расписание воркфлоу) — в postgres,
база `gp_test`, схема `ctl_mock`.

## Почему это работает без Kerberos

`KerberosHttpHook` ставит `HTTPKerberosAuth(mutual_authentication=OPTIONAL)` без
`force_preemptive`, а requests-kerberos генерирует токен только в ответ на `401` с
`WWW-Authenticate: Negotiate`. Эмулятор 401 не отдаёт — ни KDC, ни keytab не нужны.
Проверено живьём: `GET /v5/api/info` из `chk_any_conn` доходит и возвращает 200.

## Почему не нужен Greenplum

`run_exe` всегда зовёт `select pr_swf_start_ctl(...)`, но при `test_mode` подставляет в
`exe` безобидное `'Ok Test work', pg_sleep(N)`. Заглушка `pr_swf_start_ctl` выполняет
ровно то, что пришло в `exe`, и разбирает ответ теми же правилами, что боевая:

| Ответ выражения | `res` | Что делает воркер |
| :--- | :--- | :--- |
| пусто или `ok …` | `1` | SUCCESS |
| `no …` | `0` | нет данных |
| исключение | `-7` | ERROR, циклический retry |
| таймаут оператора | `-2` | ERROR |
| что-то иное | `-9` | ERROR |

Значит сценарии ошибок задаются параметром `wf_exe` у воркфлоу, а не правкой кода.

⚠️ Но только при **выключенном** `test_mode`. Со включённым `run_exe` подменяет `exe`
на `'Ok Test work', pg_sleep(N)`, где N случайное — **до 45 минут**, — и вдобавок
перебивает `res` случайным числом 0…2 (`ctl_worker.py:786`). Для проверки живости тракта
это удобно, для сценариев — нет: ставьте `"test_mode": false` в `ctl_config`, тогда
выполняется ровно то, что в `wf_exe`.

## Разворачивание

```bash
# 1. Снимок бакета и фикстуры (25 — сколько воркфлоу оставить, см. ниже)
unzip -q edpetl-ctl.zip -d /tmp/ctlsnap
/opt/aftest/venv/bin/python fixtures_from_cache.py /tmp/ctlsnap/edpetl-ctl /opt/aftest/ctl-mock/fixtures 25

# 2. Схема состояния и заглушки GP
docker exec -i aftest-postgres psql -U airflow -d gp_test < schema.sql

# 3. Сервис
cp ctl-mock.service /etc/systemd/system/ && systemctl daemon-reload && systemctl enable --now ctl-mock
curl -s http://127.0.0.1:9080/v5/api/info
```

⚠️ **Сколько воркфлоу оставлять.** `ctl_worker.py` строит по DAG-у на каждый воркфлоу
профиля. В снимке их 685 — разбор файла не укладывается в `dagbag_import_timeout` (30 сек),
и стенд не получает ни одного дага воркера. Для тракта хватает пары десятков; в набор
всегда попадают воркфлоу тех загрузок, что приехали из снимка.

## Что нужно в Airflow

| Что | Значение на стенде |
| :--- | :--- |
| Подключение `ctl` | в payload `HTTP_CONNECTIONS`: `{"schema": "http", "host": "127.0.0.1", "port": 9080}` |
| Подключения `gp` / `ppl` | `alpha-adb_dev_comm-read` / `-write` → postgres `gp_test` |
| Подключение `pg` | `airflowdb` → метабаза |
| Variable `ctl_config` | копия `ctl_config.json` с этими conn_id, `test_mode: "event"` и бакетами `edpetl-ctl` / `edpetl-files` |
| Пулы | `ctl_pool`, `gp_pool` — без них таски висят в очереди |
| Бакеты MinIO | `edpetl-ctl`, `edpetl-files` |

## Две вещи, в которых эмулятор намеренно повторяет CTL

**Список и полная загрузка отдают РАЗНОЕ.** `GET /v4/api/loading/{lid}` возвращает полный
объект (`startCondition`, `retriesLeft`, `awaitedEvents`, `workflow`, `locksSet`), а список
`/v5/api/loading/extended` — только ядро (`id`, `wf_id`, `profile`, `alive`, `auto`,
`status`, `status_log`, `start_dttm`, `end_dttm`, `params`, `stats`, `loading_status`,
`uuid`). Проверено по снимку бакета: у записей `ctl_prf_events` остальных ключей нет вовсе.
Эмулятор режет список так же (`EXTENDED_FIELDS` в `ctl_mock.py`) — иначе код, который
дотягивает условие запуска отдельным запросом, на стенде никогда бы не сработал.

**Время — московское.** Соединения эмулятора выставляют `SET TIME ZONE Europe/Moscow`:
в этой зоне CTL отдаёт `effective_from`, `satisfiedDttm` и прочие отметки, и её же ждёт
наш код (`ctl_config.tz`). Без этого стенд писал бы UTC, всё выглядело бы на три часа
старше, и пороги монитора (15 минут, 6 часов, SLA) проверялись бы неправдой — на этом
уже один прогон дал ложный `reStarted`.

⚠️ Фильтр `category_ids` эмулятор не применяет: монитор обходит категории и на каждой
видит один и тот же набор загрузок. На решения это не влияет, но запросов в журнале
получается в разы больше, чем было бы на контуре.

## Контракт загрузки, который легко пропустить

CTL кладёт в `params` загрузки два ключа, без которых наши даги её не берут:

| Ключ | Что будет без него |
| :--- | :--- |
| `loading_id` | `ctl_sensor` отбрасывает загрузку с пометкой «no loading_id» — молча, в заметке |
| `wf_id` | `ctl_add_chk` падает `KeyError: 'wf_id'` (`ctl_sensor.py:303`) |

Эмулятор проставляет оба при создании загрузки и туда же копирует параметры воркфлоу.
У загрузок из снимка их нет — поэтому сенсор их пропускает, ровно как в бою.

Ещё сенсор не берёт загрузку со статусом `RUNNING` и НЕПУСТЫМ `status_log`: это признак,
что её уже ведёт чей-то ран Airflow. Поэтому свежесозданной загрузке статус ставится
пустым логом: `PUT /v4/api/loading/{lid}/status {"status": "RUNNING", "log": ""}`.

## Сценарии

**Загрузчик.** `CTL.HR_Data.loader` собирает метаданные и раскладывает их в S3 и Variables
ровно так же, как на контуре: `ctl_workflows`, `ctl_categories`, `ctl_entities`,
`ctl_events`. Повторный запуск уходит в скип по контрольной сумме.

**Полный цикл.** Загрузка создаётся запросом к эмулятору:

```bash
curl -s -X POST 'http://127.0.0.1:9080/v4/api/wf/<wf_id>/loading?scheduleAfterStart=false' \
     -H 'Content-Type: application/json' -d '{"wfp_run_type": "NO-WAIT"}'
```

Дальше `ctl_sensor` (раз в минуту) её видит и поднимает даг воркера. Смотреть глазами:

```sql
select id, alive, status, status_log from ctl_mock.loading order by id desc limit 5;
select status, log, effective_from from ctl_mock.loading_status
 where loading_id = <lid> order by effective_from desc;
select method, path, status from ctl_mock.api_log order by id desc limit 20;
```

**Ветка ошибки.** Тому же воркфлоу задаётся `wf_exe`, возвращающее не `ok`:

```bash
curl -s -X POST 'http://127.0.0.1:9080/v4/api/wf/<wf_id>/params' \
     -H 'Content-Type: application/json' -d '{"wf_exe": "1/0"}'   # деление на ноль → res -7
```

**Событие (EVENT-WAIT).** Затравка statval'ов датирована вчерашним днём, поэтому
событие считается ненаступившим. «Наступление» — публикация свежего значения:

```bash
curl -s -X POST 'http://127.0.0.1:9080/v4/api/loading/0/entity/<eid>/stat/2/statval?profile=HR_Data' \
     -H 'Content-Type: application/json' -d '["done"]'
```

## Границы

- **Эмулятор — не спецификация.** Он повторяет то, что читает наш код; поля, которых мы
  не касаемся, приезжают из снимка как есть, но у выдуманных объектов (новая загрузка)
  их нет. Расхождение с боем ловится сверкой ответа со снимком, а не эмулятором.
- **Не эмулируются**: права (`/permission*` отвечают «всё можно»), блокировки,
  зависимости воркфлоу, `bulkOperation*`, серверная фильтрация `filtered-compact`.
- **Kerberos не проверяется** — на стенде его нет.
- **Боевые процедуры GP не выполняются**: только заглушка, разбирающая `exe`.
- **`ctl_tfs.py` на стенде не собирается**: `ProduceToTopicOperator` в
  `apache-airflow-providers-apache-kafka` 1.15 требует `delivery_callback` строкой-путём,
  а не функцией. На контуре провайдер старше, и там это работает.
