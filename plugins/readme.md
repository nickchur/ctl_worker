# CTL Plugins для Apache Airflow

Этот модуль содержит набор плагинов для интеграции Apache Airflow с системой CTL (Control Layer) и управления ETL-процессами.

## 📁 Структура модуля

```
plugins/
├── ctl_core.py      # Ядро логики управления retry, событиями, ожиданием и загрузками
├── ctl_utils.py     # Утилиты: API, БД, S3, логирование, сериализация
├── s3_utils.py      # Расширенные утилиты для работы с S3
├── utils.py         # Общие утилиты Airflow: pool slots, заметки, обратные вызовы
└── readme.md        # Документация
```

## 🔧 Основные компоненты

### ctl_core.py — Ядро логики CTL

Содержит основные функции для управления процессами загрузки данных:

- **retry-логика** (`ctl_get_retry`): Получение и восстановление конфигурации повторов
- **Проверка времени** (`ctl_chk_wait`, `ctl_chk_new`): Управление TIME-WAIT состояниями
- **Проверка событий** (`ctl_chk_event`, `ctl_events_mon`): Мониторинг событий с поддержкой стратегий AND/OR
- **Отправка HTML** (`ctl_send_html`): Отправка HTML-содержимого в CTL как статистики
- **Нормализация данных** (`ctl_loading_norm`, `ctl_wf_norm`): Преобразование сырых данных в удобный формат
- **Проверка соединений** (`chk_any_conn`, `chk_conn`): Проверка доступности соединений (Postgres, S3, HTTP)
- **Управление статусами** (`ctl_get_status`, `ctl_chk_status`): Проверка и отображение статусов загрузок

Константы:
- `MAX_HTML = 5000` — Максимальная длина фрагмента HTML для отправки
- `conns` — Конфигурация соединений (ctl, gp, pg, s3, s3_files)

### ctl_utils.py — Утилиты

Содержит вспомогательные функции:

- **`get_config()`** — Ленивая загрузка конфигурации из Airflow Variable `ctl_config`. Результат кешируется. **Импортируйте `get_config`, а не `config`.**
- **`eval_delta`** — Расчёт времени с использованием delta-выражений
- **`logging`** — Централизованное логирование с эмодзи-кодированием
- **`ctl_api`** — Обёртка для API вызовов CTL с retry-логикой и rate limiting
- **`gp_exe`** — Выполнение SQL в Greenplum с retry-логикой
- **`pg_exe`** — Выполнение SQL в PostgreSQL с retry-логикой
- **`ctl_obj_load`/`ctl_obj_save`** — Загрузка и сохранение объектов в S3 или Airflow Variables
- **`ctl_obj_etag`** — Получение ETag (MD5-хеша) объекта из S3

Функции retry используют:
- `tenacity` для автоматических повторов
- `log_retry_attempt` для логирования попыток

### s3_utils.py — Расширенные утилиты S3

Содержит расширенные функции для работы с S3:

- **`get_s3_list()`** — Ленивая инициализация списка всех S3-бакетов по всем соединениям. Результат кешируется; при импорте не выполняется никаких сетевых запросов.
- **`s3_keys`** — Список объектов в бакете с фильтрацией по маске
- **`s3_bucket_size`** — Получение общего размера бакета
- **`s3_set_ttl`** — Установка TTL для объектов (lifecycle policy)
- **`s3_get_ttl`** / **`s3_del_ttl`** — Чтение и удаление правил жизненного цикла
- **`s3_to_s3`** / **`s3_move_s3`** — Копирование и перемещение объектов с опциональным GZIP
- **`s3_from_zip`** — Потоковая распаковка ZIP в S3 без сохранения на диск
- **`s3_IterStream`** — Обёртка итератора байтов в file-like объект (защита от OOM: лимит 512 МБ)

### utils.py — Общие утилиты

Общие функции для работы с Airflow:

- **`pool_slots`** — Управление pool slots в Airflow
- **`add_note`** — Добавление заметок к DAG Run / Task Instance
- **`on_callback`** — Обработчик событий (success/failed) с отправкой уведомлений
- **`query_to_dict`** — Преобразование результатов запроса в список словарей
- **`readable_size`** — Конвертация размера файлов в читаемый формат (KB, MB, GB...)
- **`s3_*`** — Утилиты для работы с S3 (TTL, размер бакета, список объектов)
- **`str2timedelta`** — Парсинг timedelta из строки

## 📊 Логирование и статусы

### Эмодзи статусов

Все функции используют эмодзи для визуального кодирования состояний:

**Жизненный цикл:**
- ⚠️ ACTIVE — Процесс активен
- ✳️ COMPLETED — Завершено успешно
- 💀 ABORTED — Прервано/Отменено

**Статусы загрузки:**
- ⚪ INIT — Инициализация
- ⏳ TIME-WAIT — Ожидание времени
- 🔔 EVENT-WAIT — Ожидание события
- 🔒 LOCK-WAIT — Ожидание блокировки
- 📋 PREREQ — Проверка предусловий
- 🔐 LOCK — Блокировка
- ⚙️ PARAM — Параметры
- 🚀 START — Запуск
- 🔵 RUNNING — Выполнение
- ✅ SUCCESS — Успех
- ❌ ERROR — Ошибка
- 🔄 ERRORCHECK — Проверка ошибок
- 🛑 ABORTING — Прерывание

## ⚙️ Конфигурация

Конфигурация хранится в Airflow Variable `ctl_config` (JSON):

```json
{
  "tz": "Europe/Moscow",
  "ctl_timeout": [10, 30],
  "ctl_limit": 0,
  "expire": "days=-1",
  "s3_conn_id": "s3",
  "s3_bucket": "ctl-data",
  "gp_conn_id": "greenplum"
}
```

## 💡 Примеры использования

### Проверка событий

```python
from plugins.ctl_core import ctl_events_mon
import pendulum

wf = {
    'name': 'my_workflow',
    'eventAwaitStrategy': 'and',
    'wf_event_sched': {
        'profile1/entity1/stat1': True,
        'profile2/entity2/stat2': True
    },
    'start_dttm': '2024-01-01 10:00:00'
}

result = ctl_events_mon(wf['start_dttm'], wf, pendulum.now())
if result['chk']:
    # Нужно продолжать ожидание
    raise AirflowSkipException("Waiting for events")
else:
    # События наступили, можно продолжать
    pass
```

### Отправка HTML-отчета

```python
from plugins.ctl_core import ctl_send_html

html_content = [
    "<table><tr><th>Col1</th><th>Col2</th></tr><tr><td>Val1</td><td>Val2</td></tr></table>"
]

ctl_send_html(html_content, loading_id=123, entity_id=456)
```

### Управление retry

```python
from plugins.ctl_core import ctl_get_retry

retry = ctl_get_retry(
    params={'wf_retry_on': 'error', 'wf_retry_cnt': 3},
    wf={'faultTolerance': {'numAttempts': 3, 'retryDelayMs': 5000}}
)
# retry = {'try': 1, 'left': 2, 'delay': None, 'add': None}
```

## 📦 Зависимости

- Airflow
- tenacity (для retry-логики)
- psycopg2 (для Greenplum/PostgreSQL)
- boto3 (для S3)
- hrp_operators (для KerberosHttpHook)
- pendulum (для работы с датами)
- PyYAML (для сериализации YAML)

## 📝 Примечания

- Все функции с сетевыми вызовами имеют retry-логику
- Используется rate limiting для API вызовов (100 запросов/сек)
- Объекты сохраняются в S3 с проверкой MD5-хеша
- Логи записываются в Greenplum для аудита
- Конфигурация и список S3-бакетов инициализируются лениво — импорт модулей безопасен при отсутствии Airflow Variables или недоступных соединениях

---

**Автор:** EDP.ETL  
**Версия:** 1.1  
**Год:** 2026
