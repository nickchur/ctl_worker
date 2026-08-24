"""### 🔐 Скрипт: эмуляция /vault/secrets/application для тестового стенда
*2026-08-13 18:35 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

НЕ DAG — консольный скрипт, лежит в check/ рядом с остальными инструментами проверки.
Airflow разбирает файл как обычный модуль, ничего не выполняя: вся работа под
`if __name__ == "__main__"`.

Собирает файл, который на боевом контуре кладёт vault, а на стенде класть некому.
Читает его `HrpFilesystemSecretBackend` (`hrp_secret_backend/vault_secret_backend.py`),
включается секцией `[secrets]` в `airflow.cfg`. Формат — плоский JSON `{КЛЮЧ: base64}`;
незакодированные значения бэкенд берёт как есть, но мы кодируем всё, как в бою.

Набор ключей повторяет боевой payload sigma DEV — те же имена и та же форма значений,
чтобы бэкенд ходил по тем же веткам, что и на бою. Из чего что собирается:

    <ПРЕФИКС>S3_ACCESS_KEY/SECRET_KEY/ENDPOINT/BUCKET → conn_id 's3' без префикса,
        иначе 's3-<префикс через дефис в нижнем регистре>'
    SCSP_CLICKHOUSE_URLS_NATIVE/_USERNAME/_PASSWORD   → 'dlab-click' + 'click-dlab-*'
        (URL в форме 'native://host:port'; порт, отличный от 9000, включает secure)
    TFS_KAFKA_URLS_IN/OUT (+ *_EXTRA)                 → 'tfs-kafka-in' / 'tfs-kafka-out'
    PG_<ТЕНАНТ> с блоком services                     → '<тенант>-<db>-read' / '-write'

🔑 Секретов в файле нет и быть не должно: репозиторий публичный. Логины, пароли и адреса
берутся из переменных окружения, умолчания — заведомо нерабочие заглушки. Перед запуском:

    export STAND_MINIO_KEY=... STAND_MINIO_SECRET=...
    export STAND_CH_USER=...   STAND_CH_PASSWORD=...
    export STAND_PG_USER=...   STAND_PG_PASSWORD=...

Запуск:

    python make_vault.py                       # печатает payload в stdout
    python make_vault.py /vault/secrets/application

Рядом бэкенду нужен пустой файл `common`. После правки payload перезапустите scheduler
и worker — соединения кэшируются на процесс.
"""
from __future__ import annotations

import base64
import json
import os
import sys

# 🌐 Адреса стендовых сервисов. Слушают на localhost самого стенда: Airflow ходит
# в них напрямую, а не через docker-сеть.
MINIO_URL = os.getenv("STAND_MINIO_URL", "http://127.0.0.1:9000")
CH_HOST   = os.getenv("STAND_CH_HOST",   "127.0.0.1:9002")
PG_HOST   = os.getenv("STAND_PG_HOST",   "127.0.0.1:5432")
KAFKA_URL = os.getenv("STAND_KAFKA_URL", "127.0.0.1:9092")

# 🔑 Учётные данные — только из окружения. Умолчания нерабочие намеренно: скрипт,
# отработавший с ними, даст payload, на котором соединения не поднимутся, и это
# заметно сразу. Молча уехавший в git настоящий пароль — не заметно никогда.
MINIO_KEY    = os.getenv("STAND_MINIO_KEY",    "CHANGE_ME")
MINIO_SECRET = os.getenv("STAND_MINIO_SECRET", "CHANGE_ME")
CH_USER      = os.getenv("STAND_CH_USER",      "CHANGE_ME")
CH_PASSWORD  = os.getenv("STAND_CH_PASSWORD",  "CHANGE_ME")
PG_USER      = os.getenv("STAND_PG_USER",      "CHANGE_ME")
PG_PASSWORD  = os.getenv("STAND_PG_PASSWORD",  "CHANGE_ME")

REQUIRED = ("STAND_MINIO_KEY", "STAND_MINIO_SECRET", "STAND_CH_USER",
            "STAND_CH_PASSWORD", "STAND_PG_USER", "STAND_PG_PASSWORD")


def s3(prefix: str, bucket: str) -> dict:
    """Блок S3-подключения. Пустой префикс — основной conn_id 's3'."""
    return {
        f"{prefix}S3_ACCESS_KEY": MINIO_KEY,
        f"{prefix}S3_SECRET_KEY": MINIO_SECRET,
        f"{prefix}S3_ENDPOINT":   MINIO_URL,
        f"{prefix}S3_BUCKET":     bucket,
    }


def build_payload() -> dict:
    """Плоский словарь `{КЛЮЧ: значение}` до base64-кодирования."""
    return {
        **s3("",           "hrplt-test"),   # s3
        **s3("TFS_HRPLT_", "tfshrplt"),     # s3-tfs-hrplt
        **s3("ARCHIVE_",   "hrplt-test"),   # s3-archive
        **s3("CDC_",       "hrplt-test"),   # s3-cdc
        **s3("TFS_IN_",    "hrplt-tfs"),    # s3-tfs-in
        **s3("TFS_OUT_",   "hrplt-tfs"),    # s3-tfs-out
        # В боевом payload у архивного бакета вместо BUCKET встречается INFO со списком CI.
        "ARCHIVE_S3_INFO": json.dumps([{"ci": "CI02420667", "bucket": "hrplt-test"}]),

        # ClickHouse. Форма URL как в бою: схема native://, узлы через запятую с пробелом.
        # secure бэкенд выводит из порта: не 9000 — значит TLS. Стендовый клик слушает 9002
        # без TLS, поэтому secure гасим явным EXTRA.
        "SCSP_CLICKHOUSE_URLS_NATIVE": f"native://{CH_HOST}",
        "SCSP_CLICKHOUSE_USERNAME":    CH_USER,
        "SCSP_CLICKHOUSE_PASSWORD":    CH_PASSWORD,
        "SCSP_CLICKHOUSE_EXTRA":       json.dumps({"secure": False}),

        # Kafka тракта. Redpanda слушает PLAINTEXT, а parse_kafka_connection подмешивает
        # в extra security.protocol=ssl с путями к автоТУЗ-сертификатам ВСЕГДА — независимо
        # от того, заданы ли ключи JKS. Убрать их нельзя (extra собирается мержем), но
        # TFS_KAFKA_*_EXTRA мержится последним и переопределяет: достаточно сменить
        # протокол, на ssl.* librdkafka при plaintext не смотрит. Без этого consumer
        # падает ещё до соединения: "ssl.ca.location failed: … x509 … BIO lib".
        "TFS_KAFKA_URLS_IN":    KAFKA_URL,
        "TFS_KAFKA_URLS_OUT":   KAFKA_URL,
        "TFS_KAFKA_URLS_HRPLT": KAFKA_URL,
        "TFS_KAFKA_IN_EXTRA":  json.dumps({"security.protocol": "plaintext"}),
        # auto.offset.reset=earliest у приёмника — не косметика. Бэкенд задаёт его только
        # для kafka-events, а для ТФС оставляет умолчание librdkafka, то есть latest:
        # у ПУСТОЙ группы приёмник встаёт в конец топика и всё, что пришло до его первого
        # подключения, пропускает молча. Ровно это и случилось при переезде стенда на vault —
        # group.id сменился на зашитый в бэкенде Dataplatform-ETL-TFS-OUT, а новая группа
        # смещений не имела. Перечитывание безопасно: квитанции лежат под ключом rq_uid
        # и в S3, и в ReplacingMergeTree, повтор перезапишет ту же строку.
        "TFS_KAFKA_OUT_EXTRA": json.dumps({"security.protocol": "plaintext",
                                           "auto.offset.reset": "earliest"}),
        "TFS_TOPIC_IN":  "TFS.HRPLT.IN",
        "TFS_TOPIC_OUT": "TFS.HRPLT.OUT",

        # Postgres по-тенантно: даёт conn_id вида 'stand-dwh-read' / 'stand-dwh-write'.
        "PG_STAND": json.dumps({
            "USER_AIRFLOW_READ":           PG_USER,
            "USER_AIRFLOW_READ_PASSWORD":  PG_PASSWORD,
            "USER_AIRFLOW_WRITE":          PG_USER,
            "USER_AIRFLOW_WRITE_PASSWORD": PG_PASSWORD,
            "af_db_host_default": PG_HOST,
            "services": {"dwh": {"af_db_name_1": "dwh"}},
        }),

        "S3_SYNC_TIME": "60",
        "login": "worker",
        "url":   f"http://{PG_HOST.split(':')[0]}:8080",
    }


def main(argv: list[str]) -> int:
    if missing := [k for k in REQUIRED if not os.getenv(k)]:
        print(f"⚠️ не заданы {', '.join(missing)} — payload соберётся с заглушками "
              "CHANGE_ME, соединения на нём не поднимутся", file=sys.stderr)

    out = json.dumps(
        {k: base64.b64encode(v.encode()).decode() for k, v in build_payload().items()},
        indent=2, ensure_ascii=False,
    )
    if len(argv) < 2:
        print(out)
    else:
        with open(argv[1], "w", encoding="utf-8") as fh:
            fh.write(out + "\n")
        print(f"✅ {argv[1]}: ключей {len(build_payload())}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
