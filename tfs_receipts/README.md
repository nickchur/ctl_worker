# 📨 Обратные квитанции ТФС

Сбор квитанций `TransferFileCephRs` из Kafka в ClickHouse. Один даг на весь контур —
и xStream, и ЕР.

---

## Зачем это отдельно

ТФС отвечает на каждое уведомление обратной квитанцией, но кладёт её **в один топик
`TFS.HRPLT.OUT` по всем маршрутам сразу**. Сопоставление — по `RqUID`, результат
передачи — в `Status/StatusCode` (`0` = успех).

Kafka раздаёт сообщение одному потребителю в группе. Значит, читать этот топик из
выгрузок нельзя: кто первым вычитал — тот и забрал, а настоящий адресат ждёт вечно.
Именно так и было до этого дага — ожидание в ER брало **любое** сообщение из топика,
подтверждало им свой пакет, съедало чужую квитанцию и не смотрело на `StatusCode`.

Поэтому потребитель ровно один: `tfs_receipts_sync`. Он ничего не решает — просто
складывает всё, что пришло, в `export.tfs_receipts`. Ждут своих строк уже сами выгрузки.

```
ТФС → Kafka TFS.HRPLT.OUT → tfs_receipts_sync → export.tfs_receipts
                                                       ↑
                          выгрузка ждёт свой RqUID ────┘
```

---

## Файлы

| Файл | Описание |
| :--- | :--- |
| `tfs_receipts.py` | DAG `tfs_receipts_sync` — раз в минуту вычитывает топик и пишет в ClickHouse |
| `tfs_receipts.sql` | DDL `export.tfs_receipts` + полезные запросы |

---

## Как работает

Раз в минуту, `max_active_runs=1`. Ран короткий: опрашивает топик, пока идут сообщения,
и выходит по тишине (15 секунд) либо по потолку в 5000 сообщений за ран.

**Порядок операций важен:** сначала вставка в ClickHouse, только потом коммит offset.
При обратном порядке падение между операциями потеряло бы квитанцию навсегда — а её ждёт
выгрузка. Обратная сторона: at-least-once, то есть возможен повтор. Его схлопывает
`ReplacingMergeTree` по `(rq_uid, file_name)`; читать с `FINAL`.

**Битое сообщение не роняет ран** — сохраняется с `status_code = -1` и исходным текстом
в `raw_xml`. Потерять квитанцию хуже, чем сохранить её неразобранной, а застрявшее
сообщение заблокировало бы очередь всем остальным.

**Ненулевой `StatusCode` здесь только логируется.** Это не проблема сборщика: ошибку
покажет та выгрузка, которая ждёт эту квитанцию, — у неё есть контекст пакета.

---

## Кто ещё трогает этот топик

Никто не должен. Единственный законный конкурент — `tools_test_kafka_rcv` в режиме
`wait` (`ctl/check/test_kafka.py`): он работает в той же consumer group и уводит
сообщения. Запускать его на `TFS.HRPLT.OUT` можно только при остановленном
`tfs_receipts_sync`. Для разбора инцидентов обычно достаточно `export.tfs_receipts`.

---

## Диагностика

```sql
-- неуспешные передачи за сутки
SELECT rq_tm, scenario_id, file_name, status_code
FROM export.tfs_receipts FINAL
WHERE status_code != 0 AND received_at >= now() - INTERVAL 1 DAY
ORDER BY received_at DESC;

-- сообщения, которые не разобрались
SELECT received_at, kafka_partition, kafka_offset, substring(raw_xml, 1, 500)
FROM export.tfs_receipts FINAL
WHERE status_code = -1 ORDER BY received_at DESC;
```

Отправлено, но квитанции нет (по данным ЕР) — джойн с реестром отправок:

```sql
SELECT s.file_name, s.notified_at
FROM export.er_sent_files FINAL AS s
LEFT ANTI JOIN export.tfs_receipts FINAL AS r USING (rq_uid)
WHERE s.notified_at > toDateTime64(0, 3)
  AND s.notified_at < now() - INTERVAL 1 HOUR;
```

---

## Что сюда не переехало

xStream пока ждёт квитанции по-старому. Перевод его на эту таблицу — отдельная задача;
структура `export.tfs_receipts` к нему уже готова, она ничего не знает про ЕР.
