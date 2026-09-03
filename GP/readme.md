# GP — скрипты Greenplum, которые трогает ctl_worker
*2026-09-03 09:05 MSK · v1.1 · Nick Churkin · [NSChurkin@sber.ru](mailto:NSChurkin@sber.ru)*

Снимок DDL тех объектов Greenplum, вокруг которых крутится тракт CTL. Скопировано из
`HR_Data` (ветка `E360-6192`, ревизия `068018c`) 2026-09-03, чтобы не ходить туда за
каждой мелочью на связанных задачах.

**Это копия, а не источник истины.** Править — в базе и в `HR_Data`; здесь читают.
Расхождение с продом возможно: снимок обновляется руками (см. конец файла).

## Что Airflow вообще делает с Greenplum

Вызовов всего два, оба из репозитория `ctl`:

| Откуда | Вызов | Когда |
|---|---|---|
| `ctl_worker/ctl_worker.py:778`, таск `run_exe` | `select pr_swf_start_ctl('<json>'::json)` | На каждую загрузку CTL |
| `plugins/ctl_utils.py:268` | `select pr_log_ctl('<url>', '<json>')` | На каждый GET к CTL API, кроме `/statval` и `/tmpl` |

Всё остальное здесь — то, что дёргают эти две функции, и отчётность по тракту.

## `pr_swf_start_ctl(wf_jsn json) -> json`

Единственная точка входа для запуска ETL. Разбирает JSON, регистрирует ККД, собирает
строку исполняемого кода, выполняет её, превращает текстовый ответ воркфлоу в числовой
код и возвращает JSON, по которому `ctl_worker` выставляет статус загрузки в CTL.

**Входной JSON:**

| Ключ | Смысл |
|---|---|
| `wf` | имя воркфлоу |
| `sch` | краткое имя схемы (`stg`, `vd`, …) — в код подставляется как `s_grnplm_vd_hr_edp_<sch>.` |
| `exe` | исполняемый код; по умолчанию `pr_<wf>()` |
| `lid` | id загрузки в CTL |
| `rtr.try` / `rtr.left` | номер попытки и сколько осталось |
| `sdt` | дата запуска |
| `wfp` | доп. параметры воркфлоу |
| `zts ztt zta ztb zte ztp` | ККД: схема, таблица, активность, откат при ошибке, требовать успех, доп. параметры |

**Подстановки в `exe`** — до выполнения: `$wf$`, `$sdt$`, `$lid$`, `$try$`, `$left$`,
`$wfp$` (весь JSON строкой) и каждый ключ `wfp` как `$<key>$`. Значения квотируются
через `quote_literal`, кроме числовых.

**Ответ:** `res, swf, wf, exe, msg, hub, ztest, stat, cdc` и опционально `html`.
`ctl_worker` читает из него только `res` (`ctl_worker.py:213`), остальное уезжает в
XCom, заметку и в CTL.

**Коды `res`** — `> 0` успех, `= 0` нет данных, `< 0` ошибка:

| res | Когда |
|---:|---|
| `1` | ответ шага начинается с `ok ` или пустой |
| `0` | ответ начинается с `no ` — данных нет, зависимые шаги пропускаются |
| `-1` | `empty ` |
| `-2` | `query_canceled or statement_timeout` в тексте |
| `-3` | `expire ` |
| `-4` | `uniq check error` — не прошла `pr_chk_uniq` |
| `-5` | `ztest error` или «Ошибка качества данных» (`pr_chk_cnt_delta`) |
| `-7` | необработанное исключение |
| `-8` | `pxf server error` |
| `-9` | всё остальное |

Если ответ шага оказался валидным JSON, `res` берётся из его поля `result` или `res` —
это обходной путь для воркфлоу, которые считают код сами.

**Функция неидемпотентна:** повторный вызов = повторный ETL. Поэтому у `run_exe`
`retries=0`, а повторы делает CTL через `TIME-WAIT`.

## Протокол ответа шага

Любая функция загрузки возвращает `text`, и по первым символам решается всё
(`pr_swf_wf_group`, `pr_swf_start_ctl`):

- `ok ` или пустая строка — успех;
- `no ` — нечего делать, зависимые шаги пропускаются;
- что угодно ещё — ошибка.

`pr_swf_wf_group` сам повторяет шаг до трёх раз с паузой 60 с, если в тексте есть
`transfer error (18)` или `PXF server error` (`pr_swf_wf_group.sql:45-50`).

## ККД и Z-тест

`pr_swf_start_ctl` включает ККД, если пришёл хоть один ключ `zt*`. Дальше:

1. `srv_dq.pr_ztest_set(object, active, rollback, params)` — записывает настройку в
   `tb_ztest_config`; имя таблицы по умолчанию — `tb<имя воркфлоу без первого слова>`,
   схема — `stg`.
2. Сам тест считает воркфлоу через `srv_dq.pr_ztest_all_diff`, результат ложится в
   `tb_ztest_data`; `pr_swf_start_ctl` забирает последнюю запись по объекту.
3. При `zte=true` неуспешный тест (`notes.ztest = false`) превращается в `res = -5`.

Отдельно от Z-теста живут две проверки, которые воркфлоу зовёт сам:

- `pr_chk_uniq(srs, keys, fdate, tdate, raise_exc, lmt)` — дубли по ключу, даёт `-4`;
- `pr_chk_cnt_delta(srs, keys, dcol, pct, raise_exc, …)` — расхождение количества
  строк больше процента (ККД ±10% для `tagentic_skill_level`), даёт `-5`.

Смотреть результаты: `srv_dq/views/vw_ztest.sql`.

## Логи

| Объект | Что внутри |
|---|---|
| `tb_log_ctl` | все ответы CTL API, которые прошли через `pr_log_ctl`: `ts, id, obj, url, msg (json)` |
| `tb_log_ctl_all` | то же без фильтрации (вставка закомментирована, таблица осталась) |
| `tb_swf_ctl_log` | действия движка по загрузке: `id, ts, parent, wf_action, wf_message (json)` — пишет `pr_swf_log_action` |
| `tb_swf_mail_log` | тот же формат, но для отчётов: блоки HTML под общим `parent = mail_id` |
| `vw_log_ctl` | плоский разбор `tb_log_ctl` |
| `vw_log_ctl_loading` | по одной строке на загрузку: `alive, auto, start_dttm, end_dttm, profile, wf_id` |
| `vw_log_ctl_wf` | то же в разрезе воркфлоу |
| `vw_log_ctl_entity` | в разрезе сущностей |
| `vw_swf_ctl_log` | читаемый вид `tb_swf_ctl_log` |
| `vw_log_workflow_err` | ошибки воркфлоу |
| `tb_log_skew`, `vw_growth_stats` | перекос по сегментам и рост таблиц — их читает отчёт о нагрузке |

Начинать разбор инцидента удобно с `vw_log_ctl_loading` по `lid` из имени рана
(`sensor__<lid>_<попытка>_<дата>`), дальше — `vw_swf_ctl_log` по тому же `lid`.

Слот лога выбирает второй аргумент `pr_swf_log_action`: `tb_swf_<swf>_log`. У тракта
CTL это `ctl`, у отчётов — `mail`; в базе есть и другие слоты (`chk`, `dia`, `pxf`,
цифровые `0`…`9`), они к ctl_worker отношения не имеют.

## Движок, в который упирается `exe`

| Функция | Роль |
|---|---|
| `pr_swf_wf_group(fnc[], rel[])` | выполняет группу шагов с зависимостями; `rel[k]` — индексы шагов, которые должны отработать раньше `k` |
| `pr_swf_start(swf, wf)` | запуск воркфлоу по расписанию (не из CTL) |
| `pr_swf_get_next`, `pr_swf_get_status` | следующий запуск и состояние по `tb_swf` |
| `pr_swf_log_action` | запись действия в `tb_swf_ctl_log` и `tb_swf` |
| `pr_log_start`, `pr_log_error` | открыть запись лога и записать ошибку — их зовёт каждая функция загрузки |
| `tb_swf` | расписание: `wf_exec, wf_interval, wf_relations, wf_waits, wf_expire, wf_last, wf_reselt` |

## Отчёты: HTML собирается в Greenplum, письмо отправляет CTL

`pr_mail_ctl_*` — не рассыльщики. Это обычные воркфлоу CTL (отсюда даги
`CTL.pc1080.mail_ctl_report` и родня), которые считают отчёт **по логам самого CTL**
и возвращают готовый HTML. Дальше он едет по тракту как обычный результат загрузки, а
письмо по адресам `statusNotifications` шлёт уже CTL.

Цепочка целиком:

1. **Сборка блоков.** Функция гоняет `pr_tbl2html(sql, subj, over, style)` по каждому
   разделу отчёта и складывает результат в лог:
   `pr_swf_log_action(<название раздела>, 'mail', {len, html}, mail_id)`.
   Второй аргумент выбирает таблицу-слот: `tb_swf_<swf>_log`, то есть здесь —
   `tb_swf_mail_log`, а у самой загрузки (`swf = 'ctl'`) — `tb_swf_ctl_log`.
2. **Сборка письма.** `pr_send_mail(mail_id)` находит пачку по `parent = mail_id`,
   склеивает блоки в массив, помечает её действием `send` и **возвращает JSON**
   `{res, id, ts, report, html}`. Никакой отправки: имя историческое.
3. **Ответ воркфлоу.** Функция отчёта возвращает этот JSON текстом.
   `pr_swf_start_ctl` видит валидный JSON, забирает из него `res` и `html`, ставит
   `tag = 'html'` и кладёт `html` в свой ответ (строки 194-212).
4. **Передача в CTL.** `ctl_worker` в `_emit_datasets` (`ctl_worker.py:239`) зовёт
   `ctl_send_html(result['html'], lid, eid)`: тот режет HTML на куски по `max_html`
   символов, не разрывая `<tr>`, и постит их в CTL как statval `stat_id = 12`
   на сущность загрузки. Слишком длинные блоки (> `max_html` × 10) пропускаются.
5. **Письмо.** Его формирует и отправляет CTL по своим `statusNotifications`.

**Аргумент `reports text[]`** — фильтр разделов: пусто или `{All}` означает «все»,
иначе собираются только перечисленные. Названия разделов — это те же строки, что уходят
в `wf_action` лога, по ним же потом искать в `tb_swf_mail_log`.

| Функция | Разделы |
|---|---|
| `pr_mail_ctl_report` | `CTL Today`, `CTL Today Errors`, `CTL Today No data`, `CTL Today Long`, `CTL Today Done`, `CTL Today Ok`, `CTL Today Fcts`, `CTL Old and Working`, `CTL SDPUE Errors`, `CTL Not scheduled`, `GP Working`, `Lock` |
| `pr_mail_ctl_status` | `CTL Today`, `CTL All Active`, `CTL All WF`, `CTL All Today`, `CTL Not scheduled`, `Ztest Errors` |
| `pr_mail_ctl_work_load_report` | `HR_Data Lake Speed / Skew / Ratio / Uncompress`, `CTL Yesterday Summary`, `CTL and GP Yesterday Errors`, `CTL Yesterday Work Load / SDPUE / Errors / Long`, `Yesterday Errors`, `CTL Not scheduled` |

Данные берутся из `vw_log_ctl_loading`, `vw_log_ctl_wf`, `vw_swf_ctl_log`, `tb_log_ctl`,
`vw_ztest`, а отчёт о нагрузке добавляет `tb_log_skew`, `vw_growth_stats` и
`vw_log_workflow_err`.

Отдельно стоит `pr_check_ctl(obj, sch, prm)` → `pr_check_etl` — проверка свежести и
полноты загрузки; её зовёт `pr_mail_ctl_report` и можно звать руками.

## Мелочь, без которой не читается

`try_cast2int`, `try_cast2json` — «мягкое» приведение типов: возвращают NULL вместо
исключения. На них держится разбор ответа воркфлоу в `pr_swf_start_ctl` (JSON это или
текст) и поиск отчёта по id в `pr_send_mail`. `pr_log_start` / `pr_log_end` /
`pr_log_error` — открыть, закрыть и уронить запись лога; их зовёт каждая функция
загрузки и каждый отчёт.

## Чего здесь нет

Всего остального `srv_wf` (1244 функции): обёрток `pr_smdtodia_*` / `pr_diatostg_*`,
универсальных движков `pr_*_v2/v3/v4`, групп `pr_wfg_*`, `tb_etl_config`, слотов
`tb_swf_0_log`…`tb_swf_9_log`. Они к тракту CTL относятся ровно постольку, поскольку
запускаются через него, и живут в `HR_Data`.

## Как обновить снимок

```bash
H=~/HR_Data/sql/create
cp $H/s_grnplm_vd_hr_edp_srv_wf/functions/{pr_swf_start_ctl,pr_log_ctl,…}.sql GP/srv_wf/functions/
```

Список файлов — содержимое каталогов ниже; структура повторяет `HR_Data`, только без
префикса `s_grnplm_vd_hr_edp_` в имени схемы. Зеркальные `drop`-скрипты не копируются:
полная сигнатура и так стоит первой строкой каждого файла.
