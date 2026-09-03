# Tasks

## 1. Опознание владельца

- [x] 1.1 `SQL_OWNERS` вместо `SQL_RUNNING`: без фильтра `state='running'`, с `job_id`,
      `state`, `start_date` и `LEFT JOIN main.job` (`latest_heartbeat`, `job.state`), окно
      `state='running' OR end_date > now() - interval '6 hours'`; проверить прогоном
      `collect` на стенде, что список владельцев непустой
- [x] 1.2 Владелец в находке — словарь `{ti, state, age, hb_age, alive}`; `alive` считается
      по `state='running'` и `hb_age <= zombie_after_sec`; проверить на живом таске стенда
- [x] 1.3 Параметр `zombie_after_sec` (умолчание 300) в форме, `DEFAULTS` и `CFG_VAR`;
      проверить, что сохранение настроек его подхватывает

## 2. Отчёт

- [x] 2.1 `owner_alive` (`true`/`false`/`null`) в находке и счётчик `orphan` в `counts`;
      проверить на трёх случаях — владельца нет, владелец мёртв, владелец жив
- [x] 2.2 Возраст транзакции в логе и заметке по-человечески (`24ч 04м`); проверить на
      подставленной суточной транзакции

## 3. Прекращение сессий

- [x] 3.1 Кандидат — только находка с `owner_alive` не `true` (плюс прежняя защита «не своя»),
      пропуск живых объясняется в логе; проверить `terminate=True, dry_run=True` на живом и
      мёртвом владельце
- [x] 3.2 Настоящее прекращение осиротевшей сессии: `dry_run=False` закрывает её, заметка
      называет кого и почему

## 4. Документация

- [x] 4.1 `check/readme.md`: что значит `owner_alive`, почему живого не убиваем, почему
      таймаут сессии метабазе не ставится из кода; строка версии в модуле и readme
- [x] 4.2 `ruff check check/pg_activity.py` (сравнить с базой), `openspec validate --all
      --strict`, `.claude/scripts/check_context.py`
