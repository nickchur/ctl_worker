# Карта артефактов

*Файл собирается автоматически: `.claude/scripts/sync_context.py`. Правки руками
затрутся при следующем обновлении ветки — правьте источник, а не карту.*

Артефакты, к которым обращается агент и команда, лежат в четырёх местах:

| Что | Где | Кто пишет |
|---|---|---|
| **Правила работы** (rules) | `CLAUDE.md` в корне | человек |
| **Как устроено** | `<каталог>/readme.md` | человек |
| **Что обязано работать** (SDD) | `openspec/specs/<capability>/spec.md`, общий контекст — `openspec/project.md` | человек, через `/opsx:propose` |
| **Навыки и команды агента** | `.claude/skills/`, `.claude/commands/` | генерирует `openspec init`, правит человек |

Память агента (`~/.claude/projects/*/memory/`) в репозиторий не входит: она про
конкретного человека и его прошлые сессии, а не про проект.

## Свежесть

Колонки «Обновлён» и «Код» — даты последних коммитов, тронувших документ и код
каталога. ⚠️ означает отставание больше трёх дней: повод посмотреть, не разошлись
ли они по существу.

| Каталог | Как устроено | Обновлён | Что обязано работать | Обновлена | Код |
|---|---|---|---|---|---|
| `check/` | `check/readme.md` | 2026-08-31 | `openspec/specs/check/spec.md` | 2026-08-31 | 2026-08-28 |
| `ctl_worker/` | `ctl_worker/readme.md` | 2026-08-31 | `openspec/specs/ctl-worker/spec.md` | 2026-08-31 | 2026-08-28 |
| `er_export/` | `er_export/README.md` | 2026-08-28 | `openspec/specs/er-export/spec.md` | 2026-08-31 | 2026-08-28 |
| `gp_exchange/` | `gp_exchange/readme.md` | 2026-08-31 | `openspec/specs/gp-exchange/spec.md` | 2026-08-31 | 2026-08-31 |
| `plugins/` | `plugins/readme.md` | 2026-08-31 | `openspec/specs/plugins/spec.md` | 2026-08-31 | 2026-08-28 |
| `tfs_kafka/` | `tfs_kafka/README.md` | 2026-08-28 | `openspec/specs/tfs-kafka/spec.md` | 2026-08-31 | 2026-08-28 |
| `tools/` | `tools/readme.md` | 2026-08-31 | `openspec/specs/tools/spec.md` | 2026-08-31 | 2026-08-25 |
| `xs_export/` | `xs_export/readme.md` | 2026-08-31 | `openspec/specs/xs-export/spec.md` | 2026-08-31 | 2026-08-31 |

*Собрано 2026-08-31 скриптом `sync_context.py`*
