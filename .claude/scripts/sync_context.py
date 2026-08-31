#!/usr/bin/env python3
"""Синхронизация агентского контекста после обновления ветки.

Запускается git-хуками (см. .githooks/) и вручную. Делает три вещи:

  1. Раскладывает команды и навыки из репозитория в ~/.claude — Claude Code читает
     их из каталога запуска, а работают в этом проекте из домашнего каталога.
     Копии помечаются файлом .from-repo: чужие навыки (например, hrp-operators)
     не трогаются, а наши устаревшие — убираются.
  2. Пересобирает CONTEXT.md — карту артефактов со свежестью каждого документа.
  3. Зовёт check_context.py и печатает его вывод.

Ничего не ломает при ошибке: хук не должен мешать работе с git.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HOME_CLAUDE = Path.home() / '.claude'
MARKER = '.from-repo'          # чем помечены копии, которые ставит этот скрипт

sys.path.insert(0, str(REPO / '.claude' / 'scripts'))
from check_context import PROJECTS, git, last_commit, readme_of  # noqa: E402


def sync_tree(kind: str) -> tuple[int, int]:
    """Копирует .claude/<kind>/* в ~/.claude/<kind>/, убирая свои устаревшие копии."""
    src_root = REPO / '.claude' / kind
    dst_root = HOME_CLAUDE / kind
    if not src_root.is_dir():
        return 0, 0
    dst_root.mkdir(parents=True, exist_ok=True)

    ours = {p.name for p in src_root.iterdir() if p.is_dir()}
    copied = removed = 0

    for item in src_root.iterdir():
        if not item.is_dir():
            continue
        dst = dst_root / item.name
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(item, dst)
        (dst / MARKER).write_text(f'скопировано из {REPO}\n', encoding='utf-8')
        copied += 1

    # Наше, чего в репозитории больше нет: команда переименовалась или уехала
    for item in dst_root.iterdir():
        if item.is_dir() and (item / MARKER).exists() and item.name not in ours:
            shutil.rmtree(item)
            removed += 1

    return copied, removed


def freshness_table() -> str:
    """Таблица артефактов: версия документа и его отставание от кода."""
    now = datetime.now(timezone.utc)
    rows = []
    for folder_name, capability in sorted(PROJECTS.items()):
        folder = REPO / folder_name
        readme = readme_of(folder)
        spec = REPO / 'openspec' / 'specs' / capability / 'spec.md'
        code_ts = last_commit(f'{folder_name}/*.py') or last_commit(folder_name)

        def age(path: Path | None) -> str:
            if path is None or not path.exists():
                return '—'
            ts = last_commit(str(path.relative_to(REPO)))
            if ts is None:
                return 'не в git'
            gap = (code_ts - ts).total_seconds() / 86400 if code_ts else 0
            mark = ' ⚠️' if gap > 3 else ''
            return f'{ts:%Y-%m-%d}{mark}'

        rows.append(
            f"| `{folder_name}/` | "
            f"{'`' + str(readme.relative_to(REPO)) + '`' if readme else '—'} | {age(readme)} | "
            f"{'`openspec/specs/' + capability + '/spec.md`' if spec.exists() else '—'} | {age(spec)} | "
            f"{code_ts:%Y-%m-%d}" + " |" if code_ts else "— |")

    head = (
        "| Каталог | Как устроено | Обновлён | Что обязано работать | Обновлена | Код |\n"
        "|---|---|---|---|---|---|\n"
    )
    return head + '\n'.join(rows) + f"\n\n*Собрано {now:%Y-%m-%d} скриптом "
    

def write_context_md() -> None:
    body = f"""# Карта артефактов

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

{freshness_table()}`sync_context.py`*
"""
    path = REPO / 'CONTEXT.md'
    # Пишем, только если изменилось что-то кроме даты сборки: хук зовут на каждый
    # checkout, и переписанный файл висел бы в git status без единой смысловой правки.
    if path.exists():
        strip = lambda text: [ln for ln in text.splitlines() if not ln.startswith('*Собрано ')]
        if strip(path.read_text(encoding='utf-8')) == strip(body):
            return
    path.write_text(body, encoding='utf-8')


def main() -> int:
    quiet = '--quiet' in sys.argv

    c_cmd, r_cmd = sync_tree('commands')
    c_skill, r_skill = sync_tree('skills')
    write_context_md()

    if not quiet:
        print(f"→ ~/.claude: команд {c_cmd} (убрано {r_cmd}), навыков {c_skill} (убрано {r_skill})")
        print("→ CONTEXT.md пересобран")

    check = subprocess.run([sys.executable, str(REPO / '.claude/scripts/check_context.py')],
                           capture_output=True, text=True)
    print(check.stdout.strip())
    return 0


if __name__ == '__main__':
    sys.exit(main())
