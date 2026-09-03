#!/usr/bin/env python3
"""Синхронизация агентского контекста после обновления ветки.

Запускается git-хуками (см. .githooks/) и вручную. Делает три вещи:

  1. Раскладывает команды и навыки из репозитория в ~/.claude — Claude Code читает
     их из каталога запуска, а работают в этом проекте из домашнего каталога.
     Копии помечаются файлом .from-repo: чужие навыки (например, hrp-operators)
     не трогаются, а наши устаревшие — убираются.
  2. Пересобирает CONTEXT.md — карту артефактов со свежестью каждого документа.
  3. Зовёт check_context.py и печатает его вывод.

С флагом --gp вдобавок обновляет снимок чужого кода в GP/ из чекаута HR_Data и
переписывает GP/source.json. Отдельным флагом, а не всегда: снимок трогают по решению
человека, а хук ходит на каждый checkout.

Ничего не ломает при ошибке: хук не должен мешать работе с git.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
HOME_CLAUDE = Path.home() / '.claude'
MARKER = '.from-repo'          # чем помечены копии, которые ставит этот скрипт

sys.path.insert(0, str(REPO / '.claude' / 'scripts'))
from check_context import (  # noqa: E402
    PROJECTS, REFERENCES, git, gp_paths, last_commit, readme_of)


def sync_gp() -> list[str]:
    """Обновляет снимок GP/ из чекаута HR_Data и переписывает GP/source.json.

    Берётся не рабочее дерево, а `HEAD`: записанная ревизия должна соответствовать
    содержимому снимка, иначе сверка в check_context.py потеряет смысл. Набор файлов
    не расширяется — переносится ровно то, что в GP/ уже лежит: список объектов
    курируется руками, скрипт только не даёт ему протухнуть.
    """
    src = REPO / 'GP' / 'source.json'
    meta = json.loads(src.read_text(encoding='utf-8'))
    checkout = Path(meta['checkout']).expanduser()
    if not (checkout / '.git').exists():
        return [f"чекаута {meta['checkout']} нет — обновлять снимок не из чего"]

    head = git('rev-parse', 'HEAD', cwd=checkout)
    branch = git('rev-parse', '--abbrev-ref', 'HEAD', cwd=checkout)
    if not head:
        return [f"{meta['checkout']}: не читается HEAD"]

    log, changed = [], 0
    for f, path in gp_paths(meta):
        out = subprocess.run(['git', '-C', str(checkout), 'show', f'{head}:{path}'],
                             capture_output=True)
        if out.returncode:
            log.append(f"нет в источнике: {path}")
            continue
        if out.stdout != f.read_bytes():
            f.write_bytes(out.stdout)
            changed += 1
            log.append(f"обновлён {f.relative_to(REPO)}")

    meta.update(rev=head, branch=branch,
                copied=datetime.now(timezone.utc).strftime('%Y-%m-%d'))
    src.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    log.append(f"снимок GP: обновлено файлов {changed}, ревизия {head[:7]} ({branch})")
    return log


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

    # Справочники: спеки нет и не будет, поэтому вместо неё — откуда снят снимок.
    # Дата в колонке «Код» здесь про наш коммит, а годность снимка меряет не она, а
    # сверка с источником в check_context.py.
    for folder_name in sorted(REFERENCES):
        folder = REPO / folder_name
        readme = readme_of(folder)
        src = folder / 'source.json'
        origin = '—'
        if src.exists():
            meta = json.loads(src.read_text(encoding='utf-8'))
            origin = f"снимок `{meta['repo']}` @ `{meta['rev'][:7]}`"
        code_ts = last_commit(folder_name)
        rows.append(
            f"| `{folder_name}/` | "
            f"{'`' + str(readme.relative_to(REPO)) + '`' if readme else '—'} | "
            f"{last_commit(str(readme.relative_to(REPO))):%Y-%m-%d} | "
            f"{origin} | {meta['copied'] if src.exists() else '—'} | "
            f"{code_ts:%Y-%m-%d} |")

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

Последняя строка — каталог-справочник: чужой код, скопированный сюда для чтения.
Спеки у него нет и не будет (чинить его правкой в `ctl` нельзя, требования к нашей
стороне контракта записаны в спеке потребителя), а вместо неё — ревизия источника.
Даты коммитов про его годность ничего не говорят: снимок не меняется потому, что его
никто не трогает. Годность меряет сверка с источником в `check_context.py`, обновляет
`sync_context.py --gp`.

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

    if '--gp' in sys.argv:
        for line in sync_gp():
            print(f"→ {line}")

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
