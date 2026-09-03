#!/usr/bin/env python3
"""Проверка актуальности контекста, которым пользуется агент и команда.

Отвечает на один вопрос: не разошлись ли документы с кодом. Сравнивает не текст,
а даты последних коммитов — код каталога против его readme и спецификации. Точного
ответа «спека врёт» так не получить, но отставание видно сразу, а дальше смотрит
человек.

Проверяет:
  * у каждого каталога-проекта есть readme и спецификация;
  * снимок чужого кода (GP/) не разошёлся ни с записанной ревизией, ни с источником;
  * документ не отстал от кода больше, чем на STALE_DAYS дней;
  * строка версии на месте (дата · версия · автор во второй строке);
  * ссылки на файлы внутри документов не битые;
  * openspec validate --all --strict проходит.

Выход: 0 — всё хорошо, 1 — есть замечания. Хуки код возврата игнорируют (это
предупреждение, а не запрет), CI может на него опираться.
"""
from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]

# Каталог-проект: свой readme, своя спецификация, своё место в карте артефактов.
# Ключ — каталог, значение — имя capability в openspec (kebab-case).
PROJECTS = {
    'ctl_worker': 'ctl-worker',
    'plugins': 'plugins',
    'er_export': 'er-export',
    'tfs_kafka': 'tfs-kafka',
    'xs_export': 'xs-export',
    'tools': 'tools',
    'check': 'check',
    'gp_exchange': 'gp-exchange',
}

# Каталог-справочник: свой readme, но спецификации нет и не должно быть. Код там чужой
# (живёт в другом репозитории и в базе), чинить его правкой в ctl нельзя, а требования к
# нашей стороне контракта записаны в спеке потребителя — ctl-worker. Прочерк в колонке
# спеки у такого каталога стоит по праву, а не по недосмотру.
REFERENCES = {'GP'}

# Сколько дней документ может отставать от кода, прежде чем это стоит показать.
# Меньше суток — обычный рабочий разрыв: правку кода и правку документа редко
# коммитят одной секундой.
STALE_DAYS = 3

VERSION_RE = re.compile(r'^\*(\d{4}-\d{2}-\d{2})\s+\d{2}:\d{2}\s+MSK\s+·\s+v[\d.]+\s+·')
LINK_RE = re.compile(r'\[[^\]]*\]\(([^)#:]+?\.(?:md|py|sql|sh))\)')

# Собирается скриптом, у него своя подпись «Собрано … sync_context.py»
GENERATED = {'CONTEXT.md'}


def git(*args: str, cwd: Path = REPO) -> str:
    return subprocess.run(['git', '-C', str(cwd), *args],
                          capture_output=True, text=True).stdout.strip()


def tracked() -> set[str]:
    """Файлы под версией. Всё остальное — рабочий мусор вроде выгрузки context.md,
    и требовать от него меток и ссылок незачем."""
    return set(git('ls-files').splitlines())


TRACKED = tracked()

# Изменённое в рабочем дереве — это то, что правят прямо сейчас. Ругаться на него
# бессмысленно: человек уже занят ровно тем, о чём мы бы напомнили.
def _dirty() -> set[str]:
    paths = set()
    for line in git('status', '--porcelain').splitlines():
        # Срез по позиции не годится: git() снимает пробелы, и у первой строки
        # уезжает колонка. Берём путь как остаток после кода статуса.
        parts = line.split(maxsplit=1)
        if len(parts) != 2:
            continue
        path = parts[1].strip().strip('"')
        paths.add(path.split(' -> ')[-1])      # переименование: интересует новое имя
    return paths


DIRTY = _dirty()


def is_tracked(path: Path) -> bool:
    return str(path.relative_to(REPO)) in TRACKED


def last_commit(path: str) -> datetime | None:
    """Дата последнего коммита, тронувшего путь."""
    ts = git('log', '-1', '--format=%ct', '--', path)
    return datetime.fromtimestamp(int(ts), tz=timezone.utc) if ts else None


def days_between(newer: datetime, older: datetime) -> float:
    return (newer - older).total_seconds() / 86400


def readme_of(folder: Path) -> Path | None:
    for name in ('readme.md', 'README.md'):
        if (folder / name).exists():
            return folder / name
    return None


def check_projects() -> list[str]:
    """Каждому каталогу — readme и спека, и оба не отстают от кода."""
    problems = []
    for folder_name, capability in PROJECTS.items():
        folder = REPO / folder_name
        if not folder.is_dir():
            problems.append(f"каталога {folder_name}/ нет, а в карте артефактов он есть")
            continue

        code_ts = last_commit(f'{folder_name}/*.py') or last_commit(folder_name)
        readme = readme_of(folder)
        spec = REPO / 'openspec' / 'specs' / capability / 'spec.md'

        if readme is None:
            problems.append(f"{folder_name}/: нет readme")
        if not spec.exists():
            problems.append(f"{folder_name}/: нет спецификации openspec/specs/{capability}/spec.md")

        if code_ts is None:
            continue
        for doc in (readme, spec if spec.exists() else None):
            if doc is None:
                continue
            doc_ts = last_commit(str(doc.relative_to(REPO)))
            if doc_ts is None:
                continue
            rel = str(doc.relative_to(REPO))
            if rel in DIRTY:
                continue
            gap = days_between(code_ts, doc_ts)
            if gap > STALE_DAYS:
                problems.append(
                    f"{doc.relative_to(REPO)}: отстаёт от кода на {gap:.0f} дн. "
                    f"(код {code_ts:%Y-%m-%d}, документ {doc_ts:%Y-%m-%d})")

    # Справочникам спека не положена, но readme обязателен: без него каталог с чужим
    # кодом превращается в свалку файлов без объяснения, зачем они здесь.
    for folder_name in sorted(REFERENCES):
        folder = REPO / folder_name
        if not folder.is_dir():
            problems.append(f"каталога {folder_name}/ нет, а в карте артефактов он есть")
        elif readme_of(folder) is None:
            problems.append(f"{folder_name}/: нет readme")
    return problems


def gp_paths(meta: dict) -> list[tuple[Path, str]]:
    """Пары «файл снимка → путь в репозитории-источнике».

    Отображение детерминированное: GP/<схема>/<вид>/<файл> ↔
    <source_root>/<полное имя схемы>/<вид>/<файл>.
    """
    pairs = []
    root = REPO / 'GP'
    for f in sorted(root.rglob('*.sql')):
        rel = f.relative_to(root)
        schema, *rest = rel.parts
        full = meta['schemas'].get(schema)
        if full is None:
            continue
        pairs.append((f, f"{meta['source_root']}/{full}/{'/'.join(rest)}"))
    return pairs


def check_gp_snapshot() -> list[str]:
    """Снимок чужого кода в GP/ — не протух ли.

    Даты коммитов здесь ничего не значат: снимок не меняется потому, что его никто не
    трогает, а не потому, что он верен. Поэтому два разных вопроса:

      1. Наша копия совпадает с ревизией, которую мы записали? Разошлась — значит файлы
         правили руками, а такая правка никуда не уедет: источник не здесь.
      2. Источник ушёл вперёд по этим же файлам? Ушёл — снимок пора обновить.

    Нет чекаута источника — сверка не проводилась, и это говорится вслух: «не проверено»
    не должно выглядеть как «проверено и хорошо».
    """
    src = REPO / 'GP' / 'source.json'
    if not src.exists():
        return ["GP/: нет source.json — неизвестно, откуда и на какой ревизии снят снимок"]

    try:
        meta = json.loads(src.read_text(encoding='utf-8'))
        rev, checkout = meta['rev'], Path(meta['checkout']).expanduser()
    except (ValueError, KeyError) as e:
        return [f"GP/source.json: не читается ({e})"]

    if not (checkout / '.git').exists():
        return [f"GP/: чекаута {meta['checkout']} нет — снимок не сверялся с источником"]

    if subprocess.run(['git', '-C', str(checkout), 'cat-file', '-e', f'{rev}^{{commit}}'],
                      capture_output=True).returncode:
        return [f"GP/: ревизии {rev[:7]} нет в {meta['checkout']} — сверить снимок не с чем"]

    def at(ref: str, path: str) -> bytes | None:
        out = subprocess.run(['git', '-C', str(checkout), 'show', f'{ref}:{path}'],
                             capture_output=True)
        return None if out.returncode else out.stdout

    problems, pairs = [], gp_paths(meta)
    for f, path in pairs:
        name = f.relative_to(REPO / 'GP')
        recorded, here = at(rev, path), f.read_bytes()
        if recorded is None:
            problems.append(f"GP/{name}: в {rev[:7]} такого файла нет")
        elif recorded != here:
            # Разошлось по двум разным причинам, и лечатся они по-разному: либо копию
            # правили здесь (правка никуда не уедет, источник не тут), либо файл взят
            # новее записанной ревизии — тогда врёт source.json, а не снимок.
            if at('HEAD', path) == here:
                problems.append(f"GP/{name}: соответствует текущему HR_Data, но не "
                                f"записанной ревизии {rev[:7]} — врёт GP/source.json")
            else:
                problems.append(f"GP/{name}: отличается от {rev[:7]} — снимок правили "
                                "здесь, а править надо в источнике")

    ahead = git('log', '--oneline', f'{rev}..HEAD', '--', *[p for _, p in pairs],
                cwd=checkout)
    if ahead:
        n = len(ahead.splitlines())
        problems.append(f"GP/: источник ушёл вперёд на {n} коммит(ов) по этим объектам — "
                        "снимок устарел, обновить: .claude/scripts/sync_context.py --gp")
    return problems


def check_version_lines() -> list[str]:
    """Вторая строка документа — дата, версия, автор. Правило репозитория."""
    problems = []
    for md in sorted(REPO.glob('*.md')) + sorted(REPO.glob('*/readme.md')) + sorted(REPO.glob('*/README.md')):
        if '.claude' in md.parts or 'openspec' in md.parts:
            continue
        if str(md.relative_to(REPO)) in GENERATED:
            continue
        if not is_tracked(md):
            continue
        lines = md.read_text(encoding='utf-8').splitlines()
        if len(lines) < 2 or not VERSION_RE.match(lines[1].strip()):
            problems.append(f"{md.relative_to(REPO)}: во второй строке нет метки «дата · версия · автор»")
    return problems


def check_links() -> list[str]:
    """Ссылки на файлы внутри документов ведут в существующие файлы."""
    problems = []
    for md in REPO.rglob('*.md'):
        if '.git' in md.parts or '.claude' in md.parts or not is_tracked(md):
            continue
        for target in LINK_RE.findall(md.read_text(encoding='utf-8')):
            if target.startswith(('http', 'mailto')):
                continue
            if not (md.parent / target).resolve().exists():
                problems.append(f"{md.relative_to(REPO)}: битая ссылка на {target}")
    return problems


def check_specs() -> list[str]:
    """Спецификации разбираются самим openspec, а не только глазами."""
    exe = None
    for candidate in (Path.home() / '.local/bin/openspec', Path('/usr/local/bin/openspec')):
        if candidate.exists():
            exe = str(candidate)
            break
    if exe is None:
        return ["openspec не установлен — спецификации не проверены "
                "(npm i -g --prefix ~/.local @fission-ai/openspec)"]
    out = subprocess.run([exe, 'validate', '--all', '--strict'],
                         cwd=REPO, capture_output=True, text=True)
    if out.returncode != 0:
        tail = (out.stdout + out.stderr).strip().splitlines()[-3:]
        return ["openspec validate --all --strict не прошёл: " + ' / '.join(tail)]
    return []


def main() -> int:
    blocks = [
        ('каталоги, readme и спецификации', check_projects()),
        ('снимок GP', check_gp_snapshot()),
        ('метки версий', check_version_lines()),
        ('ссылки', check_links()),
        ('спецификации', check_specs()),
    ]
    total = sum(len(p) for _, p in blocks)
    if not total:
        print('✅ контекст в порядке: документы на месте и не отстают от кода')
        return 0

    print(f'⚠️  замечаний: {total}')
    for title, problems in blocks:
        if problems:
            print(f'\n{title}:')
            for p in problems:
                print(f'  • {p}')
    return 1


if __name__ == '__main__':
    sys.exit(main())
