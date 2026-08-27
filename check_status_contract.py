"""### 🔎 Линтер контракта решателей CTL
*2026-08-27 13:48 MSK · v1.0 · Чуркин Николай · [nschurkin@sber.ru](mailto:nschurkin@sber.ru)*

Решатели `ctl_chk_*` (`plugins/ctl_core.py`) таск не останавливают: они возвращают
`('ok'|'skip'|'fail', payload)`, а решение принимает вызывающий таск — обычно через
`raise_status()`. Забытый разбор здесь опаснее обычной опечатки: раньше исключение
останавливало таск само, теперь потерянный статус сделает таск зелёным вместо skip,
и заметить это можно будет только по последствиям.

Скрипт обходит DAG-и и ругается на вызов решателя, результат которого не разобран.

Допустимые формы:
    st, x = ctl_chk_status(...)   распаковка в две переменные
    return ctl_chk_new(...)       проброс статуса вызывающему
    ...  # status ignored         явное решение проигнорировать

Запуск из корня репозитория: `python3 check_status_contract.py`
"""
import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FILES = sorted(ROOT.glob('ctl_worker/*.py')) + sorted(ROOT.glob('plugins/*.py'))
RESOLVERS = {'ctl_chk_status', 'ctl_chk_wait', 'ctl_chk_new', 'ctl_chk_expire'}


def unparsed_calls(path):
    """Вызовы решателей в файле, результат которых никуда не уходит."""
    src = path.read_text(encoding='utf-8')
    lines = src.splitlines()
    tree = ast.parse(src)

    handled = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and isinstance(node.value, ast.Call)
                and len(node.targets) == 1 and isinstance(node.targets[0], ast.Tuple)
                and len(node.targets[0].elts) == 2):
            handled.add(id(node.value))
        if isinstance(node, ast.Return) and isinstance(node.value, ast.Call):
            handled.add(id(node.value))

    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, 'id', '') in RESOLVERS):
            continue
        if id(node) in handled or '# status ignored' in lines[node.lineno - 1]:
            continue
        yield node.lineno, node.func.id, lines[node.lineno - 1].strip()


def main():
    bad = 0
    for path in FILES:
        # сам ctl_core только определяет решатели, а не зовёт их
        for lineno, name, line in unparsed_calls(path):
            bad += 1
            print(f"❌ {path.relative_to(ROOT)}:{lineno}  {name} → {line[:90]}")
    print('ВСЁ РАЗОБРАНО' if not bad else f'НЕ РАЗОБРАНО: {bad}')
    return 1 if bad else 0


if __name__ == '__main__':
    sys.exit(main())
