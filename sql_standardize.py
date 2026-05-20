"""SQL Script Standardization Utility

Разбивает монолитный .sql файл задачи на стандартную структуру папок репозитория.

Использование:
    python sql_standardize.py <input.sql> <task_dir> [опции]

| Аргумент         | Описание                                                             |
|------------------|----------------------------------------------------------------------|
| `input_sql`      | Монолитный .sql файл задачи                                          |
| `task_dir`       | Папка задачи (напр. tasks/E360-1234/), создаётся если нет           |
| `--repo-root`    | Корень репозитория (по умолчанию: автопоиск по .git)                |
| `--dry-run`      | Только парсинг и валидация, файлы не записываются                   |
| `--no-annotate`  | Не создавать аннотированную копию исходного файла                   |
| `--no-drop-gen`  | Не генерировать DROP автоматически                                   |
| `--cascade`      | Добавить CASCADE в авто-генерируемые DROP (по умолчанию: без CASCADE)|
| `--encoding`     | Кодировка входного файла (default: utf-8)                           |
| `--warn-only`    | ERROR-уровень не завершает с кодом 1                                |

Структура вывода:
    sql/create/{schema}/{obj_type}/{obj_name}.sql
    sql/drop/{schema}/{obj_type}/{obj_name}.sql
    tasks/E360-XXXX/alter_{schema}_{name}.sql
    tasks/E360-XXXX/sql_order_file.txt
    tasks/E360-XXXX/passthrough.sql (нераспознанные блоки)
    {input}.annotated.sql (с комментариями MOVED TO)

Порядок выполнения:
    DROP:   view → proc → table
    CREATE: table → proc → view
    ALTER:  после соответствующего CREATE (файл в папке задачи)

Коды выхода: 0 = успех, 1 = найдены ошибки, 2 = критическая ошибка парсинга.
"""

import argparse
import logging
import re
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------

class Op(str, Enum):
    CREATE = "create"
    DROP   = "drop"
    ALTER  = "alter"


class ObjType(str, Enum):
    TABLE = "table"
    VIEW  = "view"
    PROC  = "proc"


@dataclass
class SqlObject:
    op:          Op
    obj_type:    ObjType
    schema:      str
    name:        str
    raw_sql:     str
    source_line: int          # -1 для авто-сгенерированных
    depends_on:  list = field(default_factory=list)   # list[str] "schema.name"
    dest_path:   Optional[Path] = None


@dataclass
class ValidationIssue:
    level:   str              # "ERROR" | "WARNING"
    message: str
    obj:     Optional[SqlObject] = None


# ---------------------------------------------------------------------------
# Known schemas
# ---------------------------------------------------------------------------

KNOWN_SCHEMAS = {
    's_grnplm_vd_hr_edp_dac',
    's_grnplm_vd_hr_edp_dds',
    's_grnplm_vd_hr_edp_dia',
    's_grnplm_vd_hr_edp_dm',
    's_grnplm_vd_hr_edp_fcts',
    's_grnplm_vd_hr_edp_srv_dq',
    's_grnplm_vd_hr_edp_srv_wf',
    's_grnplm_vd_hr_edp_stg',
    's_grnplm_vd_hr_edp_udlprod',
    's_grnplm_vd_hr_edp_udlapprove',
    's_grnplm_vd_hr_edp_vd',
    's_grnplm_vd_hr_edp_vda',
}

# ---------------------------------------------------------------------------
# Statement splitter (dollar-quote-aware)
# ---------------------------------------------------------------------------

_DOLLAR_TAG = re.compile(r'\$([A-Za-z0-9_]*)\$')


def split_statements(sql_text: str) -> list:
    """Split SQL text into individual statements respecting dollar-quoting."""
    statements = []
    buf = []
    i = 0
    n = len(sql_text)
    in_line_comment  = False
    in_block_comment = False
    in_single_quote  = False
    in_double_quote  = False
    dollar_tag       = None

    while i < n:
        c = sql_text[i]

        if in_line_comment:
            buf.append(c)
            if c == '\n':
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            buf.append(c)
            if sql_text[i:i+2] == '*/':
                buf.append('/')
                i += 2
                in_block_comment = False
            else:
                i += 1
            continue

        if in_single_quote:
            buf.append(c)
            if c == "'" and sql_text[i:i+2] == "''":
                buf.append("'")
                i += 2
            elif c == "'":
                in_single_quote = False
                i += 1
            else:
                i += 1
            continue

        if in_double_quote:
            buf.append(c)
            if c == '"':
                in_double_quote = False
            i += 1
            continue

        if dollar_tag is not None:
            tag_len = len(dollar_tag)
            if sql_text[i:i+tag_len] == dollar_tag:
                buf.append(dollar_tag)
                i += tag_len
                dollar_tag = None
            else:
                buf.append(c)
                i += 1
            continue

        # Detect start of special regions
        if sql_text[i:i+2] == '--':
            in_line_comment = True
            buf.append(c)
            i += 1
            continue

        if sql_text[i:i+2] == '/*':
            in_block_comment = True
            buf.append(c)
            i += 1
            continue

        if c == "'":
            in_single_quote = True
            buf.append(c)
            i += 1
            continue

        if c == '"':
            in_double_quote = True
            buf.append(c)
            i += 1
            continue

        if c == '$':
            m = _DOLLAR_TAG.match(sql_text, i)
            if m:
                dollar_tag = m.group(0)
                buf.append(dollar_tag)
                i += len(dollar_tag)
                continue

        if c == ';':
            stmt = ''.join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(c)
        i += 1

    # Trailing content without semicolon
    stmt = ''.join(buf).strip()
    if stmt:
        statements.append(stmt)

    return statements


# ---------------------------------------------------------------------------
# Statement classifier
# ---------------------------------------------------------------------------

_STRIP_COMMENTS = re.compile(r'--[^\n]*|/\*.*?\*/', re.DOTALL)

_PATTERNS = [
    (re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.CREATE, ObjType.VIEW),

    (re.compile(
        r'CREATE\s+(?:EXTERNAL\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.CREATE, ObjType.TABLE),

    (re.compile(
        r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:FUNCTION|PROCEDURE)\s+(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.CREATE, ObjType.PROC),

    (re.compile(
        r'DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.DROP, ObjType.VIEW),

    (re.compile(
        r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.DROP, ObjType.TABLE),

    (re.compile(
        r'DROP\s+(?:FUNCTION|PROCEDURE)\s+(?:IF\s+EXISTS\s+)?(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.DROP, ObjType.PROC),

    (re.compile(
        r'ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.ALTER, ObjType.TABLE),

    (re.compile(
        r'ALTER\s+(?:MATERIALIZED\s+)?VIEW\s+(?P<schema>\w+)\.(?P<name>\w+)',
        re.IGNORECASE,
    ), Op.ALTER, ObjType.VIEW),
]


def classify_statement(raw: str) -> Optional[SqlObject]:
    """Classify a SQL statement. Returns None for unrecognized statements."""
    header = _STRIP_COMMENTS.sub(' ', raw).lstrip()
    for pattern, op, obj_type in _PATTERNS:
        m = pattern.match(header)
        if m:
            return SqlObject(
                op=op,
                obj_type=obj_type,
                schema=m.group('schema').lower(),
                name=m.group('name').lower(),
                raw_sql=raw,
                source_line=0,
            )
    return None


# ---------------------------------------------------------------------------
# Dependency extractor
# ---------------------------------------------------------------------------

_REF_PATTERN = re.compile(
    r'\b(?:FROM|JOIN)\s+(?P<schema>\w+)\.(?P<name>\w+)',
    re.IGNORECASE,
)


def extract_dependencies(obj: SqlObject) -> list:
    """Return list of 'schema.name' strings referenced in this object's SQL."""
    seen = set()
    refs = []
    for m in _REF_PATTERN.finditer(obj.raw_sql):
        s = m.group('schema').lower()
        n = m.group('name').lower()
        if s in KNOWN_SCHEMAS and not (s == obj.schema and n == obj.name):
            key = f"{s}.{n}"
            if key not in seen:
                seen.add(key)
                refs.append(key)
    return refs


# ---------------------------------------------------------------------------
# File paths
# ---------------------------------------------------------------------------

def dest_path_for(repo_root: Path, task_dir: Path, op: Op, obj: SqlObject) -> Path:
    if op == Op.ALTER:
        return task_dir / f"alter_{obj.schema}_{obj.name}.sql"
    return repo_root / 'sql' / op.value / obj.schema / obj.obj_type.value / f"{obj.name}.sql"


def find_repo_root(start: Path) -> Path:
    """Walk up from start (and from cwd) until .git is found."""
    for candidate in [Path.cwd(), start.resolve()]:
        current = candidate.resolve()
        for parent in [current, *current.parents]:
            if (parent / '.git').exists():
                return parent
    return Path.cwd().resolve()


# ---------------------------------------------------------------------------
# DROP generator
# ---------------------------------------------------------------------------

_DROP_TEMPLATES = {
    ObjType.TABLE: 'DROP TABLE IF EXISTS {schema}.{name}{cascade};',
    ObjType.VIEW:  'DROP VIEW IF EXISTS {schema}.{name}{cascade};',
    ObjType.PROC:  'DROP FUNCTION IF EXISTS {schema}.{name}{cascade};',
}


def generate_drop(obj: SqlObject, cascade: bool = False) -> str:
    cascade_clause = ' CASCADE' if cascade else ''
    body = _DROP_TEMPLATES[obj.obj_type].format(
        schema=obj.schema, name=obj.name, cascade=cascade_clause,
    )
    note = (
        '-- AUTO-GENERATED: CASCADE включён — убедитесь что лишних объектов не удалится'
        if cascade else
        '-- AUTO-GENERATED'
    )
    return f"{note}\n{body}"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

_LOG_PATTERN = re.compile(r'\bRAISE\b|\bINSERT\s+INTO\s+\w+\.\w*log\w*', re.IGNORECASE)


def run_validations(creates: list, drops: list, issues: list) -> None:
    drop_keys = {(d.schema, d.name) for d in drops}

    seen_creates: dict = {}
    for obj in creates:
        key = (obj.schema, obj.name)

        if key in seen_creates:
            issues.append(ValidationIssue(
                'ERROR',
                f"Дублирующийся CREATE: {obj.schema}.{obj.name}",
                obj,
            ))

        seen_creates[key] = obj

        if obj.schema not in KNOWN_SCHEMAS:
            issues.append(ValidationIssue(
                'WARNING',
                f"Неизвестная схема '{obj.schema}' для объекта {obj.schema}.{obj.name}",
                obj,
            ))

        if key not in drop_keys:
            issues.append(ValidationIssue(
                'WARNING',
                f"Нет DROP для {obj.schema}.{obj.name} — будет авто-сгенерирован",
                obj,
            ))

        if obj.obj_type == ObjType.PROC and not _LOG_PATTERN.search(obj.raw_sql):
            issues.append(ValidationIssue(
                'WARNING',
                f"Процедура {obj.schema}.{obj.name} не содержит паттерна логирования (RAISE / INSERT INTO *log*)",
                obj,
            ))

    for obj in drops:
        if 'CASCADE' in obj.raw_sql.upper():
            issues.append(ValidationIssue(
                'WARNING',
                f"CASCADE в DROP для {obj.schema}.{obj.name} — убедитесь что лишних объектов не удалится",
                obj,
            ))


def check_dependency_order(creates: list, drops: list, issues: list) -> None:
    """Warn when a CREATE object depends on another object being modified in this task."""
    modified_keys = {(o.schema, o.name) for o in creates + drops}
    for obj in creates:
        for dep in obj.depends_on:
            dep_parts = dep.split('.')
            if len(dep_parts) == 2:
                dep_key = tuple(dep_parts)
                if dep_key in modified_keys:
                    issues.append(ValidationIssue(
                        'WARNING',
                        f"{obj.obj_type.value.upper()} {obj.schema}.{obj.name} зависит от "
                        f"{dep} (модифицируется в этой задаче) — проверьте порядок DROP/CREATE",
                        obj,
                    ))


# ---------------------------------------------------------------------------
# sql_order_file.txt
# ---------------------------------------------------------------------------

DROP_ORDER   = [ObjType.VIEW, ObjType.PROC, ObjType.TABLE]
CREATE_ORDER = [ObjType.TABLE, ObjType.PROC, ObjType.VIEW]


def _rel(path: Path, repo_root: Path) -> str:
    """Return path relative to repo_root, or absolute string as fallback."""
    try:
        return str(path.relative_to(repo_root))
    except ValueError:
        return str(path)


def build_order_file(
    repo_root: Path,
    task_dir: Path,
    drops: list,
    creates: list,
    alters: list,
) -> str:
    lines: list = []

    for obj_type in DROP_ORDER:
        for obj in sorted((o for o in drops if o.obj_type == obj_type), key=lambda o: o.name):
            p = dest_path_for(repo_root, task_dir, Op.DROP, obj)
            lines.append(_rel(p, repo_root))

    for obj_type in CREATE_ORDER:
        for obj in sorted((o for o in creates if o.obj_type == obj_type), key=lambda o: o.name):
            p = dest_path_for(repo_root, task_dir, Op.CREATE, obj)
            lines.append(_rel(p, repo_root))
            # ALTER for the same object right after its CREATE
            for alt in alters:
                if alt.schema == obj.schema and alt.name == obj.name:
                    p_a = dest_path_for(repo_root, task_dir, Op.ALTER, alt)
                    lines.append(_rel(p_a, repo_root))

    # ALTERs without a matching CREATE in this task
    create_keys = {(o.schema, o.name) for o in creates}
    for alt in sorted(alters, key=lambda o: o.name):
        if (alt.schema, alt.name) not in create_keys:
            p_a = dest_path_for(repo_root, task_dir, Op.ALTER, alt)
            lines.append(_rel(p_a, repo_root))

    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# Source annotation
# ---------------------------------------------------------------------------

def annotate_source(raw: str, moves: list) -> str:
    """Insert '-- MOVED TO: ...' comment above each moved statement."""
    result = raw
    for stmt, path in moves:
        comment = f"-- MOVED TO: {path}\n"
        # Replace only the first occurrence of this exact statement
        result = result.replace(stmt, comment + stmt, 1)
    return result


# ---------------------------------------------------------------------------
# File writer
# ---------------------------------------------------------------------------

def write_file(path: Path, content: str, dry_run: bool, issues: list) -> None:
    if path.exists():
        issues.append(ValidationIssue('WARNING', f"Файл уже существует и будет перезаписан: {path}"))
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content + '\n', encoding='utf-8')


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='sql_standardize',
        description='SQL-утилита: разбивает монолитный .sql по стандартной структуре репозитория.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('input_sql',  help='Монолитный .sql файл задачи')
    p.add_argument('task_dir',   help='Папка задачи (напр. tasks/E360-1234/)')
    p.add_argument('--repo-root', metavar='PATH',
                   help='Корень репозитория (по умолчанию: автопоиск по .git)')
    p.add_argument('--dry-run',   action='store_true',
                   help='Только парсинг и валидация, файлы не записываются')
    p.add_argument('--no-annotate', action='store_true',
                   help='Не создавать аннотированную копию исходного файла')
    p.add_argument('--no-drop-gen', action='store_true',
                   help='Не генерировать DROP автоматически')
    p.add_argument('--cascade', action='store_true',
                   help='Добавить CASCADE в авто-генерируемые DROP (по умолчанию без CASCADE)')
    p.add_argument('--encoding', default='utf-8',
                   help='Кодировка входного файла (default: utf-8)')
    p.add_argument('--warn-only', action='store_true',
                   help='ERROR не завершает с кодом 1')
    p.add_argument('-v', '--verbose', action='store_true')
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='%(levelname)s: %(message)s',
    )

    # --- Step 1: read input ---
    input_path = Path(args.input_sql).resolve()
    if not input_path.exists():
        logger.error(f"Файл не найден: {input_path}")
        sys.exit(2)

    try:
        raw = input_path.read_text(encoding=args.encoding, errors='strict')
    except UnicodeDecodeError as e:
        logger.error(f"Не удалось прочитать файл (кодировка {args.encoding}): {e}")
        sys.exit(2)

    # --- Step 2: resolve paths ---
    if args.repo_root:
        repo_root = Path(args.repo_root).resolve()
    else:
        repo_root = find_repo_root(input_path.parent)

    task_dir = Path(args.task_dir).resolve()

    if not args.dry_run:
        task_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Репозиторий: {repo_root}")
    logger.info(f"Папка задачи: {task_dir}")

    # --- Step 3: split ---
    raw_stmts = split_statements(raw)
    logger.info(f"Найдено стейтментов: {len(raw_stmts)}")

    # --- Step 4: classify ---
    creates:      list[SqlObject] = []
    drops:        list[SqlObject] = []
    alters:       list[SqlObject] = []
    passthroughs: list[str]       = []

    line_counter = 1
    for stmt in raw_stmts:
        obj = classify_statement(stmt)
        if obj is None:
            passthroughs.append(stmt)
            logger.debug(f"Passthrough: {stmt[:80].strip()!r}")
        else:
            obj.source_line = line_counter
            obj.depends_on  = extract_dependencies(obj)
            if obj.op == Op.CREATE:
                creates.append(obj)
            elif obj.op == Op.DROP:
                drops.append(obj)
            else:
                alters.append(obj)
            logger.debug(f"{obj.op.value.upper()} {obj.obj_type.value} {obj.schema}.{obj.name}")
        line_counter += stmt.count('\n') + 1

    logger.info(f"CREATE={len(creates)}, DROP={len(drops)}, ALTER={len(alters)}, passthrough={len(passthroughs)}")

    # --- Step 5: validation pass 1 ---
    issues: list[ValidationIssue] = []
    run_validations(creates, drops, issues)
    check_dependency_order(creates, drops, issues)

    # --- Step 6: auto-generate missing DROPs ---
    if not args.no_drop_gen:
        drop_keys = {(d.schema, d.name) for d in drops}
        for obj in creates:
            if (obj.schema, obj.name) not in drop_keys:
                auto_sql  = generate_drop(obj, cascade=args.cascade)
                auto_drop = SqlObject(
                    op=Op.DROP, obj_type=obj.obj_type,
                    schema=obj.schema, name=obj.name,
                    raw_sql=auto_sql, source_line=-1,
                )
                drops.append(auto_drop)

    # --- Step 7: assign destination paths ---
    all_objects = creates + drops + alters
    for obj in all_objects:
        obj.dest_path = dest_path_for(repo_root, task_dir, obj.op, obj)

    # --- Step 8: write files ---
    moves = []   # (original_stmt, relative_path) for annotation
    for obj in all_objects:
        content = obj.raw_sql + ';'
        write_file(obj.dest_path, content, args.dry_run, issues)
        if obj.source_line >= 0:
            try:
                rel = obj.dest_path.relative_to(repo_root)
            except ValueError:
                rel = obj.dest_path
            moves.append((obj.raw_sql, rel))
        if args.dry_run:
            logger.info(f"  [dry-run] → {obj.dest_path.relative_to(repo_root)}")
        else:
            logger.info(f"  Записан → {obj.dest_path.relative_to(repo_root)}")

    # Write passthrough.sql
    if passthroughs:
        passthrough_path = task_dir / 'passthrough.sql'
        passthrough_content = '\n\n'.join(s + ';' for s in passthroughs)
        write_file(passthrough_path, passthrough_content, args.dry_run, issues)
        logger.info(f"  Нераспознанные блоки → {passthrough_path.relative_to(repo_root)}")

    # --- Step 9: sql_order_file.txt ---
    order_content = build_order_file(repo_root, task_dir, drops, creates, alters)
    order_path = task_dir / 'sql_order_file.txt'
    write_file(order_path, order_content, args.dry_run, issues)
    if not args.dry_run:
        logger.info(f"  sql_order_file.txt → {order_path.relative_to(repo_root)}")
    if args.dry_run:
        logger.info("--- sql_order_file.txt (preview) ---")
        print(order_content)

    # --- Step 10: annotate source ---
    if not args.no_annotate:
        annotated   = annotate_source(raw, [(stmt, rel) for stmt, rel in moves])
        annot_path  = input_path.with_suffix('.annotated.sql')
        if not args.dry_run:
            annot_path.write_text(annotated, encoding='utf-8')
            logger.info(f"  Аннотированный файл → {annot_path.name}")

    # --- Step 11: report issues ---
    print()
    if issues:
        for issue in issues:
            prefix = '❌ ERROR' if issue.level == 'ERROR' else '⚠️  WARNING'
            obj_label = f" [{issue.obj.schema}.{issue.obj.name}]" if issue.obj else ''
            print(f"{prefix}{obj_label}: {issue.message}")
    else:
        print("✅ Нарушений стандарта не найдено.")

    errors = [i for i in issues if i.level == 'ERROR']
    if errors and not args.warn_only:
        sys.exit(1)
    sys.exit(0)


if __name__ == '__main__':
    main()
