#!/usr/bin/env bash
# Включает хуки репозитория. Один раз на клон: git-хуки не версионируются сами.
set -eu
ROOT="$(git rev-parse --show-toplevel)"
git -C "$ROOT" config core.hooksPath .githooks
chmod +x "$ROOT"/.githooks/* 2>/dev/null || true
echo "хуки включены: core.hooksPath = .githooks"
python3 "$ROOT/.claude/scripts/sync_context.py"
