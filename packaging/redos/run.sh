#!/usr/bin/env bash
# Автономный запуск NAPASTAK прямо из этой папки — без установки, без dnf.
# Использует вложенные библиотеки из lib/deps (см. сборку `make dist`).
#   ./run.sh [config.json]
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"
[ -d lib/deps ] && export LD_LIBRARY_PATH="$HERE/lib/deps${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
exec ./bin/dfo_gateway -c "${1:-config.json}"
