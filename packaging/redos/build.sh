#!/usr/bin/env bash
# Собрать Linux-дистрибутив NAPASTAK под RedOS 8 в Docker и положить tar.gz в ./dist.
#
#   ./packaging/redos/build.sh [BASE_IMAGE]
#
# По умолчанию база rockylinux:9 (класс RedOS 8). Чтобы собрать на самой RedOS:
#   BASE_IMAGE=<redos-image> ./packaging/redos/build.sh
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

BASE_IMAGE="${1:-${BASE_IMAGE:-rockylinux:9}}"
VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo redos)"
TAG="dfo-redos-build:latest"

echo "==> Сборка в образе $BASE_IMAGE (version=$VERSION)"
# Контекст сборки = только отслеживаемые файлы (без build/, .git, data/) — через stdin-tar.
git ls-files | tar -T - -c | docker build \
  --build-arg BASE_IMAGE="$BASE_IMAGE" \
  --build-arg VERSION="$VERSION" \
  -f packaging/redos/Dockerfile -t "$TAG" -

echo "==> Извлечение дистрибутива → ./dist/"
mkdir -p dist
cid="$(docker create "$TAG")"
docker cp "$cid":/src/build/release/dist/. dist/ >/dev/null
docker rm "$cid" >/dev/null

echo "==> Готово:"
ls -la dist/*.tar.gz
echo ""
echo "Разверните на сервере RedOS:"
echo "  scp dist/dataflow-os-*.tar.gz user@server:/tmp/"
echo "  ssh user@server 'cd /tmp && tar xzf dataflow-os-*.tar.gz && cd dataflow-os-* && sudo ./install.sh'"
