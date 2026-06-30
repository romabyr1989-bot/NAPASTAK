#!/usr/bin/env bash
# DataFlow OS — установка на сервер (RedOS 8 / RHEL 9-совместимые).
# Запускать из распакованного дистрибутива от root:  sudo ./install.sh
set -euo pipefail

PREFIX="/opt/dataflow-os"
STATE="/var/lib/dataflow-os"
SVC_USER="dataflow"
SRC="$(cd "$(dirname "$0")" && pwd)"

[ "$(id -u)" -eq 0 ] || { echo "Нужны права root: sudo ./install.sh"; exit 1; }

echo "==> Рантайм-зависимости (dnf)"
# Бинарь динамически линкуется с этими библиотеками
dnf -y install sqlite-libs libcurl openssl-libs zlib >/dev/null 2>&1 || \
  echo "  ! не удалось dnf install — убедитесь, что sqlite-libs/libcurl/openssl-libs/zlib установлены"
# libpq нужен только если используете коннекторы PostgreSQL/Greenplum
if ls "$SRC"/lib/pg_connector.so "$SRC"/lib/gp_connector.so >/dev/null 2>&1; then
  dnf -y install libpq >/dev/null 2>&1 || echo "  ! libpq не установлен (нужен для PG/Greenplum)"
fi

echo "==> Системный пользователь $SVC_USER"
getent group  "$SVC_USER" >/dev/null || groupadd --system "$SVC_USER"
getent passwd "$SVC_USER" >/dev/null || \
  useradd --system --gid "$SVC_USER" --home-dir "$STATE" --shell /sbin/nologin "$SVC_USER"

echo "==> Файлы программы → $PREFIX"
mkdir -p "$PREFIX"/{bin,lib,ui}
cp -a "$SRC"/bin/.  "$PREFIX"/bin/
cp -a "$SRC"/lib/.  "$PREFIX"/lib/
cp -a "$SRC"/ui/.   "$PREFIX"/ui/

echo "==> Каталоги данных → $STATE"
mkdir -p "$STATE"/data "$STATE"/pipelines
# начальные конвейеры — только если каталог пуст (не перезатираем существующие)
if [ -d "$SRC/pipelines" ] && [ -z "$(ls -A "$STATE/pipelines" 2>/dev/null)" ]; then
  cp -a "$SRC"/pipelines/. "$STATE"/pipelines/ 2>/dev/null || true
fi

echo "==> Конфигурация → $PREFIX/config.json"
if [ -f "$PREFIX/config.json" ]; then
  echo "  config.json уже есть — оставляю как есть"
else
  JWT="$(openssl rand -hex 32)"
  PASS="${DFO_ADMIN_PASSWORD:-$(openssl rand -base64 12)}"
  sed -e "s|CHANGE_ME_RUN_INSTALL_SH|$JWT|" "$SRC/config.json" > "$PREFIX/config.json.tmp"
  # второй CHANGE_ME (admin_password) → пароль
  sed -i "0,/$JWT/! s|CHANGE_ME_RUN_INSTALL_SH|$PASS|" "$PREFIX/config.json.tmp" 2>/dev/null || true
  python3 - "$PREFIX/config.json.tmp" "$JWT" "$PASS" <<'PY' 2>/dev/null || mv "$PREFIX/config.json.tmp" "$PREFIX/config.json"
import json,sys
f,jwt,pw=sys.argv[1],sys.argv[2],sys.argv[3]
c=json.load(open(f)); c["jwt_secret"]=jwt; c["admin_password"]=pw
json.dump(c,open(f.replace(".tmp",""),"w"),indent=2,ensure_ascii=False)
PY
  rm -f "$PREFIX/config.json.tmp"
  echo "  сгенерирован jwt_secret; admin-пароль: $PASS"
  echo "  (поменять можно в $PREFIX/config.json, далее: systemctl restart dataflow-os)"
fi

echo "==> Права"
chown -R "$SVC_USER":"$SVC_USER" "$PREFIX" "$STATE"
chmod 640 "$PREFIX/config.json"

echo "==> systemd-сервис"
install -m 644 "$SRC/dataflow-os.service" /etc/systemd/system/dataflow-os.service
systemctl daemon-reload
systemctl enable dataflow-os.service >/dev/null 2>&1 || true
systemctl restart dataflow-os.service

echo "==> Готово. Статус:"
sleep 1
systemctl --no-pager --full status dataflow-os.service | head -6 || true
echo ""
echo "UI:    http://<server>:8080   (логин admin)"
echo "Логи:  journalctl -u dataflow-os -f"
echo "Если включён firewalld:  firewall-cmd --add-port=8080/tcp --permanent && firewall-cmd --reload"
