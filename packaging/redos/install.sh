#!/usr/bin/env bash
# NAPASTAK — установка и обновление на сервере (RedOS 8 / RHEL 9-совместимые).
# Запускать из распакованного дистрибутива от root:  sudo ./install.sh
#
# Сам определяет, что делать:
#   чистая установка  — ничего не найдено;
#   обновление        — найден /opt/napastak (данные, конфиг и секреты сохраняются);
#   перенос           — найдена прежняя установка /opt/dataflow-os (до
#                       переименования проекта): данные, конвейеры и секреты
#                       переносятся, старый сервис гасится, старое дерево
#                       остаётся на месте для отката.
#
# Переменные окружения:
#   FORCE=1                 разрешить установку версии НИЖЕ установленной
#   SKIP_BACKUP=1           не делать резервную копию перед обновлением
#   DFO_ADMIN_PASSWORD=...  задать admin-пароль при первой установке
set -euo pipefail

PREFIX="/opt/napastak"
STATE="/var/lib/napastak"
SVC_USER="napastak"
SVC="napastak.service"
SRC="$(cd "$(dirname "$0")" && pwd)"
BACKUP_ROOT="/var/backups/napastak"

# Прежние имена — до переименования проекта в NAPASTAK.
OLD_PREFIX="/opt/dataflow-os"
OLD_STATE="/var/lib/dataflow-os"
OLD_SVC="dataflow-os.service"
OLD_USER="dataflow"

FORCE="${FORCE:-0}"
SKIP_BACKUP="${SKIP_BACKUP:-0}"
[ "${1:-}" = "--force" ] && FORCE=1

NEW_VERSION="$(cat "$SRC/VERSION" 2>/dev/null || echo unknown)"
OLD_VERSION="$(cat "$PREFIX/VERSION" 2>/dev/null || echo "")"

[ "$(id -u)" -eq 0 ] || { echo "Нужны права root: sudo ./install.sh"; exit 1; }

# ── Режим ───────────────────────────────────────────────────────────────────
# Признак прежней установки — именно наш бинарь, а не просто одноимённый
# каталог: /opt/dataflow-os мог остаться от чего угодно, и трогать чужое нельзя.
MODE="fresh"
if [ -x "$PREFIX/bin/napastak_gateway" ] || [ -f "$PREFIX/config.json" ]; then
  MODE="upgrade"
elif [ -x "$OLD_PREFIX/bin/dfo_gateway" ] && [ -f "$OLD_PREFIX/config.json" ]; then
  MODE="migrate"
fi

echo "==> NAPASTAK $NEW_VERSION"
case "$MODE" in
  fresh)   echo "    чистая установка" ;;
  upgrade) echo "    обновление${OLD_VERSION:+ с $OLD_VERSION}" ;;
  migrate) echo "    перенос прежней установки $OLD_PREFIX → $PREFIX" ;;
esac

# ── Защита от понижения версии ──────────────────────────────────────────────
# Понижение опасно не самим бинарём, а базой: новая версия могла поднять схему,
# и старый код увидит незнакомые таблицы/колонки. Явно разрешается FORCE=1.
if [ "$MODE" = "upgrade" ] && [ -n "$OLD_VERSION" ] && [ "$NEW_VERSION" != "unknown" ] \
   && [ "$OLD_VERSION" != "$NEW_VERSION" ]; then
  newest="$(printf '%s\n%s\n' "$OLD_VERSION" "$NEW_VERSION" | sort -V | tail -1)"
  if [ "$newest" = "$OLD_VERSION" ]; then
    echo "  ! установлено $OLD_VERSION, ставится $NEW_VERSION — это ПОНИЖЕНИЕ версии."
    echo "    База могла быть обновлена новой схемой, старый код её не поймёт."
    if [ "$FORCE" != "1" ]; then
      echo "    Прервано. Если это осознанно: sudo FORCE=1 ./install.sh"
      exit 1
    fi
    echo "    FORCE=1 — продолжаю на ваш риск."
  fi
fi

echo "==> Рантайм-зависимости"
if [ -d "$SRC/lib/deps" ] && [ -n "$(ls -A "$SRC/lib/deps" 2>/dev/null)" ]; then
  echo "  вложены в lib/deps ($(ls "$SRC/lib/deps" | wc -l | tr -d ' ') .so) — установка автономна, dnf/интернет не нужны"
else
  echo "  lib/deps не найден — ставлю через dnf (нужен репозиторий/интернет)"
  dnf -y install sqlite-libs libcurl openssl-libs zlib >/dev/null 2>&1 || \
    echo "  ! dnf не сработал — установите sqlite-libs/libcurl/openssl-libs/zlib вручную"
  ls "$SRC"/lib/pg_connector.so "$SRC"/lib/gp_connector.so >/dev/null 2>&1 && \
    { dnf -y install libpq >/dev/null 2>&1 || echo "  ! libpq не установлен (нужен для PG/Greenplum)"; }
  ls "$SRC"/lib/kafka_connector.so >/dev/null 2>&1 && \
    { dnf -y install epel-release >/dev/null 2>&1; dnf -y install librdkafka >/dev/null 2>&1 || echo "  ! librdkafka не установлен (нужен для Kafka; из EPEL)"; }
fi
# Oracle: коннектор и libodpic грузятся из бандла, но подключение требует
# проприетарный Oracle Instant Client (libclntsh.so) — см. README → Oracle.
if ls "$SRC"/lib/oracle_connector.so >/dev/null 2>&1 && ! ls "$SRC"/lib/deps/libclntsh.so* >/dev/null 2>&1; then
  echo "  i Oracle: для подключения установите Oracle Instant Client (libclntsh.so) — см. README"
fi
# Kafka + SASL/GSSAPI (Kerberos): librdkafka реализует SCRAM и PLAIN сама, а
# GSSAPI отдаёт libsasl2, которая грузит механизм ПЛАГИНОМ из /usr/lib64/sasl2.
# Вложить его в бандл нельзя осмысленно: плагин привязан к версии libsasl2, а
# Kerberos всё равно требует /etc/krb5.conf, realm и keytab на самом сервере.
if ls "$SRC"/lib/kafka_connector.so >/dev/null 2>&1 \
   && ! ls /usr/lib64/sasl2/libgssapiv2.so* /usr/lib/sasl2/libgssapiv2.so* >/dev/null 2>&1; then
  echo "  i Kafka: для SASL/GSSAPI (Kerberos) установите cyrus-sasl-gssapi — см. README → Kerberos"
  echo "    (SCRAM-SHA-256/512 и PLAIN работают без него)"
fi

echo "==> Системный пользователь $SVC_USER"
getent group  "$SVC_USER" >/dev/null || groupadd --system "$SVC_USER"
getent passwd "$SVC_USER" >/dev/null || \
  useradd --system --gid "$SVC_USER" --home-dir "$STATE" --shell /sbin/nologin "$SVC_USER"

# ── Остановка сервисов ──────────────────────────────────────────────────────
# Обязательно ДО копирования: ядро не даёт перезаписать файл работающего
# процесса (ETXTBSY). Прежний сервис гасим И выключаем из автозапуска — иначе он
# продолжит держать порт 8080, и новый упадёт с «Address already in use».
if systemctl is-active --quiet "$SVC" 2>/dev/null; then
  echo "==> Останавливаю $SVC на время обновления"
  systemctl stop "$SVC"
fi
if [ "$MODE" = "migrate" ]; then
  if systemctl is-enabled --quiet "$OLD_SVC" 2>/dev/null || systemctl is-active --quiet "$OLD_SVC" 2>/dev/null; then
    echo "==> Останавливаю прежний сервис $OLD_SVC (он держит порт 8080)"
    systemctl disable --now "$OLD_SVC" >/dev/null 2>&1 || true
  fi
fi

# ── Резервная копия ─────────────────────────────────────────────────────────
# Копируем и программу, и базы: программа нужна для откола бинаря, базы — потому
# что новая версия могла поднять схему, и вернуться на старую без копии нельзя.
# Делаем ПОСЛЕ остановки: у остановленного процесса SQLite уже свёл -wal.
if [ "$MODE" != "fresh" ] && [ "$SKIP_BACKUP" != "1" ]; then
  STAMP="$(date +%Y%m%d-%H%M%S)"
  BK="$BACKUP_ROOT/$STAMP${OLD_VERSION:+-$OLD_VERSION}"
  echo "==> Резервная копия → $BK"
  mkdir -p "$BK"
  if [ "$MODE" = "upgrade" ]; then
    [ -d "$PREFIX" ] && cp -a "$PREFIX" "$BK/opt-napastak" 2>/dev/null || true
    [ -d "$STATE/data" ] && cp -a "$STATE/data" "$BK/data" 2>/dev/null || true
  else
    [ -d "$OLD_PREFIX" ] && cp -a "$OLD_PREFIX" "$BK/opt-dataflow-os" 2>/dev/null || true
    [ -d "$OLD_STATE/data" ] && cp -a "$OLD_STATE/data" "$BK/data" 2>/dev/null || true
  fi
  chmod 700 "$BK"
  echo "  $(du -sh "$BK" 2>/dev/null | cut -f1) — откат: см. хвост вывода"
  # Держим последние 5 копий: базы бывают крупными, и без чистки /var/backups
  # тихо съедает диск, а обнаруживается это уже как отказ записи.
  ls -1dt "$BACKUP_ROOT"/*/ 2>/dev/null | tail -n +6 | while read -r old; do
    echo "  чищу старую копию $(basename "$old")"; rm -rf "$old"
  done
fi

# ── Файлы программы ────────────────────────────────────────────────────────
# bin/lib/ui заменяем ЦЕЛИКОМ, а не cp поверх. cp только добавляет и
# перезаписывает: коннектор или библиотека, убранные в новой версии, остались бы
# лежать, а connector_dir сканируется целиком — гейтвей подхватил бы .so от
# прошлого релиза с чужим ABI. config.json и данные тут не участвуют.
echo "==> Файлы программы → $PREFIX"
rm -rf "$PREFIX/bin" "$PREFIX/lib" "$PREFIX/ui"
mkdir -p "$PREFIX"/{bin,lib,ui}
cp -a "$SRC"/bin/.  "$PREFIX"/bin/
cp -a "$SRC"/lib/.  "$PREFIX"/lib/
cp -a "$SRC"/ui/.   "$PREFIX"/ui/
printf '%s\n' "$NEW_VERSION" > "$PREFIX/VERSION"

echo "==> Каталоги данных → $STATE"
mkdir -p "$STATE"/data "$STATE"/pipelines

# ── Перенос состояния с прежней установки ──────────────────────────────────
if [ "$MODE" = "migrate" ]; then
  echo "==> Перенос данных из $OLD_STATE"
  if [ -n "$(ls -A "$STATE/data" 2>/dev/null)" ]; then
    echo "  ! $STATE/data не пуст — оставляю как есть, перенос данных пропущен"
  elif [ -d "$OLD_STATE/data" ]; then
    cp -a "$OLD_STATE/data/." "$STATE/data/"
    echo "  базы: $(ls -1 "$STATE"/data/*.db 2>/dev/null | wc -l | tr -d ' ') шт."
  fi
  if [ -z "$(ls -A "$STATE/pipelines" 2>/dev/null)" ] && [ -d "$OLD_STATE/pipelines" ]; then
    cp -a "$OLD_STATE/pipelines/." "$STATE/pipelines/" 2>/dev/null || true
    echo "  конвейеры: $(ls -1 "$STATE"/pipelines/*.yaml 2>/dev/null | wc -l | tr -d ' ') шт."
  fi
fi

# Начальные конвейеры из дистрибутива — только в пустой каталог, чтобы
# обновление не перезатирало правки оператора и не возвращало удалённые им.
if [ -d "$SRC/pipelines" ] && [ -z "$(ls -A "$STATE/pipelines" 2>/dev/null)" ]; then
  cp -a "$SRC"/pipelines/. "$STATE"/pipelines/ 2>/dev/null || true
fi

# ── Конфигурация ───────────────────────────────────────────────────────────
echo "==> Конфигурация → $PREFIX/config.json"
if [ -f "$PREFIX/config.json" ]; then
  echo "  config.json уже есть — оставляю как есть (секреты и настройки сохранены)"
elif [ "$MODE" = "migrate" ] && [ -f "$OLD_PREFIX/config.json" ]; then
  # Секреты обязаны переехать: со новым jwt_secret слетят все выданные токены,
  # с новым admin_password оператор не войдёт своим паролем. Пути при этом надо
  # переписать на новые, иначе сервис продолжит читать /var/lib/dataflow-os.
  echo "  переношу прежний config.json, переписываю пути, секреты сохраняю"
  python3 - "$OLD_PREFIX/config.json" "$PREFIX/config.json" "$OLD_PREFIX" "$PREFIX" "$OLD_STATE" "$STATE" <<'PY'
import json, sys
src, dst, oldp, newp, olds, news = sys.argv[1:7]
c = json.load(open(src))
for k in ("data_dir", "plugins_dir", "connector_dir", "ui_dir", "tls_cert", "tls_key"):
    v = c.get(k)
    if isinstance(v, str) and v:
        c[k] = v.replace(olds, news).replace(oldp, newp)
json.dump(c, open(dst, "w"), indent=2, ensure_ascii=False)
print("  jwt_secret: %s, admin_password: %s" % (
      "перенесён" if c.get("jwt_secret") else "нет в прежнем конфиге",
      "перенесён" if c.get("admin_password") else "нет в прежнем конфиге"))
PY
else
  JWT="$(openssl rand -hex 32)"
  PASS="${DFO_ADMIN_PASSWORD:-$(openssl rand -base64 12)}"
  python3 - "$SRC/config.json" "$PREFIX/config.json" "$JWT" "$PASS" <<'PY'
import json, sys
src, dst, jwt, pw = sys.argv[1:5]
c = json.load(open(src))
c["jwt_secret"] = jwt
c["admin_password"] = pw
json.dump(c, open(dst, "w"), indent=2, ensure_ascii=False)
PY
  echo "  сгенерирован jwt_secret; admin-пароль: $PASS"
  echo "  (поменять можно в $PREFIX/config.json, далее: systemctl restart napastak)"
fi

echo "==> Права"
chown -R "$SVC_USER":"$SVC_USER" "$PREFIX" "$STATE"
chmod 640 "$PREFIX/config.json"

echo "==> systemd-сервис"
# Переопределения оператора живут в napastak.service.d/ (systemctl edit) и
# перезаписью основного юнита не затрагиваются — специально не чистим их.
install -m 644 "$SRC/$SVC" "/etc/systemd/system/$SVC"
systemctl daemon-reload
systemctl enable "$SVC" >/dev/null 2>&1 || true
systemctl restart "$SVC"

echo "==> Готово. Статус:"
sleep 1
systemctl --no-pager --full status "$SVC" | head -6 || true
echo ""
if [ "$MODE" = "migrate" ]; then
  echo "Перенос завершён. Прежняя установка ОСТАВЛЕНА и выключена — на случай отката:"
  echo "  $OLD_PREFIX, $OLD_STATE, сервис $OLD_SVC (disabled)"
  echo "Убедившись, что всё работает, удалите её:"
  echo "  sudo rm -rf $OLD_PREFIX $OLD_STATE /etc/systemd/system/$OLD_SVC"
  echo "  sudo userdel $OLD_USER"
  echo "Вход — прежним admin-паролем из перенесённого config.json."
elif [ "$MODE" = "upgrade" ]; then
  echo "Обновлено${OLD_VERSION:+ $OLD_VERSION →} $NEW_VERSION. Откат на прежнюю версию:"
  echo "  sudo systemctl stop $SVC"
  echo "  sudo rm -rf $PREFIX/bin $PREFIX/lib $PREFIX/ui"
  echo "  sudo cp -a <копия>/opt-napastak/{bin,lib,ui,VERSION} $PREFIX/"
  echo "  sudo cp -a <копия>/data/. $STATE/data/     # только если схема менялась"
  echo "  sudo chown -R $SVC_USER:$SVC_USER $PREFIX $STATE && sudo systemctl start $SVC"
  echo "Копии: $BACKUP_ROOT"
fi
echo "UI:    http://<server>:8080   (логин admin)"
echo "Логи:  journalctl -u napastak -f"
echo "Если включён firewalld:  firewall-cmd --add-port=8080/tcp --permanent && firewall-cmd --reload"
