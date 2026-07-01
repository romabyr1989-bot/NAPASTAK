# Сборка и развёртывание DataFlow OS на RedOS 8.02

RedOS 8 — RHEL-9-совместимый дистрибутив (glibc 2.34, OpenSSL 3, RPM/dnf).
Сборка работает «из коробки»: Makefile линкует gateway с `-rdynamic` (плагины
находят символы ядра при `dlopen`), а все объекты собираются с `-fPIC` (иначе на
Linux падает релокация TLS `R_X86_64_TPOFF32` при сборке `.so`).

Есть два пути: собрать **на сервере** (гарантированный ABI) или **в Docker**.

## Вариант A — сборка прямо на сервере RedOS (рекомендуется)

```bash
# зависимости сборки
sudo dnf install -y gcc make sqlite-devel libcurl-devel openssl-devel zlib-devel
# опционально:
sudo dnf install -y libpq-devel          # коннекторы PostgreSQL / Greenplum
sudo dnf install -y librdkafka-devel      # коннектор Kafka (нужен EPEL)
#   Oracle (ODPI-C) ставится отдельно — см. ниже

git clone <repo> dataflow-os && cd dataflow-os
make BUILD=release          # сборка всего (gateway + плагины)
make BUILD=release dist     # → build/release/dist/dataflow-os-<ver>-linux-x86_64.tar.gz

# установка
cd build/release/dist && tar xzf dataflow-os-*.tar.gz && cd dataflow-os-*
sudo ./install.sh
```

## Вариант B — сборка в Docker (с рабочей машины)

```bash
./packaging/redos/build.sh                 # база rockylinux:9 (класс RedOS 8)
# или на образе самой RedOS (точный ABI):
BASE_IMAGE=<redos-image> ./packaging/redos/build.sh
# → ./dist/dataflow-os-<ver>-linux-x86_64.tar.gz

# разворачивание на сервере
scp dist/dataflow-os-*.tar.gz user@server:/tmp/
ssh user@server 'cd /tmp && tar xzf dataflow-os-*.tar.gz && cd dataflow-os-* && sudo ./install.sh'
```

## Автономная (офлайн) установка

`make dist` вкладывает в `lib/deps/` все рантайм-библиотеки (`libsqlite3 libcurl
libssl libcrypto libz libpq` + транзитивные), кроме glibc/ld-linux. Поэтому папку
можно поставить на сервер **без интернета и без dnf**:

```bash
scp -r dataflow-os-<ver>-linux-x86_64 user@server:/tmp/
ssh user@server 'cd /tmp/dataflow-os-* && sudo ./install.sh'   # dnf не нужен — библиотеки вложены
```

Или запустить прямо из папки, ничего не устанавливая:

```bash
./run.sh                 # поднимет gateway с LD_LIBRARY_PATH=lib/deps
```

systemd-юнит и install.sh используют `LD_LIBRARY_PATH=/opt/dataflow-os/lib/deps`.
Если `lib/deps/` нет (сборка не на Linux), install.sh откатывается на `dnf install`.

## Что делает install.sh

- Рантайм-библиотеки: если вложены в `lib/deps` — ставить ничего не нужно (офлайн);
  иначе ставит через dnf (`sqlite-libs libcurl openssl-libs zlib`, при PG — `libpq`).
- Создаёт системного пользователя `dataflow`.
- Кладёт программу в `/opt/dataflow-os` (bin/, lib/, ui/), данные — в `/var/lib/dataflow-os` (data/, pipelines/).
- Генерирует `config.json` со случайными `jwt_secret` и admin-паролем (печатает пароль).
- Ставит и запускает systemd-сервис `dataflow-os`.

```bash
systemctl status dataflow-os
journalctl -u dataflow-os -f
# UI: http://<server>:8080  (логин admin)
# firewalld: firewall-cmd --add-port=8080/tcp --permanent && firewall-cmd --reload
```

## Раскладка на сервере

```
/opt/dataflow-os/        bin/dfo_gateway  lib/*.so  ui/  config.json   (WorkingDirectory)
/var/lib/dataflow-os/    data/  pipelines/                              (запись от пользователя dataflow)
/etc/systemd/system/dataflow-os.service
```

## Рантайм-зависимости (SONAME, должны быть на RedOS 8)

`libssl.so.3 libcrypto.so.3` (openssl-libs), `libsqlite3.so.0` (sqlite-libs),
`libcurl.so.4` (libcurl), `libz.so.1` (zlib), `libpthread/libm/libdl/libc` (glibc).
PG/Greenplum дополнительно: `libpq.so.5` (libpq).

## Опциональные коннекторы

| Коннектор | Сборочная зависимость | Примечание |
|---|---|---|
| PostgreSQL / Greenplum | `libpq-devel` | собираются автоматически при наличии |
| Kafka | `librdkafka-devel` | из EPEL: `dnf install epel-release && dnf install librdkafka-devel` |
| Oracle | ODPI-C (`libodpic`) | + Oracle Instant Client в рантайме; ставится вручную |

Makefile определяет наличие зависимостей сам и собирает только доступные плагины
(базовый набор csv/parquet/json_http/siebel/xml/soap требует только libcurl+zlib).

## Примечание про базовый образ

`rockylinux:9` совпадает с RedOS 8 по поколению библиотек (glibc 2.34, OpenSSL 3),
поэтому собранный бинарь должен запускаться на RedOS 8.02. Для 100% соответствия
ABI собирайте на самой RedOS (вариант A) или укажите образ RedOS в `BASE_IMAGE`.
