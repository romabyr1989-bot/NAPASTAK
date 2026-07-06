# Сборка и развёртывание NAPASTAK на RedOS 8.02

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

`make dist` вкладывает в `lib/deps/` все рантайм-библиотеки — `libsqlite3 libcurl
libssl libcrypto libz libpq librdkafka libodpic` + транзитивные (sasl2, lz4,
zstd, krb5, ldap, …), кроме glibc/ld-linux/libgcc_s (берутся из целевой ОС).
Docker-сборка (вариант B) ставит зависимости **всех 10 коннекторов**, поэтому
бандл self-contained. Папку можно поставить на сервер **без интернета и без dnf**:

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

## Коннекторы — все 10 в бандле

Docker-сборка (`packaging/redos/build.sh`, вариант B) собирает и вкладывает в
бандл **все 10 коннекторов** вместе с их рантайм-`.so` в `lib/deps` — работают
офлайн «из коробки»:

| Коннектор | Рантайм-либа (вложена в `lib/deps`) | Дополнительно |
|---|---|---|
| csv, parquet, json_http, siebel, xml, soap | libcurl, libz (+ ssl) | — |
| pg, greenplum | `libpq.so.5` | нужен доступ к серверу PostgreSQL/Greenplum |
| kafka | `librdkafka.so.1` | нужен брокер Kafka |
| oracle | `libodpic.so.4` (ODPI-C) | **+ Oracle Instant Client в рантайме — см. ниже** |

Dockerfile ставит `libpq-devel`, `librdkafka-devel` (EPEL+CRB) и собирает ODPI-C
из исходников (UPL/Apache — свободно распространяется). Сборка **падает**, если
хоть один из 10 коннекторов не попал в бандл (sanity-проверка в конце).

При сборке **прямо на сервере** (вариант A) для kafka/oracle доустановите:
```bash
sudo dnf install -y epel-release && sudo dnf config-manager --set-enabled crb
sudo dnf install -y librdkafka-devel                     # kafka
# ODPI-C (oracle): собрать из исходников (проприетарный клиент — отдельно):
curl -fsSL https://github.com/oracle/odpi/archive/refs/tags/v4.6.1.tar.gz | tar xz
make -C odpi-4.6.1 && sudo cp odpi-4.6.1/lib/libodpic.so* /usr/local/lib/ \
  && sudo cp odpi-4.6.1/include/dpi.h /usr/local/include/ \
  && echo /usr/local/lib | sudo tee /etc/ld.so.conf.d/odpi.conf && sudo ldconfig
```

### Oracle: Instant Client (единственная невложимая зависимость)

`oracle_connector.so` и `libodpic.so` **загружаются офлайн из бандла**, но само
подключение к Oracle требует проприетарный **Oracle Instant Client** (`libclntsh.so`),
который нельзя распространять в бандле. Установите его на сервере один раз:

```bash
# со скачанного RPM (basic-пакет) или tar.gz с сайта Oracle:
sudo dnf install -y oracle-instantclient-basic-*.rpm     # ставит libclntsh.so в /usr/lib/oracle/.../lib
# и добавьте путь в LD_LIBRARY_PATH сервиса, либо скопируйте libclntsh.so* и его
# зависимости в /opt/dataflow-os/lib/deps (systemd уже смотрит туда):
sudo cp /usr/lib/oracle/*/client64/lib/libclntsh.so* /opt/dataflow-os/lib/deps/ && sudo ldconfig
sudo systemctl restart dataflow-os
```
Остальные 9 коннекторов работают без каких-либо внешних установок.

### Kafka: Kerberos (GSSAPI)

Коннектор Kafka поддерживает механизм SASL **GSSAPI (Kerberos)** — стандарт
enterprise/банковских кластеров. В форме конвейера секретов нет: выбирается
только механизм «GSSAPI», а аутентификация настраивается **на сервере gateway**
(«под капотом»). Нужно три вещи:

1. **Рантайм-плагин SASL** — иначе librdkafka не сможет в GSSAPI:
   ```bash
   sudo dnf install -y cyrus-sasl-gssapi krb5-workstation   # даёт /usr/lib64/sasl2/libgssapiv2.so
   ```
   (в офлайн-среде — поставьте эти RPM из локального репозитория/зеркала.)

2. **`/etc/krb5.conf`** с realm → KDC вашего домена (`[libdefaults] default_realm`,
   `[realms] … kdc = …`). Проверка: `kinit <user>@REALM` должен выдать билет.

3. **Учётные данные** — один из двух режимов (никаких паролей в конвейере):

   * **Keytab (рекомендуется)** — ops кладёт keytab и задаёт две переменные
     окружения сервиса. librdkafka сама держит билет свежим:
     ```bash
     sudo install -m 0640 -o dataflow -g dataflow client.keytab /opt/dataflow-os/certs/
     sudo systemctl edit dataflow-os      # добавить в [Service]:
     #   Environment=KAFKA_KERBEROS_KEYTAB=/opt/dataflow-os/certs/client.keytab
     #   Environment=KAFKA_KERBEROS_PRINCIPAL=svc-dfo@REALM.EXAMPLE.COM
     #   Environment=KRB5_CONFIG=/etc/krb5.conf
     sudo systemctl restart dataflow-os
     ```
   * **Внешний кэш билетов** — если билет обновляется системно (cron `kinit` /
     `k5start`): ничего в env не задавайте, коннектор возьмёт готовый ccache
     (`KRB5CCNAME`). Внутренний kinit при этом отключён (kinit.cmd=`true`).

   Имя сервис-принципала брокера по умолчанию `kafka`; переопределяется
   переменной `KAFKA_KERBEROS_SERVICE_NAME`.

Проверено end-to-end против MIT KDC + Apache Kafka с SASL/GSSAPI-листенером
(оба режима: keytab и внешний ccache).

## Примечание про базовый образ

`rockylinux:9` совпадает с RedOS 8 по поколению библиотек (glibc 2.34, OpenSSL 3),
поэтому собранный бинарь должен запускаться на RedOS 8.02. Для 100% соответствия
ABI собирайте на самой RedOS (вариант A) или укажите образ RedOS в `BASE_IMAGE`.
