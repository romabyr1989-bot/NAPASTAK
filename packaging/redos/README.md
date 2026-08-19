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

git clone <repo> napastak && cd napastak
make BUILD=release          # сборка всего (gateway + плагины)
make BUILD=release dist     # → build/release/dist/napastak-<ver>-linux-x86_64.tar.gz

# установка
cd build/release/dist && tar xzf napastak-*.tar.gz && cd napastak-*
sudo ./install.sh
```

## Вариант B — сборка в Docker (с рабочей машины)

```bash
./packaging/redos/build.sh                 # база rockylinux:9 (класс RedOS 8)
# или на образе самой RedOS (точный ABI):
BASE_IMAGE=<redos-image> ./packaging/redos/build.sh
# → ./dist/napastak-<ver>-linux-x86_64.tar.gz

# разворачивание на сервере
scp dist/napastak-*.tar.gz user@server:/tmp/
ssh user@server 'cd /tmp && tar xzf napastak-*.tar.gz && cd napastak-* && sudo ./install.sh'
```

## Автономная (офлайн) установка

`make dist` вкладывает в `lib/deps/` все рантайм-библиотеки — `libsqlite3 libcurl
libssl libcrypto libz libpq librdkafka libodpic` + транзитивные (sasl2, lz4,
zstd, krb5, ldap, …), кроме glibc/ld-linux/libgcc_s (берутся из целевой ОС).
Docker-сборка (вариант B) ставит зависимости **всех 10 коннекторов**, поэтому
бандл self-contained. Папку можно поставить на сервер **без интернета и без dnf**:

```bash
scp -r napastak-<ver>-linux-x86_64 user@server:/tmp/
ssh user@server 'cd /tmp/napastak-* && sudo ./install.sh'   # dnf не нужен — библиотеки вложены
```

Или запустить прямо из папки, ничего не устанавливая:

```bash
./run.sh                 # поднимет gateway с LD_LIBRARY_PATH=lib/deps
```

systemd-юнит и install.sh используют `LD_LIBRARY_PATH=/opt/napastak/lib/deps`.
Если `lib/deps/` нет (сборка не на Linux), install.sh откатывается на `dnf install`.

## Что делает install.sh

- Рантайм-библиотеки: если вложены в `lib/deps` — ставить ничего не нужно (офлайн);
  иначе ставит через dnf (`sqlite-libs libcurl openssl-libs zlib`, при PG — `libpq`).
- Создаёт системного пользователя `napastak`.
- Кладёт программу в `/opt/napastak` (bin/, lib/, ui/), данные — в `/var/lib/napastak` (data/, pipelines/).
- Генерирует `config.json` со случайными `jwt_secret` и admin-паролем (печатает
  пароль) — **только при первой установке**; существующий конфиг не трогает.
- Записывает установленную версию в `/opt/napastak/VERSION`.
- Ставит и запускает systemd-сервис `napastak`.

Если на сервере уже что-то стоит, установщик работает как обновление или как
перенос со прежней `dataflow-os` — см. «Обновление и перенос прежних установок».

```bash
systemctl status napastak
journalctl -u napastak -f
# UI: http://<server>:8080  (логин admin)
# firewalld: firewall-cmd --add-port=8080/tcp --permanent && firewall-cmd --reload
```

## Раскладка на сервере

```
/opt/napastak/        bin/napastak_gateway  lib/*.so  ui/  config.json   (WorkingDirectory)
/var/lib/napastak/    data/  pipelines/                              (запись от пользователя napastak)
/etc/systemd/system/napastak.service
```

## Обновление и перенос прежних установок

`install.sh` сам определяет, что делать, и всё нужное выполняет без ручных шагов.

| что найдено на сервере | режим | что происходит |
|---|---|---|
| ничего | чистая установка | генерируются `jwt_secret` и admin-пароль (печатается) |
| `/opt/napastak` | обновление | данные, конвейеры, `config.json` и секреты сохраняются |
| `/opt/dataflow-os` | перенос | прежний сервис гасится и выключается, данные, конвейеры и секреты переносятся |

```bash
scp napastak-<ver>-linux-x86_64.tar.gz user@server:/tmp/
ssh user@server 'cd /tmp && tar xzf napastak-*.tar.gz && cd napastak-* && sudo ./install.sh'
```

### Что переживает обновление

Данные таблиц (`wal.bin`, `idx_*.btree`, `schema.json`), базы `catalog.db`,
`rbac.db`, `audit.db`, инкрементальные курсоры `data/.cursors/*.cur`, конвейеры в
`/var/lib/napastak/pipelines`, `config.json` целиком (вместе с `jwt_secret` —
выданные токены остаются действительными — и admin-паролем), а также
переопределения юнита в `napastak.service.d/`, сделанные через `systemctl edit`.

Начальные конвейеры из дистрибутива кладутся только в ПУСТОЙ каталог: обновление
не перезатирает правки оператора и не возвращает удалённые им конвейеры.

Имена consumer-групп Kafka (`dfo-*`) при переименовании проекта намеренно не
менялись — офсеты на брокере остаются за прежними группами, и после обновления
конвейеры продолжают читать с того места, где остановились.

### Резервная копия и откат

Перед обновлением install.sh складывает программу и базы в
`/var/backups/napastak/<дата>-<версия>/` (хранятся последние 5; отключается
`SKIP_BACKUP=1`). Копия снимается ПОСЛЕ остановки сервиса — у остановленного
процесса SQLite уже свёл `-wal`, поэтому копия целостна.

```bash
sudo systemctl stop napastak
sudo rm -rf /opt/napastak/{bin,lib,ui}
sudo cp -a /var/backups/napastak/<дата>-<версия>/opt-napastak/{bin,lib,ui,VERSION} /opt/napastak/
sudo cp -a /var/backups/napastak/<дата>-<версия>/data/. /var/lib/napastak/data/   # только если схема менялась
sudo chown -R napastak:napastak /opt/napastak /var/lib/napastak
sudo systemctl start napastak
```

### Версия и схема баз

Версия установленного лежит в `/opt/napastak/VERSION`. Установщик по ней
отличает обновление от понижения: понижение прерывается с кодом 1 и не трогает
сервис, потому что новая версия могла поднять схему. Осознанное понижение —
`sudo FORCE=1 ./install.sh`.

Версия схемы хранится в `PRAGMA user_version` каждой базы. Гейтвей отказывается
стартовать на базе, созданной более новой версией (выход с кодом 78, юнит помечен
`RestartPreventExitStatus=78`, поэтому перезапуска по кругу нет) — это защита от
подмены бинаря руками в обход установщика.

### Перенос: что остаётся после него

Старое дерево НЕ удаляется — это точка отката:

```
/opt/dataflow-os, /var/lib/dataflow-os, dataflow-os.service (disabled)
```

Убедившись, что новый сервис работает, удалите:

```bash
sudo rm -rf /opt/dataflow-os /var/lib/dataflow-os /etc/systemd/system/dataflow-os.service
sudo userdel dataflow
```

Вход после переноса — прежним admin-паролем: он переносится вместе с конфигом.
Пути `data_dir`, `plugins_dir`, `connector_dir` в перенесённом `config.json`
переписываются на новые автоматически.

Имена переменных окружения (`DFO_TOKEN`, `DFO_REST_URL`, `DFO_PGWIRE_DSN`,
`DFO_ADMIN_PASSWORD`) и ABI коннекторов не менялись — существующие скрипты и
сторонние коннекторы продолжают работать.

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
# зависимости в /opt/napastak/lib/deps (systemd уже смотрит туда):
sudo cp /usr/lib/oracle/*/client64/lib/libclntsh.so* /opt/napastak/lib/deps/ && sudo ldconfig
sudo systemctl restart napastak
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

   В бандл этот плагин намеренно не вложен: libsasl2 грузит механизмы из
   `/usr/lib64/sasl2` и они привязаны к её версии, а Kerberos всё равно требует
   `krb5.conf`, realm и keytab на самом сервере — вложенный `.so` работоспособным
   его не сделал бы. `install.sh` проверяет наличие плагина и предупреждает, если
   его нет. **SCRAM-SHA-256/512 и PLAIN от этого не зависят** — их librdkafka
   реализует сама.

2. **`/etc/krb5.conf`** с realm → KDC вашего домена (`[libdefaults] default_realm`,
   `[realms] … kdc = …`). Проверка: `kinit <user>@REALM` должен выдать билет.

3. **Учётные данные** — один из двух режимов (никаких паролей в конвейере):

   * **Keytab (рекомендуется)** — ops кладёт keytab и задаёт две переменные
     окружения сервиса. librdkafka сама держит билет свежим:
     ```bash
     sudo install -m 0640 -o napastak -g napastak client.keytab /opt/napastak/certs/
     sudo systemctl edit napastak      # добавить в [Service]:
     #   Environment=KAFKA_KERBEROS_KEYTAB=/opt/napastak/certs/client.keytab
     #   Environment=KAFKA_KERBEROS_PRINCIPAL=svc-dfo@REALM.EXAMPLE.COM
     #   Environment=KRB5_CONFIG=/etc/krb5.conf
     sudo systemctl restart napastak
     ```
   * **Внешний кэш билетов** — если билет обновляется системно (cron `kinit` /
     `k5start`): ничего в env не задавайте, коннектор возьмёт готовый ccache
     (`KRB5CCNAME`). Внутренний kinit при этом отключён (kinit.cmd=`true`).

   Имя сервис-принципала брокера по умолчанию `kafka`; переопределяется
   переменной `KAFKA_KERBEROS_SERVICE_NAME`.

Проверено end-to-end против MIT KDC + Apache Kafka с SASL/GSSAPI-листенером
(оба режима: keytab и внешний ccache).

### Kafka: требование к версии librdkafka при SCRAM

**Для брокеров Kafka 4.0+ и Confluent Platform 8.0+ нужна librdkafka не ниже
2.6.1.** Системный пакет RedOS 8 (`librdkafka-1.6.1`) с ними несовместим по SCRAM.

Причина. librdkafka до 2.6.1 приклеивала клиентский nonce второй раз в
client-final-message — дефект живёт с версии 0.0.99 (исправление #4895).
Брокеры Apache Kafka 3.8.1-3.9.x это прощали: сверяли nonce через `endsWith`.
В Kafka 4.0 вернули строгое `equals` (`ScramSaslServer.java`), и рукопожатие
отвергается с текстом `Authentication failed ... due to invalid credentials`,
неотличимым от неверного пароля. Confluent Platform 8.0 построен на Kafka 4.0.

Проверено на стенде, один и тот же пользователь и пароль:

| librdkafka | Confluent 8.0.0 | Apache Kafka 3.9.1 |
|---|---|---|
| 1.6.1 | отказ | работает |
| 2.6.1 | работает | работает |

Коннектор предупреждает об этом в журнале при старте, если механизм SCRAM, а
библиотека старше 2.6.1.

Замена без переустановки:

```bash
sudo systemctl stop napastak
sudo cp /opt/napastak/lib/deps/librdkafka.so.1 /opt/napastak/lib/deps/librdkafka.so.1.bak
sudo cp librdkafka.so.1 /opt/napastak/lib/deps/
sudo systemctl start napastak
journalctl -u napastak | grep "librdkafka"     # должно быть 2.6.1
```

Собрать самостоятельно (нужен gcc-c++):

```bash
curl -fsSL https://github.com/confluentinc/librdkafka/archive/refs/tags/v2.6.1.tar.gz | tar xz
cd librdkafka-2.6.1 && ./configure --prefix=/usr/local --enable-ssl --enable-sasl --enable-zlib
make -j$(nproc) && sudo make install
```

PLAIN и GSSAPI дефект не затрагивает — nonce есть только в SCRAM.

### Kafka: TLS/mTLS — форматы сертификатов

Коннектор Kafka читает сертификаты в разных форматах — формат каждого файла
определяется автоматически (по содержимому, не по расширению), так что банковские
PKI, выдающие материал в PEM или в Java-совместимом виде, работают без ручной
подготовки. Пути к файлам задаются в конфиге подключения (CA + клиентский
cert/key), файлы должны быть читаемы пользователем сервиса `napastak`.

| Формат | CA (`ssl_ca_location`) | Клиент (mTLS) | Как задать |
|--------|------------------------|---------------|------------|
| **PEM** (`.pem`/`.crt`/`.key`) | ✅ напрямую | ✅ cert + key раздельно | `ssl_certificate_location` + `ssl_key_location` |
| **DER** (`.der`/`.cer`) | ✅ авто-конверсия в PEM | ✅ cert + key раздельно (авто-конверсия) | те же поля, файлы в DER |
| **PKCS#12** (`.p12`/`.pfx`) | — (truststore берите в PEM/DER) | ✅ cert + key одним файлом | `ssl_keystore_location` **или** тот же `.p12` в `ssl_certificate_location`; пароль — в `ssl_key_password` |
| **JKS** (`.jks`) | ❌ не поддерживается librdkafka | ❌ | сконвертируйте в PKCS#12: `keytool -importkeystore -srckeystore x.jks -destkeystore x.p12 -deststoretype PKCS12` |

Для PKCS#12 отдельный файл ключа не нужен (cert+key внутри контейнера); пароль
контейнера указывается в поле «Пароль ключа / keystore». DER-файлы коннектор
конвертирует в PEM во временный файл (`/tmp/dfo_kafka_*`), который удаляется при
закрытии подключения. Проверка сертификата брокера не отключается.

Проверено end-to-end против Apache Kafka с SSL-листенером (mTLS): PEM, DER
(cert/key и CA), PKCS#12 (авто-определение и явный keystore), а также их
комбинации — все читаются и подключаются.

## Примечание про базовый образ

`rockylinux:9` совпадает с RedOS 8 по поколению библиотек (glibc 2.34, OpenSSL 3),
поэтому собранный бинарь должен запускаться на RedOS 8.02. Для 100% соответствия
ABI собирайте на самой RedOS (вариант A) или укажите образ RedOS в `BASE_IMAGE`.
