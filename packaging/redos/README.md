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

### Подключения живут постоянно

Сессия к внешней системе не поднимается на время прогона и не гасится после
него: гейтвей держит её постоянно и пингует раз в минуту. В разделе
«Подключения» это видно плашкой — «подключено», «нет связи» или «отключено», —
а в форме подключения тем же состоянием окрашена кнопка.

  - **Подключить** — поднять сессию немедленно, не дожидаясь фонового обхода.
    Считается поднятой только после успешной проверки связи.
  - **Отключить** — разорвать по решению оператора, например на время
    обслуживания внешней системы. Разрыв держится до явного «Подключить»: он
    записан в справочнике и переживает и обход, и перезапуск гейтвея.
  - **Обрыв по технической причине** гасит индикатор сам в течение минуты, с
    указанием причины. Когда система возвращается, сессия восстанавливается
    без участия оператора.

Если параметры доступа заданы прямо в шаге, а не взяты из справочника,
постоянной сессии для них не существует — там кнопка выполняет разовую
проверку связи и подписана «Связь есть».

### MS SQL Server: драйвер ODBC и охват версий

Коннектор говорит с **unixODBC**, а конкретный драйвер под ней подменяется без
изменения кода — та же схема, что у Oracle (наш код → ODPI-C → Instant Client).

**Имя драйвера указывать не нужно.** Поле «Драйвер ODBC» в подключении можно
оставить пустым: коннектор переберёт кандидатов по порядку — `NAPASTAK-FreeTDS`
(вложенный в поставку, его регистрирует `install.sh`), `FreeTDS` (системный),
`ODBC Driver 18 for SQL Server`, `ODBC Driver 17 for SQL Server`, `SQL Server`.
Первым идёт вложенный, потому что он собран и проверен вместе с коннектором, а
системный FreeTDS может оказаться сколь угодно старым.

Перебор включается ТОЛЬКО когда драйвера нет в системе (`SQLSTATE IM002`).
Отказ сервера — неверный пароль, закрытый порт, нет базы — прекращает попытки
сразу: иначе одна опечатка в пароле превращалась бы в пять подключений подряд.
Если поле заполнено, используется ровно указанный драйвер и ошибка приходит
про него. Когда не подошёл ни один кандидат, в сообщении перечислено, какие
имена искали.

| драйвер | откуда | когда нужен |
|---|---|---|
| **FreeTDS** | **вложен в бандл**, регистрируется установщиком | по умолчанию; офлайн-установка работает без доустановок |
| msodbcsql18 | ставится отдельно с сайта Microsoft | Always Encrypted, Azure AD, специфика Microsoft |

`install.sh` регистрирует вложенный драйвер в `/etc/odbcinst.ini` под именами
`[NAPASTAK-FreeTDS]` и `[FreeTDS]` (второе — только если системной записи нет,
чтобы не перебивать уже настроенную). Имя драйвера при необходимости задаётся
в подключении полем **«Драйвер ODBC»**: пусто — автоподбор (см. выше), для
драйвера Microsoft пишется `ODBC Driver 18 for SQL Server`.

**Охват версий.** Версия протокола TDS согласуется автоматически (`auto`), что
покрывает ряд от SQL Server 7.0 до 2022. Синтаксис пагинации выбирается по
`SERVERPROPERTY('ProductMajorVersion')`:

| версия сервера | major | пагинация |
|---|---|---|
| 2012 и новее | ≥ 11 | `OFFSET … FETCH NEXT` |
| 2005 – 2008 R2 | 9–10 | `ROW_NUMBER()` |
| 2000 и старше | < 9 | вложенный `TOP` |

Выбранный режим виден в журнале при подключении:

```
mssql: подключено к sql.prod (мажорная версия 16, драйвер FreeTDS, пагинация OFFSET/FETCH)
```

На серверах 2000 и старше коннектор дополнительно предупреждает: вложенный
`TOP` устойчив **только при уникальной колонке сортировки** — на дубликатах
страницы могут повторять строки. Там следует задавать `cursor_column` с
уникальным значением.

**Кодировка.** Для FreeTDS подставляется `ClientCharset=UTF-8`: без него он
работает в ISO-8859-1 и молча заменяет кириллицу из `NVARCHAR` на «?» — в базе
данные целы, а до приложения доезжает мусор. Драйверу Microsoft этот ключ не
нужен, и он на него ругается, поэтому добавляется только для FreeTDS.

**Если версия не определяется.** Обычно коннектор узнаёт её сам через
`SERVERPROPERTY`. Сервер за прокси или учётная запись с урезанными правами
могут её не отдать — тогда без подсказки выбирается самый консервативный
синтаксис. Поле `server_version` в настройках подключения задаёт мажорную
версию вручную (`16` — 2022, `10` — 2008 R2, `8` — 2000); выбор синтаксиса
пагинации и типов колонок идёт по ней.

**Типы.** Значения читаются текстом, логический тип несут метаданные (как в
PostgreSQL и Oracle). `decimal`, `numeric` и `money` намеренно НЕ приводятся к
двоичному `DOUBLE` — это деньги и идентификаторы, потеря точности недопустима.
Как приёмник коннектор создаёт колонки текстовыми: `NVARCHAR(MAX)` на 2005+ и
`NTEXT` на 2000 (там `NVARCHAR(MAX)` ещё не существует). Значения уходят
параметрами широкого типа (`SQL_WVARCHAR`, для длинных — `SQL_WLONGVARCHAR`):
при узком `SQL_VARCHAR` драйвер сворачивает UTF-8 в кодировку сервера, и на
латинской сортировке кириллица не проходит вовсе — FreeTDS отвечает
`[HY000] Unknown error` без указания причины. Пустое значение и `NULL`
различаются: `NULL` передаётся индикатором и в таблице остаётся `NULL`.

**Повторный прогон и дубликаты.** Поле «Ключ идемпотентности» у приёмника
задаёт колонки, по которым строка считается той же самой. Заполнено — повторный
прогон обновит существующую строку; пусто — добавит ещё одну. На SQL Server
2008 и новее это делается запросом `MERGE` (с подсказкой `HOLDLOCK`, иначе две
параллельные записи могут вставить дубликат ключа), на 2005 и старше —
парой «обновить, а если строки не было — вставить». Колонки ключа создаются
типом `NVARCHAR(450)`: `NTEXT` сравнивать запрещено вовсе, а `NVARCHAR(MAX)`
нельзя проиндексировать, и поиск совпадения шёл бы перебором таблицы.

**Запрос вместо таблицы.** В поле источника можно написать не имя таблицы, а
`SELECT`. Постранично такой источник читается только при заданном
поле-отметке: у произвольного запроса нет устойчивого порядка строк, поэтому
смещение разъезжалось бы между страницами. Без отметки берётся одна порция
размером «Строк за раз», и если она уперлась в предел, в журнал пишется
предупреждение — молча отдать часть строк за все нельзя.

**Шифрование канала** называется у драйверов по-разному: у Microsoft это
`Encrypt` вместе с `TrustServerCertificate`, у FreeTDS — `Encryption`, а чужой
ключ драйвер просто игнорирует. Коннектор подставляет тот, который понимает
выбранный драйвер; проверить результат можно запросом
`SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID`.

**Батч атомарен.** Пачка строк пишется одной транзакцией: либо принята
целиком, либо приёмник остаётся ровно в прежнем состоянии. Без этого
оборванная на середине вставка оставляла бы «хвост», а повтор шага
планировщиком дублировал бы уже записанные строки. В режиме перезаписи
очистка входит в ту же транзакцию — неудачный прогон не оставит таблицу
пустой. Оборотная сторона: очень большой `batch_size` держит блокировки на
таблице до конца пачки; по умолчанию 8192 строки.

### Oracle: клиент вложен в поставку

`oracle_connector.so` — только обёртка над ODPI-C, а сам клиент Oracle она
ищет через `dlopen` по имени `libclntsh.so`. Без него коннектор отвечает
`DPI-1047: Cannot locate a 64-bit Oracle Client library`, и доустановить его в
закрытом контуре неоткуда. Поэтому **Instant Client Basic Light вложен в
поставку**: `libclntsh`, `libclntshcore`, `libnnz` и `libociicus` лежат в
`lib/deps`, куда systemd-сервис и так смотрит через `LD_LIBRARY_PATH`. Ничего
доустанавливать не требуется.

Из-за этого поставка весит около 55 МБ вместо 15 — почти весь прирост даёт
одна `libclntsh.so` (95 МБ в распакованном виде).

Условия использования клиента — в файле `ORACLE_INSTANT_CLIENT_LICENSE` в
корне поставки; он распространяется по лицензии Oracle OTN, разрешающей
передачу в составе приложения.

Свой клиент вместо вложенного ставится обычным образом — коннектор возьмёт тот,
который найдёт первым по `LD_LIBRARY_PATH`:

```bash
sudo rm /opt/napastak/lib/deps/libclntsh.so*     # убрать вложенный
sudo cp /usr/lib/oracle/*/client64/lib/libclntsh.so* /opt/napastak/lib/deps/
sudo systemctl restart napastak
```

Сборка вкладывает клиент, если находит его на машине сборки в `/opt/oracle/`,
`/usr/lib/oracle/*/client64/lib` или по пути из переменной:
`make dist ORACLE_IC=/путь/к/instantclient_XX_YY`. Если не найден — печатается
предупреждение, и поставка собирается без него.

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
