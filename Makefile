# NAPASTAK — build system
CC      = gcc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic \
           -D_POSIX_C_SOURCE=200809L -D_GNU_SOURCE -fPIC \
           -Ilib -Ilib/core -Ilib/net -Ilib/storage \
           -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler \
           -Ilib/observ -Ilib/connector -Ilib/index
LDFLAGS = -lpthread -lm -ldl -lsqlite3 -lcurl -lcrypto -lssl

# Release flags (compression enabled)
REL_FLAGS = -O2 -DNDEBUG -flto -DENABLE_COMPRESSION=1
# Debug flags (compression disabled — упрощает отладку WAL-формата)
DBG_FLAGS = -O0 -g3 -fsanitize=address,undefined -DDEBUG

BUILD ?= debug
ifeq ($(BUILD),release)
  CFLAGS  += $(REL_FLAGS)
else
  CFLAGS  += $(DBG_FLAGS)
  LDFLAGS += -fsanitize=address,undefined
endif

OUTDIR  = build/$(BUILD)
BINDIR  = $(OUTDIR)/bin
LIBDIR  = $(OUTDIR)/lib

# ── Core library objects ──
CORE_SRCS = lib/core/arena.c lib/core/log.c lib/core/hashmap.c \
            lib/core/threadpool.c lib/core/json.c lib/auth/auth.c \
            lib/auth/rbac.c lib/auth/audit.c
NET_SRCS  = lib/net/http.c lib/net/tls.c
STOR_SRCS = lib/storage/storage.c lib/storage/compress.c lib/storage/txn.c lib/index/btree.c
SQL_SRCS  = lib/sql_parser/sql.c lib/sql_template/sql_template.c
QE_SRCS   = lib/qengine/qengine.c
SCHED_SRCS= lib/scheduler/scheduler.c lib/scheduler/file_watcher.c
YAML_SRCS = lib/yaml/yaml_loader.c lib/yaml/yaml_template.c
PG_SRCS   = lib/pgwire/pgwire.c
OBS_SRCS  = lib/observ/observ.c
CONN_SRCS = lib/connector/connector.c lib/connector/conn_pool.c
MV_SRCS   = lib/matview/matview.c
CL_SRCS   = lib/cluster/proto.c lib/cluster/storage_client.c lib/cluster/replicator.c

ALL_LIB_SRCS = $(CORE_SRCS) $(NET_SRCS) $(STOR_SRCS) $(SQL_SRCS) \
               $(QE_SRCS) $(SCHED_SRCS) $(OBS_SRCS) $(CONN_SRCS) \
               $(MV_SRCS) $(CL_SRCS) $(YAML_SRCS) $(PG_SRCS)

GW_SRCS   = src/gateway/main.c src/gateway/api.c
SN_SRCS   = src/storage_node/main.c
MCP_SRCS  = src/mcp_server/main.c src/mcp_server/dispatch.c \
            src/mcp_server/tools.c src/mcp_server/http_client.c

# convert .c to .o in OUTDIR
ALL_OBJS  = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS) $(GW_SRCS))
LIB_OBJS  = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS))

GATEWAY        = $(BINDIR)/napastak_gateway
STORAGE_NODE   = $(BINDIR)/napastak_storage
MCP_SERVER     = $(BINDIR)/napastak_mcp_server
CSV_PLUGIN     = $(LIBDIR)/csv_connector.so
PG_PLUGIN      = $(LIBDIR)/pg_connector.so
PARQUET_PLUGIN = $(LIBDIR)/parquet_connector.so
JSONHTTP_PLUGIN= $(LIBDIR)/json_http_connector.so
SIEBEL_PLUGIN  = $(LIBDIR)/siebel_connector.so
KAFKA_PLUGIN   = $(LIBDIR)/kafka_connector.so
GP_PLUGIN      = $(LIBDIR)/gp_connector.so
ORACLE_PLUGIN  = $(LIBDIR)/oracle_connector.so
XML_PLUGIN     = $(LIBDIR)/xml_connector.so
SOAP_PLUGIN    = $(LIBDIR)/soap_connector.so

# Detect librdkafka
_RDKAFKA_LOCAL := $(shell test -d /usr/local/opt/librdkafka/include && echo /usr/local/opt/librdkafka)
_RDKAFKA_BREW  := $(shell test -d /opt/homebrew/opt/librdkafka/include && echo /opt/homebrew/opt/librdkafka)
RDKAFKA_PREFIX := $(or $(_RDKAFKA_LOCAL),$(_RDKAFKA_BREW))
ifneq ($(RDKAFKA_PREFIX),)
  KAFKACFLAGS  = -I$(RDKAFKA_PREFIX)/include
  KAFKALDFLAGS = -L$(RDKAFKA_PREFIX)/lib -lrdkafka
  HAS_RDKAFKA  = yes
else
  KAFKACFLAGS  := $(shell pkg-config --cflags rdkafka 2>/dev/null)
  KAFKALDFLAGS := $(shell pkg-config --libs rdkafka 2>/dev/null)
  HAS_RDKAFKA  := $(if $(KAFKALDFLAGS),yes,no)
endif

# Detect libpq — homebrew keg-only or pkg-config
_LIBPQ_LOCAL := $(shell test -d /usr/local/opt/libpq/include && echo /usr/local/opt/libpq)
_LIBPQ_BREW  := $(shell test -d /opt/homebrew/opt/libpq/include && echo /opt/homebrew/opt/libpq)
LIBPQ_PREFIX := $(or $(_LIBPQ_LOCAL),$(_LIBPQ_BREW))
ifneq ($(LIBPQ_PREFIX),)
  PGCFLAGS  = -I$(LIBPQ_PREFIX)/include
  PGLDFLAGS = -L$(LIBPQ_PREFIX)/lib -lpq
  HAS_PQ    = yes
else
  PGCFLAGS  := $(shell pkg-config --cflags libpq 2>/dev/null)
  PGLDFLAGS := $(shell pkg-config --libs libpq 2>/dev/null)
  HAS_PQ    := $(if $(PGLDFLAGS),yes,no)
endif

# Detect ODPI-C (Oracle Database Programming Interface for C). Header is dpi.h;
# library is libodpic. Compile-time needs only ODPI-C — Oracle Instant Client
# is a RUNTIME dependency loaded by ODPI-C via dlopen.
_ODPI_LOCAL := $(shell test -f /usr/local/include/dpi.h && echo /usr/local)
_ODPI_BREW  := $(shell test -f /opt/homebrew/include/dpi.h && echo /opt/homebrew)
_ODPI_SYS   := $(shell test -f /usr/include/dpi.h && echo /usr)
ODPI_PREFIX := $(or $(_ODPI_LOCAL),$(_ODPI_BREW),$(_ODPI_SYS))
ifneq ($(ODPI_PREFIX),)
  ORACLECFLAGS  = -I$(ODPI_PREFIX)/include
  ORACLELDFLAGS = -L$(ODPI_PREFIX)/lib -lodpic
  HAS_ODPI      = yes
else
  ORACLECFLAGS  := $(shell pkg-config --cflags odpi 2>/dev/null)
  ORACLELDFLAGS := $(shell pkg-config --libs odpi 2>/dev/null)
  HAS_ODPI      := $(if $(ORACLELDFLAGS),yes,no)
endif

.PHONY: all clean run test dirs release debug dist \
        test-integration test-sql test-all bench flight gp oracle

# `make` без цели собирает всё (иначе default goal стал бы первый файловый
# таргет — конвенция-таргет gp, что собирало только один плагин).
.DEFAULT_GOAL := all

# Base targets always built
_ALL_TARGETS = dirs $(GATEWAY) $(STORAGE_NODE) $(MCP_SERVER) $(CSV_PLUGIN) $(PARQUET_PLUGIN) $(JSONHTTP_PLUGIN) $(SIEBEL_PLUGIN) $(XML_PLUGIN) $(SOAP_PLUGIN)
ifeq ($(HAS_PQ),yes)
  _ALL_TARGETS += $(PG_PLUGIN) $(GP_PLUGIN)   # Greenplum reuses libpq
endif
ifeq ($(HAS_RDKAFKA),yes)
  _ALL_TARGETS += $(KAFKA_PLUGIN)
endif
ifeq ($(HAS_ODPI),yes)
  _ALL_TARGETS += $(ORACLE_PLUGIN)
endif

# Convenience targets to build a single new connector explicitly:
#   make gp       — Greenplum plugin (needs libpq)
#   make oracle   — Oracle plugin    (needs ODPI-C; runtime Instant Client)
gp:     dirs $(GP_PLUGIN)
oracle: dirs $(ORACLE_PLUGIN)

all: $(_ALL_TARGETS)

release:
	$(MAKE) BUILD=release all

debug:
	$(MAKE) BUILD=debug all

# ── Дистрибутив для сервера (RedOS 8 / Linux): tar.gz с бинарём, плагинами,
# UI, конвейерами, конфигом, systemd-юнитом и install.sh. См. packaging/redos/.
VERSION   ?= $(shell git describe --tags --always 2>/dev/null || echo dev)
DIST_NAME := napastak-$(VERSION)-linux-$(shell uname -m)
DIST_DIR  := $(OUTDIR)/dist/$(DIST_NAME)
dist: all
	@echo "  DIST $(DIST_NAME).tar.gz"
	@rm -rf $(DIST_DIR)
	@mkdir -p $(DIST_DIR)/bin $(DIST_DIR)/lib $(DIST_DIR)/ui $(DIST_DIR)/pipelines
	@cp $(GATEWAY) $(DIST_DIR)/bin/
	@cp $(LIBDIR)/*.so $(DIST_DIR)/lib/
	@cp -a ui/. $(DIST_DIR)/ui/
	@cp -a pipelines/. $(DIST_DIR)/pipelines/ 2>/dev/null || true
	@cp packaging/redos/config.json packaging/redos/napastak.service \
	    packaging/redos/install.sh packaging/redos/run.sh packaging/redos/README.md $(DIST_DIR)/
	@chmod +x $(DIST_DIR)/install.sh $(DIST_DIR)/run.sh
	@# Автономность (офлайн-установка): вложить рантайм-.so, кроме glibc/ld-linux,
	@# чтобы сервер не требовал dnf/интернет. Только на Linux (нужен ldd).
	@# libgcc_s НЕ вкладываем: он жёстко связан с системным glibc — вложенный с
	@# хоста может требовать GLIBC-версию новее целевой; берём его из целевой ОС.
	@if command -v ldd >/dev/null 2>&1; then \
	  mkdir -p $(DIST_DIR)/lib/deps; \
	  for b in $(DIST_DIR)/bin/napastak_gateway $(DIST_DIR)/lib/*.so; do ldd "$$b" 2>/dev/null; done \
	    | awk '/=> \//{print $$3}' | sort -u \
	    | grep -vE '/(ld-linux|libc|libm|libdl|librt|libpthread|libresolv|libnsl|libgcc_s)\.so' \
	    | while read -r so; do cp -Lu "$$so" $(DIST_DIR)/lib/deps/ 2>/dev/null || true; done; \
	  echo "  bundled $$(ls $(DIST_DIR)/lib/deps 2>/dev/null | wc -l | tr -d ' ') runtime .so → lib/deps"; \
	fi
	@tar -C $(OUTDIR)/dist -czf $(OUTDIR)/dist/$(DIST_NAME).tar.gz $(DIST_NAME)
	@echo "  → $(OUTDIR)/dist/$(DIST_NAME).tar.gz"

dirs:
	@mkdir -p $(BINDIR) $(LIBDIR)
	@mkdir -p $(OUTDIR)/lib/core $(OUTDIR)/lib/net $(OUTDIR)/lib/storage \
	           $(OUTDIR)/lib/sql_parser $(OUTDIR)/lib/sql_template $(OUTDIR)/lib/qengine \
	           $(OUTDIR)/lib/scheduler $(OUTDIR)/lib/observ \
	           $(OUTDIR)/lib/connector $(OUTDIR)/lib/index \
	           $(OUTDIR)/lib/auth $(OUTDIR)/lib/matview $(OUTDIR)/lib/cluster \
	           $(OUTDIR)/lib/yaml $(OUTDIR)/lib/pgwire \
	           $(OUTDIR)/src/gateway $(OUTDIR)/src/storage_node $(OUTDIR)/src/mcp_server \
	           $(OUTDIR)/lib/connector/plugins/kafka

# compile rule
$(OUTDIR)/%.o: %.c
	@echo "  CC  $<"
	@$(CC) $(CFLAGS) -c $< -o $@

# gateway binary
# Connector .so files are built with -undefined dynamic_lookup (Darwin) and
# defer resolving symbols like btree_close, write_rs_to_table, etc. to runtime
# dlopen(). For that to work the gateway must export its own symbols — hence
# -rdynamic (GNU ld) / -Wl,-export_dynamic (ld64).
EXPORT_DYNAMIC = $(if $(filter Darwin,$(shell uname)),-Wl$(comma)-export_dynamic,-rdynamic)
comma := ,
$(GATEWAY): $(ALL_OBJS)
	@echo "  LD  $@"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) $(EXPORT_DYNAMIC)

# storage node binary (cluster replica)
SN_OBJS = $(patsubst %.c,$(OUTDIR)/%.o,$(ALL_LIB_SRCS) $(SN_SRCS))
$(STORAGE_NODE): $(SN_OBJS)
	@echo "  LD  $@"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# MCP server binary (stdio JSON-RPC bridge for AI agents)
# Only links the small core needed for arg parsing + JSON + libcurl HTTP client.
MCP_CORE_OBJS = $(OUTDIR)/lib/core/arena.o \
                $(OUTDIR)/lib/core/json.o \
                $(OUTDIR)/lib/core/log.o
MCP_OBJS = $(patsubst %.c,$(OUTDIR)/%.o,$(MCP_SRCS)) $(MCP_CORE_OBJS)
$(MCP_SERVER): $(MCP_OBJS)
	@echo "  LD  $@"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lcurl

# CSV connector shared library
$(CSV_PLUGIN): lib/connector/plugins/csv/csv_connector.c \
               $(OUTDIR)/lib/core/arena.o \
               $(OUTDIR)/lib/storage/storage.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# PostgreSQL connector shared library (requires libpq)
$(PG_PLUGIN): lib/connector/plugins/pg/pg_connector.c \
              $(OUTDIR)/lib/core/arena.o \
              $(OUTDIR)/lib/storage/storage.o \
              $(OUTDIR)/lib/core/log.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) $(PGCFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) $(PGLDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Parquet connector (requires zlib)
$(PARQUET_PLUGIN): lib/connector/plugins/parquet/parquet_connector.c \
                   $(OUTDIR)/lib/core/arena.o \
                   $(OUTDIR)/lib/storage/storage.o \
                   $(OUTDIR)/lib/core/log.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lz \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# JSON HTTP connector (requires libcurl)
$(JSONHTTP_PLUGIN): lib/connector/plugins/json_http/json_http_connector.c \
                    $(OUTDIR)/lib/core/arena.o \
                    $(OUTDIR)/lib/storage/storage.o \
                    $(OUTDIR)/lib/core/log.o \
                    $(OUTDIR)/lib/core/json.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lcurl \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Siebel/EIM connector — source+sink (requires libcurl — EAI REST Inbound Web
# Service; core/json for parsing read responses)
$(SIEBEL_PLUGIN): lib/connector/plugins/siebel/siebel_connector.c \
                  $(OUTDIR)/lib/core/arena.o \
                  $(OUTDIR)/lib/storage/storage.o \
                  $(OUTDIR)/lib/core/log.o \
                  $(OUTDIR)/lib/core/json.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lcurl \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# XML connector — source+sink (libcurl for HTTP; core/xml for parsing)
$(XML_PLUGIN): lib/connector/plugins/xml/xml_connector.c \
               $(OUTDIR)/lib/core/arena.o \
               $(OUTDIR)/lib/storage/storage.o \
               $(OUTDIR)/lib/core/log.o \
               $(OUTDIR)/lib/core/xml.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lcurl \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# SOAP connector — source+sink (libcurl HTTP envelope; core/xml for parsing)
$(SOAP_PLUGIN): lib/connector/plugins/soap/soap_connector.c \
                $(OUTDIR)/lib/core/arena.o \
                $(OUTDIR)/lib/storage/storage.o \
                $(OUTDIR)/lib/core/log.o \
                $(OUTDIR)/lib/core/xml.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) -lcurl \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Kafka connector (requires librdkafka; libcurl for Schema Registry — Avro)
$(KAFKA_PLUGIN): lib/connector/plugins/kafka/kafka_connector.c \
                 lib/connector/plugins/kafka/avro_decode.h \
                 lib/connector/plugins/kafka/avro_record.h \
                 $(OUTDIR)/lib/core/arena.o \
                 $(OUTDIR)/lib/storage/storage.o \
                 $(OUTDIR)/lib/core/log.o \
                 $(OUTDIR)/lib/core/json.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) $(KAFKACFLAGS) -shared -fPIC \
	    $(filter %.c %.o,$^) -o $@ $(LDFLAGS) $(KAFKALDFLAGS) -lcurl \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Greenplum connector (reuses libpq — same transport as the pg plugin)
$(GP_PLUGIN): lib/connector/plugins/greenplum/gp_connector.c \
              $(OUTDIR)/lib/core/arena.o \
              $(OUTDIR)/lib/storage/storage.o \
              $(OUTDIR)/lib/core/log.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) $(PGCFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) $(PGLDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# Oracle connector (requires ODPI-C at compile time; Oracle Instant Client at
# runtime — ODPI-C dlopen()s it). Build explicitly with `make oracle` once
# ODPI-C is installed (apt install libodpi-dev / dnf install odpi-c-devel).
$(ORACLE_PLUGIN): lib/connector/plugins/oracle/oracle_connector.c \
                  $(OUTDIR)/lib/core/arena.o \
                  $(OUTDIR)/lib/storage/storage.o \
                  $(OUTDIR)/lib/core/log.o
	@echo "  SO  $@"
	@$(CC) $(CFLAGS) $(ORACLECFLAGS) -shared -fPIC $^ -o $@ $(LDFLAGS) $(ORACLELDFLAGS) \
	    $(if $(filter Darwin,$(shell uname)),-undefined dynamic_lookup,)

# ── Unit tests ──
TEST_SRCS = $(wildcard tests/unit/*.c)
TEST_BINS = $(patsubst tests/unit/%.c,$(BINDIR)/test_%,$(TEST_SRCS))

$(BINDIR)/test_%: tests/unit/%.c $(LIB_OBJS)
	@echo "  TC  $<"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

test: dirs $(TEST_BINS)
	@for t in $(TEST_BINS); do echo "\n=== $$t ==="; $$t; done

# ── SQL unit tests ──
SQL_TEST_SRCS = $(wildcard tests/sql/*.c)
SQL_TEST_BINS = $(patsubst tests/sql/%.c,$(BINDIR)/sql_%,$(SQL_TEST_SRCS))

$(BINDIR)/sql_%: tests/sql/%.c $(LIB_OBJS)
	@echo "  TC  $<"
	@$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS) -lm

test-sql: dirs $(SQL_TEST_BINS)
	@for t in $(SQL_TEST_BINS); do echo "\n=== $$t ==="; $$t; done

# ── Integration tests (require running gateway) ──
ITEST_SRCS = $(wildcard tests/integration/test_*.c)
ITEST_BINS = $(patsubst tests/integration/%.c,$(BINDIR)/itest_%,$(ITEST_SRCS))

$(BINDIR)/itest_%: tests/integration/%.c
	@echo "  IC  $<"
	@$(CC) -std=c11 -Wall -Ilib -Ilib/core -Ilib/net -Ilib/storage \
	    -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler -Ilib/observ \
	    -Ilib/connector -Ilib/index \
	    $< -o $@ -lcurl -lpthread -lm

test-integration: dirs all $(ITEST_BINS)
	@for t in $(ITEST_BINS); do \
	    echo "\n=== $$t ==="; \
	    $$t $(GATEWAY); \
	done

# ── Load benchmarks ──
BENCH_SRCS = $(wildcard tests/load/*.c)
BENCH_BINS = $(patsubst tests/load/%.c,$(BINDIR)/bench_%,$(BENCH_SRCS))

$(BINDIR)/bench_%: tests/load/%.c
	@echo "  BC  $<"
	@$(CC) -std=c11 -Wall -Ilib -Ilib/core -Ilib/net -Ilib/storage \
	    -Ilib/sql_parser -Ilib/qengine -Ilib/scheduler -Ilib/observ \
	    -Ilib/connector -Ilib/index \
	    $< -o $@ -lcurl -lpthread -lm

bench: dirs all $(BENCH_BINS)
	@for b in $(BENCH_BINS); do \
	    echo "\n=== $$b ==="; \
	    $$b $(GATEWAY); \
	done

# ── All tests combined ──
test-all: test test-sql test-integration

# ── Arrow Flight server (Step 2) — optional C++ bridge ──
# Built separately because it pulls in Arrow C++ + gRPC + nlohmann_json.
# Not part of `make all` to keep the base build dependency-free.
#
# Install deps first:
#   macOS:  brew install apache-arrow grpc nlohmann-json
#   Debian: apt install libarrow-flight-dev libgrpc++-dev nlohmann-json3-dev libcurl4-openssl-dev cmake
#
# Then:    make flight
# Output:  build/release/bin/napastak_flight_server
FLIGHT_BIN     = $(BINDIR)/napastak_flight_server
FLIGHT_BUILD   = src/flight_server/build

# Auto-detect conda if installed — Arrow C++ via conda-forge is the
# fastest path to the deps on macOS Intel (brew compiles for hours).
# Override with FLIGHT_PREFIX=/path/to/your/install if you have a custom
# install (e.g. /opt/homebrew on Apple Silicon).
_CONDA_BASE := $(shell { test -d $(HOME)/miniconda3 && echo $(HOME)/miniconda3; } || \
                       { test -d $(HOME)/anaconda3  && echo $(HOME)/anaconda3;  } || \
                       true)
FLIGHT_PREFIX ?= $(_CONDA_BASE)
FLIGHT_CMAKE_ARGS = -DCMAKE_BUILD_TYPE=Release
ifneq ($(FLIGHT_PREFIX),)
  FLIGHT_CMAKE_ARGS += -DCMAKE_PREFIX_PATH=$(FLIGHT_PREFIX)
  FLIGHT_CMAKE_ARGS += -Dnlohmann_json_DIR=$(FLIGHT_PREFIX)/share/cmake/nlohmann_json
  # Arrow 24.x needs C++20 features (std::log2p1) that Apple Clang 14
  # doesn't ship; fall back to conda's clang when available.
  ifneq ($(wildcard $(FLIGHT_PREFIX)/bin/clang++),)
    FLIGHT_CMAKE_ARGS += -DCMAKE_C_COMPILER=$(FLIGHT_PREFIX)/bin/clang
    FLIGHT_CMAKE_ARGS += -DCMAKE_CXX_COMPILER=$(FLIGHT_PREFIX)/bin/clang++
  endif
endif

flight: dirs
	@if ! command -v cmake >/dev/null 2>&1; then \
		echo "ERROR: cmake not found — install with brew/apt and retry"; exit 1; \
	fi
	@if [ -z "$(FLIGHT_PREFIX)" ] && ! pkg-config --exists arrow 2>/dev/null; then \
		echo "ERROR: Arrow C++ not found. Install via:"; \
		echo "  conda install -c conda-forge libarrow-all libgrpc nlohmann_json cxx-compiler"; \
		echo "  # or, on Apple Silicon: brew install apache-arrow grpc nlohmann-json"; \
		exit 1; \
	fi
	@mkdir -p $(FLIGHT_BUILD)
	@cd $(FLIGHT_BUILD) && cmake $(FLIGHT_CMAKE_ARGS) .. && cmake --build . -j 2>&1
	@cp $(FLIGHT_BUILD)/napastak_flight_server $(FLIGHT_BIN)
	@echo "  LD  $(FLIGHT_BIN)"
	@if [ -n "$(FLIGHT_PREFIX)" ]; then \
		echo "  HINT: run with DYLD_LIBRARY_PATH=$(FLIGHT_PREFIX)/lib so libarrow*.dylib is found"; \
	fi

flight-clean:
	@rm -rf $(FLIGHT_BUILD)

# ── Run ──
run: all
	@mkdir -p data
	./$(GATEWAY) -p 8080

clean:
	rm -rf build/ src/flight_server/build/

# ── Install UI (copy to served dir) ──
install-ui:
	@mkdir -p $(BINDIR)/ui
	cp ui/index.html ui/style.css ui/app.js $(BINDIR)/ui/
	@echo "UI installed to $(BINDIR)/ui/"

# dependency tracking
-include $(ALL_OBJS:.o=.d)
$(OUTDIR)/%.o: %.c
	@$(CC) $(CFLAGS) -MMD -MP -c $< -o $@
