/*
 * connector.c — динамическая загрузка connector-плагинов NAPASTAK.
 * Открывает .so/.dylib через dlopen, валидирует ABI, создаёт и хранит
 * экземпляр коннектора (ConnectorInst) поверх таблицы функций DfoConnector.
 */
#include "connector.h"
#include "../core/log.h"
#include <dlfcn.h>
#include <stdlib.h>
#include <string.h>

/* Загруженный экземпляр коннектора: хэндл dlopen, его API-таблица,
 * приватный контекст плагина и арена для аллокаций. */
struct ConnectorInst {
    void          *dl_handle;
    DfoConnector  *api;
    void          *ctx;
    Arena         *arena;
};

/* Every plugin .so links its own copy of log.o, so it gets its own g_log —
 * zero-initialised, i.e. out == NULL, i.e. log_write() drops the line. Point
 * the plugin's logger at the host's stream (using the plugin's own log_init so
 * its mutex is set up), otherwise everything a connector reports — bad
 * credentials, failed subscribe, unreachable broker — is silently discarded and
 * the step just looks like it returned no rows. */
static void plugin_log_bind(void *dl, const char *path) {
    Logger *plugin_log = (Logger *)dlsym(dl, "g_log");
    void (*plugin_log_init)(Logger *, FILE *, LogLevel, int) =
        (void (*)(Logger *, FILE *, LogLevel, int))dlsym(dl, "log_init");

    if (!plugin_log || !plugin_log_init) {
        LOG_DEBUG("connector %s has no embedded logger to bind", path);
        return;
    }
    if (plugin_log == &g_log) return;   /* already the host's — nothing to do */

    plugin_log_init(plugin_log, g_log.out, g_log.min_level, g_log.json_mode);
}

/* Загружает плагин по пути path, проверяет совместимость ABI и инициализирует
 * его контекст из конфигурации cfg. Возвращает NULL при любой ошибке (с логом). */
ConnectorInst *connector_load(const char *path, const char *cfg, Arena *a) {
    void *dl = dlopen(path, RTLD_NOW|RTLD_LOCAL);
    if (!dl) { LOG_ERROR("dlopen %s: %s", path, dlerror()); return NULL; }

    /* Bind before create() — create() is where connection errors happen. */
    plugin_log_bind(dl, path);

    /* Извлекаем обязательный экспортируемый символ с API-таблицей коннектора. */
    DfoConnector *api = dlsym(dl, DFO_CONNECTOR_EXPORT_SYM);
    if (!api) { LOG_ERROR("dlsym %s: %s", DFO_CONNECTOR_EXPORT_SYM, dlerror()); dlclose(dl); return NULL; }

    /* Несовпадение версии ABI делает таблицу функций несовместимой — отказ. */
    if (api->abi_version != DFO_CONNECTOR_ABI_VERSION) {
        LOG_ERROR("connector %s ABI version mismatch: got %u want %u",
                  path, api->abi_version, DFO_CONNECTOR_ABI_VERSION);
        dlclose(dl); return NULL;
    }

    /* Плагин создаёт свой приватный контекст из конфигурации. */
    void *ctx = api->create(cfg, a);
    if (!ctx) { LOG_ERROR("connector create failed"); dlclose(dl); return NULL; }

    ConnectorInst *inst = malloc(sizeof(ConnectorInst));
    inst->dl_handle = dl; inst->api = api; inst->ctx = ctx; inst->arena = a;
    LOG_INFO("loaded connector %s v%s", api->name, api->version);
    return inst;
}

/* Освобождает контекст плагина (если есть destroy), закрывает dl-хэндл
 * и сам инстанс. Безопасно при inst == NULL. */
void connector_unload(ConnectorInst *inst) {
    if (!inst) return;
    if (inst->api->destroy) inst->api->destroy(inst->ctx);
    dlclose(inst->dl_handle);
    free(inst);
}

/* Аксессоры: API-таблица плагина и его приватный контекст. */
const DfoConnector *connector_api(ConnectorInst *inst) { return inst->api; }
void               *connector_ctx(ConnectorInst *inst) { return inst->ctx; }
