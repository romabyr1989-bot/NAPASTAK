/* conn_pool.c — реализация пула постоянных подключений. См. conn_pool.h. */

#include "conn_pool.h"
#include "../core/arena.h"
#include "../core/log.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <errno.h>

#define POOL_MAX_SLOTS 64          /* всего живых экземпляров на гейтвей */

typedef struct {
    bool           used;           /* слот занят записью (не обязательно в аренде) */
    bool           leased;         /* выдан в аренду прямо сейчас */
    bool           opening;        /* сессия устанавливается прямо сейчас (вне мьютекса) */
    char           conn_id[64];
    char           so_path[512];
    char           fp[64];         /* отпечаток конфига: смена → закрыть слот */
    char          *cfg;            /* копия конфига в арене слота */
    ConnectorInst *inst;
    Arena         *arena;          /* живёт ровно столько, сколько экземпляр */
    int64_t        last_used;
    int64_t        last_ok;
    char           last_error[256];
} PoolSlot;

struct ConnPool {
    PoolSlot        slots[POOL_MAX_SLOTS];
    int             max_per_conn;
    int             idle_ping_sec;
    pthread_mutex_t mu;
    pthread_cond_t  cv;            /* сигналится при освобождении аренды */
};

struct ConnLease {
    ConnPool *pool;
    int       slot;
};

/* Дешёвый отпечаток конфига (FNV-1a). Нужен только для сравнения «тот же
 * конфиг или уже другой», криптостойкость не требуется. */
static void cfg_fingerprint(const char *s, char *out, size_t outsz) {
    uint64_t h = 1469598103934665603ULL;
    for (const unsigned char *p = (const unsigned char *)(s ? s : ""); *p; p++) {
        h ^= (uint64_t)*p;
        h *= 1099511628211ULL;
    }
    snprintf(out, outsz, "%016llx", (unsigned long long)h);
}

static int64_t now_sec(void) { return (int64_t)time(NULL); }

/* Закрыть экземпляр слота. Вызывать под мьютексом, слот не должен быть в аренде. */
static void slot_close(PoolSlot *s, const char *why) {
    if (s->inst) {
        connector_unload(s->inst);
        s->inst = NULL;
    }
    if (s->arena) {
        arena_destroy(s->arena);
        s->arena = NULL;
    }
    s->cfg = NULL;
    if (why && why[0])
        LOG_INFO("conn_pool: подключение '%s' — сессия закрыта (%s)", s->conn_id, why);
}

/* Полностью освободить слот. Под мьютексом. */
static void slot_free(PoolSlot *s, const char *why) {
    slot_close(s, why);
    s->used = false;
    s->leased = false;
    s->conn_id[0] = '\0';
    s->fp[0] = '\0';
    s->last_error[0] = '\0';
}

ConnPool *conn_pool_create(int max_per_conn, int idle_ping_sec) {
    ConnPool *p = calloc(1, sizeof(ConnPool));
    if (!p) return NULL;
    p->max_per_conn  = (max_per_conn  > 0) ? max_per_conn  : 4;
    p->idle_ping_sec = (idle_ping_sec > 0) ? idle_ping_sec : 60;
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cv, NULL);
    return p;
}

void conn_pool_destroy(ConnPool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->mu);

    /* Сначала ДОЖДАТЬСЯ возврата аренд. HTTP-обработчики исполняют шаги инлайном
     * (POST /run?wait=1, предпросмотр шага), их потоки detached и никем не
     * join-ятся, поэтому на момент остановки кто-то может сидеть внутри
     * read_batch с указателями в контекст плагина. Закрыть такой слот — значит
     * сделать dlclose под работающим потоком. Ждём ограниченно, чтобы не
     * подвесить остановку навсегда. */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 5;
    for (;;) {
        int busy = 0;
        for (int i = 0; i < POOL_MAX_SLOTS; i++)
            if (p->slots[i].used && p->slots[i].leased) busy++;
        if (!busy) break;
        if (pthread_cond_timedwait(&p->cv, &p->mu, &deadline) == ETIMEDOUT) {
            LOG_WARN("conn_pool: при остановке %d сессий всё ещё в работе — "
                     "оставляем их как есть, чтобы не рвать чужой поток", busy);
            /* Осознанная утечка: пул и занятые слоты не освобождаем. Отдать
             * память сейчас = гарантированный use-after-free; процесс всё равно
             * завершается, память вернёт ОС. */
            pthread_mutex_unlock(&p->mu);
            return;
        }
    }

    for (int i = 0; i < POOL_MAX_SLOTS; i++)
        if (p->slots[i].used) slot_free(&p->slots[i], "остановка гейтвея");
    pthread_mutex_unlock(&p->mu);
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cv);
    free(p);
}

/* Поднять экземпляр в слоте. ВЫЗЫВАЕТСЯ БЕЗ МЬЮТЕКСА: dlopen, установка
 * соединения и ping могут висеть до таймаута (недоступная база — секунды), а
 * под общим мьютексом это подвешивало бы весь пул, включая чтение статусов для
 * UI. Слот к этому моменту уже помечен opening+leased, поэтому никто другой его
 * не тронет. */
static bool slot_open(PoolSlot *s, const char *conn_id, const char *so_path,
                      const char *cfg_json, const char *fp, bool verify,
                      char *err, size_t errsz) {
    s->arena = arena_create(65536);
    if (!s->arena) {
        snprintf(err, errsz, "нет памяти под сессию подключения");
        return false;
    }
    /* Конфиг копируем в арену слота: экземпляр живёт дольше вызывающего кода. */
    s->cfg = arena_strdup(s->arena, cfg_json ? cfg_json : "{}");
    s->inst = connector_load(so_path, s->cfg, s->arena);
    /* Слот помечаем занятым в любом случае: при неудаче он хранит причину для
     * UI и время попытки — по нему же троттлится повтор, чтобы не долбить
     * недоступную систему на каждом тике keepalive. */
    snprintf(s->conn_id, sizeof(s->conn_id), "%s", conn_id);
    snprintf(s->so_path, sizeof(s->so_path), "%s", so_path);
    snprintf(s->fp, sizeof(s->fp), "%s", fp);
    s->used = true;
    s->last_used = now_sec();
    if (!s->inst) {
        arena_destroy(s->arena);
        s->arena = NULL;
        s->cfg = NULL;
        snprintf(err, errsz, "не удалось поднять коннектор (%s)", so_path);
        snprintf(s->last_error, sizeof(s->last_error), "%s", err);
        return false;
    }
    /* connector_load только создаёт контекст плагина — у большинства коннекторов
     * соединение ленивое, поэтому «загрузился» ещё не значит «подключено».
     * Проверяем сессию пингом, иначе статус в UI врал бы про недоступную систему. */
    const DfoConnector *api = connector_api(s->inst);
    void *cx = connector_ctx(s->inst);
    if (verify && api->ping && api->ping(cx) != 0) {
        const char *why = api->last_error ? api->last_error(cx) : NULL;
        char msg[256];
        snprintf(msg, sizeof(msg), "%s", (why && why[0]) ? why : "система не отвечает");
        connector_unload(s->inst); s->inst = NULL;
        arena_destroy(s->arena);   s->arena = NULL;
        s->cfg = NULL;
        snprintf(s->last_error, sizeof(s->last_error), "%s", msg);
        snprintf(err, errsz, "%s", msg);
        LOG_WARN("conn_pool: подключение '%s' — не удалось установить сессию: %s", conn_id, msg);
        return false;
    }
    s->last_ok = s->last_used;
    s->last_error[0] = '\0';
    LOG_INFO("conn_pool: подключение '%s' — сессия установлена", conn_id);
    return true;
}


/* Открыть сессию слота, отпустив мьютекс на время подключения. Слот должен быть
 * уже зарезервирован (used+leased+opening). Возвращает результат slot_open. */
static bool slot_open_unlocked(ConnPool *p, int idx, const char *conn_id,
                               const char *so_path, const char *cfg_json,
                               const char *fp, bool verify, char *err, size_t errsz) {
    PoolSlot *s = &p->slots[idx];
    snprintf(s->conn_id, sizeof(s->conn_id), "%s", conn_id);
    snprintf(s->so_path, sizeof(s->so_path), "%s", so_path);
    snprintf(s->fp, sizeof(s->fp), "%s", fp);
    s->used = true; s->leased = true; s->opening = true;
    pthread_mutex_unlock(&p->mu);

    bool ok = slot_open(s, conn_id, so_path, cfg_json, fp, verify, err, errsz);

    pthread_mutex_lock(&p->mu);
    s->opening = false;
    if (!ok) { s->leased = false; pthread_cond_broadcast(&p->cv); }
    return ok;
}

ConnLease *conn_pool_acquire(ConnPool *p, const char *conn_id,
                             const char *so_path, const char *cfg_json,
                             bool verify, char *err, size_t errsz) {
    if (!p || !conn_id || !conn_id[0]) {
        snprintf(err, errsz, "conn_pool: не задан идентификатор подключения");
        return NULL;
    }
    char fp[64];
    cfg_fingerprint(cfg_json, fp, sizeof(fp));

    pthread_mutex_lock(&p->mu);
    for (;;) {
        int mine = 0;        /* слотов у этого подключения (живых и занятых) */
        int dead = -1;       /* слот этого подключения без сессии — можно поднять на месте */

        for (int i = 0; i < POOL_MAX_SLOTS; i++) {
            PoolSlot *s = &p->slots[i];
            if (!s->used) continue;
            if (strcmp(s->conn_id, conn_id) != 0) continue;

            /* Конфиг подключения изменился — прежние сессии больше не годятся. */
            if (strcmp(s->fp, fp) != 0) {
                if (!s->leased) { slot_free(s, "изменился конфиг"); continue; }
                continue;   /* занятую переиспользовать нельзя, закроется при возврате */
            }
            mine++;
            if (s->leased) continue;
            if (s->inst) {
                s->leased = true;
                s->last_used = now_sec();
                pthread_mutex_unlock(&p->mu);
                ConnLease *l = malloc(sizeof(ConnLease));
                l->pool = p; l->slot = i;
                return l;
            }
            if (dead < 0) dead = i;
        }

        /* Есть слот без сессии — поднимаем её на месте (с троттлингом повторов). */
        if (dead >= 0) {
            PoolSlot *s = &p->slots[dead];
            if (s->last_error[0] && now_sec() - s->last_used < 5) {
                snprintf(err, errsz, "%s", s->last_error);
                pthread_mutex_unlock(&p->mu);
                return NULL;      /* недавно не получилось — не долбим систему */
            }
            if (!slot_open_unlocked(p, dead, conn_id, so_path, cfg_json, fp, verify, err, errsz)) {
                pthread_mutex_unlock(&p->mu);
                return NULL;
            }
            pthread_mutex_unlock(&p->mu);
            ConnLease *l = malloc(sizeof(ConnLease));
            l->pool = p; l->slot = dead;
            return l;
        }

        /* Свободного экземпляра нет — можно ли создать ещё один? */
        if (mine < p->max_per_conn) {
            int idx = -1;
            for (int i = 0; i < POOL_MAX_SLOTS; i++)
                if (!p->slots[i].used) { idx = i; break; }
            if (idx < 0) {
                /* Пул забит чужими подключениями — вытесняем самое старое простаивающее. */
                int64_t oldest = 0; int victim = -1;
                for (int i = 0; i < POOL_MAX_SLOTS; i++) {
                    PoolSlot *s = &p->slots[i];
                    if (s->used && !s->leased && (victim < 0 || s->last_used < oldest)) {
                        oldest = s->last_used; victim = i;
                    }
                }
                if (victim >= 0) { slot_free(&p->slots[victim], "вытеснение по размеру пула"); idx = victim; }
            }
            if (idx >= 0) {
                if (!slot_open_unlocked(p, idx, conn_id, so_path, cfg_json, fp, verify, err, errsz)) {
                    pthread_mutex_unlock(&p->mu);
                    return NULL;
                }
                pthread_mutex_unlock(&p->mu);
                ConnLease *l = malloc(sizeof(ConnLease));
                l->pool = p; l->slot = idx;
                return l;
            }
        }

        /* Все экземпляры этого подключения заняты — ждём освобождения. */
        pthread_cond_wait(&p->cv, &p->mu);
    }
}

ConnectorInst *conn_lease_inst(ConnLease *l) {
    if (!l) return NULL;
    return l->pool->slots[l->slot].inst;
}

void conn_pool_release(ConnLease *l, bool healthy) {
    if (!l) return;
    ConnPool *p = l->pool;
    pthread_mutex_lock(&p->mu);
    PoolSlot *s = &p->slots[l->slot];
    s->leased = false;
    s->last_used = now_sec();
    if (!s->fp[0]) {
        /* Слот помечен инвалидацией, пока был в аренде (правка или удаление
         * подключения). Освобождаем именно здесь: скан acquire его больше не
         * увидит — при удалении по этому conn_id никто уже не придёт, и сессия
         * к несуществующему подключению жила бы до конца процесса. */
        slot_free(s, "подключение изменено или удалено");
    } else if (healthy) {
        s->last_ok = s->last_used;
        s->last_error[0] = '\0';
    } else {
        /* После ошибки состояние сессии неизвестно — закрываем, чтобы
         * следующий прогон не унаследовал повисшую транзакцию. */
        slot_free(s, "ошибка работы с источником");
    }
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
    free(l);
}

void conn_pool_invalidate(ConnPool *p, const char *conn_id) {
    if (!p || !conn_id || !conn_id[0]) return;
    pthread_mutex_lock(&p->mu);
    for (int i = 0; i < POOL_MAX_SLOTS; i++) {
        PoolSlot *s = &p->slots[i];
        if (!s->used || strcmp(s->conn_id, conn_id) != 0) continue;
        if (s->leased) {
            /* Занятую сессию не рвём на полпути: помечаем отпечаток мёртвым,
             * и она закроется при возврате аренды. */
            s->fp[0] = '\0';
        } else {
            slot_free(s, "подключение изменено или удалено");
        }
    }
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->mu);
}

void conn_pool_tick(ConnPool *p) {
    if (!p) return;
    int64_t t = now_sec();
    /* Пинг тоже может висеть до таймаута, поэтому берём слот в аренду, отпускаем
     * мьютекс и проверяем связь снаружи — иначе на недоступной системе
     * подвисает весь пул, включая чтение статусов для UI. */
    for (int i = 0; i < POOL_MAX_SLOTS; i++) {
        pthread_mutex_lock(&p->mu);
        PoolSlot *s = &p->slots[i];
        if (!s->used || s->leased || !s->inst || t - s->last_used < p->idle_ping_sec) {
            pthread_mutex_unlock(&p->mu);
            continue;
        }
        s->leased = true;                       /* резервируем на время пинга */
        const DfoConnector *api = connector_api(s->inst);
        void *ctx = connector_ctx(s->inst);
        pthread_mutex_unlock(&p->mu);

        int rc = api->ping ? api->ping(ctx) : 0;
        const char *why = (rc != 0 && api->last_error) ? api->last_error(ctx) : NULL;
        char msg[256];
        snprintf(msg, sizeof(msg), "%s", (why && why[0]) ? why : "сессия не отвечает");

        pthread_mutex_lock(&p->mu);
        s->leased = false;
        if (rc == 0) {
            s->last_ok = s->last_used = now_sec();
            s->last_error[0] = '\0';
        } else {
            slot_close(s, "keepalive: сессия не отвечает");
            snprintf(s->last_error, sizeof(s->last_error), "%s", msg);
            s->last_used = now_sec();           /* троттлинг повторного подъёма */
        }
        pthread_cond_broadcast(&p->cv);
        pthread_mutex_unlock(&p->mu);
    }
}

int conn_pool_status(ConnPool *p, const char *conn_id, const char **state,
                     int64_t *last_ok, const char **last_err_msg) {
    if (state)        *state = "idle";
    if (last_ok)      *last_ok = 0;
    if (last_err_msg) *last_err_msg = NULL;
    if (!p || !conn_id || !conn_id[0]) return 0;

    int live = 0; int64_t best_ok = 0; const char *err = NULL; bool seen = false, connecting = false;
    pthread_mutex_lock(&p->mu);
    for (int i = 0; i < POOL_MAX_SLOTS; i++) {
        PoolSlot *s = &p->slots[i];
        if (!s->used || strcmp(s->conn_id, conn_id) != 0) continue;
        seen = true;
        if (s->inst) live++;
        if (s->last_ok > best_ok) best_ok = s->last_ok;
        if (s->opening) connecting = true;
        if (!s->inst && !s->opening && s->last_error[0] && !err) err = s->last_error;
    }
    pthread_mutex_unlock(&p->mu);

    if (state) *state = live ? "ok" : (connecting ? "idle" : (seen && err ? "error" : "idle"));
    if (last_ok) *last_ok = best_ok;
    if (last_err_msg) *last_err_msg = err;
    return live;
}
