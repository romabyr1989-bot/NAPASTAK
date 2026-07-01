/*
 * rbac.c — хранилище и проверка RBAC-политик NAPASTAK.
 *
 * Политики хранятся в SQLite (таблица rbac_policies): для каждой роли —
 * набор glob-паттернов имён таблиц, битовая маска разрешённых действий и
 * опциональный row-level фильтр (RLS). Доступ к БД сериализуется мьютексом.
 */
#include "rbac.h"
#include "../core/log.h"
#include <sqlite3.h>
#include <fnmatch.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <time.h>

/* DDL хранилища политик: уникальный индекс не даёт дублировать (role, pattern) */
static const char *SCHEMA_SQL =
    "CREATE TABLE IF NOT EXISTS rbac_policies ("
    "  id              INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  role            INTEGER NOT NULL,"
    "  table_pattern   TEXT NOT NULL,"
    "  allowed_actions INTEGER NOT NULL,"
    "  row_filter      TEXT DEFAULT '',"
    "  created_at      INTEGER NOT NULL"
    ");"
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_rbac_role_pattern "
    "  ON rbac_policies(role, table_pattern);";

/* Открывает БД политик, включает WAL, создаёт схему. NULL при ошибке. */
RbacStore *rbac_store_create(const char *db_path, bool enabled) {
    RbacStore *s = calloc(1, sizeof(RbacStore));
    s->enabled = enabled;
    if (sqlite3_open(db_path, &s->db) != SQLITE_OK) {
        LOG_ERROR("rbac: failed to open db: %s", sqlite3_errmsg(s->db));
        free(s);
        return NULL;
    }
    sqlite3_exec(s->db, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    if (sqlite3_exec(s->db, SCHEMA_SQL, NULL, NULL, NULL) != SQLITE_OK) {
        LOG_ERROR("rbac: schema init: %s", sqlite3_errmsg(s->db));
        sqlite3_close(s->db);
        free(s);
        return NULL;
    }
    pthread_mutex_init(&s->mu, NULL);
    return s;
}

/* Закрывает БД и освобождает хранилище (безопасно при s == NULL). */
void rbac_store_destroy(RbacStore *s) {
    if (!s) return;
    pthread_mutex_destroy(&s->mu);
    sqlite3_close(s->db);
    free(s);
}

/* Загружаем все политики для роли и проверяем по glob-паттерну.
 * Возврат: 0 — разрешено, -1 — запрещено/нет подходящей политики. */
int rbac_check(RbacStore *s, const AuthClaims *claims,
               RbacAction action, const char *table_name) {
    if (!s || !s->enabled) return 0; /* RBAC выключен — всё разрешено */
    if (!claims) return -1;
    if (claims->role == ROLE_ADMIN) return 0; /* admin всегда разрешено */

    pthread_mutex_lock(&s->mu);
    const char *sql =
        "SELECT table_pattern, allowed_actions FROM rbac_policies "
        "WHERE role = ? ORDER BY id ASC;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu);
        return -1;
    }
    sqlite3_bind_int(stmt, 1, (int)claims->role);

    int result = -1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *pat     = (const char *)sqlite3_column_text(stmt, 0);
        uint32_t    allowed = (uint32_t)sqlite3_column_int(stmt, 1);

        const char *tname_check = table_name ? table_name : "*";
        /* Совпадение по glob либо политика-wildcard; проверяем бит действия */
        if (fnmatch(pat, tname_check, 0) == 0 || strcmp(pat, "*") == 0) {
            if (allowed & (uint32_t)action) {
                result = 0;
                break;
            }
        }
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    return result;
}

/* Безопасная подстановка user_id: запрещаем одинарные кавычки */
static bool safe_user_id(const char *uid) {
    if (!uid) return false;
    for (const char *p = uid; *p; p++)
        if (*p == '\'' || *p == '\\') return false;
    return true;
}

/* Возвращает SQL-фрагмент row-level фильтра для первой подходящей политики
 * (с подстановкой {user_id}) или NULL, если фильтра нет. Буфер из арены a. */
const char *rbac_row_filter(RbacStore *s, const AuthClaims *claims,
                             const char *table_name, Arena *a) {
    if (!s || !s->enabled || !claims) return NULL;
    if (claims->role == ROLE_ADMIN) return NULL;

    pthread_mutex_lock(&s->mu);
    const char *sql =
        "SELECT table_pattern, row_filter FROM rbac_policies "
        "WHERE role = ? AND row_filter != '' ORDER BY id ASC;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu);
        return NULL;
    }
    sqlite3_bind_int(stmt, 1, (int)claims->role);

    const char *result = NULL;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *pat    = (const char *)sqlite3_column_text(stmt, 0);
        const char *filter = (const char *)sqlite3_column_text(stmt, 1);

        if (!filter || !*filter) continue;
        const char *tname_check = table_name ? table_name : "*";
        if (fnmatch(pat, tname_check, 0) == 0 || strcmp(pat, "*") == 0) {
            /* Подставляем {user_id} */
            if (strstr(filter, "{user_id}")) {
                if (!safe_user_id(claims->user_id)) {
                    LOG_WARN("rbac: unsafe user_id '%s' rejected for RLS", claims->user_id);
                    break;
                }
                char *buf = arena_alloc(a, 1024);
                const char *p = filter;
                int off = 0;
                /* Посимвольное копирование с заменой токена {user_id}; лимит 1020 защищает буфер */
                while (*p && off < 1020) {
                    if (strncmp(p, "{user_id}", 9) == 0) {
                        int ul = (int)strlen(claims->user_id);
                        if (off + ul < 1020) {
                            memcpy(buf + off, claims->user_id, (size_t)ul);
                            off += ul;
                        }
                        p += 9;
                    } else {
                        buf[off++] = *p++;
                    }
                }
                buf[off] = '\0';
                result = buf;
            } else {
                result = arena_strdup(a, filter);
            }
            break;
        }
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    return result;
}

/* Upsert политики по ключу (role, table_pattern). Возврат 0 / -1. */
int rbac_policy_set(RbacStore *s, AuthRole role, const char *table_pattern,
                    uint32_t actions, const char *row_filter) {
    pthread_mutex_lock(&s->mu);
    const char *sql =
        "INSERT INTO rbac_policies (role, table_pattern, allowed_actions, row_filter, created_at) "
        "VALUES (?,?,?,?,?) "
        "ON CONFLICT(role, table_pattern) DO UPDATE SET "
        "  allowed_actions=excluded.allowed_actions,"
        "  row_filter=excluded.row_filter;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu);
        return -1;
    }
    sqlite3_bind_int(stmt, 1, (int)role);
    sqlite3_bind_text(stmt, 2, table_pattern, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, (int)actions);
    sqlite3_bind_text(stmt, 4, row_filter ? row_filter : "", -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 5, (int64_t)time(NULL));
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Удаляет политику по её id. Возврат 0 / -1. */
int rbac_policy_del(RbacStore *s, int policy_id) {
    pthread_mutex_lock(&s->mu);
    const char *sql = "DELETE FROM rbac_policies WHERE id = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        pthread_mutex_unlock(&s->mu);
        return -1;
    }
    sqlite3_bind_int(stmt, 1, policy_id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Сериализует политики в JSON-массив (все при role == -1, иначе по роли).
 * Результат пишется в *json_out из арены a. */
int rbac_policy_list(RbacStore *s, AuthRole role, char **json_out, Arena *a) {
    pthread_mutex_lock(&s->mu);
    const char *sql;
    sqlite3_stmt *stmt;
    if (role == (AuthRole)-1) {
        sql = "SELECT id,role,table_pattern,allowed_actions,row_filter FROM rbac_policies ORDER BY role,id;";
        if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            pthread_mutex_unlock(&s->mu); return -1;
        }
    } else {
        sql = "SELECT id,role,table_pattern,allowed_actions,row_filter FROM rbac_policies WHERE role=? ORDER BY id;";
        if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            pthread_mutex_unlock(&s->mu); return -1;
        }
        sqlite3_bind_int(stmt, 1, (int)role);
    }

    char *buf = arena_alloc(a, 65536);
    int off = 0;
    off += snprintf(buf + off, 65536 - (size_t)off, "[");
    int first = 1;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int      id      = sqlite3_column_int(stmt, 0);
        int      r       = sqlite3_column_int(stmt, 1);
        const char *pat  = (const char *)sqlite3_column_text(stmt, 2);
        int      acts    = sqlite3_column_int(stmt, 3);
        const char *rf   = (const char *)sqlite3_column_text(stmt, 4);
        if (!first) off += snprintf(buf + off, 65536 - (size_t)off, ",");
        off += snprintf(buf + off, 65536 - (size_t)off,
            "{\"id\":%d,\"role\":%d,\"table_pattern\":\"%s\","
            "\"allowed_actions\":%d,\"row_filter\":\"%s\"}",
            id, r, pat ? pat : "", acts, rf ? rf : "");
        first = 0;
    }
    off += snprintf(buf + off, 65536 - (size_t)off, "]");
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&s->mu);
    *json_out = buf;
    return 0;
}

/* Засевает дефолтные политики (admin/analyst/viewer) при пустом хранилище. */
void rbac_init_defaults(RbacStore *s) {
    /* Проверяем есть ли уже политики */
    pthread_mutex_lock(&s->mu);
    sqlite3_stmt *chk;
    sqlite3_prepare_v2(s->db, "SELECT COUNT(*) FROM rbac_policies;", -1, &chk, NULL);
    int cnt = 0;
    if (sqlite3_step(chk) == SQLITE_ROW) cnt = sqlite3_column_int(chk, 0);
    sqlite3_finalize(chk);
    pthread_mutex_unlock(&s->mu);
    if (cnt > 0) return;

    /* admin: всё */
    uint32_t all = ACTION_TABLE_READ | ACTION_TABLE_WRITE | ACTION_TABLE_CREATE |
                   ACTION_TABLE_DROP | ACTION_PIPELINE_RUN | ACTION_PIPELINE_EDIT |
                   ACTION_ADMIN | ACTION_METRICS_READ;
    rbac_policy_set(s, ROLE_ADMIN,   "*", all, "");

    /* analyst */
    uint32_t analyst = ACTION_TABLE_READ | ACTION_TABLE_WRITE | ACTION_TABLE_CREATE |
                       ACTION_PIPELINE_RUN | ACTION_PIPELINE_EDIT | ACTION_METRICS_READ;
    rbac_policy_set(s, ROLE_ANALYST, "*", analyst, "");

    /* viewer */
    uint32_t viewer = ACTION_TABLE_READ | ACTION_METRICS_READ;
    rbac_policy_set(s, ROLE_VIEWER,  "*", viewer, "");

    LOG_INFO("rbac: default policies initialized");
}
