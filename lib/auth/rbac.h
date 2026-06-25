#pragma once
/* RBAC: ролевая модель доступа поверх Auth.
   Хранит политики (роль + шаблон таблицы + битовая маска действий + RLS-фильтр)
   в SQLite и проверяет права на действия над таблицами/пайплайнами. */
#include "auth.h"
#include "../core/arena.h"
#include <stdbool.h>
#include <stdint.h>
#include <pthread.h>

/* Действия, разрешаемые политикой; битовые флаги для маски allowed_actions. */
typedef enum {
    ACTION_TABLE_READ    = 1 << 0,
    ACTION_TABLE_WRITE   = 1 << 1,
    ACTION_TABLE_CREATE  = 1 << 2,
    ACTION_TABLE_DROP    = 1 << 3,
    ACTION_PIPELINE_RUN  = 1 << 4,
    ACTION_PIPELINE_EDIT = 1 << 5,
    ACTION_ADMIN         = 1 << 6,
    ACTION_METRICS_READ  = 1 << 7,
} RbacAction;

/* Одна запись политики доступа. */
typedef struct {
    int      id;
    AuthRole role;
    char     table_pattern[128];  /* glob-шаблон имени таблицы, напр. "*" */
    uint32_t allowed_actions;     /* маска RbacAction */
    char     row_filter[512];     /* RLS WHERE-выражение, может содержать плейсхолдер user_id */
} RbacPolicy;

/* Хранилище политик: SQLite-БД под защитой мьютекса; enabled выключает проверки целиком. */
typedef struct RbacStore {
    sqlite3         *db;
    pthread_mutex_t  mu;
    bool             enabled;
} RbacStore;

/* Открывает/создаёт БД политик; enabled=false => rbac_check всегда разрешает. */
RbacStore *rbac_store_create(const char *db_path, bool enabled);
void       rbac_store_destroy(RbacStore *s);

/* 0 = разрешено, -1 = запрещено */
int rbac_check(RbacStore *s, const AuthClaims *claims,
               RbacAction action, const char *table_name);

/* NULL если нет RLS; иначе WHERE-выражение с подставленным user_id */
const char *rbac_row_filter(RbacStore *s, const AuthClaims *claims,
                             const char *table_name, Arena *a);

/* CRUD над политиками: создать/обновить, удалить по id, выгрузить в JSON. */
int rbac_policy_set(RbacStore *s, AuthRole role, const char *table_pattern,
                    uint32_t actions, const char *row_filter);
int rbac_policy_del(RbacStore *s, int policy_id);
int rbac_policy_list(RbacStore *s, AuthRole role, char **json_out, Arena *a);

/* Устанавливает дефолтные политики:
   admin:   ACTION_ADMIN|все права на "*"
   analyst: TABLE_READ|TABLE_WRITE|PIPELINE_RUN|PIPELINE_EDIT|METRICS_READ на "*"
   viewer:  TABLE_READ|METRICS_READ на "*" */
void rbac_init_defaults(RbacStore *s);
