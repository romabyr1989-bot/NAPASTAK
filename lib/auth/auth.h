#pragma once
/* auth.h — аутентификация и авторизация DataFlow OS:
 * API-ключи и JWT (HS256), роли пользователей, проверка HTTP-запросов.
 * Хранилище учётных записей — таблица users в SQLite-каталоге. */
#include "../core/arena.h"
#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>

#define AUTH_TOKEN_LEN   32    /* API-key random bytes */
#define AUTH_JWT_SECRET_LEN 64

/* Роли по убыванию прав: ADMIN > ANALYST > VIEWER */
typedef enum { ROLE_ADMIN = 0, ROLE_ANALYST = 1, ROLE_VIEWER = 2 } AuthRole;

/* Полезная нагрузка токена: кто, с какой ролью и до какого момента валиден */
typedef struct {
    char     user_id[64];
    AuthRole role;
    int64_t  exp;          /* unix timestamp expiry */
} AuthClaims;

/* Хранилище учётных записей поверх открытого SQLite-соединения */
typedef struct AuthStore {
    sqlite3 *db;
} AuthStore;

/* Инициализация: создаёт/открывает таблицу users в SQLite catalog */
AuthStore *auth_store_create(const char *db_path);
void       auth_store_destroy(AuthStore *s);

/* API-ключи: генерация и проверка */
int  auth_apikey_create(AuthStore *s, const char *user_id, AuthRole role,
                        char *key_out, size_t key_len); /* key_out: "dfo_<32hex>" */
int  auth_apikey_verify(AuthStore *s, const char *key,
                        AuthClaims *claims_out);         /* 0=ok, -1=invalid */
int  auth_apikey_revoke(AuthStore *s, const char *key);

/* JWT: HS256, payload: {"sub":"user_id","role":N,"exp":ts} */
int  auth_jwt_sign(const char *secret, const AuthClaims *claims,
                   char *token_out, size_t token_len); /* token_out: "xxx.yyy.zzz" */
int  auth_jwt_verify(const char *secret, const char *token,
                     AuthClaims *claims_out);           /* 0=ok, -1=invalid/expired */

/* Middleware: извлекает claims из HttpReq (Authorization header или ?api_key=) */
int  auth_check_request(AuthStore *s, const char *jwt_secret,
                        void *req, AuthClaims *claims_out);
