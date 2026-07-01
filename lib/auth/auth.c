/*
 * auth.c — модуль аутентификации NAPASTAK.
 * Реализует два механизма доступа: API-ключи (хранятся в SQLite) и
 * JWT-токены (HS256/HMAC-SHA256). auth_check_request связывает их с
 * HTTP-запросом, проверяя заголовки и query-параметры.
 */
#include "auth.h"
#include "../core/log.h"
#include "../net/http.h"
#include <sqlite3.h>
#include <openssl/hmac.h>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/crypto.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <strings.h>

/* Кодирование в base64url (алфавит с '-'/'_', без паддинга) для частей JWT. */
static int base64url_encode(const unsigned char *data, size_t len, char *out, size_t out_len) {
    static const char b64[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    size_t i = 0, j = 0;
    while (i < len) {
        uint32_t octet_a = i < len ? data[i++] : 0;
        uint32_t octet_b = i < len ? data[i++] : 0;
        uint32_t octet_c = i < len ? data[i++] : 0;
        uint32_t triple = (octet_a << 16) | (octet_b << 8) | octet_c;
        if (j + 4 > out_len) return -1;
        out[j++] = b64[(triple >> 18) & 0x3F];
        out[j++] = b64[(triple >> 12) & 0x3F];
        if (i - 1 < len) out[j++] = b64[(triple >> 6) & 0x3F];
        if (i < len) out[j++] = b64[triple & 0x3F];
    }
    out[j] = '\0';
    return 0;
}

/* Обратное преобразование base64url в байты; b64d — таблица значений символов. */
static int base64url_decode(const char *in, unsigned char *out, size_t out_len) {
    static const unsigned char b64d[256] = {
        0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
        0,0,0,0,0,0,0,0,0,0,0,62,0,0,0,63,52,53,54,55,56,57,58,59,60,61,0,0,0,0,0,0,
        0,0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,0,0,0,0,0,
        0,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,0,0,0,0,0
    };
    size_t len = strlen(in), i = 0, j = 0;
    while (i < len) {
        uint32_t sextet_a = in[i] == '=' ? 0 & i++ : b64d[(unsigned char)in[i++]];
        uint32_t sextet_b = in[i] == '=' ? 0 & i++ : b64d[(unsigned char)in[i++]];
        uint32_t sextet_c = in[i] == '=' ? 0 & i++ : b64d[(unsigned char)in[i++]];
        uint32_t sextet_d = in[i] == '=' ? 0 & i++ : b64d[(unsigned char)in[i++]];
        uint32_t triple = (sextet_a << 18) | (sextet_b << 12) | (sextet_c << 6) | sextet_d;
        if (j < out_len) out[j++] = (triple >> 16) & 0xFF;
        if (j < out_len) out[j++] = (triple >> 8) & 0xFF;
        if (j < out_len) out[j++] = triple & 0xFF;
    }
    return (int)j;
}

/* Открывает (или создаёт) SQLite-БД ключей и гарантирует наличие таблицы auth_keys. */
AuthStore *auth_store_create(const char *db_path) {
    AuthStore *s = calloc(1, sizeof(AuthStore));
    /* FULLMUTEX: соединение используется из нескольких conn_thread одновременно
     * (auth-проверка ключей + эндпоинты users/apikeys) — сериализованный режим
     * заставляет sqlite защищать доступ своим мьютексом, иначе — гонка и segfault. */
    if (sqlite3_open_v2(db_path, &s->db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX,
                        NULL) != SQLITE_OK) {
        LOG_ERROR("failed to open auth db: %s", sqlite3_errmsg(s->db));
        free(s);
        return NULL;
    }
    const char *sql = "CREATE TABLE IF NOT EXISTS auth_keys ("
                      "key TEXT PRIMARY KEY, "
                      "user_id TEXT NOT NULL, "
                      "role INTEGER NOT NULL, "
                      "created_at INTEGER NOT NULL, "
                      "revoked INTEGER DEFAULT 0);";
    if (sqlite3_exec(s->db, sql, NULL, NULL, NULL) != SQLITE_OK) {
        LOG_ERROR("failed to create auth_keys table: %s", sqlite3_errmsg(s->db));
        sqlite3_close(s->db);
        free(s);
        return NULL;
    }
    /* Таблица учётных записей для входа (логин+пароль+роль). */
    const char *usql = "CREATE TABLE IF NOT EXISTS users ("
                       "username TEXT PRIMARY KEY, "
                       "pass_hash TEXT NOT NULL, "
                       "pass_salt TEXT NOT NULL, "
                       "role INTEGER NOT NULL, "
                       "created_at INTEGER NOT NULL, "
                       "enabled INTEGER NOT NULL DEFAULT 1);";
    if (sqlite3_exec(s->db, usql, NULL, NULL, NULL) != SQLITE_OK) {
        LOG_ERROR("failed to create users table: %s", sqlite3_errmsg(s->db));
        sqlite3_close(s->db);
        free(s);
        return NULL;
    }
    return s;
}

/* ── Учётные записи для входа: PBKDF2-HMAC-SHA256 ── */
#define PW_SALT_LEN 16
#define PW_HASH_LEN 32
#define PW_ITER     100000

/* Хэширует пароль PBKDF2-HMAC-SHA256 с данной солью; out_hex >= 2*PW_HASH_LEN+1. */
static int pw_hash_hex(const char *password, const unsigned char *salt,
                       char *out_hex, size_t out_len) {
    if (out_len < (size_t)PW_HASH_LEN * 2 + 1) return -1;
    unsigned char key[PW_HASH_LEN];
    if (PKCS5_PBKDF2_HMAC(password, (int)strlen(password), salt, PW_SALT_LEN,
                          PW_ITER, EVP_sha256(), PW_HASH_LEN, key) != 1) return -1;
    for (int i = 0; i < PW_HASH_LEN; i++) sprintf(out_hex + i * 2, "%02x", key[i]);
    return 0;
}

/* hex-строка → байты (для восстановления соли при проверке). */
static int hex_to_bytes(const char *hex, unsigned char *out, size_t out_len) {
    size_t n = strlen(hex) / 2;
    if (n > out_len) return -1;
    for (size_t i = 0; i < n; i++) {
        unsigned int b;
        if (sscanf(hex + i * 2, "%2x", &b) != 1) return -1;
        out[i] = (unsigned char)b;
    }
    return (int)n;
}

/* Создаёт учётку: генерит соль, хэширует пароль, пишет в users. */
int auth_user_create(AuthStore *s, const char *username, const char *password,
                     AuthRole role) {
    if (!s || !username || !password || !*username || !*password) return -1;
    unsigned char salt[PW_SALT_LEN];
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f || fread(salt, 1, sizeof(salt), f) != sizeof(salt)) { if (f) fclose(f); return -1; }
    fclose(f);
    char salt_hex[PW_SALT_LEN * 2 + 1], hash_hex[PW_HASH_LEN * 2 + 1];
    for (int i = 0; i < PW_SALT_LEN; i++) sprintf(salt_hex + i * 2, "%02x", salt[i]);
    if (pw_hash_hex(password, salt, hash_hex, sizeof(hash_hex)) != 0) return -1;
    int64_t now = (int64_t)time(NULL);
    const char *sql = "INSERT INTO users (username, pass_hash, pass_salt, role, created_at, enabled) "
                      "VALUES (?, ?, ?, ?, ?, 1);";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, hash_hex, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 3, salt_hex, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 4, (int)role);
    sqlite3_bind_int64(stmt, 5, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc == SQLITE_CONSTRAINT) return -2;   /* логин уже существует */
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Проверяет логин/пароль; при успехе заполняет claims (user_id, role). */
int auth_user_verify(AuthStore *s, const char *username, const char *password,
                     AuthClaims *claims_out) {
    if (!s || !username || !password) return -1;
    const char *sql = "SELECT pass_hash, pass_salt, role FROM users "
                      "WHERE username = ? AND enabled = 1;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_STATIC);
    int result = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *hash_hex = (const char *)sqlite3_column_text(stmt, 0);
        const char *salt_hex = (const char *)sqlite3_column_text(stmt, 1);
        int role = sqlite3_column_int(stmt, 2);
        unsigned char salt[PW_SALT_LEN];
        char calc_hex[PW_HASH_LEN * 2 + 1];
        if (hash_hex && salt_hex &&
            hex_to_bytes(salt_hex, salt, sizeof(salt)) == PW_SALT_LEN &&
            pw_hash_hex(password, salt, calc_hex, sizeof(calc_hex)) == 0 &&
            strlen(hash_hex) == strlen(calc_hex) &&
            CRYPTO_memcmp(hash_hex, calc_hex, strlen(calc_hex)) == 0) {
            if (claims_out) {
                strncpy(claims_out->user_id, username, sizeof(claims_out->user_id) - 1);
                claims_out->user_id[sizeof(claims_out->user_id) - 1] = '\0';
                claims_out->role = (AuthRole)role;
                claims_out->exp = 0;
            }
            result = 0;
        }
    }
    sqlite3_finalize(stmt);
    return result;
}

/* Удаляет учётку по логину. */
int auth_user_delete(AuthStore *s, const char *username) {
    if (!s || !username) return -1;
    const char *sql = "DELETE FROM users WHERE username = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) return -1;
    sqlite3_bind_text(stmt, 1, username, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    int changes = sqlite3_changes(s->db);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) return -1;
    return changes > 0 ? 0 : -1;
}

/* Закрывает соединение с БД и освобождает хранилище. */
void auth_store_destroy(AuthStore *s) {
    if (s) {
        sqlite3_close(s->db);
        free(s);
    }
}

/* Генерирует новый API-ключ ("dfo_" + hex случайных байт), сохраняет его в БД
 * и возвращает строку ключа в key_out. */
int auth_apikey_create(AuthStore *s, const char *user_id, AuthRole role,
                       char *key_out, size_t key_len) {
    unsigned char rand_bytes[AUTH_TOKEN_LEN];
    /* Источник криптослучайности — /dev/urandom. */
    FILE *f = fopen("/dev/urandom", "rb");
    if (!f || fread(rand_bytes, 1, sizeof(rand_bytes), f) != sizeof(rand_bytes)) {
        if (f) fclose(f);
        return -1;
    }
    fclose(f);
    char hex[65];
    for (int i = 0; i < AUTH_TOKEN_LEN; i++) {
        sprintf(hex + i*2, "%02x", rand_bytes[i]);
    }
    snprintf(key_out, key_len, "dfo_%s", hex);
    int64_t now = (int64_t)time(NULL);
    const char *sql = "INSERT INTO auth_keys (key, user_id, role, created_at) VALUES (?, ?, ?, ?);";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, key_out, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, user_id, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 3, (int)role);
    sqlite3_bind_int64(stmt, 4, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Проверяет API-ключ по БД (только не отозванные) и заполняет claims_out. */
int auth_apikey_verify(AuthStore *s, const char *key, AuthClaims *claims_out) {
    const char *sql = "SELECT user_id, role, created_at FROM auth_keys WHERE key = ? AND revoked = 0;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        const char *user_id = (const char *)sqlite3_column_text(stmt, 0);
        int role = sqlite3_column_int(stmt, 1);
        int64_t created_at = sqlite3_column_int64(stmt, 2);
        strncpy(claims_out->user_id, user_id, sizeof(claims_out->user_id) - 1);
        claims_out->role = (AuthRole)role;
        claims_out->exp = created_at + 86400 * 365;  // 1 year expiry
        sqlite3_finalize(stmt);
        return 0;
    }
    sqlite3_finalize(stmt);
    return -1;
}

/* Помечает ключ как отозванный (revoked = 1); сама запись не удаляется. */
int auth_apikey_revoke(AuthStore *s, const char *key) {
    const char *sql = "UPDATE auth_keys SET revoked = 1 WHERE key = ?;";
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* Формирует подписанный JWT (HS256): header.payload.signature в base64url. */
int auth_jwt_sign(const char *secret, const AuthClaims *claims,
                  char *token_out, size_t token_len) {
    // Header: {"alg":"HS256","typ":"JWT"}
    const char *header = "{\"alg\":\"HS256\",\"typ\":\"JWT\"}";
    char header_b64[256];
    if (base64url_encode((const unsigned char *)header, strlen(header), header_b64, sizeof(header_b64)) != 0) {
        return -1;
    }

    // Payload: {"sub":"user_id","role":N,"exp":ts}
    char payload[512];
    snprintf(payload, sizeof(payload), "{\"sub\":\"%s\",\"role\":%d,\"exp\":%lld}",
             claims->user_id, (int)claims->role, claims->exp);
    char payload_b64[512];
    if (base64url_encode((const unsigned char *)payload, strlen(payload), payload_b64, sizeof(payload_b64)) != 0) {
        return -1;
    }

    // Signature
    char data[1024];
    snprintf(data, sizeof(data), "%s.%s", header_b64, payload_b64);
    unsigned char sig[SHA256_DIGEST_LENGTH];
    unsigned int sig_len;
    HMAC(EVP_sha256(), secret, (int)strlen(secret), (const unsigned char *)data, strlen(data), sig, &sig_len);
    char sig_b64[256];
    if (base64url_encode(sig, sig_len, sig_b64, sizeof(sig_b64)) != 0) {
        return -1;
    }

    snprintf(token_out, token_len, "%s.%s.%s", header_b64, payload_b64, sig_b64);
    return 0;
}

/* Проверяет подпись и срок действия JWT, извлекает sub/role/exp в claims_out. */
int auth_jwt_verify(const char *secret, const char *token, AuthClaims *claims_out) {
    char *parts[3];
    char token_copy[2048];
    strncpy(token_copy, token, sizeof(token_copy) - 1);
    char *p = token_copy;
    int i = 0;
    char *dot;
    /* Разбиваем по первым двум точкам: header и payload; остаток — подпись. */
    while ((dot = strchr(p, '.')) && i < 2) {
        *dot = '\0';
        parts[i++] = p;
        p = dot + 1;
    }
    if (i != 2) return -1;  /* JWT must have exactly 2 dots */
    parts[2] = p;           /* signature = remainder after second dot */

    // Verify signature: re-encode fresh HMAC and compare strings (avoids decode table issues)
    char data[1024];
    snprintf(data, sizeof(data), "%s.%s", parts[0], parts[1]);
    unsigned char sig[SHA256_DIGEST_LENGTH];
    unsigned int sig_len;
    HMAC(EVP_sha256(), secret, (int)strlen(secret), (const unsigned char *)data, strlen(data), sig, &sig_len);
    char computed_sig_b64[256];
    if (base64url_encode(sig, sig_len, computed_sig_b64, sizeof(computed_sig_b64)) != 0)
        return -1;
    if (strcmp(computed_sig_b64, parts[2]) != 0)
        return -1;

    // Decode payload
    unsigned char payload_dec[512];
    size_t payload_len = base64url_decode(parts[1], payload_dec, sizeof(payload_dec));
    if (payload_len >= sizeof(payload_dec)) payload_len = sizeof(payload_dec) - 1;
    payload_dec[payload_len] = '\0';

    // Parse JSON without modifying the decoded buffer (in-place null breaks subsequent strstr)
    const char *json = (const char *)payload_dec;
    const char *sub = strstr(json, "\"sub\":\"");
    if (!sub) return -1;
    sub += 7;
    const char *sub_end = strchr(sub, '\"');
    if (!sub_end) return -1;
    size_t sub_len = (size_t)(sub_end - sub);
    if (sub_len >= sizeof(claims_out->user_id)) sub_len = sizeof(claims_out->user_id) - 1;
    memcpy(claims_out->user_id, sub, sub_len);
    claims_out->user_id[sub_len] = '\0';

    const char *role_str = strstr(json, "\"role\":");
    if (!role_str) return -1;
    claims_out->role = (AuthRole)atoi(role_str + 7);

    const char *exp_str = strstr(json, "\"exp\":");
    if (!exp_str) return -1;
    claims_out->exp = atoll(exp_str + 6);

    // Check expiry
    int64_t now = (int64_t)time(NULL);
    if (claims_out->exp < now) return -1;

    return 0;
}

/* Точка входа авторизации HTTP-запроса: пробует JWT/API-ключ из заголовка
 * Authorization, затем X-Api-Key, затем query-параметр ?api_key=. Возвращает 0
 * при успехе с заполненным claims_out, иначе -1. */
int auth_check_request(AuthStore *s, const char *jwt_secret, void *req_void, AuthClaims *claims_out) {
    HttpReq *req = (HttpReq *)req_void;
    // Check Authorization header
    const char *auth = hm_get(&req->headers, "authorization");
    if (auth) {
        if (strncasecmp(auth, "Bearer ", 7) == 0) {
            const char *token = auth + 7;
            if (auth_jwt_verify(jwt_secret, token, claims_out) == 0) {
                return 0;
            }
            /* Fallback: clients commonly send API keys as Bearer too.
             * Recognize the "dfo_" prefix and dispatch to apikey verify. */
            if (strncmp(token, "dfo_", 4) == 0 &&
                auth_apikey_verify(s, token, claims_out) == 0) {
                return 0;
            }
        } else if (strncasecmp(auth, "ApiKey ", 7) == 0) {
            const char *key = auth + 7;
            if (auth_apikey_verify(s, key, claims_out) == 0) {
                return 0;
            }
        }
    }

    // Check X-Api-Key header
    const char *x_api_key = hm_get(&req->headers, "x-api-key");
    if (x_api_key && auth_apikey_verify(s, x_api_key, claims_out) == 0)
        return 0;

    // Check query param ?api_key= (parse req->query string directly)
    const char *query = req->query;
    if (query) {
        const char *p = query;
        while (*p) {
            const char *amp = strchr(p, '&');
            size_t kv_len = amp ? (size_t)(amp - p) : strlen(p);
            if (kv_len > 8 && strncmp(p, "api_key=", 8) == 0) {
                size_t vlen = kv_len - 8;
                char api_key_val[256];
                if (vlen < sizeof(api_key_val)) {
                    memcpy(api_key_val, p + 8, vlen);
                    api_key_val[vlen] = '\0';
                    if (auth_apikey_verify(s, api_key_val, claims_out) == 0)
                        return 0;
                }
            }
            if (!amp) break;
            p = amp + 1;
        }
    }

    return -1;
}
