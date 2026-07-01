#pragma once
/*
 * http.h — публичный API HTTP-слоя NAPASTAK.
 * Описывает структуры запроса/ответа, роутер с захватом :param,
 * исходящие запросы и встроенный TLS-сервер.
 */
#include "../core/arena.h"
#include "../core/hashmap.h"
#include "../auth/auth.h"
#include "tls.h"
#include <stddef.h>
#include <stdbool.h>

#define HTTP_MAX_HEADERS 64
#define HTTP_MAX_ROUTES  128

/* ── Request ── */
/* Разобранный входящий запрос; вся память живёт в req->arena. */
typedef struct {
    const char *method;
    const char *path;
    const char *query;           /* raw query string after '?' */
    const char *body;
    size_t      body_len;
    HashMap     headers;
    HashMap     params;          /* URL :param captures */
    Arena      *arena;
    int         fd;
    bool        upgrade_ws;
    AuthClaims  auth;        /* заполняется middleware, role=ROLE_VIEWER по умолчанию */
    bool        auth_ok;     /* false = не прошёл аутентификацию */
    char        correlation_id[37]; /* UUID4, копируется из X-Correlation-Id или генерируется */
    uint64_t    txn_id;             /* 0 = нет активной транзакции (Шаг 2) */
} HttpReq;

/* ── Response ── */
/* Ответ, заполняемый обработчиком и сериализуемый сервером. */
typedef struct {
    int         status;
    const char *content_type;
    const char *body;
    size_t      body_len;
    bool        is_ws;
    char        correlation_id[37]; /* эхо из req, возвращается в X-Correlation-Id */
} HttpResp;

/* ── Router ── */
/* Обработчик маршрута: читает req, заполняет resp. */
typedef void (*HttpHandler)(HttpReq *req, HttpResp *resp);

/* Хелперы заполнения ответа нужным Content-Type и телом. */
void http_resp_json(HttpResp *r, int status, const char *json);
void http_resp_text(HttpResp *r, int status, const char *text);
void http_resp_error(HttpResp *r, int status, const char *msg);

/* Один зарегистрированный маршрут: метод + шаблон пути + обработчик. */
typedef struct {
    char        method[8];
    char        pattern[256];
    HttpHandler handler;
} Route;

/* Таблица маршрутов с фиксированной ёмкостью HTTP_MAX_ROUTES. */
typedef struct {
    Route routes[HTTP_MAX_ROUTES];
    int   nroutes;
    void *userdata;  /* pointer to App */
} Router;

/* Регистрация маршрута и поиск+вызов подходящего обработчика по запросу. */
void router_add(Router *r, const char *method, const char *pattern, HttpHandler h);
void router_dispatch(Router *r, HttpReq *req, HttpResp *resp);

/* ── Outbound HTTP */
/* Синхронный POST JSON на url; возвращает HTTP-код или <0 при ошибке. */
int http_post_json(const char *url, const char *body, int timeout_ms);

/* ── Server ── */
typedef struct HttpServer HttpServer; /* непрозрачный дескриптор сервера */

/* Создаёт сервер (tls_ctx != NULL — включает HTTPS), запускает/останавливает цикл приёма. */
HttpServer *http_server_create(Router *r, int port, int backlog, TlsCtx *tls_ctx);
void        http_server_run(HttpServer *s);   /* blocks */
void        http_server_stop(HttpServer *s);
