#pragma once
/* Тонкая обёртка над TLS: серверный контекст и соединение поверх сокета.
   Даёт read/write с тем же интерфейсом, что и обычные системные вызовы. */
#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

/* Непрозрачные типы: контекст (cert/key, общий на сервер) и одно соединение. */
typedef struct TlsCtx TlsCtx;
typedef struct TlsConn TlsConn;

/* Серверный контекст: загружает cert.pem + key.pem */
TlsCtx *tls_server_ctx_create(const char *cert_pem_path,
                               const char *key_pem_path);
void    tls_server_ctx_destroy(TlsCtx *ctx);

/* На принятый fd навешиваем TLS handshake */
TlsConn *tls_conn_accept(TlsCtx *ctx, int fd);
void     tls_conn_destroy(TlsConn *conn);

/* Замена read/write — одинаковый интерфейс */
ssize_t  tls_read (TlsConn *conn, void *buf, size_t n);
ssize_t  tls_write(TlsConn *conn, const void *buf, size_t n);

/* Получить fd из TLS connection (для select/epoll) */
int      tls_conn_fd(TlsConn *conn);
