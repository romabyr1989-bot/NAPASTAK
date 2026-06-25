/*
 * storage_client.c — клиент к узлу хранения кластера.
 * Устанавливает TCP-соединение и обменивается сообщениями по протоколу proto_*
 * (ping/pong, репликация WAL, запрос статуса). Соединение ленивое: открывается
 * при первом запросе и переоткрывается после ошибок ввода-вывода.
 */
#include "storage_client.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>
#include <arpa/inet.h>

/* Создаёт клиент для адреса host:port; сокет ещё не открыт (fd = -1). */
StorageClient *storage_client_create(const char *host, int port) {
    StorageClient *c = calloc(1, sizeof(StorageClient));
    strncpy(c->host, host, sizeof(c->host) - 1);
    c->port = port;
    c->fd = -1;
    return c;
}

/* Закрывает соединение (если открыто) и освобождает клиент. */
void storage_client_destroy(StorageClient *c) {
    if (!c) return;
    storage_client_disconnect(c);
    free(c);
}

/* Резолвит host, открывает TCP-сокет и подключается. Идемпотентно: при уже
 * установленном соединении возвращает 0. При ошибке возвращает -1. */
int storage_client_connect(StorageClient *c) {
    if (c->connected) return 0;
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_socktype = SOCK_STREAM;
    char port_str[16];
    snprintf(port_str, sizeof(port_str), "%d", c->port);
    if (getaddrinfo(c->host, port_str, &hints, &res) != 0) {
        LOG_WARN("storage_client: can't resolve %s", c->host);
        return -1;
    }
    int fd = socket(res->ai_family, SOCK_STREAM, 0);
    if (fd < 0) { freeaddrinfo(res); return -1; }
    if (connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        close(fd);
        freeaddrinfo(res);
        LOG_WARN("storage_client: can't connect to %s:%d", c->host, c->port);
        return -1;
    }
    freeaddrinfo(res);
    c->fd = fd;
    c->connected = true;
    return 0;
}

/* Закрывает сокет и помечает клиент как отключённый. */
void storage_client_disconnect(StorageClient *c) {
    if (c->fd >= 0) { close(c->fd); c->fd = -1; }
    c->connected = false;
}

/* Проверка живости узла: шлёт MSG_PING и ждёт MSG_PONG. При ошибке I/O
 * разрывает соединение (чтобы следующий вызов переподключился) и возвращает -1. */
int storage_client_ping(StorageClient *c) {
    if (!c->connected && storage_client_connect(c) < 0) return -1;
    uint32_t id = c->next_req_id++;
    if (proto_send(c->fd, MSG_PING, id, NULL, 0) < 0) {
        storage_client_disconnect(c); return -1;
    }
    ProtoHeader hdr; void *body = NULL;
    if (proto_recv(c->fd, &hdr, &body, NULL) < 0) {
        storage_client_disconnect(c); return -1;
    }
    proto_free_body(body);
    return (hdr.msg_type == (uint8_t)MSG_PONG) ? 0 : -1;
}

/* Отправляет WAL-запись (lsn + полезные данные) для репликации на узел хранения
 * и ждёт подтверждения MSG_REPL_ACK. Тело = заголовок ProtoReplicateHdr + WAL. */
int storage_client_replicate(StorageClient *c, const char *table_name,
                              uint64_t lsn, const void *wal_data, size_t wal_len) {
    if (!c->connected && storage_client_connect(c) < 0) return -1;

    /* Единый буфер: фиксированный заголовок, следом сырые WAL-данные. */
    size_t total = sizeof(ProtoReplicateHdr) + wal_len;
    void *body = malloc(total);
    if (!body) return -1;

    ProtoReplicateHdr rhdr;
    memset(&rhdr, 0, sizeof(rhdr));
    strncpy(rhdr.table_name, table_name, sizeof(rhdr.table_name) - 1);
    rhdr.lsn             = lsn;
    rhdr.wal_payload_len = (uint32_t)wal_len;
    memcpy(body, &rhdr, sizeof(rhdr));
    if (wal_len > 0) memcpy((char *)body + sizeof(rhdr), wal_data, wal_len);

    uint32_t id = c->next_req_id++;
    if (proto_send(c->fd, MSG_REPLICATE, id, body, (uint32_t)total) < 0) {
        free(body); storage_client_disconnect(c); return -1;
    }
    free(body);

    ProtoHeader hdr; void *resp = NULL; size_t rlen = 0;
    if (proto_recv(c->fd, &hdr, &resp, &rlen) < 0) {
        storage_client_disconnect(c); return -1;
    }
    proto_free_body(resp);
    return (hdr.msg_type == (uint8_t)MSG_REPL_ACK) ? 0 : -1;
}

/* Запрашивает статус узла (MSG_STATUS_REQ -> MSG_STATUS_RESP). Если out задан и
 * тело ответа достаточного размера — копирует его в out. */
int storage_client_status(StorageClient *c, ProtoStatusBody *out) {
    if (!c->connected && storage_client_connect(c) < 0) return -1;
    uint32_t id = c->next_req_id++;
    if (proto_send(c->fd, MSG_STATUS_REQ, id, NULL, 0) < 0) {
        storage_client_disconnect(c); return -1;
    }
    ProtoHeader hdr; void *body = NULL; size_t blen = 0;
    if (proto_recv(c->fd, &hdr, &body, &blen) < 0) {
        storage_client_disconnect(c); return -1;
    }
    if (out && body && blen >= sizeof(*out))
        memcpy(out, body, sizeof(*out));
    proto_free_body(body);
    return (hdr.msg_type == (uint8_t)MSG_STATUS_RESP) ? 0 : -1;
}
