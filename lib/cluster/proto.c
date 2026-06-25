/*
 * proto.c — кодирование/декодирование бинарного протокола межузлового
 * взаимодействия кластера: фиксированный заголовок ProtoHeader (поля в сетевом
 * порядке байт) и опциональное тело сообщения поверх потокового сокета.
 */
#include "proto.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>

/* Записать ровно len байт, повторяя write при частичной записи и EINTR. */
static int write_all(int fd, const void *buf, size_t len) {
    const uint8_t *p = (const uint8_t *)buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += n; len -= (size_t)n;
    }
    return 0;
}

/* Прочитать ровно len байт; EOF до их получения считается ошибкой (-1). */
static int read_all(int fd, void *buf, size_t len) {
    uint8_t *p = (uint8_t *)buf;
    while (len > 0) {
        ssize_t n = read(fd, p, len);
        if (n == 0) return -1;  /* EOF */
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        p += n; len -= (size_t)n;
    }
    return 0;
}

/* Сформировать заголовок (поля -> сетевой порядок) и отправить его вместе с
 * телом одним сообщением. Возвращает 0 при успехе, -1 при ошибке записи. */
int proto_send(int fd, ProtoMsgType type, uint32_t req_id,
               const void *body, uint32_t body_len) {
    ProtoHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.magic      = htons(PROTO_MAGIC);
    hdr.version    = PROTO_VERSION;
    hdr.msg_type   = (uint8_t)type;
    hdr.request_id = htonl(req_id);
    hdr.body_len   = htonl(body_len);
    if (write_all(fd, &hdr, sizeof(hdr)) < 0) return -1;
    if (body_len > 0 && body && write_all(fd, body, body_len) < 0) return -1;
    return 0;
}

/* Принять одно сообщение: читает заголовок, проверяет magic, переводит поля из
 * сетевого порядка в хостовый и, при наличии тела, выделяет под него буфер
 * (владение передаётся вызывающему — освобождать через proto_free_body).
 * Возвращает 0 при успехе, -1 при ошибке/повреждённом заголовке. */
int proto_recv(int fd, ProtoHeader *hdr, void **body_out, size_t *body_len_out) {
    if (body_out)     *body_out     = NULL;
    if (body_len_out) *body_len_out = 0;

    if (read_all(fd, hdr, sizeof(*hdr)) < 0) return -1;
    if (ntohs(hdr->magic) != PROTO_MAGIC) {
        LOG_ERROR("proto: bad magic %04x", ntohs(hdr->magic));
        return -1;
    }
    hdr->request_id = ntohl(hdr->request_id);
    uint32_t blen   = ntohl(hdr->body_len);
    hdr->body_len   = blen;

    if (blen == 0) return 0;
    /* Ограничение размера тела (64 МиБ) — защита от чрезмерного выделения памяти. */
    if (blen > 64u * 1024u * 1024u) {
        LOG_ERROR("proto: body too large (%u)", blen);
        return -1;
    }
    void *buf = malloc(blen);
    if (!buf) return -1;
    if (read_all(fd, buf, blen) < 0) { free(buf); return -1; }
    if (body_out)     *body_out     = buf;
    if (body_len_out) *body_len_out = blen;
    return 0;
}

/* Освободить буфер тела, выделенный в proto_recv. */
void proto_free_body(void *body) {
    free(body);
}
