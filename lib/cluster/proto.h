/*
 * proto.h — бинарный сетевой протокол межузлового взаимодействия кластера.
 * Описывает фиксированный заголовок (ProtoHeader), типы сообщений и тела
 * пакетов для репликации WAL, heartbeat/ping, выбора лидера и обмена статусом.
 * Все структуры упакованы (packed) для стабильного wire-формата между узлами.
 */
#pragma once
#include <stdint.h>
#include <stddef.h>

/* Сигнатура и версия протокола: проверяются в заголовке каждого пакета */
#define PROTO_MAGIC   0xDF0A
#define PROTO_VERSION 1

/* Типы сообщений протокола (поле msg_type в ProtoHeader) */
typedef enum {
    MSG_PING        = 1,
    MSG_PONG        = 2,
    MSG_REPLICATE   = 3,
    MSG_ACK         = 4,
    MSG_PROMOTE     = 5,
    MSG_STATUS_REQ  = 6,
    MSG_STATUS_RESP = 7,
    MSG_REPL_ACK    = 8,
} ProtoMsgType;

/* Фиксированный заголовок, предшествующий телу каждого пакета */
typedef struct __attribute__((packed)) {
    uint16_t magic;       /* 0xDF0A */
    uint8_t  version;     /* PROTO_VERSION */
    uint8_t  msg_type;    /* ProtoMsgType */
    uint32_t request_id;
    uint32_t body_len;
    uint32_t reserved;
} ProtoHeader;

/* Заголовок сообщения MSG_REPLICATE; далее следует WAL-payload длиной wal_payload_len */
typedef struct __attribute__((packed)) {
    char     table_name[128];
    uint64_t lsn;
    uint64_t offset;
    uint32_t wal_payload_len;
    uint32_t reserved;
} ProtoReplicateHdr;

/* Тело MSG_REPL_ACK: подтверждение применения реплики до указанного lsn */
typedef struct __attribute__((packed)) {
    uint64_t lsn;
    int32_t  result_code; /* 0=ok, -1=error */
} ProtoReplAckBody;

/* Тело MSG_STATUS_RESP: текущее состояние узла (роль, прогресс WAL, кворум) */
typedef struct __attribute__((packed)) {
    uint8_t  is_leader;
    uint64_t wal_offset;
    uint32_t replica_count;
    char     node_id[37];
} ProtoStatusBody;

/* Сериализует и отправляет в сокет fd заголовок + тело; 0 при успехе */
int  proto_send(int fd, ProtoMsgType type, uint32_t req_id,
                const void *body, uint32_t body_len);
/* Читает один пакет: заполняет hdr и выделяет тело (*body_out); тело освобождает proto_free_body */
int  proto_recv(int fd, ProtoHeader *hdr, void **body_out, size_t *body_len_out);
/* Освобождает буфер тела, выделенный proto_recv */
void proto_free_body(void *body);
