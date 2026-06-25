/*
 * storage_client.h — клиент для связи с узлом хранения кластера.
 * Описывает TCP-соединение и операции протокола: ping, репликация WAL
 * по LSN и запрос статуса узла.
 */
#pragma once
#include "proto.h"
#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>

/* Состояние подключения к одному узлу хранения. */
typedef struct {
    int      fd;            /* дескриптор сокета (-1 если не подключён) */
    char     host[256];
    int      port;
    bool     connected;
    uint32_t next_req_id;   /* монотонный счётчик id запросов */
} StorageClient;

/* Создаёт/освобождает клиент (без установки соединения). */
StorageClient *storage_client_create(const char *host, int port);
void           storage_client_destroy(StorageClient *c);
/* Управление TCP-соединением с узлом. */
int            storage_client_connect(StorageClient *c);
void           storage_client_disconnect(StorageClient *c);
/* Проверка доступности узла. */
int            storage_client_ping(StorageClient *c);
/* Отправляет блок WAL (с привязкой к LSN) для репликации таблицы. */
int            storage_client_replicate(StorageClient *c, const char *table_name,
                                        uint64_t lsn, const void *wal_data, size_t wal_len);
/* Запрашивает статус узла, результат пишется в *out. */
int            storage_client_status(StorageClient *c, ProtoStatusBody *out);
