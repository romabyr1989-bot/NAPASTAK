#pragma once
/*
 * replicator.h — асинхронная репликация WAL с лидера на реплики.
 * Лидер перехватывает записи WAL через callback, складывает их в очередь
 * и фоновым потоком рассылает на подключённые реплики (StorageClient).
 */
#include "storage_client.h"
#include "../storage/storage.h"
#include <stdbool.h>
#include <pthread.h>
#include <stdint.h>

#define MAX_REPLICAS   8     /* максимум подключённых реплик на одного лидера */
#define REPL_QUEUE_CAP 1024  /* ёмкость кольцевой очереди отложенной отправки */

/* Один элемент очереди репликации: копия записи WAL, ожидающая рассылки. */
typedef struct {
    char     table_name[128];
    uint64_t lsn;
    void    *data;  /* malloc'd copy of WAL payload */
    size_t   len;
} ReplItem;

/* Состояние узла репликации: набор реплик, фоновый поток и его очередь. */
typedef struct {
    StorageClient  *replicas[MAX_REPLICAS];
    int             nreplicas;
    bool            is_leader;
    char            node_id[37];

    /* async work queue */
    ReplItem        queue[REPL_QUEUE_CAP];
    int             head, tail, count;
    pthread_mutex_t q_mu;
    pthread_cond_t  q_cv;
    pthread_t       worker;
    volatile int    running;

    /* metrics */
    uint64_t        last_acked_lsn;
    int             lag_count;
} Replicator;

/* Создаёт репликатор; на лидере запускает фоновый поток-рассыльщик. */
Replicator *replicator_create(bool is_leader, const char *node_id);
/* Останавливает поток, дренирует очередь и освобождает ресурсы. */
void        replicator_destroy(Replicator *r);

/* Регистрирует реплику по адресу host:port; возвращает индекс или <0 при ошибке. */
int  replicator_add_replica(Replicator *r, const char *host, int port);

/* Called from WAL callback — enqueues (non-blocking, drops on overflow) */
void replicator_enqueue(Replicator *r, const char *table_name,
                        uint64_t lsn, const void *data, size_t len);

/* WalWriteCallback-compatible signature — can be passed to table_set_wal_callback */
void replicator_wal_cb(const char *table_name, uint64_t lsn,
                       const void *data, size_t len, void *userdata);

/* Формирует JSON со статусом репликации (роль, число реплик, lag) в json_out. */
void replicator_get_status(Replicator *r, char *json_out, size_t len);
