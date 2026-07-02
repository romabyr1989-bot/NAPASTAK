/*
 * replicator.c — асинхронная репликация WAL с лидера на реплики.
 *
 * Лидер кладёт WAL-записи в кольцевую очередь (replicator_enqueue),
 * фоновый поток (worker_fn) разбирает очередь и рассылает каждую запись
 * всем репликам по протоколу MSG_REPLICATE, дожидаясь ACK.
 * lag_count отражает текущую глубину очереди (отставание реплик).
 */
#include "replicator.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

/*
 * Отправляет одну WAL-запись конкретной реплике и ждёт ACK.
 * При ошибке соединения/отправки/приёма реплика отключается (молча),
 * чтобы при следующей отправке переподключиться.
 */
static void send_to_replica(StorageClient *sc, const ReplItem *item) {
    /* Ленивое подключение: соединяемся только при необходимости */
    if (!sc->connected && storage_client_connect(sc) < 0) return;

    /* Тело сообщения = заголовок + сырой WAL-payload одним буфером */
    size_t total = sizeof(ProtoReplicateHdr) + item->len;
    void *body = malloc(total);
    if (!body) return;

    ProtoReplicateHdr hdr;
    memset(&hdr, 0, sizeof(hdr));
    strncpy(hdr.table_name, item->table_name, sizeof(hdr.table_name)-1);
    hdr.lsn             = item->lsn;
    hdr.wal_payload_len = (uint32_t)item->len;
    memcpy(body, &hdr, sizeof(hdr));
    if (item->len > 0) memcpy((char *)body + sizeof(hdr), item->data, item->len);

    /* req_id — младшие 32 бита LSN, чтобы сопоставить ответ с запросом */
    uint32_t req_id = (uint32_t)(item->lsn & 0xFFFFFFFF);
    if (proto_send(sc->fd, MSG_REPLICATE, req_id, body, (uint32_t)total) < 0) {
        free(body);
        storage_client_disconnect(sc);
        return;
    }
    free(body);

    /* Ждём ACK: сам факт получения ответа = успех; тип и result_code внутри
     * ahdr/abody не проверяются, ответ лишь синхронизирует темп отправки. */
    ProtoHeader ahdr; void *abody = NULL; size_t alen = 0;
    if (proto_recv(sc->fd, &ahdr, &abody, &alen) < 0) {
        storage_client_disconnect(sc);
        return;
    }
    proto_free_body(abody);
}

/*
 * Фоновый поток репликации: вынимает записи из очереди и рассылает их
 * репликам. Работает, пока репликатор активен либо очередь не пуста
 * (чтобы корректно дослать остаток при остановке).
 */
static void *worker_fn(void *arg) {
    Replicator *r = (Replicator *)arg;
    while (r->running || r->count > 0) {
        pthread_mutex_lock(&r->q_mu);
        while (r->count == 0 && r->running)
            pthread_cond_wait(&r->q_cv, &r->q_mu);
        /* Пробудились с пустой очередью => остановка: выходим */
        if (r->count == 0) { pthread_mutex_unlock(&r->q_mu); break; }

        /* Снимаем элемент с головы кольцевого буфера под мьютексом */
        ReplItem item = r->queue[r->head];
        r->head = (r->head + 1) % REPL_QUEUE_CAP;
        r->count--;
        r->lag_count = r->count;
        pthread_mutex_unlock(&r->q_mu);

        /* Рассылка вне мьютекса: сетевой I/O не блокирует enqueue */
        for (int i = 0; i < r->nreplicas; i++)
            send_to_replica(r->replicas[i], &item);

        /* Обновляем «дошедший» LSN оптимистично: send_to_replica проглатывает
         * ошибки/содержимое ACK, поэтому значение отражает разосланное, а не
         * гарантированно применённое всеми репликами. */
        r->last_acked_lsn = item.lsn;
        free(item.data);  /* копия payload'а, выделенная при enqueue */
    }
    return NULL;
}

/* Создаёт репликатор и сразу запускает фоновый поток рассылки */
Replicator *replicator_create(bool is_leader, const char *node_id) {
    Replicator *r = calloc(1, sizeof(Replicator));
    r->is_leader = is_leader;
    if (node_id) strncpy(r->node_id, node_id, sizeof(r->node_id)-1);
    pthread_mutex_init(&r->q_mu, NULL);
    pthread_cond_init(&r->q_cv, NULL);
    r->running = 1;
    pthread_create(&r->worker, NULL, worker_fn, r);
    return r;
}

/*
 * Останавливает фоновый поток, ждёт его завершения, освобождает
 * непереданные элементы очереди, закрывает реплики и сам репликатор.
 */
void replicator_destroy(Replicator *r) {
    if (!r) return;
    /* Сигнализируем потоку остановку и будим его, если он спит на cv */
    pthread_mutex_lock(&r->q_mu);
    r->running = 0;
    pthread_cond_signal(&r->q_cv);
    pthread_mutex_unlock(&r->q_mu);
    pthread_join(r->worker, NULL);
    /* free remaining items */
    while (r->count > 0) {
        free(r->queue[r->head].data);
        r->head = (r->head + 1) % REPL_QUEUE_CAP;
        r->count--;
    }
    for (int i = 0; i < r->nreplicas; i++)
        storage_client_destroy(r->replicas[i]);
    pthread_mutex_destroy(&r->q_mu);
    pthread_cond_destroy(&r->q_cv);
    free(r);
}

/* Добавляет реплику-получателя; -1 при достижении лимита MAX_REPLICAS */
int replicator_add_replica(Replicator *r, const char *host, int port) {
    if (r->nreplicas >= MAX_REPLICAS) return -1;
    r->replicas[r->nreplicas++] = storage_client_create(host, port);
    return 0;
}

/*
 * Кладёт WAL-запись в очередь репликации (только на лидере).
 * Делает собственную копию payload'а; при переполнении очереди
 * запись отбрасывается с ошибкой в лог.
 */
void replicator_enqueue(Replicator *r, const char *table_name,
                        uint64_t lsn, const void *data, size_t len) {
    if (!r->is_leader) return;
    pthread_mutex_lock(&r->q_mu);
    if (r->count >= REPL_QUEUE_CAP) {
        LOG_ERROR("replicator: queue full, dropping lsn=%llu", (unsigned long long)lsn);
        pthread_mutex_unlock(&r->q_mu);
        return;
    }
    ReplItem *it = &r->queue[r->tail];
    strncpy(it->table_name, table_name, sizeof(it->table_name)-1);
    it->lsn  = lsn;
    it->len  = len;
    it->data = NULL;
    if (len > 0) {
        it->data = malloc(len);
        if (it->data) memcpy(it->data, data, len);
    }
    r->tail = (r->tail + 1) % REPL_QUEUE_CAP;
    r->count++;
    r->lag_count = r->count;
    pthread_cond_signal(&r->q_cv);
    pthread_mutex_unlock(&r->q_mu);
}

/* Колбэк для WAL-подсистемы: userdata — это Replicator* */
void replicator_wal_cb(const char *table_name, uint64_t lsn,
                       const void *data, size_t len, void *userdata) {
    replicator_enqueue((Replicator *)userdata, table_name, lsn, data, len);
}

/* Формирует JSON-снимок состояния репликации (роль, LSN, лаг, реплики) */
void replicator_get_status(Replicator *r, char *json_out, size_t len) {
    pthread_mutex_lock(&r->q_mu);
    int off = snprintf(json_out, len,
        "{\"is_leader\":%s,\"node_id\":\"%s\","
        "\"last_acked_lsn\":%llu,\"lag_count\":%d,\"replica_count\":%d,\"replicas\":[",
        r->is_leader ? "true" : "false", r->node_id,
        (unsigned long long)r->last_acked_lsn, r->lag_count, r->nreplicas);
    for (int i = 0; i < r->nreplicas && off < (int)len - 10; i++) {
        StorageClient *sc = r->replicas[i];
        if (i > 0) off += snprintf(json_out+off, len-(size_t)off, ",");
        off += snprintf(json_out+off, len-(size_t)off,
            "{\"host\":\"%s\",\"port\":%d,\"connected\":%s}",
            sc->host, sc->port, sc->connected ? "true" : "false");
    }
    snprintf(json_out+off, len-(size_t)off, "]}");
    pthread_mutex_unlock(&r->q_mu);
}
