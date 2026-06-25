/* Пул потоков с ограниченной очередью задач и поддержкой graceful-shutdown.
 * Один мьютекс + два condvar управляют тремя состояниями:
 *   cond_work — разбудить воркеров при появлении задачи
 *   cond_idle — разбудить tp_wait/tp_submit при опустении очереди или освобождении места */
#include "threadpool.h"
#include <stdlib.h>
#include <stdio.h>

static void *worker(void *arg) {
    ThreadPool *p = arg;
    for (;;) {
        pthread_mutex_lock(&p->mu);
        /* Ждём задачу или сигнал остановки. */
        while (!p->head && !p->shutdown)
            pthread_cond_wait(&p->cond_work, &p->mu);
        /* Останавливаемся только после полного дренажа очереди — задачи не теряются. */
        if (p->shutdown && !p->head) { pthread_mutex_unlock(&p->mu); break; }
        /* Забираем задачу с головы FIFO-очереди. */
        Task *t = p->head;
        p->head = t->next;
        if (!p->head) p->tail = NULL;
        p->queue_size--; p->active++;
        /* Сигналим: освободилось место в очереди (если tp_submit ждал). */
        pthread_cond_signal(&p->cond_idle);
        pthread_mutex_unlock(&p->mu);

        t->fn(t->arg);
        free(t);

        pthread_mutex_lock(&p->mu);
        p->active--;
        /* Если все воркеры простаивают и очередь пуста — будим tp_wait. */
        if (!p->active && !p->head) pthread_cond_broadcast(&p->cond_idle);
        pthread_mutex_unlock(&p->mu);
    }
    return NULL;
}

/* Создаёт пул из n воркеров с очередью не более qmax задач. */
ThreadPool *tp_create(int n, int qmax) {
    ThreadPool *p = calloc(1, sizeof(ThreadPool));
    p->nthreads = n; p->queue_max = qmax;
    pthread_mutex_init(&p->mu, NULL);
    pthread_cond_init(&p->cond_work, NULL);
    pthread_cond_init(&p->cond_idle, NULL);
    p->threads = malloc(n * sizeof(pthread_t));
    for (int i = 0; i < n; i++) pthread_create(&p->threads[i], NULL, worker, p);
    return p;
}

/* tp_submit блокируется при заполненной очереди — встроенное backpressure.
 * Это предотвращает неограниченный рост памяти при перегрузке. */
void tp_submit(ThreadPool *p, task_fn fn, void *arg) {
    Task *t = malloc(sizeof(Task));
    t->fn = fn; t->arg = arg; t->next = NULL;
    pthread_mutex_lock(&p->mu);
    while (p->queue_size >= p->queue_max && !p->shutdown)
        pthread_cond_wait(&p->cond_idle, &p->mu);
    if (p->tail) p->tail->next = t; else p->head = t;
    p->tail = t; p->queue_size++;
    pthread_cond_signal(&p->cond_work);
    pthread_mutex_unlock(&p->mu);
}

/* Ждём завершения всех активных задач и опустения очереди. */
void tp_wait(ThreadPool *p) {
    pthread_mutex_lock(&p->mu);
    while (p->active || p->head) pthread_cond_wait(&p->cond_idle, &p->mu);
    pthread_mutex_unlock(&p->mu);
}

/* Плановое завершение: устанавливаем флаг, будим все потоки,
 * join ждёт их выхода из цикла после обработки оставшихся задач. */
void tp_destroy(ThreadPool *p) {
    pthread_mutex_lock(&p->mu);
    p->shutdown = 1;
    pthread_cond_broadcast(&p->cond_work);
    pthread_mutex_unlock(&p->mu);
    for (int i = 0; i < p->nthreads; i++) pthread_join(p->threads[i], NULL);
    free(p->threads);
    pthread_mutex_destroy(&p->mu);
    pthread_cond_destroy(&p->cond_work);
    pthread_cond_destroy(&p->cond_idle);
    free(p);
}

/* active + queue_size: оба компонента определяют «загруженность» пула. */
int tp_active(ThreadPool *p) {
    pthread_mutex_lock(&p->mu); int a = p->active + p->queue_size; pthread_mutex_unlock(&p->mu);
    return a;
}
