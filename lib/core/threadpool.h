/*
 * threadpool.h — пул потоков с очередью задач (FIFO).
 * Фиксированное число рабочих потоков разбирают задачи из общей очереди;
 * синхронизация через мьютекс и две условные переменные.
 */
#pragma once
#include <stddef.h>
#include <pthread.h>

/* Сигнатура задачи: функция и её непрозрачный аргумент. */
typedef void (*task_fn)(void *arg);

/* Узел односвязного списка — элемент очереди задач. */
typedef struct Task { task_fn fn; void *arg; struct Task *next; } Task;

typedef struct {
    pthread_t       *threads;          /* массив рабочих потоков, nthreads штук */
    int              nthreads;
    Task            *head, *tail;      /* голова/хвост FIFO-очереди задач */
    int              queue_size, queue_max; /* текущая длина и верхний предел очереди */
    pthread_mutex_t  mu;               /* защищает всё состояние ниже */
    pthread_cond_t   cond_work;        /* сигнал воркерам: появилась задача/shutdown */
    pthread_cond_t   cond_idle;        /* сигнал ожидающим: очередь пуста и нет активных */
    int              shutdown;         /* флаг остановки: воркеры завершаются */
    int              active;           /* число задач, выполняемых прямо сейчас */
} ThreadPool;

/* Создаёт пул с nthreads воркерами и очередью не более queue_max задач. */
ThreadPool *tp_create(int nthreads, int queue_max);
/* Ставит задачу в очередь; блокируется, пока очередь заполнена. */
void        tp_submit(ThreadPool *p, task_fn fn, void *arg); /* blocks if full */
/* Блокирует вызывающего, пока все задачи не выполнены (очередь пуста, active == 0). */
void        tp_wait(ThreadPool *p);
/* Останавливает воркеры, дожидается их и освобождает ресурсы пула. */
void        tp_destroy(ThreadPool *p);
/* Возвращает число задач, выполняемых в данный момент. */
int         tp_active(ThreadPool *p);
