#pragma once
/* log.h — потокобезопасный логгер ядра DataFlow OS.
 * Предоставляет уровни логирования, опциональный JSON-вывод и
 * per-thread correlation ID для трассировки HTTP-запросов. */
#include <stdio.h>
#include <time.h>
#include <pthread.h>

/* Уровни логирования по возрастанию важности (значения упорядочены для сравнения с min_level). */
typedef enum { LOG_DEBUG=0, LOG_INFO, LOG_WARN, LOG_ERROR } LogLevel;

/* Состояние логгера: целевой поток вывода, порог уровня и блокировка для потокобезопасной записи. */
typedef struct {
    FILE     *out;
    LogLevel  min_level;
    int       json_mode;
    pthread_mutex_t mu;
} Logger;

/* Глобальный логгер процесса, используемый макросами LOG_*. */
extern Logger g_log;

/* Per-thread correlation ID (UUID4: 36 chars + NUL).
 * Automatically included in JSON log output.
 * Set once per HTTP request by the HTTP layer. */
extern _Thread_local char g_correlation_id[37];

/* Инициализирует логгер: задаёт поток вывода, минимальный уровень и режим JSON. */
void log_init(Logger *l, FILE *out, LogLevel min_level, int json_mode);
/* Форматирует и пишет одну запись лога (с блокировкой); вызывается через макросы LOG_*. */
void log_write(Logger *l, LogLevel level, const char *file, int line,
               const char *fmt, ...) __attribute__((format(printf,5,6)));

/* Copy id (up to 36 chars) into g_correlation_id for current thread */
void log_set_correlation_id(const char *id);

/* Generate a new UUID4 and store in g_correlation_id for current thread */
void log_new_correlation_id(void);

/* Удобные макросы: автоматически подставляют файл/строку и пишут в g_log. */
#define LOG_DEBUG(...) log_write(&g_log, LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define LOG_INFO(...)  log_write(&g_log, LOG_INFO,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_WARN(...)  log_write(&g_log, LOG_WARN,  __FILE__, __LINE__, __VA_ARGS__)
#define LOG_ERROR(...) log_write(&g_log, LOG_ERROR, __FILE__, __LINE__, __VA_ARGS__)
