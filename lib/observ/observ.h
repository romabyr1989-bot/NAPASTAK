#pragma once
/*
 * observ.h — подсистема наблюдаемости NAPASTAK.
 * Объединяет три механизма: сбор runtime-метрик (кольцевые буферы),
 * проверки качества данных (quality checks) и обнаружение аномалий
 * по Z-оценке на скользящем окне.
 */
#include "../core/arena.h"
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

/* ── Metrics ── */
#define METRIC_RING_SIZE 3600  /* 1 hour at 1/sec */

/* Потокобезопасный кольцевой буфер семплов метрики (значение + метка времени). */
typedef struct {
    double   values[METRIC_RING_SIZE];
    int64_t  timestamps[METRIC_RING_SIZE];
    int      head, count;            /* head — индекс записи; count — заполненность */
    pthread_mutex_t mu;
} MetricRing;

/* Сводный набор всех метрик процесса: счётчики и кольцевые буферы. */
typedef struct {
    /* Оригинальные метрики */
    MetricRing rows_ingested;
    MetricRing rows_failed;
    MetricRing pipeline_latency_ms;
    MetricRing query_latency_ms;
    int64_t    total_rows;
    int64_t    total_pipelines_run;
    int64_t    total_queries;
    int64_t    uptime_start;

    /* HTTP метрики */
    int64_t    http_requests_total;
    int64_t    http_errors_4xx;
    int64_t    http_errors_5xx;
    MetricRing http_request_duration_ms;

    /* Storage метрики */
    int64_t    wal_bytes_written;
    int64_t    wal_bytes_compressed;
    int64_t    tables_count;

    /* Транзакции (заполняются Шагом 2) */
    int64_t    txn_begin_total;
    int64_t    txn_commit_total;
    int64_t    txn_rollback_total;
    int64_t    txn_timeout_total;
    int64_t    txn_active;
} Metrics;

void metrics_init(Metrics *m);                  /* обнулить счётчики и буферы */
void metrics_push(MetricRing *r, double val);   /* добавить семпл в кольцо (потокобезопасно) */
double metrics_avg(MetricRing *r, int last_n);  /* average over last n samples */
char  *metrics_to_json(Metrics *m, Arena *a);   /* сериализовать снимок метрик в JSON */

/* ── Quality checks ── */
/* Виды проверок качества: не-null, уникальность, диапазон, regex, кастом. */
typedef enum { QC_NOT_NULL, QC_UNIQUE, QC_RANGE, QC_REGEX, QC_CUSTOM } QCType;

/* Описание одной проверки качества над колонкой и её результаты. */
typedef struct {
    QCType       type;
    const char  *column;
    double       range_min, range_max;  /* for QC_RANGE */
    const char  *regex;                 /* for QC_REGEX */
    int          pass_count, fail_count;
} QualityCheck;

/* Набор проверок качества, применяемых к одной таблице. */
typedef struct {
    QualityCheck *checks;
    int           nchecks;
} QualityRule;

/* Выполнить все проверки правила; формирует JSON-отчёт в report_out. */
int qc_run(QualityRule *rule, const char *table_name,
           int64_t row_count, Arena *a, char **report_out);

/* ── Anomaly detection (Z-score on rolling window) ── */
/* Детектор аномалий: хранит скользящее окно значений для расчёта Z-оценки. */
typedef struct {
    double   window[128];
    int      head, n;
    double   threshold;          /* default: 3.0 sigma */
} AnomalyDetector;

void   anomaly_init(AnomalyDetector *d, double threshold);  /* задать порог в сигмах */
bool   anomaly_check(AnomalyDetector *d, double val);  /* true = anomaly; добавляет val в окно */
double anomaly_zscore(AnomalyDetector *d, double val); /* Z-оценка val без записи в окно */
