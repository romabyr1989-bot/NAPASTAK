#pragma once
#include "../core/arena.h"
#include "../core/json.h"
#include <stdint.h>
#include <stdbool.h>
#include <pthread.h>

#define MAX_STEPS    64
#define MAX_TRIGGERS  8

typedef enum { STEP_PENDING=0, STEP_RUNNING, STEP_SUCCESS, STEP_FAILED } StepStatus;
typedef enum { RUN_PENDING=0, RUN_RUNNING, RUN_SUCCESS, RUN_FAILED, RUN_CANCELLED } RunStatus;

/* Event-driven triggers — see docs/TRIGGERS.md.
 * TRIGGER_CRON kept first so that 0 == legacy default. */
typedef enum {
    TRIGGER_CRON         = 0,
    TRIGGER_WEBHOOK      = 1,
    TRIGGER_FILE_ARRIVAL = 2,
    TRIGGER_MANUAL       = 3,
    /* TRIGGER_KAFKA_MSG / TRIGGER_DB_CHANGE reserved for future steps */
} TriggerType;

typedef struct {
    TriggerType type;
    char        cron_expr[64];        /* TRIGGER_CRON */
    char        webhook_token[64];    /* TRIGGER_WEBHOOK — opaque secret in URL path */
    char        webhook_method[8];    /* TRIGGER_WEBHOOK — "POST" (default) | "GET" */
    char        watch_dir[256];       /* TRIGGER_FILE_ARRIVAL */
    char        file_pattern[64];     /* TRIGGER_FILE_ARRIVAL — glob ("*.csv") */
} PipelineTrigger;

typedef struct {
    char        id[64];
    char        name[128];
    char        connector_type[64];
    char        connector_config[1024];
    char        transform_sql[4096];
    char        target_table[128];
    int         deps[MAX_STEPS];
    int         ndeps;
    StepStatus  status;

    /* NEW: retry policy */
    int         max_retries;       /* default: 3 */
    int         retry_count;       /* current retry counter */
    int         retry_delay_sec;   /* base delay, doubles on each retry */
    int64_t     retry_after;       /* unix ts — when to retry */

    /* Python step (subprocess `python3 -c ...`).
     * If python_code is non-empty, this step runs Python:
     *   1. transform_sql (if set) is executed first → result fed as CSV
     *      to the Python subprocess on stdin
     *   2. user code operates on a `df` variable (pandas DataFrame)
     *   3. final `df` is read from stdout and ingested into target_table
     * Requires `pandas` available to the system `python3`. */
    char        python_code[8192];
    int         python_timeout_sec;   /* default: 300 */

    /* SCD2 (slowly-changing-dimension, type 2) step config. All optional —
     * empty strings mean "not an SCD2 step", preserving back-compat for
     * pipelines saved before these fields existed. Column names below refer
     * to columns of target_table. */
    char        scd2_business_key[256];      /* natural key col(s), comma-separated for composite */
    char        scd2_effective_from_col[128];/* timestamp col: when a version becomes valid */
    char        scd2_effective_to_col[128];  /* timestamp col: when a version is superseded */
    char        scd2_current_flag_col[128];  /* bool col: is this the current version */
    char        scd2_version_col[128];       /* monotonic version-number col */
    char        scd2_hash_col[128];          /* row-hash col used for change detection */
    char        scd2_compare_columns[512];   /* cols compared to detect change, comma-separated */
    char        scd2_transaction_time[128];  /* col ordered DESC to pick the current version (window ORDER BY); defaults to effective_from when empty */
    char        scd2_deleted_flag[128];      /* source col: truthy → close the key without a new version (soft delete) */
    char        scd2_ignored_columns[512];   /* cols excluded from change comparison when compare_columns is empty, comma-separated */

    /* Sink step (приёмник). When is_sink=true, this step does NOT load data
     * into a target_table — instead it READS its input rows from transform_sql
     * (a SELECT over existing tables) and WRITES them OUT to an external system
     * via the connector's write_batch. connector_type/connector_config pick and
     * configure the sink (csv/pg/s3/json_http/kafka). Empty → not a sink. */
    bool        is_sink;
    char        sink_entity[256];   /* destination: table / object key / topic / file path */
    char        sink_mode[16];      /* "append" (default) | "overwrite" */
} PipelineStep;

typedef struct {
    char          id[64];
    char          name[128];
    char          cron[64];      /* legacy: cron expression — kept for back-compat */
    PipelineStep  steps[MAX_STEPS];
    int           nsteps;
    bool          enabled;
    int64_t       last_run;
    int64_t       next_run;
    RunStatus     run_status;
    char          error_msg[512];

    /* NEW: alerting */
    char          webhook_url[512];  /* Slack/Telegram/custom webhook */
    char          webhook_on[32];    /* "failure", "success", "all" */
    int           alert_cooldown;    /* seconds between alerts */
    int64_t       last_alert_at;

    /* NEW: event-driven triggers (Step 4)
     * If empty AND `cron` is set, scheduler treats it as a single CRON
     * trigger — preserves behavior for pipelines created before this
     * field existed. */
    PipelineTrigger triggers[MAX_TRIGGERS];
    int             ntriggers;
} Pipeline;

/* Callback invoked in scheduler thread when a pipeline should run */
typedef void (*RunCallback)(Pipeline *p, void *userdata);

typedef struct {
    Pipeline       pipelines[256];
    int            npipelines;
    pthread_t      thread;
    pthread_mutex_t mu;
    volatile int   running;
    RunCallback    on_run;
    void          *on_run_data;
} Scheduler;

/* Cron */
int64_t cron_next(const char *expr, int64_t after_ts);  /* returns unix ts */

/* Pipeline JSON de/serialization */
int  pipeline_from_json(Pipeline *p, const char *json);
char *pipeline_to_json(const Pipeline *p, Arena *a);

/* Scheduler */
Scheduler *scheduler_create(RunCallback cb, void *userdata);
void       scheduler_add(Scheduler *s, Pipeline *p);
void       scheduler_remove(Scheduler *s, const char *id);
Pipeline  *scheduler_find(Scheduler *s, const char *id);
void       scheduler_start(Scheduler *s);
void       scheduler_stop(Scheduler *s);

/* Step 4: event-driven trigger helpers.
 * scheduler_find_by_webhook_token: O(npipelines × ntriggers) — scans triggers.
 * scheduler_run_pipeline_now:      synchronously fires the run-callback in
 *                                  the caller's thread (used by webhook /
 *                                  file-watcher). Safe to call from any
 *                                  thread; the run-callback is responsible
 *                                  for offloading actual work. */
Pipeline  *scheduler_find_by_webhook_token(Scheduler *s, const char *token);
void       scheduler_run_pipeline_now(Scheduler *s, Pipeline *p);
