/* Планировщик пайплайнов: разбирает cron-выражения, хранит список пайплайнов
 * и в фоновом потоке каждые 30 секунд проверяет, не пора ли запустить очередной. */
#include "scheduler.h"
#include "../core/log.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdio.h>

#ifdef __APPLE__
/* timegm — POSIX-расширение, которого нет в macOS без _GNU_SOURCE.
 * Реализуем сами через формулу пролептического григорианского календаря. */
static time_t timegm(struct tm *tm) {
    int y = tm->tm_year + 1900, m = tm->tm_mon + 1;
    if (m <= 2) { m += 12; y--; }
    long d = 365L*y + y/4 - y/100 + y/400 + (153*m - 457)/5 + tm->tm_mday - 719469L;
    return (time_t)(d * 86400L + tm->tm_hour * 3600L + tm->tm_min * 60L + tm->tm_sec);
}
#endif

/* ── Парсер cron-выражений ── */

/* CronField хранит развёрнутый список допустимых значений для одного поля (минуты, часы и т.д.).
 * Это упрощает проверку «подходит ли текущий момент» до одного вызова field_has(). */
typedef struct { int vals[60]; int n; bool star; } CronField;

static void parse_field(const char *s, CronField *f, int lo, int hi) {
    f->n = 0;
    if (strcmp(s,"*")==0) { f->star=true; for(int i=lo;i<=hi;i++) f->vals[f->n++]=i; return; }
    f->star = false;
    /* Разбираем запятые, диапазоны (a-b) и шаги (step-синтаксис start/n). */
    char buf[64]; strncpy(buf,s,sizeof(buf)-1); buf[sizeof(buf)-1]='\0';
    char *tok=strtok(buf,",");
    while(tok) {
        char *slash=strchr(tok,'/');
        char *dash=strchr(tok,'-');
        if(slash) {
            int start=atoi(tok), step=atoi(slash+1);
            if(step<1)step=1;
            for(int v=start;v<=hi;v+=step) f->vals[f->n++]=v;
        } else if(dash) {
            int a=atoi(tok), b=atoi(dash+1);
            for(int v=a;v<=b;v++) f->vals[f->n++]=v;
        } else {
            int v=atoi(tok); if(v>=lo&&v<=hi) f->vals[f->n++]=v;
        }
        tok=strtok(NULL,",");
    }
}

static bool field_has(CronField *f, int v) {
    for(int i=0;i<f->n;i++) if(f->vals[i]==v) return true;
    return false;
}

/* Находим следующий момент запуска после after (unix ts).
 * Перебираем минуты вперёд — максимум 527040 итераций (1 год). */
int64_t cron_next(const char *expr, int64_t after) {
    /* Алиасы @hourly/@daily и т.д. разворачиваем в стандартный формат "мин час д м дн_нед". */
    if(strcmp(expr,"@hourly")==0)   expr="0 * * * *";
    if(strcmp(expr,"@daily")==0)    expr="0 0 * * *";
    if(strcmp(expr,"@weekly")==0)   expr="0 0 * * 0";
    if(strcmp(expr,"@monthly")==0)  expr="0 0 1 * *";
    if(strcmp(expr,"@yearly")==0)   expr="0 0 1 1 *";
    if(strcmp(expr,"@reboot")==0)   return after; /* triggers once; scheduler pins next_run=INT64_MAX after */

    char fields[5][64];
    if(sscanf(expr,"%63s %63s %63s %63s %63s",fields[0],fields[1],fields[2],fields[3],fields[4])!=5)
        return -1;

    CronField min,hr,dom,mon,dow;
    parse_field(fields[0],&min,0,59);
    parse_field(fields[1],&hr,0,23);
    parse_field(fields[2],&dom,1,31);
    parse_field(fields[3],&mon,1,12);
    parse_field(fields[4],&dow,0,6);

    time_t t = (time_t)(after + 60); /* start from next minute */
    struct tm tm; gmtime_r(&t,&tm); tm.tm_sec=0;

    for(int iter=0; iter<527040; iter++) { /* max 1 year of minutes */
        if(field_has(&mon,tm.tm_mon+1) &&
           field_has(&dom,tm.tm_mday) &&
           field_has(&dow,tm.tm_wday) &&
           field_has(&hr,tm.tm_hour) &&
           field_has(&min,tm.tm_min)) {
            return (int64_t)timegm(&tm);
        }
        tm.tm_min++;
        if(tm.tm_min>=60){tm.tm_min=0;tm.tm_hour++;}
        if(tm.tm_hour>=24){tm.tm_hour=0;tm.tm_mday++;}
        /* simple overflow handling */
        if(tm.tm_mday>28){time_t t2=timegm(&tm);gmtime_r(&t2,&tm);tm.tm_sec=0;}
    }
    return -1;
}

/* ── Топологическая сортировка DAG зависимостей шагов (алгоритм Кана) ──
 * Возвращает 0 и порядок выполнения в order[], или -1 при обнаружении цикла. */
static int topo_sort(Pipeline *p, int *order) {
    int indegree[MAX_STEPS] = {0};
    for(int i=0;i<p->nsteps;i++)
        for(int j=0;j<p->steps[i].ndeps;j++)
            indegree[p->steps[i].deps[j]]++;
    int queue[MAX_STEPS], qh=0, qt=0, n=0;
    for(int i=0;i<p->nsteps;i++) if(!indegree[i]) queue[qt++]=i;
    while(qh<qt) {
        int u=queue[qh++]; order[n++]=u;
        for(int i=0;i<p->nsteps;i++)
            for(int j=0;j<p->steps[i].ndeps;j++)
                if(p->steps[i].deps[j]==u && --indegree[i]==0) queue[qt++]=i;
    }
    return n == p->nsteps ? 0 : -1; /* -1 = cycle */
}

/* ── Сериализация/десериализация пайплайнов в JSON ── */

/* Рекурсивная запись JVal в JBuf: нужна для сохранения connector_config,
 * который может прийти как JSON-объект в теле запроса, а храниться должен как строка. */
static void jval_write(JBuf *jb, JVal *v) {
    if (!v) { jb_null(jb); return; }
    switch (v->type) {
        case JV_NULL:    jb_null(jb); break;
        case JV_BOOL:    jb_bool(jb, v->b); break;
        case JV_NUMBER:  jb_double(jb, v->n); break;
        case JV_STRING:  jb_strn(jb, v->s, v->len); break;
        case JV_ARRAY:
            jb_arr_begin(jb);
            for (size_t i = 0; i < v->nitems; i++) jval_write(jb, v->items[i]);
            jb_arr_end(jb);
            break;
        case JV_OBJECT:
            jb_obj_begin(jb);
            for (size_t i = 0; i < v->nkeys; i++) {
                jb_key(jb, v->keys[i]);
                jval_write(jb, v->vals[i]);
            }
            jb_obj_end(jb);
            break;
        default: jb_null(jb); break;
    }
}

static const char *jval_to_json(JVal *v, Arena *a) {
    if (!v) return "null";
    JBuf jb; jb_init(&jb, a, 256);
    jval_write(&jb, v);
    return jb_done(&jb);
}

/* Копируем JVal-поле в char[]-буфер фиксированного размера.
 * Если значение — объект/массив, сериализуем в JSON-строку, чтобы connector_config
 * можно было передавать в create() коннектора как обычную C-строку. */
static void copy_jval_str(JVal *v, char *dst, size_t dstsz, Arena *a) {
    if (!v) { dst[0] = '\0'; return; }
    if (v->type == JV_STRING) {
        size_t n = v->len < dstsz-1 ? v->len : dstsz-1;
        memcpy(dst, v->s, n); dst[n] = '\0';
    } else if (v->type == JV_OBJECT || v->type == JV_ARRAY) {
        const char *s = jval_to_json(v, a);
        strncpy(dst, s, dstsz-1); dst[dstsz-1] = '\0';
    } else {
        const char *s = json_str(v, "");
        strncpy(dst, s, dstsz-1); dst[dstsz-1] = '\0';
    }
}

/* Map textual trigger type to enum. Unknown values default to MANUAL
 * (no automatic firing) — fail-closed for safety. */
static TriggerType trigger_type_from_str(const char *s) {
    if (!s || !*s)                      return TRIGGER_CRON;
    if (strcmp(s, "cron")          == 0) return TRIGGER_CRON;
    if (strcmp(s, "webhook")       == 0) return TRIGGER_WEBHOOK;
    if (strcmp(s, "file_arrival")  == 0) return TRIGGER_FILE_ARRIVAL;
    if (strcmp(s, "manual")        == 0) return TRIGGER_MANUAL;
    return TRIGGER_MANUAL;
}

static const char *trigger_type_to_str(TriggerType t) {
    switch (t) {
        case TRIGGER_CRON:         return "cron";
        case TRIGGER_WEBHOOK:      return "webhook";
        case TRIGGER_FILE_ARRIVAL: return "file_arrival";
        case TRIGGER_MANUAL:       return "manual";
        default:                   return "manual";
    }
}

int pipeline_from_json(Pipeline *p, const char *json) {
    Arena *a = arena_create(32768);
    JVal *root = json_parse(a, json, strlen(json));
    if (!root || root->type != JV_OBJECT) { arena_destroy(a); return -1; }

    strncpy(p->id,   json_str(json_get(root,"id"),""),  sizeof(p->id)-1);
    strncpy(p->name, json_str(json_get(root,"name"),""),sizeof(p->name)-1);
    strncpy(p->cron, json_str(json_get(root,"cron"),""),sizeof(p->cron)-1);
    p->enabled = json_bool(json_get(root,"enabled"),true);
    strncpy(p->webhook_url, json_str(json_get(root,"webhook_url"),""), sizeof(p->webhook_url)-1);
    strncpy(p->webhook_on,  json_str(json_get(root,"webhook_on"),"failure"), sizeof(p->webhook_on)-1);
    p->last_run_failed = (int)json_int(json_get(root,"last_run_failed"), 0);

    /* Step 4: parse triggers[] if present.
     * Back-compat: when absent, synthesize a single TRIGGER_CRON if `cron`
     * is set; otherwise leave ntriggers=0 (manual-only). */
    p->ntriggers = 0;
    JVal *trigs = json_get(root, "triggers");
    if (trigs && trigs->type == JV_ARRAY) {
        size_t n = trigs->nitems > MAX_TRIGGERS ? MAX_TRIGGERS : trigs->nitems;
        for (size_t i = 0; i < n; i++) {
            JVal *t = trigs->items[i];
            if (!t || t->type != JV_OBJECT) continue;
            PipelineTrigger *pt = &p->triggers[p->ntriggers];
            memset(pt, 0, sizeof(*pt));
            pt->type = trigger_type_from_str(json_str(json_get(t, "type"), "cron"));
            strncpy(pt->cron_expr,
                    json_str(json_get(t, "cron_expr"), ""), sizeof(pt->cron_expr) - 1);
            strncpy(pt->webhook_token,
                    json_str(json_get(t, "webhook_token"), ""), sizeof(pt->webhook_token) - 1);
            strncpy(pt->webhook_method,
                    json_str(json_get(t, "webhook_method"), "POST"), sizeof(pt->webhook_method) - 1);
            strncpy(pt->watch_dir,
                    json_str(json_get(t, "watch_dir"), ""), sizeof(pt->watch_dir) - 1);
            strncpy(pt->file_pattern,
                    json_str(json_get(t, "file_pattern"), "*"), sizeof(pt->file_pattern) - 1);
            p->ntriggers++;
        }
    }
    if (p->ntriggers == 0 && p->cron[0]) {
        PipelineTrigger *pt = &p->triggers[0];
        memset(pt, 0, sizeof(*pt));
        pt->type = TRIGGER_CRON;
        strncpy(pt->cron_expr, p->cron, sizeof(pt->cron_expr) - 1);
        p->ntriggers = 1;
    }

    JVal *steps = json_get(root,"steps");
    if(steps && steps->type==JV_ARRAY) {
        p->nsteps=(int)steps->nitems;
        if(p->nsteps>MAX_STEPS) p->nsteps=MAX_STEPS;
        for(int i=0;i<p->nsteps;i++){
            JVal *s=steps->items[i]; PipelineStep *st=&p->steps[i];
            memset(st,0,sizeof(*st));
            strncpy(st->id,  json_str(json_get(s,"id"),""),  sizeof(st->id)-1);
            strncpy(st->name,json_str(json_get(s,"name"),""),sizeof(st->name)-1);
            strncpy(st->connector_type, json_str(json_get(s,"connector_type"),""), sizeof(st->connector_type)-1);
            copy_jval_str(json_get(s,"connector_config"), st->connector_config, sizeof(st->connector_config), a);
            strncpy(st->transform_sql,   json_str(json_get(s,"transform_sql"),""),   sizeof(st->transform_sql)-1);
            strncpy(st->target_table,    json_str(json_get(s,"target_table"),""),    sizeof(st->target_table)-1);
            /* SQL templates (optional) */
            strncpy(st->sql_template,    json_str(json_get(s,"sql_template"),""),    sizeof(st->sql_template)-1);
            copy_jval_str(json_get(s,"sql_params"), st->sql_params, sizeof(st->sql_params), a);
            /* Python step (optional) */
            strncpy(st->python_code,
                    json_str(json_get(s, "python_code"), ""),
                    sizeof(st->python_code) - 1);
            st->python_timeout_sec = (int)json_int(json_get(s, "python_timeout_sec"), 300);
            strncpy(st->python_file,
                    json_str(json_get(s, "python_file"), ""),
                    sizeof(st->python_file) - 1);
            strncpy(st->python_context_dir,
                    json_str(json_get(s, "python_context_dir"), ""),
                    sizeof(st->python_context_dir) - 1);
            /* Scala step (optional) */
            strncpy(st->scala_code,
                    json_str(json_get(s, "scala_code"), ""),
                    sizeof(st->scala_code) - 1);
            st->scala_timeout_sec = (int)json_int(json_get(s, "scala_timeout_sec"), 600);
            /* SCD2 step (optional) — missing fields default to "" (back-compat) */
            strncpy(st->scd2_business_key,       json_str(json_get(s,"scd2_business_key"),""),       sizeof(st->scd2_business_key)-1);
            strncpy(st->scd2_effective_from_col, json_str(json_get(s,"scd2_effective_from_col"),""), sizeof(st->scd2_effective_from_col)-1);
            strncpy(st->scd2_effective_to_col,   json_str(json_get(s,"scd2_effective_to_col"),""),   sizeof(st->scd2_effective_to_col)-1);
            strncpy(st->scd2_current_flag_col,   json_str(json_get(s,"scd2_current_flag_col"),""),   sizeof(st->scd2_current_flag_col)-1);
            strncpy(st->scd2_version_col,        json_str(json_get(s,"scd2_version_col"),""),        sizeof(st->scd2_version_col)-1);
            strncpy(st->scd2_hash_col,           json_str(json_get(s,"scd2_hash_col"),""),           sizeof(st->scd2_hash_col)-1);
            strncpy(st->scd2_compare_columns,    json_str(json_get(s,"scd2_compare_columns"),""),    sizeof(st->scd2_compare_columns)-1);
            strncpy(st->scd2_transaction_time,   json_str(json_get(s,"scd2_transaction_time"),""),   sizeof(st->scd2_transaction_time)-1);
            strncpy(st->scd2_deleted_flag,       json_str(json_get(s,"scd2_deleted_flag"),""),       sizeof(st->scd2_deleted_flag)-1);
            strncpy(st->scd2_ignored_columns,    json_str(json_get(s,"scd2_ignored_columns"),""),    sizeof(st->scd2_ignored_columns)-1);
            /* Sink step (optional) — missing fields default to off (back-compat) */
            st->is_sink = json_bool(json_get(s,"is_sink"), false);
            strncpy(st->sink_entity, json_str(json_get(s,"sink_entity"),""), sizeof(st->sink_entity)-1);
            strncpy(st->sink_mode,   json_str(json_get(s,"sink_mode"),"append"), sizeof(st->sink_mode)-1);
            /* Match step (optional) */
            strncpy(st->match_rules,       json_str(json_get(s,"match_rules"),""),       sizeof(st->match_rules)-1);
            strncpy(st->match_input_table, json_str(json_get(s,"match_input_table"),""), sizeof(st->match_input_table)-1);
            st->max_retries = (int)json_int(json_get(s,"max_retries"), 3);
            st->retry_delay_sec = (int)json_int(json_get(s,"retry_delay_sec"), 30);
            st->retry_count = 0;
            st->retry_after = 0;
            JVal *deps=json_get(s,"deps");
            if(deps&&deps->type==JV_ARRAY)
                for(int j=0;j<(int)deps->nitems&&j<MAX_STEPS;j++)
                    st->deps[st->ndeps++]=(int)json_int(deps->items[j],0);
        }
    }
    arena_destroy(a);
    return 0;
}

char *pipeline_to_json(const Pipeline *p, Arena *a) {
    JBuf jb; jb_init(&jb,a,4096);
    jb_obj_begin(&jb);
    jb_key(&jb,"id");      jb_str(&jb,p->id);
    jb_key(&jb,"name");    jb_str(&jb,p->name);
    jb_key(&jb,"cron");    jb_str(&jb,p->cron);
    jb_key(&jb,"enabled"); jb_bool(&jb,p->enabled);
    jb_key(&jb,"webhook_url");   jb_str(&jb,p->webhook_url);
    jb_key(&jb,"webhook_on");    jb_str(&jb,p->webhook_on);
    jb_key(&jb,"last_run_failed"); jb_int(&jb,p->last_run_failed);
    jb_key(&jb,"last_run");jb_int(&jb,p->last_run);
    jb_key(&jb,"next_run");jb_int(&jb,p->next_run);
    jb_key(&jb,"status");  jb_int(&jb,(int)p->run_status);
    jb_key(&jb,"steps"); jb_arr_begin(&jb);
    for(int i=0;i<p->nsteps;i++){
        const PipelineStep *st=&p->steps[i];
        jb_obj_begin(&jb);
        jb_key(&jb,"id");               jb_str(&jb,st->id);
        jb_key(&jb,"name");             jb_str(&jb,st->name);
        jb_key(&jb,"connector_type");   jb_str(&jb,st->connector_type);
        jb_key(&jb,"connector_config"); jb_str(&jb,st->connector_config);
        jb_key(&jb,"transform_sql");    jb_str(&jb,st->transform_sql);
        jb_key(&jb,"target_table");     jb_str(&jb,st->target_table);
        if (st->sql_template[0])       { jb_key(&jb,"sql_template");       jb_str(&jb, st->sql_template); }
        if (st->sql_params[0])         { jb_key(&jb,"sql_params");         jb_str(&jb, st->sql_params); }
        if (st->python_code[0]) {
            jb_key(&jb,"python_code");        jb_str(&jb, st->python_code);
            jb_key(&jb,"python_timeout_sec"); jb_int(&jb, st->python_timeout_sec);
        }
        if (st->python_file[0])        { jb_key(&jb,"python_file");        jb_str(&jb, st->python_file); }
        if (st->python_context_dir[0]) { jb_key(&jb,"python_context_dir"); jb_str(&jb, st->python_context_dir); }
        if (st->scala_code[0]) {
            jb_key(&jb,"scala_code");        jb_str(&jb, st->scala_code);
            jb_key(&jb,"scala_timeout_sec"); jb_int(&jb, st->scala_timeout_sec);
        }
        /* SCD2 fields — emitted only when set, so pre-SCD2 pipelines round-trip unchanged */
        if (st->scd2_business_key[0])       { jb_key(&jb,"scd2_business_key");       jb_str(&jb, st->scd2_business_key); }
        if (st->scd2_effective_from_col[0]) { jb_key(&jb,"scd2_effective_from_col"); jb_str(&jb, st->scd2_effective_from_col); }
        if (st->scd2_effective_to_col[0])   { jb_key(&jb,"scd2_effective_to_col");   jb_str(&jb, st->scd2_effective_to_col); }
        if (st->scd2_current_flag_col[0])   { jb_key(&jb,"scd2_current_flag_col");   jb_str(&jb, st->scd2_current_flag_col); }
        if (st->scd2_version_col[0])        { jb_key(&jb,"scd2_version_col");        jb_str(&jb, st->scd2_version_col); }
        if (st->scd2_hash_col[0])           { jb_key(&jb,"scd2_hash_col");           jb_str(&jb, st->scd2_hash_col); }
        if (st->scd2_compare_columns[0])    { jb_key(&jb,"scd2_compare_columns");    jb_str(&jb, st->scd2_compare_columns); }
        if (st->scd2_transaction_time[0])   { jb_key(&jb,"scd2_transaction_time");   jb_str(&jb, st->scd2_transaction_time); }
        if (st->scd2_deleted_flag[0])       { jb_key(&jb,"scd2_deleted_flag");       jb_str(&jb, st->scd2_deleted_flag); }
        if (st->scd2_ignored_columns[0])    { jb_key(&jb,"scd2_ignored_columns");    jb_str(&jb, st->scd2_ignored_columns); }
        /* Sink fields — emitted only for sink steps, so non-sink pipelines round-trip unchanged */
        if (st->is_sink) {
            jb_key(&jb,"is_sink");     jb_bool(&jb, st->is_sink);
            jb_key(&jb,"sink_entity"); jb_str(&jb, st->sink_entity);
            jb_key(&jb,"sink_mode");   jb_str(&jb, st->sink_mode[0] ? st->sink_mode : "append");
        }
        /* Match fields — emitted only when set, so non-match pipelines round-trip unchanged */
        if (st->match_rules[0])       { jb_key(&jb,"match_rules");       jb_str(&jb, st->match_rules); }
        if (st->match_input_table[0]) { jb_key(&jb,"match_input_table"); jb_str(&jb, st->match_input_table); }
        jb_key(&jb,"max_retries");      jb_int(&jb,st->max_retries);
        jb_key(&jb,"retry_delay_sec");  jb_int(&jb,st->retry_delay_sec);
        jb_key(&jb,"status");           jb_int(&jb,(int)st->status);
        jb_key(&jb,"deps"); jb_arr_begin(&jb);
        for(int j=0;j<st->ndeps;j++) jb_int(&jb,st->deps[j]);
        jb_arr_end(&jb);
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);

    /* Step 4: emit triggers[] */
    jb_key(&jb, "triggers"); jb_arr_begin(&jb);
    for (int i = 0; i < p->ntriggers; i++) {
        const PipelineTrigger *t = &p->triggers[i];
        jb_obj_begin(&jb);
        jb_key(&jb, "type"); jb_str(&jb, trigger_type_to_str(t->type));
        if (t->type == TRIGGER_CRON) {
            jb_key(&jb, "cron_expr"); jb_str(&jb, t->cron_expr);
        } else if (t->type == TRIGGER_WEBHOOK) {
            jb_key(&jb, "webhook_token");  jb_str(&jb, t->webhook_token);
            jb_key(&jb, "webhook_method"); jb_str(&jb, t->webhook_method[0] ? t->webhook_method : "POST");
        } else if (t->type == TRIGGER_FILE_ARRIVAL) {
            jb_key(&jb, "watch_dir");    jb_str(&jb, t->watch_dir);
            jb_key(&jb, "file_pattern"); jb_str(&jb, t->file_pattern);
        }
        jb_obj_end(&jb);
    }
    jb_arr_end(&jb);

    jb_obj_end(&jb);
    return (char*)jb_done(&jb);
}

/* ── Поток планировщика ──
 * Step 4 update: cron-trigger lookup walks triggers[] and uses the first
 * TRIGGER_CRON entry. Pipelines without any cron trigger sit idle here —
 * they fire from webhook/file-watcher/manual paths instead. */

/* Returns the cron expression a pipeline should currently follow, or NULL
 * if none. Prefers triggers[].cron_expr over the legacy `cron` field. */
static const char *pipeline_active_cron(const Pipeline *p) {
    for (int i = 0; i < p->ntriggers; i++) {
        if (p->triggers[i].type == TRIGGER_CRON && p->triggers[i].cron_expr[0])
            return p->triggers[i].cron_expr;
    }
    return p->cron[0] ? p->cron : NULL;
}

static void *sched_loop(void *arg) {
    Scheduler *s = arg;
    LOG_INFO("scheduler started");
    while(s->running) {
        /* Проверяем расписание каждые 30 секунд — достаточно для minutely-cron. */
        sleep(30);
        if(!s->running) break;
        int64_t now = (int64_t)time(NULL);
        pthread_mutex_lock(&s->mu);
        for(int i=0;i<s->npipelines;i++){
            Pipeline *p=&s->pipelines[i];
            if(!p->enabled) continue;
            const char *cron = pipeline_active_cron(p);
            if (!cron) continue;   /* manual / webhook / file-arrival only */
            if(p->next_run==0)
                p->next_run=cron_next(cron,now);
            if(p->next_run>0 && now>=p->next_run && p->run_status!=RUN_RUNNING){
                p->last_run=now;
                /* @reboot запускается один раз при старте; INT64_MAX исключает повторный запуск. */
                if(strcmp(cron,"@reboot")==0)
                    p->next_run=INT64_MAX;
                else
                    p->next_run=cron_next(cron,now);
                p->run_status=RUN_RUNNING;
                LOG_INFO("scheduler triggering pipeline %s (cron)", p->id);
                if(s->on_run) s->on_run(p, s->on_run_data);
            }
        }
        pthread_mutex_unlock(&s->mu);
    }
    return NULL;
}

Scheduler *scheduler_create(RunCallback cb, void *ud) {
    Scheduler *s=calloc(1,sizeof(Scheduler));
    pthread_mutex_init(&s->mu,NULL);
    s->on_run=cb; s->on_run_data=ud;
    return s;
}

void scheduler_add(Scheduler *s, Pipeline *p) {
    pthread_mutex_lock(&s->mu);
    if(s->npipelines<256) s->pipelines[s->npipelines++]=*p;
    pthread_mutex_unlock(&s->mu);
}

void scheduler_remove(Scheduler *s, const char *id) {
    pthread_mutex_lock(&s->mu);
    for(int i=0;i<s->npipelines;i++)
        if(strcmp(s->pipelines[i].id,id)==0){
            s->pipelines[i]=s->pipelines[--s->npipelines]; break;
        }
    pthread_mutex_unlock(&s->mu);
}

Pipeline *scheduler_find(Scheduler *s, const char *id) {
    pthread_mutex_lock(&s->mu);
    Pipeline *found = NULL;
    for(int i=0;i<s->npipelines;i++)
        if(strcmp(s->pipelines[i].id,id)==0){ found=&s->pipelines[i]; break; }
    pthread_mutex_unlock(&s->mu);
    return found;
}

void scheduler_start(Scheduler *s) { s->running=1; pthread_create(&s->thread,NULL,sched_loop,s); }
void scheduler_stop(Scheduler *s)  { s->running=0; pthread_join(s->thread,NULL); }

/* Step 4: webhook-trigger lookup. O(N×T) — fine for typical pipeline counts. */
Pipeline *scheduler_find_by_webhook_token(Scheduler *s, const char *token) {
    if (!token || !*token) return NULL;
    pthread_mutex_lock(&s->mu);
    Pipeline *found = NULL;
    for (int i = 0; i < s->npipelines && !found; i++) {
        Pipeline *p = &s->pipelines[i];
        if (!p->enabled) continue;
        for (int j = 0; j < p->ntriggers; j++) {
            if (p->triggers[j].type == TRIGGER_WEBHOOK &&
                strcmp(p->triggers[j].webhook_token, token) == 0) {
                found = p;
                break;
            }
        }
    }
    pthread_mutex_unlock(&s->mu);
    return found;
}

/* Step 4: synchronous trigger entry-point used by webhook + file watcher.
 * Updates last_run/next_run to "now" and invokes the run callback. The
 * callback is responsible for offloading actual pipeline execution. */
void scheduler_run_pipeline_now(Scheduler *s, Pipeline *p) {
    if (!s || !p) return;
    pthread_mutex_lock(&s->mu);
    p->last_run    = (int64_t)time(NULL);
    p->run_status  = RUN_RUNNING;
    /* Don't update next_run — cron schedule (if any) keeps its own cadence */
    LOG_INFO("scheduler triggering pipeline %s (event)", p->id);
    if (s->on_run) s->on_run(p, s->on_run_data);
    pthread_mutex_unlock(&s->mu);
}
