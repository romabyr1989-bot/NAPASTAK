/*
 * Load benchmark — SCD2 historisation + op_window memory profile.
 *
 * Reports for SCD2 over ~100k rows: wall time + peak gateway RSS.
 * Separately probes op_window (ROW_NUMBER OVER, a BLOCKING operator that
 * materialises the whole partition) at growing sizes to show that peak memory
 * scales linearly with partition volume.
 *
 * Measure-only: this benchmark does NOT change engine behaviour. See
 * docs/MDM_E2E_REPORT.md for the analysis.
 *
 * Usage: bench_scd2 <gateway_binary> [port]
 */
#include "../integration/framework.h"
#include <stdio.h>
#include <time.h>
#include <pthread.h>

#define BINARY_DEFAULT "build/release/bin/napastak_gateway"
#define PORT_DEFAULT   19083

static uint64_t now_ns(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

/* Resident set of the gateway child via ps (KB). */
static long rss_kb(pid_t pid) {
    char cmd[64]; snprintf(cmd, sizeof(cmd), "ps -o rss= -p %d 2>/dev/null", (int)pid);
    FILE *f = popen(cmd, "r"); if (!f) return -1;
    long kb = -1; if (fscanf(f, "%ld", &kb) != 1) kb = -1; pclose(f); return kb;
}

/* ── Peak-RSS sampler: polls the gateway RSS in a thread so we catch the
 *    transient high-water mark of a blocking operator even if its arena is
 *    freed before the request returns. ── */
static volatile int g_sampling = 0; static long g_peak = 0; static pid_t g_pid = 0;
static pthread_t    g_th;
static void *sampler_fn(void *a) {
    (void)a;
    while (g_sampling) { long r = rss_kb(g_pid); if (r > g_peak) g_peak = r; usleep(5000); }
    return NULL;
}
static long g_baseline = 0;
static void peak_start(void) { g_baseline = rss_kb(g_pid); g_peak = g_baseline; g_sampling = 1;
                               pthread_create(&g_th, NULL, sampler_fn, NULL); }
static long peak_stop(void)  { g_sampling = 0; pthread_join(g_th, NULL); return g_peak; }

/* CSV: id,name,email,updated_at,is_deleted (business key = id). */
static char *gen_scd2_csv(int n, int delta, size_t *len_out) {
    size_t cap = (size_t)n * 48 + 64;
    char *buf = malloc(cap);
    int off = snprintf(buf, cap, "id,name,email,updated_at,is_deleted\n");
    for (int i = 1; i <= n; i++)
        off += snprintf(buf + off, cap - (size_t)off,
            "id%d,name%d,u%d@x.com,%d,0\n", i, i + (delta ? 1000000 : 0), i, delta ? 200 : 100);
    *len_out = (size_t)off; return buf;
}

/* CSV: id,grp,v — grp constant so the window sees ONE partition of all rows. */
static char *gen_win_csv(int n, size_t *len_out) {
    size_t cap = (size_t)n * 24 + 64;
    char *buf = malloc(cap);
    int off = snprintf(buf, cap, "id,grp,v\n");
    for (int i = 1; i <= n; i++)
        off += snprintf(buf + off, cap - (size_t)off, "%d,g,%d\n", i, i * 7 % 1000);
    *len_out = (size_t)off; return buf;
}

static void ingest(const char *table, char *csv, size_t len) {
    HttpResp r;
    char dpath[160]; snprintf(dpath, sizeof(dpath), "/api/tables/%s", table);
    http_do("DELETE", dpath, NULL, NULL, &r); resp_free(&r);   /* reset so ingest replaces */
    char path[128]; snprintf(path, sizeof(path), "/api/ingest/csv?table=%s", table);
    http_do("POST", path, csv, "text/csv", &r); resp_free(&r);
    (void)len;
}

int main(int argc, char **argv) {
    const char *binary = (argc > 1) ? argv[1] : BINARY_DEFAULT;
    int port = (argc > 2) ? atoi(argv[2]) : PORT_DEFAULT;
    curl_global_init(CURL_GLOBAL_DEFAULT);

    if (test_server_start(binary, port) != 0) { fprintf(stderr, "FATAL: fork\n"); return 1; }
    if (test_server_wait_ready(5000) != 0)    { fprintf(stderr, "FATAL: not ready\n"); return 1; }
    if (test_login("admin") != 0)             { fprintf(stderr, "FATAL: auth\n"); return 1; }
    g_pid = _srv_pid;

    const char *SCD2_PIPE =
      "{\"name\":\"b\",\"enabled\":false,\"steps\":[{\"id\":\"s1\",\"name\":\"scd2\","
      "\"transform_sql\":\"SELECT * FROM scd2_src\",\"target_table\":\"dim_scd2\","
      "\"scd2_business_key\":\"id\",\"scd2_compare_columns\":\"name,email\","
      "\"scd2_transaction_time\":\"updated_at\",\"scd2_effective_from_col\":\"valid_from\","
      "\"scd2_effective_to_col\":\"valid_to\",\"scd2_deleted_flag\":\"is_deleted\"}]}";

    printf("# bench_scd2 — SCD2 first-run historisation (fresh target each size)\n");
    printf("# note: client curl timeout is 10s (framework.h); '0 TIMEOUT' = exceeded\n");
    printf("# %-10s %10s %12s %12s %12s %10s\n",
           "rows", "elapsed_s", "base_rss_kb", "peak_rss_kb", "delta_kb", "status");

    int scd2_sizes[] = { 25000, 50000, 100000 };
    for (size_t i = 0; i < sizeof(scd2_sizes)/sizeof(scd2_sizes[0]); i++) {
        int n = scd2_sizes[i];
        HttpResp d; http_do("DELETE", "/api/tables/dim_scd2", NULL, NULL, &d); resp_free(&d); /* fresh */
        size_t len; char *csv = gen_scd2_csv(n, 0, &len);
        ingest("scd2_src", csv, len); free(csv);
        peak_start();
        uint64_t t0 = now_ns();
        HttpResp r; http_do("POST", "/api/pipelines/preview-step?save=1&limit=1", SCD2_PIPE, "application/json", &r);
        uint64_t t1 = now_ns();
        long peak = peak_stop();
        printf("  %-10d %10.3f %12ld %12ld %12ld %10ld%s\n",
               n, (double)(t1 - t0)/1e9, g_baseline, peak, peak - g_baseline, r.status,
               (r.status/100 != 2) ? " TIMEOUT/FAIL" : "");
        resp_free(&r);
    }

    /* ── Delta re-run: classify is O(source × current) (see run_scd2_step
     *    TODO). Build dim at N, then re-run with N all-changed rows. ── */
    printf("\n# SCD2 delta re-run — exercises the O(source x current) classify loop\n");
    printf("# %-10s %10s %12s %12s %12s %10s\n",
           "rows", "elapsed_s", "base_rss_kb", "peak_rss_kb", "delta_kb", "status");
    {
        int n = 10000;
        HttpResp d; http_do("DELETE", "/api/tables/dim_scd2", NULL, NULL, &d); resp_free(&d);
        size_t len; char *csv = gen_scd2_csv(n, 0, &len); ingest("scd2_src", csv, len); free(csv);
        HttpResp r0; http_do("POST", "/api/pipelines/preview-step?save=1&limit=1", SCD2_PIPE, "application/json", &r0); resp_free(&r0);
        csv = gen_scd2_csv(n, 1, &len); ingest("scd2_src", csv, len); free(csv);   /* all names changed */
        peak_start();
        uint64_t t0 = now_ns();
        HttpResp r; http_do("POST", "/api/pipelines/preview-step?save=1&limit=1", SCD2_PIPE, "application/json", &r);
        uint64_t t1 = now_ns();
        long peak = peak_stop();
        printf("  %-10d %10.3f %12ld %12ld %12ld %10ld%s\n",
               n, (double)(t1 - t0)/1e9, g_baseline, peak, peak - g_baseline, r.status,
               (r.status/100 != 2) ? " TIMEOUT/FAIL" : "");
        resp_free(&r);
    }

    /* ── op_window: ROW_NUMBER over ONE partition of growing size ── */
    printf("\n# op_window (ROW_NUMBER OVER, single partition) — blocking materialisation\n");
    printf("# %-10s %10s %12s %12s %12s\n",
           "rows", "elapsed_s", "base_rss_kb", "peak_rss_kb", "delta_kb");
    int win_sizes[] = { 25000, 50000, 100000 };
    for (size_t i = 0; i < sizeof(win_sizes)/sizeof(win_sizes[0]); i++) {
        int n = win_sizes[i];
        size_t len; char *csv = gen_win_csv(n, &len);
        ingest("win", csv, len); free(csv);
        peak_start();
        uint64_t t0 = now_ns();
        HttpResp r; http_do("POST", "/api/tables/query",
            "{\"sql\":\"SELECT id, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY id) AS rn FROM win\"}",
            "application/json", &r);
        uint64_t t1 = now_ns();
        long peak = peak_stop();
        printf("  %-10d %10.3f %12ld %12ld %12ld\n",
               n, (double)(t1 - t0)/1e9, g_baseline, peak, peak - g_baseline);
        resp_free(&r);
    }

    curl_global_cleanup();
    return 0;
}
