/*
 * Load benchmark — CSV ingest throughput.
 * Reports: rows/s, MB/s, latency per batch.
 *
 * Usage: bench_ingest <gateway_binary> [port]
 */
#include "../integration/framework.h"
#include <stdio.h>
#include <time.h>
#include <math.h>

#define BINARY_DEFAULT "build/debug/bin/napastak_gateway"
#define PORT_DEFAULT   19082

/* nanoseconds since epoch */
static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

typedef struct {
    int    rows;
    double elapsed_s;
    double rows_per_s;
    double mb_per_s;
    long   status;
} BenchResult;

/*
 * Generate N rows of CSV data: id,value,label,score
 * Returns malloc'd buffer (caller must free). Sets *len.
 */
static char *gen_csv(int nrows, size_t *len_out) {
    /* estimate: ~40 bytes/row + header */
    size_t cap = (size_t)nrows * 48 + 64;
    char *buf = (char *)malloc(cap);
    if (!buf) return NULL;
    int off = snprintf(buf, cap, "id,value,label,score\n");
    for (int i = 1; i <= nrows; i++) {
        /* label cycles through AAAA..ZZZZ — good for dictionary encoding */
        char label[8];
        int li = (i - 1) % 26;
        snprintf(label, sizeof(label), "%c%c%c", 'A'+li, 'A'+li, 'A'+li);
        off += snprintf(buf + off, cap - (size_t)off,
                        "%d,%d,%s,%.2f\n",
                        i, i * 7 % 1000, label, (double)(i % 100) / 10.0);
    }
    *len_out = (size_t)off;
    return buf;
}

static BenchResult bench_once(int nrows, const char *table) {
    BenchResult res = {0};
    size_t csv_len = 0;
    char *csv = gen_csv(nrows, &csv_len);

    char path[128];
    snprintf(path, sizeof(path), "/api/ingest/csv?table=%s", table);

    uint64_t t0 = now_ns();
    HttpResp r;
    http_do("POST", path, csv, "text/csv", &r);
    uint64_t t1 = now_ns();

    res.rows = nrows;
    res.status = r.status;
    res.elapsed_s = (double)(t1 - t0) / 1e9;
    res.rows_per_s = (res.elapsed_s > 0) ? (double)nrows / res.elapsed_s : 0;
    res.mb_per_s   = (res.elapsed_s > 0) ? ((double)csv_len / (1024.0*1024.0)) / res.elapsed_s : 0;

    resp_free(&r);
    free(csv);
    return res;
}

int main(int argc, char **argv) {
    const char *binary = (argc > 1) ? argv[1] : BINARY_DEFAULT;
    int port = (argc > 2) ? atoi(argv[2]) : PORT_DEFAULT;

    curl_global_init(CURL_GLOBAL_DEFAULT);

    if (test_server_start(binary, port) != 0) {
        fprintf(stderr, "FATAL: server fork failed\n"); return 1;
    }
    if (test_server_wait_ready(5000) != 0) {
        fprintf(stderr, "FATAL: server not ready\n"); return 1;
    }
    if (test_login("admin") != 0) {
        fprintf(stderr, "FATAL: auth failed\n"); return 1;
    }

    printf("# bench_ingest — CSV ingest throughput\n");
    printf("# %-12s %10s %12s %10s %8s\n",
           "rows", "elapsed_s", "rows/s", "MB/s", "status");

    static const int sizes[] = { 1000, 10000, 100000, 500000 };
    for (size_t i = 0; i < sizeof(sizes)/sizeof(sizes[0]); i++) {
        char tbl[32];
        snprintf(tbl, sizeof(tbl), "bench_%d", sizes[i]);
        BenchResult r = bench_once(sizes[i], tbl);
        printf("  %-12d %10.3f %12.0f %10.2f %8ld%s\n",
               r.rows, r.elapsed_s, r.rows_per_s, r.mb_per_s, r.status,
               (r.status != 200 && r.status != 201) ? " FAIL" : "");
    }

    /* ── Repeated small ingests — measure per-request overhead ── */
    printf("\n# Repeated 1k-row ingests (10 runs)\n");
    printf("# %-4s %10s %12s\n", "run", "elapsed_s", "rows/s");
    double total = 0;
    for (int i = 0; i < 10; i++) {
        char tbl[32];
        snprintf(tbl, sizeof(tbl), "bench_rep_%d", i);
        BenchResult r = bench_once(1000, tbl);
        printf("  %-4d %10.3f %12.0f\n", i+1, r.elapsed_s, r.rows_per_s);
        total += r.rows_per_s;
    }
    printf("# avg rows/s: %.0f\n", total / 10.0);

    curl_global_cleanup();
    return 0;
}
