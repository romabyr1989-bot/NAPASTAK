/*
 * Load benchmark — query throughput and latency.
 * Tests: SELECT *, WHERE, GROUP BY, ORDER BY under repeated load.
 *
 * Usage: bench_query <gateway_binary> [port]
 */
#include "../integration/framework.h"
#include <stdio.h>
#include <time.h>
#include <math.h>

#define BINARY_DEFAULT "build/debug/bin/napastak_gateway"
#define PORT_DEFAULT   19083
#define SEED_ROWS      50000
#define QUERY_REPS     50

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

typedef struct {
    double min_ms;
    double max_ms;
    double avg_ms;
    double p95_ms;
    int    ok_count;
    int    err_count;
} LatStat;

static int cmp_double(const void *a, const void *b) {
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static LatStat bench_query(const char *sql, int reps) {
    double *lat = (double *)malloc(sizeof(double) * (size_t)reps);
    LatStat s = { .min_ms = 1e18, .max_ms = 0 };
    char body[512];
    snprintf(body, sizeof(body), "{\"sql\":\"%s\"}", sql);

    for (int i = 0; i < reps; i++) {
        uint64_t t0 = now_ns();
        HttpResp r;
        http_post_json("/api/tables/query", body, &r);
        uint64_t t1 = now_ns();
        double ms = (double)(t1 - t0) / 1e6;
        lat[i] = ms;
        if (r.status == 200) s.ok_count++;
        else s.err_count++;
        if (ms < s.min_ms) s.min_ms = ms;
        if (ms > s.max_ms) s.max_ms = ms;
        s.avg_ms += ms;
        resp_free(&r);
    }
    s.avg_ms /= reps;
    qsort(lat, (size_t)reps, sizeof(double), cmp_double);
    s.p95_ms = lat[(int)(reps * 0.95)];
    free(lat);
    return s;
}

static void print_stat(const char *label, LatStat s) {
    printf("  %-40s min=%6.1f avg=%6.1f p95=%6.1f max=%6.1f ms  ok=%d err=%d\n",
           label, s.min_ms, s.avg_ms, s.p95_ms, s.max_ms, s.ok_count, s.err_count);
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

    /* ── Seed 50k rows ── */
    printf("# Seeding %d rows...\n", SEED_ROWS);
    {
        size_t cap = (size_t)SEED_ROWS * 48 + 64;
        char *csv = (char *)malloc(cap);
        int off = snprintf(csv, cap, "id,dept,salary,active\n");
        static const char *depts[] = { "Engineering","Marketing","HR","Finance","Legal" };
        for (int i = 1; i <= SEED_ROWS; i++) {
            off += snprintf(csv + off, cap - (size_t)off,
                            "%d,%s,%d,%s\n",
                            i, depts[i % 5], 50000 + (i % 100) * 500,
                            (i % 3 == 0) ? "false" : "true");
        }
        HttpResp r;
        http_do("POST", "/api/ingest/csv?table=bench_employees",
                csv, "text/csv", &r);
        printf("# Seed status: %ld\n\n", r.status);
        resp_free(&r);
        free(csv);
    }

    printf("# Query latency benchmarks (%d reps each)\n", QUERY_REPS);
    printf("# %-40s %6s %6s %6s %6s %s\n",
           "query", "min_ms", "avg_ms", "p95_ms", "max_ms", "ok/err");

    print_stat("SELECT * LIMIT 100",
        bench_query("SELECT * FROM bench_employees LIMIT 100", QUERY_REPS));

    print_stat("SELECT * (all rows)",
        bench_query("SELECT * FROM bench_employees", QUERY_REPS));

    print_stat("WHERE equality",
        bench_query("SELECT id,salary FROM bench_employees WHERE dept = 'Engineering'",
                    QUERY_REPS));

    print_stat("WHERE range",
        bench_query("SELECT id FROM bench_employees WHERE salary BETWEEN 70000 AND 80000",
                    QUERY_REPS));

    print_stat("COUNT(*)",
        bench_query("SELECT COUNT(*) FROM bench_employees", QUERY_REPS));

    print_stat("SUM(salary)",
        bench_query("SELECT SUM(salary) FROM bench_employees", QUERY_REPS));

    print_stat("GROUP BY dept",
        bench_query("SELECT dept, COUNT(*), AVG(salary) FROM bench_employees GROUP BY dept",
                    QUERY_REPS));

    print_stat("GROUP BY + ORDER BY",
        bench_query("SELECT dept, SUM(salary) as s FROM bench_employees "
                    "GROUP BY dept ORDER BY s DESC",
                    QUERY_REPS));

    print_stat("GROUP BY + HAVING",
        bench_query("SELECT dept, COUNT(*) FROM bench_employees "
                    "GROUP BY dept HAVING COUNT(*) > 5000",
                    QUERY_REPS));

    print_stat("ORDER BY + LIMIT",
        bench_query("SELECT id,salary FROM bench_employees ORDER BY salary DESC LIMIT 10",
                    QUERY_REPS));

    print_stat("LIKE pattern",
        bench_query("SELECT id FROM bench_employees WHERE dept LIKE 'En%'",
                    QUERY_REPS));

    /* ── Throughput summary ── */
    printf("\n# Throughput (SELECT * full scan, %d reps)\n", QUERY_REPS);
    LatStat full = bench_query("SELECT * FROM bench_employees", QUERY_REPS);
    if (full.avg_ms > 0) {
        double qps = 1000.0 / full.avg_ms;
        printf("# avg latency: %.1f ms  →  %.1f queries/s\n", full.avg_ms, qps);
    }

    curl_global_cleanup();
    return 0;
}
