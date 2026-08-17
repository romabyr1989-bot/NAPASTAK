/*
 * Integration tests — Table CRUD API
 * Tests: create via CSV ingest, list, schema, delete, X-Correlation-Id header.
 *
 * Usage: test_api_tables <gateway_binary> [port]
 */
#include "framework.h"
#include <stdio.h>

#define BINARY_DEFAULT "build/debug/bin/napastak_gateway"
#define PORT_DEFAULT   19080

int main(int argc, char **argv) {
    puts("TAP version 14");

    const char *binary = (argc > 1) ? argv[1] : BINARY_DEFAULT;
    int port = (argc > 2) ? atoi(argv[2]) : PORT_DEFAULT;

    curl_global_init(CURL_GLOBAL_DEFAULT);

    /* ── Start server ── */
    if (test_server_start(binary, port) != 0) {
        printf("not ok 1 — server fork failed\n");
        printf("1..1\n");
        return 1;
    }
    if (test_server_wait_ready(5000) != 0) {
        printf("not ok 1 — server did not become ready in 5 s\n");
        printf("1..1\n");
        return 1;
    }

    /* ── Auth ── */
    ok(test_login("admin") == 0, "login as admin succeeds");

    /* ── /health ── */
    {
        HttpResp r;
        CURLcode rc = http_get("/health", &r);
        ok(rc == CURLE_OK && r.status == 200, "GET /health returns 200");
        ok(json_contains(r.data, "\"ok\"") || json_contains(r.data, "status"),
           "health body contains status");
        resp_free(&r);
    }

    /* ── Ingest CSV ── */
    {
        const char *csv =
            "id,name,score\n"
            "1,Alice,95.5\n"
            "2,Bob,87.0\n"
            "3,Carol,72.3\n"
            "4,Dave,91.1\n"
            "5,Eve,60.0\n";

        /* POST to /api/ingest/csv?table=students */
        HttpResp r;
        CURLcode rc = http_do("POST", "/api/ingest/csv?table=students",
                              csv, "text/csv", &r);
        ok(rc == CURLE_OK, "POST /api/ingest/csv — curl ok");
        ok(r.status == 200 || r.status == 201, "ingest CSV status 200/201");
        ok(json_contains(r.data, "students") || json_contains(r.data, "rows"),
           "ingest CSV response mentions table or rows");
        resp_free(&r);
    }

    /* ── List tables ── */
    {
        HttpResp r;
        http_get("/api/tables", &r);
        ok(r.status == 200, "GET /api/tables returns 200");
        ok(json_contains(r.data, "students"), "tables list contains 'students'");
        resp_free(&r);
    }

    /* ── Schema ── */
    {
        HttpResp r;
        http_get("/api/tables/students/schema", &r);
        ok(r.status == 200, "GET /api/tables/students/schema returns 200");
        ok(json_contains(r.data, "id"), "schema contains 'id' column");
        ok(json_contains(r.data, "name"), "schema contains 'name' column");
        ok(json_contains(r.data, "score"), "schema contains 'score' column");
        resp_free(&r);
    }

    /* ── X-Correlation-Id header ── */
    {
        HttpResp r;
        http_get("/api/tables", &r);
        ok(r.status == 200, "tables request succeeds for correlation-id test");
        ok(strlen(r.correlation_id) == 36, "X-Correlation-Id is a UUID (36 chars)");
        resp_free(&r);
    }

    /* ── Two consecutive requests have distinct correlation IDs ── */
    {
        HttpResp r1, r2;
        http_get("/health", &r1);
        http_get("/health", &r2);
        ok(strcmp(r1.correlation_id, r2.correlation_id) != 0,
           "consecutive requests have distinct X-Correlation-Id");
        resp_free(&r1);
        resp_free(&r2);
    }

    /* ── Ingest second table ── */
    {
        const char *csv2 =
            "dept,budget\n"
            "Engineering,500000\n"
            "Marketing,200000\n"
            "HR,150000\n";
        HttpResp r;
        http_do("POST", "/api/ingest/csv?table=departments",
                csv2, "text/csv", &r);
        ok(r.status == 200 || r.status == 201, "ingest departments table");
        resp_free(&r);
    }

    /* ── List tables — both present ── */
    {
        HttpResp r;
        http_get("/api/tables", &r);
        ok(json_contains(r.data, "students") && json_contains(r.data, "departments"),
           "both tables visible after ingestion");
        resp_free(&r);
    }

    /* ── Delete table ── */
    {
        HttpResp r;
        http_delete("/api/tables/departments", &r);
        ok(r.status == 200 || r.status == 204, "DELETE /api/tables/departments returns 200/204");
        resp_free(&r);
    }

    /* ── Verify deleted table gone ── */
    {
        HttpResp r;
        http_get("/api/tables", &r);
        ok(!json_contains(r.data, "departments"), "deleted table absent from list");
        resp_free(&r);
    }

    /* ── Schema on missing table returns 404 ── */
    {
        HttpResp r;
        http_get("/api/tables/no_such_table/schema", &r);
        ok(r.status == 404, "schema for nonexistent table is 404");
        resp_free(&r);
    }

    /* ── Prometheus metrics endpoint ── */
    {
        HttpResp r;
        /* /metrics is public (no auth header needed) */
        memset(_srv_jwt, 0, sizeof(_srv_jwt));
        http_get("/metrics", &r);
        ok(r.status == 200, "GET /metrics returns 200");
        ok(json_contains(r.data, "dfo_http_requests_total"),
           "/metrics contains dfo_http_requests_total");
        ok(json_contains(r.data, "dfo_tables_count"),
           "/metrics contains dfo_tables_count");
        /* restore JWT */
        test_login("admin");
        resp_free(&r);
    }

    curl_global_cleanup();
    tap_done();
}
