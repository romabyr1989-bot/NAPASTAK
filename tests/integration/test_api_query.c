/*
 * Integration tests — SQL correctness via /api/tables/query
 * Tests: SELECT *, WHERE, ORDER BY, LIMIT, GROUP BY, HAVING, LIKE, BETWEEN,
 *        IS NULL, aggregate functions (COUNT/SUM/AVG/MIN/MAX).
 *
 * Usage: test_api_query <gateway_binary> [port]
 */
#include "framework.h"
#include <stdio.h>
#include <math.h>

#define BINARY_DEFAULT "build/debug/bin/napastak_gateway"
#define PORT_DEFAULT   19081

/* Run a SQL query and return resp (caller must resp_free). */
static HttpResp do_query(const char *sql) {
    char body[1024];
    snprintf(body, sizeof(body), "{\"sql\":\"%s\"}", sql);
    HttpResp r;
    http_post_json("/api/tables/query", body, &r);
    return r;
}

/* Count occurrences of needle in haystack */
static int count_str(const char *haystack, const char *needle) {
    int n = 0;
    const char *p = haystack;
    size_t nl = strlen(needle);
    while ((p = strstr(p, needle)) != NULL) { n++; p += nl; }
    return n;
}

/* Extract "row_count" field from query response JSON */
static int row_count_from_resp(const char *data) {
    const char *p = strstr(data, "\"row_count\":");
    if (!p) return -1;
    return atoi(p + 12);
}

int main(int argc, char **argv) {
    puts("TAP version 14");

    const char *binary = (argc > 1) ? argv[1] : BINARY_DEFAULT;
    int port = (argc > 2) ? atoi(argv[2]) : PORT_DEFAULT;

    curl_global_init(CURL_GLOBAL_DEFAULT);

    if (test_server_start(binary, port) != 0) {
        printf("not ok 1 — server fork failed\n1..1\n"); return 1;
    }
    if (test_server_wait_ready(5000) != 0) {
        printf("not ok 1 — server not ready\n1..1\n"); return 1;
    }
    ok(test_login("admin") == 0, "auth login ok");

    /* ── Seed dataset ── */
    {
        /* 10 rows: id, name, dept, salary, active */
        const char *csv =
            "id,name,dept,salary,active\n"
            "1,Alice,Engineering,95000,true\n"
            "2,Bob,Marketing,72000,true\n"
            "3,Carol,Engineering,88000,false\n"
            "4,Dave,HR,65000,true\n"
            "5,Eve,Engineering,102000,true\n"
            "6,Frank,Marketing,68000,false\n"
            "7,Grace,HR,71000,true\n"
            "8,Heidi,Engineering,55000,true\n"
            "9,Ivan,Marketing,80000,true\n"
            "10,Judy,HR,90000,false\n";
        HttpResp r;
        http_do("POST", "/api/ingest/csv?table=employees",
                csv, "text/csv", &r);
        ok(r.status == 200 || r.status == 201, "seed employees table");
        resp_free(&r);
    }

    /* ── SELECT * ── */
    {
        HttpResp r = do_query("SELECT * FROM employees");
        ok(r.status == 200, "SELECT * status 200");
        /* 10 names should appear */
        ok(json_contains(r.data, "Alice") && json_contains(r.data, "Judy"),
           "SELECT * returns all rows (Alice and Judy present)");
        int rows = row_count_from_resp(r.data);
        ok(rows == 10, "SELECT * returns 10 rows (got %d)", rows);
        resp_free(&r);
    }

    /* ── WHERE equality ── */
    {
        HttpResp r = do_query("SELECT name FROM employees WHERE dept = 'Engineering'");
        ok(r.status == 200, "WHERE equality status 200");
        ok(json_contains(r.data, "Alice"), "WHERE dept=Engineering includes Alice");
        ok(!json_contains(r.data, "Bob"), "WHERE dept=Engineering excludes Bob");
        resp_free(&r);
    }

    /* ── WHERE numeric comparison ── */
    {
        HttpResp r = do_query("SELECT name,salary FROM employees WHERE salary > 85000");
        ok(r.status == 200, "WHERE > status 200");
        ok(json_contains(r.data, "Alice"), "salary > 85000 includes Alice (95000)");
        ok(json_contains(r.data, "Eve"), "salary > 85000 includes Eve (102000)");
        ok(!json_contains(r.data, "Dave"), "salary > 85000 excludes Dave (65000)");
        resp_free(&r);
    }

    /* ── ORDER BY ASC ── */
    {
        HttpResp r = do_query("SELECT name,salary FROM employees ORDER BY salary ASC LIMIT 3");
        ok(r.status == 200, "ORDER BY ASC LIMIT 3 status 200");
        /* Heidi(55000) < Dave(65000) < Frank(68000) */
        const char *heidi = strstr(r.data, "Heidi");
        const char *dave  = strstr(r.data, "Dave");
        ok(heidi && dave && heidi < dave, "ORDER BY salary ASC: Heidi before Dave");
        resp_free(&r);
    }

    /* ── ORDER BY DESC ── */
    {
        HttpResp r = do_query("SELECT name,salary FROM employees ORDER BY salary DESC LIMIT 2");
        ok(r.status == 200, "ORDER BY DESC LIMIT 2 status 200");
        /* Eve(102000) first, Alice(95000) second */
        const char *eve   = strstr(r.data, "Eve");
        const char *alice = strstr(r.data, "Alice");
        ok(eve && alice && eve < alice, "ORDER BY salary DESC: Eve before Alice");
        resp_free(&r);
    }

    /* ── LIMIT ── */
    {
        HttpResp r = do_query("SELECT id FROM employees LIMIT 4");
        ok(r.status == 200, "LIMIT 4 status 200");
        int cnt = row_count_from_resp(r.data);
        ok(cnt == 4, "LIMIT 4 returns exactly 4 rows (got %d)", cnt);
        resp_free(&r);
    }

    /* ── LIKE ── */
    {
        HttpResp r = do_query("SELECT name FROM employees WHERE name LIKE 'A%'");
        ok(r.status == 200, "LIKE status 200");
        ok(json_contains(r.data, "Alice"), "LIKE 'A%' matches Alice");
        ok(!json_contains(r.data, "Bob"), "LIKE 'A%' does not match Bob");
        resp_free(&r);
    }

    /* ── BETWEEN ── */
    {
        HttpResp r = do_query("SELECT name,salary FROM employees WHERE salary BETWEEN 70000 AND 90000");
        ok(r.status == 200, "BETWEEN status 200");
        ok(json_contains(r.data, "Carol"), "BETWEEN 70k-90k includes Carol (88000)");
        ok(json_contains(r.data, "Bob"), "BETWEEN 70k-90k includes Bob (72000)");
        ok(!json_contains(r.data, "Eve"), "BETWEEN 70k-90k excludes Eve (102000)");
        resp_free(&r);
    }

    /* ── AND / OR ── */
    {
        HttpResp r = do_query(
            "SELECT name FROM employees WHERE dept = 'HR' AND salary > 70000");
        ok(r.status == 200, "AND condition status 200");
        ok(json_contains(r.data, "Grace"), "HR AND salary>70k includes Grace (71000)");
        ok(json_contains(r.data, "Judy"), "HR AND salary>70k includes Judy (90000)");
        ok(!json_contains(r.data, "Dave"), "HR AND salary>70k excludes Dave (65000)");
        resp_free(&r);
    }

    /* ── COUNT(*) ── */
    {
        HttpResp r = do_query("SELECT COUNT(*) FROM employees");
        ok(r.status == 200, "COUNT(*) status 200");
        ok(json_contains(r.data, "10"), "COUNT(*) returns 10");
        resp_free(&r);
    }

    /* ── SUM ── */
    {
        HttpResp r = do_query("SELECT SUM(salary) FROM employees WHERE dept = 'Engineering'");
        ok(r.status == 200, "SUM status 200");
        /* 95000+88000+102000+55000 = 340000 */
        ok(json_contains(r.data, "340000"), "SUM(salary) Engineering = 340000");
        resp_free(&r);
    }

    /* ── AVG ── */
    {
        HttpResp r = do_query("SELECT AVG(salary) FROM employees WHERE dept = 'HR'");
        ok(r.status == 200, "AVG status 200");
        /* (65000+71000+90000)/3 = 75333.33 */
        ok(json_contains(r.data, "75333") || json_contains(r.data, "75334"),
           "AVG(salary) HR ≈ 75333");
        resp_free(&r);
    }

    /* ── MIN / MAX ── */
    {
        HttpResp r = do_query("SELECT MIN(salary), MAX(salary) FROM employees");
        ok(r.status == 200, "MIN/MAX status 200");
        ok(json_contains(r.data, "55000"), "MIN(salary) = 55000");
        ok(json_contains(r.data, "102000"), "MAX(salary) = 102000");
        resp_free(&r);
    }

    /* ── GROUP BY ── */
    {
        HttpResp r = do_query("SELECT dept, COUNT(*) FROM employees GROUP BY dept");
        ok(r.status == 200, "GROUP BY status 200");
        ok(json_contains(r.data, "Engineering") &&
           json_contains(r.data, "Marketing") &&
           json_contains(r.data, "HR"),
           "GROUP BY returns all departments");
        resp_free(&r);
    }

    /* ── GROUP BY + HAVING ── */
    {
        HttpResp r = do_query(
            "SELECT dept, COUNT(*) FROM employees GROUP BY dept HAVING COUNT(*) >= 3");
        ok(r.status == 200, "GROUP BY HAVING status 200");
        ok(json_contains(r.data, "Engineering"), "HAVING >= 3 includes Engineering (4 rows)");
        ok(json_contains(r.data, "Marketing"), "HAVING >= 3 includes Marketing (3 rows)");
        resp_free(&r);
    }

    /* ── GROUP BY + ORDER BY ── */
    {
        HttpResp r = do_query(
            "SELECT dept, SUM(salary) as total FROM employees GROUP BY dept ORDER BY total DESC");
        ok(r.status == 200, "GROUP BY ORDER BY status 200");
        /* Engineering sum > Marketing sum > HR sum */
        const char *eng = strstr(r.data, "Engineering");
        const char *mkt = strstr(r.data, "Marketing");
        ok(eng && mkt && eng < mkt, "GROUP BY ORDER BY: Engineering before Marketing by salary sum");
        resp_free(&r);
    }

    /* ── IS NULL / IS NOT NULL ── */
    {
        /* ingest a table with a NULL value represented as empty field */
        const char *csv =
            "id,note\n"
            "1,hello\n"
            "2,\n"
            "3,world\n";
        HttpResp ri;
        http_do("POST", "/api/ingest/csv?table=notes", csv, "text/csv", &ri);
        ok(ri.status == 200 || ri.status == 201, "ingest notes table with NULL");
        resp_free(&ri);

        HttpResp r = do_query("SELECT id FROM notes WHERE note IS NOT NULL");
        ok(r.status == 200, "IS NOT NULL status 200");
        ok(json_contains(r.data, "1") && json_contains(r.data, "3"),
           "IS NOT NULL returns rows 1 and 3");
        resp_free(&r);
    }

    /* ── Alias ── */
    {
        HttpResp r = do_query("SELECT salary * 12 AS annual FROM employees WHERE id = 1");
        ok(r.status == 200, "alias status 200");
        ok(json_contains(r.data, "annual") || json_contains(r.data, "1140000"),
           "alias or computed value present (Alice annual = 1140000)");
        resp_free(&r);
    }

    /* ── Multi-table join ── */
    {
        /* ingest a dept_budget table */
        const char *csv =
            "dept,budget\n"
            "Engineering,500000\n"
            "Marketing,200000\n"
            "HR,150000\n";
        HttpResp ri;
        http_do("POST", "/api/ingest/csv?table=dept_budget", csv, "text/csv", &ri);
        ok(ri.status == 200 || ri.status == 201, "ingest dept_budget for join test");
        resp_free(&ri);

        HttpResp r = do_query(
            "SELECT e.name, d.budget "
            "FROM employees e JOIN dept_budget d ON e.dept = d.dept "
            "WHERE e.id = 1");
        ok(r.status == 200, "JOIN status 200");
        ok(json_contains(r.data, "Alice"), "JOIN result includes Alice");
        ok(json_contains(r.data, "500000"), "JOIN result includes Engineering budget");
        resp_free(&r);
    }

    /* ── Invalid SQL returns error ── */
    {
        HttpResp r = do_query("SEELECT * FORM employees");
        ok(r.status == 400 || r.status == 422, "invalid SQL returns 4xx");
        resp_free(&r);
    }

    curl_global_cleanup();
    tap_done();
}
