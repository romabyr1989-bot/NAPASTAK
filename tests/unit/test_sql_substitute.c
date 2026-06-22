/* Unit tests for the SQL-template substitution engine (lib/sql_template). */
#include "../../lib/sql_template/sql_template.h"
#include "../../lib/core/arena.h"
#include "../../lib/core/json.h"
#include <stdio.h>
#include <string.h>

static int plan=0, pass=0, fail=0;
#define ok(c, desc) do{ plan++; \
    if (c) { pass++; printf("ok %d - %s\n", plan, desc); } \
    else   { fail++; printf("not ok %d - %s\n", plan, desc); } } while(0)

/* A trivial computed-param resolver: @now → 'NOW', @x → fail. */
static char *test_resolver(void *ctx, Arena *a, const char *expr, char *err, size_t errsz) {
    (void)ctx;
    if (!strcmp(expr, "now")) return arena_strdup(a, "'NOW'");
    snprintf(err, errsz, "unknown computed param @%s", expr);
    return NULL;
}

int main(void) {
    puts("TAP version 14");
    Arena *a = arena_create(0);
    char err[256];

    /* ── sql_quote_literal ── */
    ok(strcmp(sql_quote_literal(a, "abc"), "'abc'") == 0,        "quote: abc -> 'abc'");
    ok(strcmp(sql_quote_literal(a, "O'Brien"), "'O''Brien'") == 0, "quote: O'Brien -> 'O''Brien'");
    ok(strcmp(sql_quote_literal(a, ""), "''") == 0,             "quote: empty -> ''");

    /* ── literal substitution: _raw verbatim, string quoted ── */
    JVal *p1 = json_parse(a, "{\"t_raw\":\"users\",\"name\":\"Bob\"}", 30);
    char *r1 = sql_substitute_params(a, "SELECT * FROM {t_raw} WHERE name = {name}",
                                     p1, NULL, NULL, err, sizeof(err));
    ok(r1 != NULL,                          "literal subst: returns non-NULL");
    ok(r1 && strstr(r1, "FROM users WHERE"), "literal subst: {t_raw} -> users (verbatim)");
    ok(r1 && strstr(r1, "name = 'Bob'"),     "literal subst: {name} -> 'Bob' (quoted)");

    /* ── number param inserted without quotes ── */
    JVal *p2 = json_parse(a, "{\"min\":42}", 11);
    char *r2 = sql_substitute_params(a, "score > {min}", p2, NULL, NULL, err, sizeof(err));
    ok(r2 && strcmp(r2, "score > 42") == 0,  "number param -> 42 (no quotes)");

    /* ── injection: quotes in value are doubled ── */
    JVal *p3 = json_parse(a, "{\"n\":\"a' OR '1'='1\"}", 22);
    char *r3 = sql_substitute_params(a, "x={n}", p3, NULL, NULL, err, sizeof(err));
    ok(r3 && strcmp(r3, "x='a'' OR ''1''=''1'") == 0, "injection: quotes doubled");

    /* ── {{ escape → literal { ── */
    char *r4 = sql_substitute_params(a, "SELECT '{{not_a_param}'", NULL, NULL, NULL, err, sizeof(err));
    ok(r4 && strstr(r4, "{not_a_param}"),   "escape: {{ -> literal {");

    /* ── unresolved param → NULL + message ── */
    err[0] = '\0';
    char *r5 = sql_substitute_params(a, "WHERE x = {missing}", NULL, NULL, NULL, err, sizeof(err));
    ok(r5 == NULL && strstr(err, "unresolved SQL param"), "unresolved param -> NULL + msg");

    /* ── computed param via resolver ── */
    char *r6 = sql_substitute_params(a, "t = {@now}", NULL, test_resolver, NULL, err, sizeof(err));
    ok(r6 && strcmp(r6, "t = 'NOW'") == 0,  "computed: {@now} -> 'NOW' via resolver");

    /* ── computed param with no resolver → error ── */
    err[0] = '\0';
    char *r7 = sql_substitute_params(a, "t = {@now}", NULL, NULL, NULL, err, sizeof(err));
    ok(r7 == NULL && strstr(err, "runtime context"), "computed w/o resolver -> error");

    printf("1..%d\n", plan);
    fprintf(stderr, "sql_substitute: %d/%d passed\n", pass, plan);
    arena_destroy(a);
    return fail ? 1 : 0;
}
