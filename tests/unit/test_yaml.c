/* Unit tests for the minimal YAML→JSON converter. */
#include "../../lib/yaml/yaml_loader.h"
#include "../../lib/core/arena.h"
#include "../../lib/core/json.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int pass = 0, fail = 0;

static void check(const char *name, int cond, const char *got) {
    if (cond) { pass++; printf("PASS: %s\n", name); }
    else      { fail++; printf("FAIL: %s\n  got: %.300s\n", name, got ? got : "(null)"); }
}

static char *yconvert(const char *src, Arena *a) {
    YamlError err = {0};
    char *out = NULL;
    int rc = yaml_to_json(src, strlen(src), a, &out, &err);
    if (rc < 0) {
        printf("  yaml_to_json error line %d: %s\n", err.line, err.buf);
        return NULL;
    }
    return out;
}

static int contains(const char *hay, const char *needle) {
    return hay && strstr(hay, needle) != NULL;
}

int main(void) {
    Arena *a = arena_create(64 * 1024);

    /* 1. simple mapping */
    char *j = yconvert("name: foo\nport: 8080\n", a);
    check("simple mapping name+port",
          j && contains(j, "\"name\":\"foo\"") && contains(j, "\"port\":8080"), j);

    /* 2. boolean and null auto-typing */
    j = yconvert("a: true\nb: false\nc: null\nd: ~\n", a);
    check("auto-types bool/null",
          j && contains(j, "\"a\":true") && contains(j, "\"b\":false")
            && contains(j, "\"c\":null") && contains(j, "\"d\":null"), j);

    /* 3. quoted string preserved literally */
    j = yconvert("title: \"hello world\"\n", a);
    check("double-quoted string",
          j && contains(j, "\"title\":\"hello world\""), j);

    /* 4. block sequence of scalars */
    j = yconvert("items:\n  - one\n  - two\n  - three\n", a);
    check("block sequence of scalars",
          j && contains(j, "\"items\":[\"one\",\"two\",\"three\"]"), j);

    /* 5. flow array */
    j = yconvert("depends_on: [a, b, c]\n", a);
    check("flow sequence of scalars",
          j && contains(j, "\"depends_on\":[\"a\",\"b\",\"c\"]"), j);

    /* 6. nested mapping */
    j = yconvert(
        "user:\n"
        "  name: alice\n"
        "  age: 30\n", a);
    check("nested mapping",
          j && contains(j, "\"user\":{") && contains(j, "\"name\":\"alice\"")
            && contains(j, "\"age\":30"), j);

    /* 7. sequence of mappings (the typical pipeline-step shape) */
    j = yconvert(
        "steps:\n"
        "  - id: extract\n"
        "    type: bash\n"
        "    command: ls\n"
        "  - id: load\n"
        "    type: sql\n", a);
    check("sequence of mappings",
          j && contains(j, "\"steps\":[")
            && contains(j, "\"id\":\"extract\"")
            && contains(j, "\"command\":\"ls\"")
            && contains(j, "\"id\":\"load\""), j);

    /* 8. block scalar (literal | preserves newlines) */
    j = yconvert(
        "sql: |\n"
        "  SELECT 1\n"
        "  FROM users\n"
        "next: ok\n", a);
    check("block scalar literal preserves newlines",
          j && contains(j, "\"sql\":\"SELECT 1\\nFROM users\"")
            && contains(j, "\"next\":\"ok\""), j);

    /* 9. block scalar (folded > joins with spaces) */
    j = yconvert(
        "msg: >\n"
        "  hello\n"
        "  world\n", a);
    check("block scalar folded joins with space",
          j && contains(j, "\"msg\":\"hello world\""), j);

    /* 10. comments are ignored */
    j = yconvert(
        "# top comment\n"
        "name: foo  # inline comment\n"
        "# trailing comment\n", a);
    check("comments stripped",
          j && contains(j, "\"name\":\"foo\"") && !contains(j, "comment"), j);

    /* 11. realistic pipeline doc */
    const char *pipeline =
        "name: users_etl\n"
        "description: Sync users from Postgres\n"
        "triggers:\n"
        "  - type: cron\n"
        "    cron_expr: \"0 */6 * * *\"\n"
        "  - type: webhook\n"
        "    webhook_token: secret123\n"
        "steps:\n"
        "  - id: extract\n"
        "    connector_type: postgresql\n"
        "    target_table: users_raw\n"
        "  - id: dedupe\n"
        "    transform_sql: |\n"
        "      INSERT INTO users_clean\n"
        "      SELECT * FROM users_raw\n"
        "    target_table: users_clean\n"
        "    depends_on: [extract]\n";
    j = yconvert(pipeline, a);
    check("realistic pipeline parses",
          j && contains(j, "\"name\":\"users_etl\"")
            && contains(j, "\"triggers\":[")
            && contains(j, "\"type\":\"cron\"")
            && contains(j, "\"webhook_token\":\"secret123\"")
            && contains(j, "\"steps\":[")
            && contains(j, "\"depends_on\":[\"extract\"]")
            && contains(j, "INSERT INTO users_clean\\nSELECT"), j);

    /* 12. round-trip: parsed YAML → JSON parses to same shape */
    {
        JVal *v = json_parse(a, j, strlen(j));
        check("round-trip JSON re-parses",
              v && v->type == JV_OBJECT
              && json_get(v, "name")
              && json_get(v, "triggers")
              && json_get(v, "steps"), j);
    }

    /* 13. error: missing colon */
    {
        YamlError err = {0};
        char *out = NULL;
        int rc = yaml_to_json("just a bare line\n",
                              strlen("just a bare line\n"), a, &out, &err);
        check("error on bare line", rc < 0 && err.buf[0], err.buf);
    }

    /* 14. blank lines in block scalar preserved */
    j = yconvert(
        "msg: |\n"
        "  line1\n"
        "\n"
        "  line2\n"
        "key: x\n", a);
    check("blank lines inside block scalar",
          j && contains(j, "\"msg\":\"line1\\n\\nline2\""), j);

    /* Regression: a dedented comment after a `|` block scalar must END the block
     * (not be slurped). Previously it leaked into the value AND a child_indent
     * strip cut the multibyte `═` mid-character → invalid UTF-8 in the JSON. */
    j = yconvert(
        "steps:\n"
        "  - id: a\n"
        "    rules: |\n"
        "      [1,2]\n"
        "\n"
        "  # ═══ note ═══\n"
        "  - id: b\n", a);
    check("block scalar stops at dedented comment (no UTF-8 corruption)",
          j && contains(j, "\"rules\":\"[1,2]") && !contains(j, "note") && contains(j, "\"id\":\"b\""), j);

    printf("\nYAML loader: %d passed, %d failed\n", pass, fail);
    arena_destroy(a);
    return fail == 0 ? 0 : 1;
}
