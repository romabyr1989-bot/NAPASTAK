/* yaml_loader.h — minimal pure-C YAML → JSON converter.
 *
 * Targets the constrained subset of YAML that NAPASTAK pipeline files
 * need; not a general-purpose YAML library. Returns a JSON string in the
 * supplied arena; the caller passes that to pipeline_from_json (or
 * json_parse) for further processing.
 *
 * Supported subset:
 *   - block mappings:  `key: value`
 *   - block sequences: `- item`
 *   - flow sequences:  `[a, b, c]`  (used for short inline arrays)
 *   - quoted scalars:  `"..."` and `'...'`
 *   - plain scalars:   bare tokens, with `true`/`false`/`null`/numbers
 *                      auto-typed
 *   - block scalars:   `|` (literal, preserves newlines)
 *                      `>` (folded, joins lines with spaces)
 *   - comments:        `# ...`  (line-rest, never inside quoted strings)
 *
 * NOT supported (out of scope; just write JSON if you need them):
 *   - anchors `&` and aliases `*`
 *   - tags `!!str`
 *   - flow-style mappings `{a: 1}`
 *   - merge keys `<<:`
 *   - multi-document streams (`---` is silently ignored)
 *
 * On error returns -1 and fills `err_msg` (line:col + reason).
 */
#pragma once
#include "../core/arena.h"
#include <stddef.h>

/* Описание ошибки разбора: текст причины и позиция (1-based) в исходнике. */
typedef struct {
    char buf[256];  /* человекочитаемое сообщение об ошибке */
    int  line;      /* номер строки, где обнаружена ошибка */
    int  col;       /* номер колонки в строке */
} YamlError;

/* On success: *json_out is set (NUL-terminated, in arena `a`) and 0 returned.
 * On failure: *json_out unset, err filled, -1 returned. */
int yaml_to_json(const char *src, size_t len, Arena *a,
                 char **json_out, YamlError *err);
