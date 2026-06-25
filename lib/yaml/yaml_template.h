#pragma once
#include "../core/arena.h"
#include <stddef.h>

/*
 * Expand {{ var }} placeholders in `src`.
 *
 * Variables are resolved in order of priority (highest first):
 *   1. keys[]/vals[] passed by the caller
 *   2. vars: section inside the YAML source itself
 *
 * The vars: section (a top-level "vars:" block at column 0 followed by indented
 * "  key: value" lines) is stripped from the returned string so the YAML parser
 * never sees it.
 *
 * Unresolved {{ var }} tokens are left verbatim; a LOG_WARN is emitted for each.
 * Returns NULL (+ LOG_ERROR) only on arena exhaustion.
 *
 * The returned string is NUL-terminated and arena-allocated (do not free).
 */
char *yaml_expand_vars(Arena *a,
                       const char *src, size_t srclen,
                       /* keys[i] -> vals[i], всего nkvs пар (параллельные массивы) */
                       const char **keys, const char **vals, int nkvs);
