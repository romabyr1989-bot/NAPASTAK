#pragma once
/* Parameterized SQL templates: {name} / {@computed} placeholder substitution.
 *
 * Pure (no App/Pipeline/RS dependency) so it can be unit-tested standalone.
 * @computed params (@now / @last_run / @step:...) are resolved by a caller-
 * supplied callback — the gateway wires runtime state in; this layer only does
 * the text substitution and SQL quoting. */
#include "../core/arena.h"
#include "../core/json.h"
#include <stddef.h>

/* Resolver for an @-prefixed computed param. `expr` is the text after '@'
 * (e.g. "now", "last_run", "step:find:owner_id"). Return an arena-allocated,
 * SQL-ready (already quoted / formatted) value, or NULL on failure (set err). */
typedef char *(*SqlComputedFn)(void *ctx, Arena *a, const char *expr,
                               char *err, size_t errsz);

/* Wrap a string in single quotes, doubling internal single quotes. */
char *sql_quote_literal(Arena *a, const char *s);

/* Substitute {name}, {@computed} and {{ in `sql`.
 *   - {name}      → literal param from `params` (JSON object; may be NULL)
 *                   · string  → quoted SQL literal ('Bob')
 *                   · number  → inserted verbatim (42)
 *                   · name ending in _raw → inserted verbatim (identifiers)
 *   - {@expr}     → resolved via `resolver` (NULL → such a param is an error)
 *   - {{          → literal '{'
 * Returns arena-allocated SQL, or NULL on an unresolved param (sets err). */
char *sql_substitute_params(Arena *a, const char *sql, JVal *params,
                            SqlComputedFn resolver, void *resolver_ctx,
                            char *err, size_t errsz);
