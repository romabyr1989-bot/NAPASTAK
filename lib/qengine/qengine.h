#pragma once
/* Physical query executor — volcano model with columnar batches */
#include "../storage/storage.h"
#include "../sql_parser/sql.h"
#include "../core/arena.h"
#include <stdbool.h>

/* ── Operator interface (iterator / volcano model) ── */
typedef struct Operator Operator;

typedef struct {
    int (*open)(Operator *op);
    int (*next)(Operator *op, ColBatch **out);  /* 0=ok 1=eof -1=error */
    void(*close)(Operator *op);
} OpVtable;

struct Operator {
    const OpVtable *vt;
    Operator       *left;
    Operator       *right;
    Arena          *arena;
    Schema         *output_schema;
    void           *state;
};

/* ── Concrete operators ── */
Operator *op_scan(Arena *a, const char *table_name,
                  Schema *schema, const char *data_dir);

Operator *op_filter(Arena *a, Operator *src, Expr *predicate);

Operator *op_project(Arena *a, Operator *src,
                     Expr **exprs, int nexprs, Schema *out_schema);

Operator *op_sort(Arena *a, Operator *src,
                  OrderItem *order, int norder);

Operator *op_limit(Arena *a, Operator *src, int64_t limit, int64_t offset);

Operator *op_window(Arena *a, Operator *src, Expr **window_exprs, int nwexprs);

Operator *op_hash_join(Arena *a, Operator *left, Operator *right,
                       Expr *on, JoinType jtype);

Operator *op_hash_agg(Arena *a, Operator *src,
                      Expr **group_keys, int ngroup,
                      Expr **agg_exprs, int nagg);

/* ── Build operator tree from logical plan ── */
Operator *qengine_build(Arena *a, PlanNode *plan,
                        const char *data_dir);

/* ── Execute to JSON (streaming) ── */
typedef void (*RowJsonCb)(const char *json_row, void *userdata);

int qengine_exec_json(Operator *root, Arena *a,
                      RowJsonCb cb, void *userdata,
                      int *rows_out);

/* ── Evaluate a scalar expression against one row ── */
typedef struct {
    Schema     *schema;
    ColBatch   *batch;
    int         row;
} EvalCtx;

typedef union {
    int64_t     ival;
    double      fval;
    const char *sval;
    bool        bval;
} ScalarVal;

typedef enum { SV_NULL, SV_INT, SV_DOUBLE, SV_TEXT, SV_BOOL } ScalarType;

typedef struct {
    ScalarType type;
    ScalarVal  val;
} Scalar;

Scalar eval_expr(Expr *e, EvalCtx *ctx, Arena *a);
bool   eval_bool(Expr *e, EvalCtx *ctx, Arena *a);   /* for predicates */

/* ── Fuzzy-match / normalisation scalars (also exposed as SQL functions) ──
 * Shared with the gateway's match-rule engine (run_match_step). */
int         qe_levenshtein    (const char *x, const char *y, Arena *a);   /* edit distance */
double      qe_jaro_winkler   (const char *s1, const char *s2, Arena *a); /* [0,1] */
double      qe_word_similarity(const char *query, const char *doc, Arena *a); /* trigram overlap [0,1] */
const char *qe_normalize_inn  (const char *s, Arena *a);   /* digits only */
const char *qe_normalize_name (const char *s, Arena *a);   /* lower+trim+collapse spaces */
