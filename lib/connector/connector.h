#pragma once
#include "../core/arena.h"
#include "../storage/storage.h"
#include <stdint.h>

#define DFO_CONNECTOR_ABI_VERSION 3

typedef struct {
    const char *entity;  /* table / endpoint name */
    const char *type;    /* "table","view","stream" */
} DfoEntity;

typedef struct {
    DfoEntity *items;
    int        count;
} DfoEntityList;

typedef struct {
    const char *cursor;    /* opaque bookmark for pagination */
    int64_t     limit;
    const char *filter;    /* optional SQL-like predicate */
} DfoReadReq;

typedef enum { CDC_INSERT=1, CDC_UPDATE=2, CDC_DELETE=3 } CdcOp;

typedef struct {
    CdcOp       op;
    const char *entity;
    ColBatch   *before;  /* NULL for inserts */
    ColBatch   *after;   /* NULL for deletes */
    int64_t     lsn;
} CdcEvent;

typedef void (*DfoCdcHandler)(CdcEvent *ev, void *userdata);

/* ── Plugin ABI — never change field order ── */
typedef struct {
    uint32_t    abi_version;
    const char *name;
    const char *version;
    const char *description;

    /* lifecycle */
    void *(*create)(const char *config_json, Arena *arena);
    void  (*destroy)(void *ctx);

    /* discovery */
    int (*list_entities)(void *ctx, Arena *a, DfoEntityList *out);
    int (*describe)(void *ctx, Arena *a, const char *entity, Schema **out);

    /* data */
    int (*read_batch)(void *ctx, Arena *a, DfoReadReq *req,
                      const char *entity, ColBatch **out);

    /* CDC (optional) */
    int (*cdc_start)(void *ctx, DfoCdcHandler handler, void *userdata);
    int (*cdc_stop)(void *ctx);

    /* health */
    int (*ping)(void *ctx);

    /* ── Sink / приёмник (optional, ABI v2) ──
     * Write a batch of rows OUT to an external entity (table / object key /
     * topic / file). `entity` is the destination name; `schema` describes the
     * batch columns; `batch` holds the rows (all cells are TEXT for pipeline
     * output). `mode`: SINK_APPEND keeps existing data, SINK_OVERWRITE replaces
     * it (truncate / recreate / new object). On the first call of a multi-batch
     * write the connector applies `mode`; subsequent calls always append.
     * Returns rows written (>=0) or <0 on error. NULL if the connector is
     * read-only. */
    int (*write_batch)(void *ctx, Arena *a, const char *entity,
                       const Schema *schema, const ColBatch *batch, int mode);

    /* ── Last error (optional, ABI v3) ──
     * Human-readable reason for the most recent failure (e.g. a libpq connect
     * error). NULL or empty when there's none. Used by the connection-probe UI
     * to show why a test failed. May be NULL if the connector doesn't track it. */
    const char *(*last_error)(void *ctx);
} DfoConnector;

/* write_batch mode */
#define DFO_SINK_APPEND    0
#define DFO_SINK_OVERWRITE 1

/* export symbol name in every plugin .so */
#define DFO_CONNECTOR_EXPORT_SYM "dfo_connector_entry"

/* ── Loader ── */
typedef struct ConnectorInst ConnectorInst;

ConnectorInst *connector_load(const char *so_path, const char *config_json, Arena *a);
void           connector_unload(ConnectorInst *inst);
const DfoConnector *connector_api(ConnectorInst *inst);
void          *connector_ctx(ConnectorInst *inst);
