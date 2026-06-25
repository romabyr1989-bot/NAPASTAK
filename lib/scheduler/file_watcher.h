/* file_watcher.h — fires pipeline triggers of type TRIGGER_FILE_ARRIVAL
 * when a file matching `file_pattern` appears in `watch_dir`.
 *
 * Implementation:
 *   Linux:    inotify (one fd, one background thread)
 *   macOS:    kqueue (best-effort dir-level granularity)
 *   other:    stub — logs a warning at start, never fires
 *
 * The watcher is created once in app_init() AFTER pipelines are loaded,
 * iterates triggers[] and registers watches. Runtime add/remove of
 * pipelines is not supported yet — gateway restart picks up changes. */
#pragma once
#include "scheduler.h"

typedef struct FileWatcher FileWatcher;

/* Returns NULL if no pipeline has a TRIGGER_FILE_ARRIVAL trigger,
 * or the platform doesn't support file watching. The caller must
 * still call file_watcher_destroy() — it's NULL-safe. */
FileWatcher *file_watcher_create(Scheduler *s);
/* Останавливает фоновый поток и освобождает ресурсы; безопасно для NULL. */
void         file_watcher_destroy(FileWatcher *fw);
