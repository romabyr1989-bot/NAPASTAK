/* file_watcher.c — TRIGGER_FILE_ARRIVAL implementation.
 *
 * Linux: inotify in a dedicated thread; one watch per (watch_dir, pipeline)
 *        registration. IN_CLOSE_WRITE + IN_MOVED_TO catch both fresh writes
 *        and atomic-rename uploads.
 *
 * Other platforms (macOS, *BSD): stub. We log a warning at create time and
 *        return NULL so callers know the trigger won't fire here. macOS
 *        kqueue support would need a separate code path; not in scope yet.
 *
 * The Scheduler pointer the watcher holds is shared with the scheduler
 * thread; both are protected by Scheduler::mu when reading/firing
 * pipelines. */
#include "file_watcher.h"
#include "../core/log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <errno.h>
#include <fnmatch.h>

#ifdef __linux__
#  include <sys/inotify.h>
#  define FW_LINUX 1
#else
#  define FW_LINUX 0
#endif

#define FW_MAX_WATCHES  64

typedef struct {
    int   wd;                /* inotify watch descriptor */
    char  pipeline_id[64];   /* which pipeline this watch belongs to */
    char  pattern[64];       /* glob — fnmatch'd against created file names */
    char  dir[256];
} FwEntry;

/* Состояние watcher'а: дескриптор inotify, таблица активных watch'ей
 * и фоновый поток, читающий события. */
struct FileWatcher {
    Scheduler *sched;
    int        inotify_fd;
    FwEntry    entries[FW_MAX_WATCHES];
    int        nentries;
    pthread_t  thread;
    volatile int running;
};

#if FW_LINUX

/* Поиск записи watch'а по дескриптору inotify (ev->wd из события). */
static FwEntry *find_by_wd(FileWatcher *fw, int wd) {
    for (int i = 0; i < fw->nentries; i++)
        if (fw->entries[i].wd == wd) return &fw->entries[i];
    return NULL;
}

/* Фоновый поток: блокируется на read() inotify, разбирает поток событий
 * и для каждого подходящего файла запускает связанный pipeline. */
static void *watcher_loop(void *arg) {
    FileWatcher *fw = arg;
    /* Buffer big enough for several events; events are variable-length so
     * we read into a generic buffer and walk it. */
    char buf[8192] __attribute__((aligned(__alignof__(struct inotify_event))));

    LOG_INFO("file_watcher: thread started (%d watches)", fw->nentries);
    while (fw->running) {
        ssize_t n = read(fw->inotify_fd, buf, sizeof(buf));
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            break;
        }
        for (char *ptr = buf; ptr < buf + n; ) {
            struct inotify_event *ev = (struct inotify_event *)ptr;
            ptr += sizeof(struct inotify_event) + ev->len;
            if (ev->len == 0) continue;
            FwEntry *e = find_by_wd(fw, ev->wd);
            if (!e) continue;
            /* Match created/moved file name against the configured glob */
            if (fnmatch(e->pattern, ev->name, 0) != 0) continue;
            Pipeline *p = scheduler_find(fw->sched, e->pipeline_id);
            if (!p || !p->enabled) continue;
            LOG_INFO("file_watcher: firing pipeline %s on %s/%s",
                     p->id, e->dir, ev->name);
            scheduler_run_pipeline_now(fw->sched, p);
        }
    }
    LOG_INFO("file_watcher: thread stopped");
    return NULL;
}

/* Создаёт watcher: собирает file_arrival-триггеры, регистрирует inotify-watch
 * на их каталоги и запускает фоновый поток. Возвращает NULL, если триггеров
 * нет или ни один watch зарегистрировать не удалось. */
FileWatcher *file_watcher_create(Scheduler *s) {
    if (!s) return NULL;

    /* Pre-scan: collect all file_arrival triggers */
    FileWatcher tmp = {0};
    pthread_mutex_t *mu = NULL;
    (void)mu;
    /* Lock-free read is safe here because file_watcher_create is called
     * during app_init AFTER pipelines are loaded but BEFORE
     * scheduler_start — no concurrent mutation. */
    for (int i = 0; i < s->npipelines && tmp.nentries < FW_MAX_WATCHES; i++) {
        Pipeline *p = &s->pipelines[i];
        for (int j = 0; j < p->ntriggers && tmp.nentries < FW_MAX_WATCHES; j++) {
            PipelineTrigger *t = &p->triggers[j];
            if (t->type != TRIGGER_FILE_ARRIVAL || !t->watch_dir[0]) continue;
            FwEntry *e = &tmp.entries[tmp.nentries];
            e->wd = -1;
            strncpy(e->pipeline_id, p->id,           sizeof(e->pipeline_id) - 1);
            strncpy(e->dir,         t->watch_dir,    sizeof(e->dir) - 1);
            strncpy(e->pattern,     t->file_pattern[0] ? t->file_pattern : "*",
                                                     sizeof(e->pattern) - 1);
            tmp.nentries++;
        }
    }
    if (tmp.nentries == 0) {
        LOG_INFO("file_watcher: no file_arrival triggers configured — skip");
        return NULL;
    }

    int ifd = inotify_init1(IN_CLOEXEC | IN_NONBLOCK);
    if (ifd < 0) {
        /* IN_NONBLOCK so we can shut down cleanly with read returning 0 on close */
        ifd = inotify_init1(IN_CLOEXEC);
        if (ifd < 0) { LOG_ERROR("file_watcher: inotify_init failed: %s", strerror(errno)); return NULL; }
    }

    FileWatcher *fw = calloc(1, sizeof(*fw));
    fw->sched      = s;
    fw->inotify_fd = ifd;
    fw->running    = 1;

    /* Add watches */
    int registered = 0;
    for (int i = 0; i < tmp.nentries; i++) {
        FwEntry *src = &tmp.entries[i];
        int wd = inotify_add_watch(ifd, src->dir, IN_CLOSE_WRITE | IN_MOVED_TO);
        if (wd < 0) {
            LOG_WARN("file_watcher: cannot watch %s (%s) — pipeline %s skipped",
                     src->dir, strerror(errno), src->pipeline_id);
            continue;
        }
        fw->entries[fw->nentries] = *src;
        fw->entries[fw->nentries].wd = wd;
        fw->nentries++;
        registered++;
    }
    if (registered == 0) { close(ifd); free(fw); return NULL; }

    /* watcher_loop fires pipeline runs inline on file arrival — needs the same
     * 8 MiB stack as the worker pool / scheduler, not the 512 KiB default. */
    pthread_attr_t wattr; pthread_attr_t *wap = NULL;
    if (pthread_attr_init(&wattr) == 0) {
        pthread_attr_setstacksize(&wattr, 8 * 1024 * 1024);
        wap = &wattr;
    }
    if (pthread_create(&fw->thread, wap, watcher_loop, fw) != 0) {
        LOG_ERROR("file_watcher: pthread_create failed");
        if (wap) pthread_attr_destroy(&wattr);
        close(ifd); free(fw); return NULL;
    }
    if (wap) pthread_attr_destroy(&wattr);
    LOG_INFO("file_watcher: %d watch(es) active", registered);
    return fw;
}

/* Останавливает поток (закрытие fd прерывает read), дожидается его
 * завершения и освобождает структуру. */
void file_watcher_destroy(FileWatcher *fw) {
    if (!fw) return;
    fw->running = 0;
    /* Closing the fd interrupts the blocking read in the worker thread */
    if (fw->inotify_fd >= 0) { close(fw->inotify_fd); fw->inotify_fd = -1; }
    pthread_join(fw->thread, NULL);
    free(fw);
}

#else /* not Linux */

/* Заглушка для не-Linux платформ: watch не поддерживается, лишь сообщаем
 * оператору, что настроенные file_arrival-триггеры не сработают. */
FileWatcher *file_watcher_create(Scheduler *s) {
    /* Walk pipelines once to know whether anything would have used the
     * watcher — log a warning so the operator notices. */
    int n = 0;
    for (int i = 0; i < s->npipelines; i++)
        for (int j = 0; j < s->pipelines[i].ntriggers; j++)
            if (s->pipelines[i].triggers[j].type == TRIGGER_FILE_ARRIVAL) n++;
    if (n > 0)
        LOG_WARN("file_watcher: %d file_arrival trigger(s) configured but "
                 "this build only supports inotify (Linux). Triggers will not fire.", n);
    else
        LOG_INFO("file_watcher: not on Linux — file_arrival triggers disabled");
    return NULL;
}

void file_watcher_destroy(FileWatcher *fw) { (void)fw; }

#endif
