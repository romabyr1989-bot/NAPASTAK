/* Потокобезопасный логгер с поддержкой двух режимов вывода:
 * - text (цветной ANSI) — для разработки/просмотра в терминале
 * - json — для передачи в Loki/ELK/Vector без дополнительного парсинга */
#include "log.h"
#include <stdarg.h>
#include <string.h>
#include <stdint.h>

#include <fcntl.h>
#include <unistd.h>
/* Заполняет buf случайными байтами из /dev/urandom.
 * Если устройство недоступно — детерминированный fallback (только для UUID, не для криптографии). */
static void gen_random_bytes(uint8_t *buf, size_t n) {
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) { (void)read(fd, buf, n); close(fd); }
    else { for (size_t i = 0; i < n; i++) buf[i] = (uint8_t)(i ^ 0xA5); }
}

/* Глобальный логгер процесса (каждый .so линкует свою копию — см. комментарий в log_write). */
Logger g_log;

/* Thread-local correlation ID — lives for the lifetime of one worker thread */
_Thread_local char g_correlation_id[37] = {0};

/* Текстовые имена и ANSI-цвета уровней; индексируются значением LogLevel. */
static const char *level_str[]   = {"DEBUG","INFO","WARN","ERROR"};
static const char *level_color[] = {"\033[37m","\033[32m","\033[33m","\033[31m"};

/* Инициализирует логгер: поток вывода, минимальный уровень, режим (text/json) и мьютекс. */
void log_init(Logger *l, FILE *out, LogLevel min_level, int json_mode) {
    l->out = out; l->min_level = min_level; l->json_mode = json_mode;
    pthread_mutex_init(&l->mu, NULL);
}

/* Устанавливает correlation ID текущего потока (NULL или усечение до 36 символов). */
void log_set_correlation_id(const char *id) {
    if (!id) { g_correlation_id[0] = '\0'; return; }
    strncpy(g_correlation_id, id, 36);
    g_correlation_id[36] = '\0';
}

/* Генерирует новый случайный UUIDv4 и кладёт его в correlation ID текущего потока. */
void log_new_correlation_id(void) {
    uint8_t b[16];
    gen_random_bytes(b, 16);
    b[6] = (b[6] & 0x0f) | 0x40;  /* UUID version 4 */
    b[8] = (b[8] & 0x3f) | 0x80;  /* UUID variant 1 */
    snprintf(g_correlation_id, 37,
        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
        b[0],b[1],b[2],b[3], b[4],b[5], b[6],b[7],
        b[8],b[9], b[10],b[11],b[12],b[13],b[14],b[15]);
}

/* Форматирует и выводит одну строку лога (printf-стиль): метка времени UTC,
 * уровень, файл:строка, correlation_id и сообщение — в text или json режиме. */
void log_write(Logger *l, LogLevel level, const char *file, int line,
               const char *fmt, ...) {
    /* A plugin .so links its own copy of this logger; loaded via dlopen its
     * g_log is zero-initialised (out == NULL). Never deref a NULL stream —
     * otherwise the first LOG_* from inside a connector crashes the host. */
    if (!l || !l->out) return;
    if (level < l->min_level) return;
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm; gmtime_r(&ts.tv_sec, &tm);
    char tbuf[32];
    strftime(tbuf, sizeof(tbuf), "%Y-%m-%dT%H:%M:%S", &tm);

    va_list ap; va_start(ap, fmt);
    char msg[4096]; vsnprintf(msg, sizeof(msg), fmt, ap); va_end(ap);

    /* Убираем путь к файлу, оставляем только имя — лаконичнее в логах. */
    const char *f = strrchr(file, '/'); f = f ? f+1 : file;

    /* Мьютекс обеспечивает атомарность строки: строки от разных потоков не перемежаются. */
    pthread_mutex_lock(&l->mu);
    if (l->json_mode) {
        if (g_correlation_id[0]) {
            fprintf(l->out,
                "{\"ts\":\"%s.%03ldZ\",\"level\":\"%s\","
                "\"file\":\"%s\",\"line\":%d,"
                "\"correlation_id\":\"%s\",\"msg\":\"%s\"}\n",
                tbuf, ts.tv_nsec/1000000, level_str[level],
                f, line, g_correlation_id, msg);
        } else {
            fprintf(l->out,
                "{\"ts\":\"%s.%03ldZ\",\"level\":\"%s\","
                "\"file\":\"%s\",\"line\":%d,\"msg\":\"%s\"}\n",
                tbuf, ts.tv_nsec/1000000, level_str[level], f, line, msg);
        }
    } else {
        fprintf(l->out, "%s%s.%03ldZ [%5s] %s:%d — %s\033[0m\n",
                level_color[level], tbuf, ts.tv_nsec/1000000,
                level_str[level], f, line, msg);
    }
    fflush(l->out);   /* немедленный сброс — при краше теряем меньше строк */
    pthread_mutex_unlock(&l->mu);
}
