/* conn_pool.h — пул ПОСТОЯННЫХ подключений к внешним системам.
 *
 * Зачем. Раньше коннектор поднимался на каждый шаг каждого прогона
 * (dlopen + create) и сразу закрывался (connector_unload). Для подключений из
 * справочника это расточительно и неверно по смыслу: сессия к Oracle/PG/Kafka
 * должна жить постоянно, а не только на время запуска конвейера.
 *
 * Что делает пул. Держит загруженные экземпляры коннекторов, привязанные к id
 * подключения, и выдаёт их в аренду. Экземпляр в аренде занят монопольно —
 * контексты плагинов НЕ потокобезопасны, а конвейеры исполняются на пуле
 * воркеров. Когда все экземпляры подключения заняты, пул либо создаёт ещё один
 * (до max_per_conn), либо ждёт освобождения.
 *
 * Инвалидация. Смена конфига подключения меняет его отпечаток, и старые
 * экземпляры закрываются: пароль, поправленный в справочнике, не «залипает» в
 * живой сессии. Удаление подключения закрывает все его экземпляры.
 *
 * Keepalive. conn_pool_tick() пингует простаивающие экземпляры и закрывает
 * мёртвые; он же поднимает подключение впервые, чтобы сессия существовала до
 * первого запуска конвейера. Вызывается из фонового потока гейтвея.
 */
#ifndef DFO_CONN_POOL_H
#define DFO_CONN_POOL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include "connector.h"

typedef struct ConnPool  ConnPool;
typedef struct ConnLease ConnLease;

/* max_per_conn — сколько одновременных экземпляров на одно подключение
 * (параллельные конвейеры на одном источнике); idle_ping_sec — через сколько
 * секунд простоя пинговать экземпляр в conn_pool_tick(). */
ConnPool *conn_pool_create(int max_per_conn, int idle_ping_sec);
void      conn_pool_destroy(ConnPool *p);

/* Взять экземпляр в аренду. Блокируется, пока не освободится место.
 * cfg_json — уже слитый конфиг (подключение + оверлей шага).
 * NULL при ошибке загрузки, причина в err. */
/* verify=true — после подъёма проверить связь ping'ом (чтение: источник,
 * прогрев, статус в UI). verify=false — не проверять: приёмник может писать в
 * ещё не существующий файл или таблицу, где ping закономерно не проходит. */
ConnLease     *conn_pool_acquire(ConnPool *p, const char *conn_id,
                                 const char *so_path, const char *cfg_json,
                                 bool verify, char *err, size_t errsz);
ConnectorInst *conn_lease_inst(ConnLease *l);

/* Вернуть аренду. healthy=false → экземпляр закрывается, а не переиспользуется
 * (после ошибки чтения/записи состояние сессии считаем неизвестным). */
void conn_pool_release(ConnLease *l, bool healthy);

/* Закрыть все экземпляры подключения (смена конфига или удаление записи). */
void conn_pool_invalidate(ConnPool *p, const char *conn_id);

/* Обслуживание: пинг простаивающих, закрытие мёртвых. */
void conn_pool_tick(ConnPool *p);

/* Состояние подключения для UI: "ok" | "error" | "idle" (ещё не поднимали).
 * last_ok/last_err_msg могут быть NULL. Возвращает число живых экземпляров. */
int conn_pool_status(ConnPool *p, const char *conn_id, const char **state,
                     int64_t *last_ok, const char **last_err_msg);

#endif
