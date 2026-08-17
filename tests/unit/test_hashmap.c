#include "../../lib/core/hashmap.h"
#include "../../lib/core/arena.h"
#include <stdio.h>
#include <string.h>

static int plan=0,pass=0,fail=0;
#define ok(c,...) do{plan++;if(c){pass++;printf("ok %d — " __VA_ARGS__);puts("");}else{fail++;printf("not ok %d — " __VA_ARGS__);puts("");}}while(0)

int main(void) {
    puts("TAP version 14");
    Arena *a = arena_create(0);
    HashMap m; hm_init(&m, a, 8);

    hm_set(&m, "foo", (void*)1);
    hm_set(&m, "bar", (void*)2);
    hm_set(&m, "baz", (void*)3);

    ok(hm_get(&m,"foo")==(void*)1, "get foo");
    ok(hm_get(&m,"bar")==(void*)2, "get bar");
    ok(hm_get(&m,"baz")==(void*)3, "get baz");
    ok(hm_get(&m,"qux")==NULL, "get missing returns NULL");

    hm_set(&m, "foo", (void*)42);
    ok(hm_get(&m,"foo")==(void*)42, "update existing key");

    bool del = hm_del(&m, "bar");
    ok(del, "del bar returns true");
    ok(hm_get(&m,"bar")==NULL, "deleted key returns NULL");

    /* fill beyond initial cap to force grow */
    for(int i=0;i<64;i++){
        char key[16]; snprintf(key,sizeof(key),"key%d",i);
        hm_set(&m,key,(void*)(intptr_t)i);
    }
    ok(hm_get(&m,"key0")==(void*)0, "key0 after grow");
    ok(hm_get(&m,"key63")==(void*)63, "key63 after grow");

    /* iterate */
    int count=0;
    const char *k; void *v;
    for(int idx=hm_next(&m,0,&k,&v);idx>=0;idx=hm_next(&m,idx,&k,&v)) count++;
    ok(count>=65, "iteration visits all entries (count=%d)", count);

    /* Слоты таблицы живут в heap, а не в арене — без hm_free ёмкость после
     * роста (3 КБ) утекала, что и показал LeakSanitizer на Linux. */
    hm_free(&m);
    arena_destroy(a);
    printf("1..%d\n# pass=%d fail=%d\n",plan,pass,fail);
    return fail>0?1:0;
}
