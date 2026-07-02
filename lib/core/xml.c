/* xml.c — минимальный recursive-descent XML-парсер (см. xml.h). */
#include "xml.h"
#include <stdlib.h>
#include <string.h>

/* ── Растущий байтовый буфер (malloc) для сборки текста при разборе ── */
typedef struct { char *d; size_t n, cap; } Buf;
static void buf_put(Buf *b, char c) {
    if (b->n + 1 > b->cap) { b->cap = b->cap ? b->cap*2 : 64; b->d = realloc(b->d, b->cap); }
    b->d[b->n++] = c;
}
static void buf_putn(Buf *b, const char *s, size_t n) { for (size_t i=0;i<n;i++) buf_put(b,s[i]); }
/* UTF-8 кодирование кодпоинта (для числовых сущностей &#NN; / &#xHH;). */
static void buf_put_cp(Buf *b, unsigned cp) {
    if (cp < 0x80) buf_put(b,(char)cp);
    else if (cp < 0x800) { buf_put(b,(char)(0xC0|(cp>>6))); buf_put(b,(char)(0x80|(cp&0x3F))); }
    else if (cp < 0x10000) { buf_put(b,(char)(0xE0|(cp>>12))); buf_put(b,(char)(0x80|((cp>>6)&0x3F))); buf_put(b,(char)(0x80|(cp&0x3F))); }
    else { buf_put(b,(char)(0xF0|(cp>>18))); buf_put(b,(char)(0x80|((cp>>12)&0x3F))); buf_put(b,(char)(0x80|((cp>>6)&0x3F))); buf_put(b,(char)(0x80|(cp&0x3F))); }
}

/* ── Динамический вектор указателей (malloc) для kids/attrs при разборе ── */
typedef struct { void **items; size_t n, cap; } PtrVec;
static void pv_push(PtrVec *v, void *p) {
    if (v->n == v->cap) { v->cap = v->cap ? v->cap*2 : 8; v->items = realloc(v->items, v->cap*sizeof(void*)); }
    v->items[v->n++] = p;
}

/* ── Состояние парсера ── */
typedef struct { const char *p, *end; Arena *a; } PS;

/* Первое вхождение символа c в [s, end); NULL если нет. */
static const char *find_char(const char *s, const char *end, char c) {
    for (; s < end; s++) if (*s == c) return s;
    return NULL;
}
/* Первое вхождение подстроки needle в [s, end); NULL если нет. */
static const char *find_str(const char *s, const char *end, const char *needle) {
    size_t nl = strlen(needle);
    if (nl == 0) return s;
    for (; s + nl <= end; s++) if (memcmp(s, needle, nl) == 0) return s;
    return NULL;
}
static void skip_ws(PS *p) {
    while (p->p < p->end) { char c=*p->p; if (c==' '||c=='\t'||c=='\r'||c=='\n') p->p++; else break; }
}
/* Пропуск пролога/комментариев/инструкций/DOCTYPE между элементами. */
static void skip_misc(PS *p) {
    for (;;) {
        skip_ws(p);
        if (p->end - p->p >= 4 && memcmp(p->p,"<!--",4)==0) {
            const char *e = find_str(p->p+4, p->end, "-->"); p->p = e ? e+3 : p->end;
        } else if (p->end - p->p >= 2 && p->p[1]=='?') {        /* <?xml?> / PI */
            const char *e = find_str(p->p+2, p->end, "?>"); p->p = e ? e+2 : p->end;
        } else if (p->end - p->p >= 2 && p->p[1]=='!') {        /* <!DOCTYPE ...> */
            const char *e = find_char(p->p+2, p->end, '>'); p->p = e ? e+1 : p->end;
        } else break;
    }
}
/* Имя тега/атрибута: до пробела/>/'/'/=/?. */
static char *read_name(PS *p) {
    const char *s = p->p;
    while (p->p < p->end) {
        char c=*p->p;
        if (c==' '||c=='\t'||c=='\r'||c=='\n'||c=='>'||c=='/'||c=='='||c=='?') break;
        p->p++;
    }
    return arena_strndup(p->a, s, (size_t)(p->p - s));
}
/* Раскодировать одну сущность начиная с '&' (append в out, продвинуть p). */
static void decode_entity(PS *p, Buf *out) {
    const char *semi = find_char(p->p+1, p->end, ';');
    if (!semi || (size_t)(semi - p->p) > 12) { buf_put(out,'&'); p->p++; return; }
    const char *e = p->p+1; size_t n = (size_t)(semi - e);
    if      (n==2 && memcmp(e,"lt",2)==0)   buf_put(out,'<');
    else if (n==2 && memcmp(e,"gt",2)==0)   buf_put(out,'>');
    else if (n==3 && memcmp(e,"amp",3)==0)  buf_put(out,'&');
    else if (n==4 && memcmp(e,"quot",4)==0) buf_put(out,'"');
    else if (n==4 && memcmp(e,"apos",4)==0) buf_put(out,'\'');
    else if (n>=2 && e[0]=='#') {
        unsigned cp = (e[1]=='x'||e[1]=='X') ? (unsigned)strtoul(e+2,NULL,16)
                                             : (unsigned)strtoul(e+1,NULL,10);
        buf_put_cp(out, cp);
    } else { buf_put(out,'&'); buf_putn(out,e,n); buf_put(out,';'); }   /* неизвестная — как есть */
    p->p = semi+1;
}
/* Прочитать значение в кавычках q, раскодировать сущности → строка на арене.
 * tmp — общий переиспользуемый буфер: сбрасываем его длину перед накоплением. */
static char *read_quoted(PS *p, char q, Buf *tmp) {
    tmp->n = 0;
    while (p->p < p->end && *p->p != q) {
        if (*p->p == '&') decode_entity(p, tmp);
        else { buf_put(tmp, *p->p); p->p++; }
    }
    if (p->p < p->end && *p->p == q) p->p++;
    return arena_strndup(p->a, tmp->d ? tmp->d : "", tmp->n);
}

/* Копирует накопленные атрибуты из временного вектора av в массив на арене
 * node->attrs и освобождает malloc-буфер вектора. */
static void finalize_attrs(XmlNode *node, PtrVec *av, Arena *a) {
    if (av->n) {
        node->attrs = arena_alloc(a, av->n * sizeof(XmlAttr));
        for (size_t i=0;i<av->n;i++) node->attrs[i] = *(XmlAttr*)av->items[i];
        node->nattrs = av->n;
    }
    free(av->items);
}

/* Разбор одного элемента начиная с '<' (рекурсивно для вложенных).
 * Две фазы: сначала имя+атрибуты до '>' или '/>', затем содержимое до </tag>.
 * tmp переиспользуется для значений атрибутов; текст и kids копятся в свои буферы. */
static XmlNode *parse_element(PS *p, XmlNode *parent, Buf *tmp) {
    if (p->p >= p->end || *p->p != '<') return NULL;
    p->p++;                                   /* '<' */
    XmlNode *node = arena_calloc(p->a, sizeof(XmlNode));
    node->parent = parent;
    node->name   = read_name(p);
    node->text   = "";

    /* атрибуты */
    PtrVec av = {0};
    for (;;) {
        skip_ws(p);
        if (p->p >= p->end) { finalize_attrs(node,&av,p->a); return node; }
        char c = *p->p;
        if (c == '>') { p->p++; break; }
        if (c == '/') {                       /* самозакрывающийся '/>' */
            p->p++; if (p->p < p->end && *p->p=='>') p->p++;
            finalize_attrs(node,&av,p->a);
            return node;
        }
        char *an = read_name(p);
        skip_ws(p);
        char *aval = "";
        if (p->p < p->end && *p->p=='=') {
            p->p++; skip_ws(p);
            if (p->p < p->end && (*p->p=='"' || *p->p=='\'')) {
                char q=*p->p; p->p++;
                aval = read_quoted(p, q, tmp);
            }
        }
        XmlAttr *at = arena_alloc(p->a, sizeof(XmlAttr));
        at->name = an; at->value = aval;
        pv_push(&av, at);
    }
    finalize_attrs(node, &av, p->a);

    /* содержимое */
    PtrVec kv = {0};
    Buf textb = {0};
    for (;;) {
        if (p->p >= p->end) break;
        if (*p->p == '<') {
            if (p->end - p->p >= 2 && p->p[1]=='/') {              /* конец тега */
                const char *gt = find_char(p->p, p->end, '>');
                p->p = gt ? gt+1 : p->end;
                break;
            } else if (p->end - p->p >= 9 && memcmp(p->p,"<![CDATA[",9)==0) {
                const char *e = find_str(p->p+9, p->end, "]]>");
                const char *de = e ? e : p->end;
                buf_putn(&textb, p->p+9, (size_t)(de - (p->p+9)));
                p->p = e ? e+3 : p->end;
            } else if (p->end - p->p >= 4 && memcmp(p->p,"<!--",4)==0) {
                const char *e = find_str(p->p+4, p->end, "-->");
                p->p = e ? e+3 : p->end;
            } else if (p->end - p->p >= 2 && (p->p[1]=='?' || p->p[1]=='!')) {
                const char *gt = find_char(p->p, p->end, '>');
                p->p = gt ? gt+1 : p->end;
            } else {                                               /* дочерний элемент */
                XmlNode *kid = parse_element(p, node, tmp);
                if (!kid) break;
                pv_push(&kv, kid);
            }
        } else {                                                   /* текстовый отрезок */
            while (p->p < p->end && *p->p != '<') {
                if (*p->p == '&') decode_entity(p, &textb);
                else { buf_put(&textb, *p->p); p->p++; }
            }
        }
    }
    node->text = textb.n ? arena_strndup(p->a, textb.d, textb.n) : "";
    free(textb.d);
    if (kv.n) {
        node->kids = arena_alloc(p->a, kv.n * sizeof(XmlNode*));
        memcpy(node->kids, kv.items, kv.n * sizeof(XmlNode*));
        node->nkids = kv.n;
    }
    free(kv.items);
    return node;
}

XmlNode *xml_parse(Arena *a, const char *src, size_t len) {
    if (!src) return NULL;
    PS p = { src, src + len, a };
    Buf tmp = {0};
    skip_misc(&p);
    XmlNode *root = (p.p < p.end && *p.p == '<') ? parse_element(&p, NULL, &tmp) : NULL;
    free(tmp.d);
    return root;
}

/* ── Навигация ── */
const char *xml_local(const char *name) {
    if (!name) return "";
    const char *c = strrchr(name, ':');
    return c ? c+1 : name;
}
/* Сравнение имён по local-name — namespace-префикс игнорируется. */
static int name_eq(const char *a, const char *b) {
    return strcmp(xml_local(a), xml_local(b)) == 0;
}
XmlNode *xml_child(const XmlNode *node, const char *name) {
    if (!node) return NULL;
    for (size_t i=0;i<node->nkids;i++)
        if (name_eq(node->kids[i]->name, name)) return node->kids[i];
    return NULL;
}
/* Обход поддерева в глубину: cnt — сквозной счётчик совпадений (в т.ч. сверх max,
 * тогда в out записываются только первые max). Возвращает обновлённый счётчик. */
static size_t find_all_rec(const XmlNode *n, const char *name, XmlNode **out, size_t max, size_t cnt) {
    for (size_t i=0;i<n->nkids;i++) {
        XmlNode *k = n->kids[i];
        if (name_eq(k->name, name)) { if (cnt < max) out[cnt] = k; cnt++; }
        cnt = find_all_rec(k, name, out, max, cnt);
    }
    return cnt;
}
size_t xml_find_all(const XmlNode *root, const char *name, XmlNode **out, size_t max) {
    if (!root) return 0;
    return find_all_rec(root, name, out, max, 0);
}
const char *xml_attr(const XmlNode *node, const char *name, const char *def) {
    if (!node) return def;
    for (size_t i=0;i<node->nattrs;i++)
        if (name_eq(node->attrs[i].name, name)) return node->attrs[i].value;
    return def;
}
const char *xml_text(const XmlNode *node, const char *def) {
    return (node && node->text && node->text[0]) ? node->text : def;
}
