/* xml.h — минимальный XML-парсер DataFlow OS (для xml/soap-коннекторов).
 *
 * Recursive-descent разбор в дерево XmlNode на арене. Поддержано: элементы с
 * атрибутами, самозакрывающиеся теги, текст, CDATA, декодирование сущностей
 * (&lt; &gt; &amp; &quot; &apos; &#NN; &#xHH;). Пропускаются: пролог <?xml?>,
 * комментарии <!-- -->, DOCTYPE/инструкции <!...>/<?...?>. Namespace-префиксы
 * хранятся как есть; сравнение имён — по local-name (часть после ':').
 *
 * Сборка XML делается в самих коннекторах (CurlBuf + локальный xml_escape) —
 * как и JSON-сборка в json_http/siebel. Здесь только разбор. */
#pragma once
#include "arena.h"
#include <stddef.h>
#include <stdbool.h>

/* Атрибут элемента (имя=значение, значение уже с раскодированными сущностями). */
typedef struct { const char *name; const char *value; } XmlAttr;

/* Узел дерева. text — конкатенация прямого текста элемента (раскодированного);
 * "" если текста нет. kids/attrs — массивы на арене. */
typedef struct XmlNode XmlNode;
struct XmlNode {
    const char *name;            /* имя тега (с префиксом, если был) */
    const char *text;            /* прямой текст элемента (никогда не NULL) */
    XmlAttr    *attrs; size_t nattrs;
    XmlNode   **kids;  size_t nkids;
    XmlNode    *parent;          /* NULL у корня */
};

/* Разбирает XML (len байт) в дерево на арене a; возвращает корневой элемент
 * или NULL при фатальной ошибке (нет корневого элемента). */
XmlNode *xml_parse(Arena *a, const char *src, size_t len);

/* local-name имени: указатель на часть после последнего ':' (или само имя). */
const char *xml_local(const char *name);

/* Первый ПРЯМОЙ потомок node с local-name == name (NULL если нет). */
XmlNode *xml_child(const XmlNode *node, const char *name);

/* Рекурсивно собрать в out[] (до max) все узлы поддерева root (исключая сам
 * root) с local-name == name. Возвращает число найденных (может быть > max,
 * тогда записаны первые max). */
size_t xml_find_all(const XmlNode *root, const char *name, XmlNode **out, size_t max);

/* Значение атрибута node с local-name == name, либо def. */
const char *xml_attr(const XmlNode *node, const char *name, const char *def);

/* Текст узла (node->text), либо def если пусто/NULL. */
const char *xml_text(const XmlNode *node, const char *def);
