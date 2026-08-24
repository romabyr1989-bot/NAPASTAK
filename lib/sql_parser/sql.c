/*
 * sql.c — мини-движок SQL: лексер, рекурсивно-нисходящий парсер и планировщик.
 *
 * Преобразует текст запроса в AST (Stmt/Expr), а затем в дерево плана
 * исполнения (PlanNode). Поддерживается подмножество SQL: SELECT с JOIN,
 * WHERE/GROUP BY/HAVING/ORDER BY/LIMIT, оконные функции, CTE (WITH),
 * множественные операции (UNION/INTERSECT/EXCEPT), а также UPDATE/DELETE
 * и управление транзакциями. Все узлы AST/плана размещаются в Arena.
 */
#include "sql.h"
#include "../core/json.h"   /* json_fmt_double — единый формат double */
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

/* ── Lexer ── */
typedef enum {
    TK_EOF, TK_IDENT, TK_NUM_INT, TK_NUM_FLOAT, TK_STRING,
    TK_STAR, TK_COMMA, TK_DOT, TK_LPAREN, TK_RPAREN,
    TK_EQ, TK_NE, TK_LT, TK_LE, TK_GT, TK_GE,
    TK_AND, TK_OR, TK_NOT,
    TK_PLUS, TK_MINUS, TK_SLASH, TK_PERCENT, TK_CONCAT,
    TK_SEMICOLON, TK_ERROR,
    /* keywords */
    TK_SELECT, TK_FROM, TK_WHERE, TK_GROUP, TK_BY, TK_HAVING,
    TK_ORDER, TK_LIMIT, TK_OFFSET, TK_AS, TK_JOIN, TK_LEFT,
    TK_RIGHT, TK_FULL, TK_INNER, TK_OUTER, TK_ON, TK_DISTINCT,
    TK_INSERT, TK_INTO, TK_VALUES,
    TK_NULL, TK_TRUE, TK_FALSE,
    TK_IS, TK_LIKE, TK_ILIKE, TK_IN, TK_BETWEEN, TK_ASC, TK_DESC,
    TK_CASE, TK_WHEN, TK_THEN, TK_ELSE, TK_END,
    TK_UNION, TK_INTERSECT, TK_EXCEPT, TK_ALL, TK_CROSS,
    TK_WITH, TK_RECURSIVE,
    TK_NULLS, TK_FIRST, TK_LAST,
    TK_EXISTS, TK_CAST,
    TK_OVER, TK_PARTITION, TK_ROWS, TK_RANGE, TK_UNBOUNDED,
    TK_PRECEDING, TK_FOLLOWING, TK_CURRENT, TK_ROW,
    TK_UPDATE, TK_SET, TK_DELETE,
    TK_BEGIN, TK_COMMIT, TK_ROLLBACK, TK_TRANSACTION
} TkType;

/* Лексический токен: тип + ссылка на исходный текст (start/len, без копии) и числовое значение. */
typedef struct { TkType type; const char *start; size_t len; int64_t ival; double fval; } Token;

/* Состояние лексера: позиция в исходнике + один токен заглядывания вперёд (peek). */
typedef struct {
    const char *src; size_t pos, len;
    Token       peek;
    bool        has_peek;
    Arena      *a;
    const char *error;   /* первая обнаруженная синтаксическая ошибка (NULL = нет) */
} Lexer;

/* Зафиксировать первую синтаксическую ошибку парсинга (последующие не перетирают). */
static void lex_set_error(Lexer *l, const char *msg) {
    if (!l->error) l->error = msg;
}

/* Таблица ключевых слов (регистронезависимо); терминируется записью {NULL, TK_EOF}. */
static struct { const char *kw; TkType tk; } KEYWORDS[] = {
    {"SELECT",TK_SELECT},{"FROM",TK_FROM},{"WHERE",TK_WHERE},{"GROUP",TK_GROUP},
    {"BY",TK_BY},{"HAVING",TK_HAVING},{"ORDER",TK_ORDER},{"LIMIT",TK_LIMIT},
    {"OFFSET",TK_OFFSET},{"AS",TK_AS},{"JOIN",TK_JOIN},{"LEFT",TK_LEFT},
    {"RIGHT",TK_RIGHT},{"FULL",TK_FULL},{"INNER",TK_INNER},{"OUTER",TK_OUTER},
    {"ON",TK_ON},{"DISTINCT",TK_DISTINCT},{"INSERT",TK_INSERT},{"INTO",TK_INTO},
    {"VALUES",TK_VALUES},{"NULL",TK_NULL},{"TRUE",TK_TRUE},{"FALSE",TK_FALSE},
    {"IS",TK_IS},{"LIKE",TK_LIKE},{"ILIKE",TK_ILIKE},{"IN",TK_IN},
    {"BETWEEN",TK_BETWEEN},{"AND",TK_AND},{"OR",TK_OR},{"NOT",TK_NOT},
    {"ASC",TK_ASC},{"DESC",TK_DESC},
    {"CASE",TK_CASE},{"WHEN",TK_WHEN},{"THEN",TK_THEN},{"ELSE",TK_ELSE},{"END",TK_END},
    {"UNION",TK_UNION},{"INTERSECT",TK_INTERSECT},{"EXCEPT",TK_EXCEPT},{"ALL",TK_ALL},
    {"CROSS",TK_CROSS},{"WITH",TK_WITH},{"RECURSIVE",TK_RECURSIVE},
    {"NULLS",TK_NULLS},{"FIRST",TK_FIRST},{"LAST",TK_LAST},
    {"EXISTS",TK_EXISTS},{"CAST",TK_CAST},
    {"OVER",TK_OVER},{"PARTITION",TK_PARTITION},{"ROWS",TK_ROWS},{"RANGE",TK_RANGE},
    {"UNBOUNDED",TK_UNBOUNDED},{"PRECEDING",TK_PRECEDING},{"FOLLOWING",TK_FOLLOWING},
    {"CURRENT",TK_CURRENT},{"ROW",TK_ROW},
    {"UPDATE",TK_UPDATE},{"SET",TK_SET},{"DELETE",TK_DELETE},
    {"BEGIN",TK_BEGIN},{"COMMIT",TK_COMMIT},{"ROLLBACK",TK_ROLLBACK},
    {"TRANSACTION",TK_TRANSACTION},
    {NULL,TK_EOF}
};

/* Считывает следующий токен из исходника, пропуская пробелы и комментарии. */
static Token lex_next(Lexer *l) {
    while (l->pos < l->len && isspace((unsigned char)l->src[l->pos])) l->pos++;
    if (l->pos >= l->len) return (Token){TK_EOF};
    char c = l->src[l->pos];
    switch(c) {
        case '*': l->pos++; return (Token){TK_STAR};
        case ',': l->pos++; return (Token){TK_COMMA};
        case '.': l->pos++; return (Token){TK_DOT};
        case '(': l->pos++; return (Token){TK_LPAREN};
        case ')': l->pos++; return (Token){TK_RPAREN};
        case '+': l->pos++; return (Token){TK_PLUS};
        case '-': l->pos++;
            /* -- comment: skip to end of line */
            if (l->pos < l->len && l->src[l->pos] == '-') {
                while (l->pos < l->len && l->src[l->pos] != '\n') l->pos++;
                return lex_next(l);
            }
            return (Token){TK_MINUS};
        case '/': l->pos++;
            /* /* block comment */
            if (l->pos < l->len && l->src[l->pos] == '*') {
                l->pos++;
                while (l->pos + 1 < l->len && !(l->src[l->pos]=='*' && l->src[l->pos+1]=='/')) l->pos++;
                if (l->pos + 1 < l->len) l->pos += 2;
                return lex_next(l);
            }
            return (Token){TK_SLASH};
        case '%': l->pos++; return (Token){TK_PERCENT};
        case ';': l->pos++; return (Token){TK_SEMICOLON};
        case '=': l->pos++; return (Token){TK_EQ};
        case '<': l->pos++;
            if (l->pos<l->len&&l->src[l->pos]=='='){l->pos++;return (Token){TK_LE};}
            if (l->pos<l->len&&l->src[l->pos]=='>'){l->pos++;return (Token){TK_NE};}
            return (Token){TK_LT};
        case '>': l->pos++;
            if (l->pos<l->len&&l->src[l->pos]=='='){l->pos++;return (Token){TK_GE};}
            return (Token){TK_GT};
        case '!': l->pos++;
            if (l->pos<l->len&&l->src[l->pos]=='='){l->pos++;return (Token){TK_NE};}
            return (Token){TK_ERROR};
        case '|': l->pos++;
            if (l->pos<l->len&&l->src[l->pos]=='|'){l->pos++;return (Token){TK_CONCAT};}
            return (Token){TK_ERROR};
    }
    /* quoted identifier */
    if (c == '"' || c == '`') {
        char close = (c == '"') ? '"' : '`';
        l->pos++;
        size_t s = l->pos;
        while (l->pos < l->len && l->src[l->pos] != close) l->pos++;
        Token t = {TK_IDENT, l->src+s, l->pos-s};
        if (l->pos < l->len) l->pos++;
        return t;
    }
    /* string literal — handles '' escape */
    if (c == '\'') {
        l->pos++;
        size_t s = l->pos;
        while (l->pos < l->len) {
            if (l->src[l->pos] == '\'') {
                if (l->pos+1 < l->len && l->src[l->pos+1] == '\'') { l->pos += 2; continue; }
                break;
            }
            l->pos++;
        }
        Token t = {TK_STRING, l->src+s, l->pos-s};
        if (l->pos < l->len) l->pos++;
        return t;
    }
    /* number */
    if (isdigit((unsigned char)c)) {
        size_t s = l->pos;
        bool has_dot = false;
        while (l->pos<l->len && (isdigit((unsigned char)l->src[l->pos])||l->src[l->pos]=='.')) {
            if (l->src[l->pos]=='.') { if (has_dot) break; has_dot=true; }
            l->pos++;
        }
        /* optional E notation */
        if (l->pos < l->len && (l->src[l->pos]=='e'||l->src[l->pos]=='E')) {
            has_dot = true; l->pos++;
            if (l->pos < l->len && (l->src[l->pos]=='+'||l->src[l->pos]=='-')) l->pos++;
            while (l->pos < l->len && isdigit((unsigned char)l->src[l->pos])) l->pos++;
        }
        Token t = {has_dot?TK_NUM_FLOAT:TK_NUM_INT, l->src+s, l->pos-s};
        if (has_dot) t.fval = strtod(l->src+s, NULL);
        else t.ival = strtoll(l->src+s, NULL, 10);
        return t;
    }
    /* identifier or keyword */
    if (isalpha((unsigned char)c) || c == '_') {
        size_t s = l->pos;
        while (l->pos<l->len && (isalnum((unsigned char)l->src[l->pos])||l->src[l->pos]=='_')) l->pos++;
        size_t klen = l->pos - s;
        char up[128]; size_t ul = klen<127?klen:127;
        for (size_t i=0;i<ul;i++) up[i]=toupper((unsigned char)l->src[s+i]);
        up[ul]='\0';
        for (int i=0; KEYWORDS[i].kw; i++)
            if (strcmp(KEYWORDS[i].kw, up)==0) return (Token){KEYWORDS[i].tk, l->src+s, klen};
        return (Token){TK_IDENT, l->src+s, klen};
    }
    l->pos++; return (Token){TK_ERROR};
}

/* Заглянуть на текущий токен, не сдвигая позицию (результат кэшируется). */
static Token lex_peek(Lexer *l) {
    if (!l->has_peek) { l->peek = lex_next(l); l->has_peek = true; }
    return l->peek;
}
/* Вернуть текущий токен и продвинуться дальше. */
static Token lex_consume(Lexer *l) {
    if (l->has_peek) { l->has_peek=false; return l->peek; }
    return lex_next(l);
}
/* Если текущий токен имеет тип t — поглотить его и вернуть true, иначе false. */
static bool lex_eat(Lexer *l, TkType t) {
    if (lex_peek(l).type == t) { lex_consume(l); return true; }
    return false;
}
static bool lex_peek_is(Lexer *l, TkType t) {
    return lex_peek(l).type == t;
}

/* forward declarations */
static Expr         *parse_expr(Lexer *l, int prec);
static Stmt         *parse_stmt(Lexer *l);
static WinFrameBound parse_frame_bound(Lexer *l);
static WindowSpec   *parse_window_spec(Lexer *l);

/* ── Window spec parser ── */
/* Разбирает одну границу оконного фрейма: UNBOUNDED PRECEDING/FOLLOWING,
   CURRENT ROW или «N PRECEDING/FOLLOWING». */
static WinFrameBound parse_frame_bound(Lexer *l) {
    WinFrameBound b = {0};
    Token t = lex_peek(l);
    if (t.type == TK_UNBOUNDED) {
        lex_consume(l);
        if (lex_eat(l, TK_FOLLOWING)) b.kind = WBOUND_UNBOUNDED_FOLL;
        else { lex_eat(l, TK_PRECEDING); b.kind = WBOUND_UNBOUNDED_PREC; }
    } else if (t.type == TK_CURRENT) {
        lex_consume(l);
        lex_eat(l, TK_ROW);
        b.kind = WBOUND_CURRENT_ROW;
    } else {
        Token n = lex_consume(l);
        b.n = (n.type == TK_NUM_INT) ? n.ival : 1;
        if (lex_eat(l, TK_FOLLOWING)) b.kind = WBOUND_N_FOLL;
        else { lex_eat(l, TK_PRECEDING); b.kind = WBOUND_N_PREC; }
    }
    return b;
}

/* Разбирает содержимое OVER (...): PARTITION BY, ORDER BY и спецификацию фрейма ROWS/RANGE. */
static WindowSpec *parse_window_spec(Lexer *l) {
    lex_eat(l, TK_LPAREN);
    WindowSpec *ws = arena_calloc(l->a, sizeof(WindowSpec));

    /* PARTITION BY col, ... */
    if (lex_peek_is(l, TK_PARTITION)) {
        lex_consume(l); lex_eat(l, TK_BY);
        int cap = 4;
        ws->partition_by = arena_alloc(l->a, cap * sizeof(Expr*));
        do {
            if (ws->npartition == cap) {
                cap *= 2;
                Expr **nb = arena_alloc(l->a, cap*sizeof(Expr*));
                memcpy(nb, ws->partition_by, ws->npartition*sizeof(Expr*));
                ws->partition_by = nb;
            }
            ws->partition_by[ws->npartition++] = parse_expr(l, 0);
        } while (lex_eat(l, TK_COMMA));
    }

    /* ORDER BY col [ASC|DESC], ... */
    if (lex_peek_is(l, TK_ORDER)) {
        lex_consume(l); lex_eat(l, TK_BY);
        int cap = 4;
        ws->order_by = arena_alloc(l->a, cap * sizeof(OrderItem));
        do {
            if (ws->norder == cap) {
                cap *= 2;
                OrderItem *nb = arena_alloc(l->a, cap*sizeof(OrderItem));
                memcpy(nb, ws->order_by, ws->norder*sizeof(OrderItem));
                ws->order_by = nb;
            }
            OrderItem *oi = &ws->order_by[ws->norder++];
            memset(oi, 0, sizeof(*oi));
            oi->expr = parse_expr(l, 0);
            oi->nulls_last = true;
            if (lex_peek_is(l, TK_DESC)) { lex_consume(l); oi->desc = true; }
            else lex_eat(l, TK_ASC);
        } while (lex_eat(l, TK_COMMA));
    }

    /* ROWS/RANGE [BETWEEN bound AND bound | bound] */
    if (lex_peek_is(l, TK_ROWS) || lex_peek_is(l, TK_RANGE)) {
        ws->has_frame = true;
        ws->frame_type = lex_peek_is(l, TK_ROWS) ? WF_ROWS : WF_RANGE;
        lex_consume(l);
        if (lex_eat(l, TK_BETWEEN)) {
            ws->frame_start = parse_frame_bound(l);
            lex_eat(l, TK_AND);
            ws->frame_end = parse_frame_bound(l);
        } else {
            ws->frame_start = parse_frame_bound(l);
            ws->frame_end = (WinFrameBound){WBOUND_CURRENT_ROW, 0};
        }
    }

    lex_eat(l, TK_RPAREN);
    return ws;
}

/* ── Helper: deep scan for EXPR_WINDOW in expression tree ── */
static bool has_window_expr(Expr *e) {
    if (!e) return false;
    if (e->type == EXPR_WINDOW) return true;
    if (e->type == EXPR_ALIAS)  return has_window_expr(e->expr);
    if (e->type == EXPR_BINOP)  return has_window_expr(e->left) || has_window_expr(e->right);
    if (e->type == EXPR_UNOP)   return has_window_expr(e->left);
    if (e->type == EXPR_FUNC) {
        for (int i = 0; i < e->nargs; i++) if (has_window_expr(e->args[i])) return true;
    }
    return false;
}

/* ── Parser ── */
/* Разбирает первичное выражение: CASE, EXISTS, CAST, литералы, унарные операторы,
   скобки/подзапрос, идентификатор/обращение к столбцу или вызов функции (в т.ч. оконной). */
static Expr *parse_primary(Lexer *l) {
    Token t = lex_peek(l);
    Expr *e = arena_calloc(l->a, sizeof(Expr));

    /* CASE WHEN ... THEN ... [ELSE ...] END */
    if (t.type == TK_CASE) {
        lex_consume(l);
        e->type = EXPR_CASE;
        /* simple CASE x WHEN val ... vs searched CASE WHEN cond ... */
        if (!lex_peek_is(l, TK_WHEN)) {
            e->case_op = parse_expr(l, 0);
        }
        int cap = 4;
        e->whens = arena_alloc(l->a, cap * sizeof(Expr*));
        e->thens = arena_alloc(l->a, cap * sizeof(Expr*));
        e->nwhens = 0;
        while (lex_eat(l, TK_WHEN)) {
            if (e->nwhens == cap) {
                cap *= 2;
                Expr **nw = arena_alloc(l->a, cap*sizeof(Expr*));
                Expr **nt = arena_alloc(l->a, cap*sizeof(Expr*));
                memcpy(nw, e->whens, e->nwhens*sizeof(Expr*));
                memcpy(nt, e->thens, e->nwhens*sizeof(Expr*));
                e->whens = nw; e->thens = nt;
            }
            e->whens[e->nwhens] = parse_expr(l, 0);
            lex_eat(l, TK_THEN);
            e->thens[e->nwhens] = parse_expr(l, 0);
            e->nwhens++;
        }
        if (lex_eat(l, TK_ELSE)) e->else_expr = parse_expr(l, 0);
        lex_eat(l, TK_END);
        return e;
    }

    /* EXISTS (subquery) */
    if (t.type == TK_EXISTS) {
        lex_consume(l);
        lex_eat(l, TK_LPAREN);
        Stmt *sub = parse_stmt(l);
        lex_eat(l, TK_RPAREN);
        e->type = EXPR_FUNC;
        e->func_name = "exists";
        e->args = arena_alloc(l->a, sizeof(Expr*));
        Expr *se = arena_calloc(l->a, sizeof(Expr));
        se->type = EXPR_SUBQUERY; se->subq = sub;
        e->args[0] = se; e->nargs = 1;
        return e;
    }

    /* CAST(expr AS type) */
    if (t.type == TK_CAST) {
        lex_consume(l);
        lex_eat(l, TK_LPAREN);
        Expr *inner = parse_expr(l, 0);
        lex_eat(l, TK_AS);
        /* consume type name (may be multi-word: DOUBLE PRECISION, CHARACTER VARYING) */
        char type_buf[64]; type_buf[0]='\0';
        while (lex_peek_is(l, TK_IDENT)) {
            Token tt = lex_peek(l);
            if (tt.type != TK_IDENT) break;
            lex_consume(l);
            if (type_buf[0]) strncat(type_buf, "_", sizeof(type_buf)-strlen(type_buf)-1);
            size_t tl = tt.len < 32 ? tt.len : 31;
            strncat(type_buf, tt.start, tl < sizeof(type_buf)-strlen(type_buf)-1 ? tl : sizeof(type_buf)-strlen(type_buf)-1);
            break; /* just consume first word for simplicity */
        }
        lex_eat(l, TK_RPAREN);
        e->type = EXPR_FUNC;
        e->func_name = "cast";
        e->args = arena_alloc(l->a, 2*sizeof(Expr*));
        e->args[0] = inner;
        Expr *te = arena_calloc(l->a, sizeof(Expr));
        te->type = EXPR_LITERAL_STR;
        te->sval = arena_strdup(l->a, type_buf);
        e->args[1] = te; e->nargs = 2;
        return e;
    }

    lex_consume(l);

    switch (t.type) {
        case TK_NUM_INT:   e->type=EXPR_LITERAL_INT; e->ival=t.ival; e->litstr=arena_strndup(l->a,t.start,t.len); return e;
        case TK_NUM_FLOAT: e->type=EXPR_LITERAL_FLOAT; e->fval=t.fval; return e;
        case TK_STRING:    e->type=EXPR_LITERAL_STR; e->sval=arena_strndup(l->a,t.start,t.len); return e;
        case TK_NULL:      e->type=EXPR_LITERAL_NULL; return e;
        case TK_TRUE:      e->type=EXPR_LITERAL_BOOL; e->bval=true; return e;
        case TK_FALSE:     e->type=EXPR_LITERAL_BOOL; e->bval=false; return e;
        case TK_STAR:      e->type=EXPR_STAR; return e;
        case TK_MINUS: {
            Expr *inner = parse_primary(l);
            e->type=EXPR_UNOP; e->op=OP_SUB; e->left=inner; return e;
        }
        case TK_NOT: {
            Expr *inner = parse_primary(l);
            e->type=EXPR_UNOP; e->op=OP_NOT; e->left=inner; return e;
        }
        case TK_LPAREN: {
            /* subquery or parenthesized expression */
            if (lex_peek_is(l, TK_SELECT) || lex_peek_is(l, TK_WITH)) {
                Stmt *sub = parse_stmt(l);
                lex_eat(l, TK_RPAREN);
                e->type = EXPR_SUBQUERY; e->subq = sub; return e;
            }
            Expr *inner = parse_expr(l, 0);
            lex_eat(l, TK_RPAREN);
            return inner;
        }
        case TK_IDENT: {
            char *name = arena_strndup(l->a, t.start, t.len);
            /* function call? */
            if (lex_peek_is(l, TK_LPAREN)) {
                lex_consume(l);
                e->type = EXPR_FUNC; e->func_name = name;
                int cap=4; e->args=arena_alloc(l->a,cap*sizeof(Expr*)); e->nargs=0;
                /* optional leading DISTINCT in aggregate args, e.g. COUNT(DISTINCT col) */
                if (lex_eat(l, TK_DISTINCT)) e->func_distinct = true;
                if (!lex_peek_is(l, TK_RPAREN) && !lex_peek_is(l, TK_STAR)) {
                    do {
                        if (lex_peek_is(l, TK_RPAREN)) break;
                        if (e->nargs==cap){cap*=2;Expr**nb=arena_alloc(l->a,cap*sizeof(Expr*));memcpy(nb,e->args,e->nargs*sizeof(Expr*));e->args=nb;}
                        e->args[e->nargs++]=parse_expr(l,0);
                    } while (lex_eat(l,TK_COMMA));
                } else if (lex_peek_is(l, TK_STAR)) {
                    /* COUNT(*) */
                    lex_consume(l);
                    Expr *star=arena_calloc(l->a,sizeof(Expr)); star->type=EXPR_STAR;
                    e->args[e->nargs++]=star;
                }
                lex_eat(l, TK_RPAREN);
                /* window function: func(...) OVER (...) */
                if (lex_peek_is(l, TK_OVER)) {
                    lex_consume(l);
                    e->type = EXPR_WINDOW;
                    e->win_spec = parse_window_spec(l);
                }
                return e;
            }
            /* table.col or col */
            e->type = EXPR_COL;
            if (lex_peek_is(l, TK_DOT)) {
                lex_consume(l);
                Token col = lex_consume(l);
                e->table = name;
                e->name  = arena_strndup(l->a, col.start, col.len);
            } else {
                e->name = name;
            }
            return e;
        }
        default:
            /* Ожидалось первичное выражение, но встретился неподходящий токен
               (ключевое слово-разделитель, EOF, мусор и т.п.) — это структурная ошибка. */
            lex_set_error(l, "syntax error: expected expression");
            e->type = EXPR_LITERAL_NULL; return e;
    }
}

/* Приоритет бинарного оператора для алгоритма precedence climbing; -1 — не оператор. */
static int binop_prec(TkType t) {
    switch(t) {
        case TK_OR:  return 1;
        case TK_AND: return 2;
        case TK_NOT: return 3;
        case TK_EQ: case TK_NE: case TK_LT: case TK_LE: case TK_GT: case TK_GE:
        case TK_LIKE: case TK_ILIKE: case TK_IS: case TK_IN: case TK_BETWEEN: return 4;
        case TK_CONCAT: return 5;
        case TK_PLUS: case TK_MINUS: return 6;
        case TK_STAR: case TK_SLASH: case TK_PERCENT: return 7;
        default: return -1;
    }
}

/* Сопоставляет тип токена-оператора с соответствующим OpType узла BINOP. */
static OpType tktype_to_op(TkType t) {
    switch(t) {
        case TK_EQ:    return OP_EQ;   case TK_NE:    return OP_NE;
        case TK_LT:    return OP_LT;   case TK_LE:    return OP_LE;
        case TK_GT:    return OP_GT;   case TK_GE:    return OP_GE;
        case TK_AND:   return OP_AND;  case TK_OR:    return OP_OR;
        case TK_PLUS:  return OP_ADD;  case TK_MINUS: return OP_SUB;
        case TK_STAR:  return OP_MUL;  case TK_SLASH: return OP_DIV;
        case TK_PERCENT: return OP_MOD;
        case TK_LIKE:  return OP_LIKE; case TK_ILIKE: return OP_ILIKE;
        case TK_IN:    return OP_IN;   case TK_CONCAT: return OP_CONCAT;
        default: return OP_EQ;
    }
}

/* Разбирает выражение методом precedence climbing: начинает с первичного,
   затем присоединяет бинарные операторы с приоритетом >= min_prec, а также
   постфиксные конструкции IS [NOT] NULL, [NOT] LIKE/ILIKE/BETWEEN/IN. */
static Expr *parse_expr(Lexer *l, int min_prec) {
    Expr *lhs = parse_primary(l);
    for (;;) {
        Token pt = lex_peek(l);

        /* IS [NOT] NULL */
        if (pt.type == TK_IS) {
            lex_consume(l);
            bool negate = lex_eat(l, TK_NOT);
            lex_eat(l, TK_NULL);
            Expr *e = arena_calloc(l->a, sizeof(Expr));
            e->type = EXPR_UNOP;
            e->op   = negate ? OP_IS_NOT_NULL : OP_IS_NULL;
            e->left = lhs; lhs = e; continue;
        }

        /* NOT LIKE / NOT ILIKE / NOT BETWEEN / NOT IN */
        if (pt.type == TK_NOT) {
            lex_consume(l);
            Token after = lex_peek(l);
            if (after.type == TK_LIKE || after.type == TK_ILIKE ||
                after.type == TK_BETWEEN || after.type == TK_IN) {
                lex_consume(l);
                Expr *e = arena_calloc(l->a, sizeof(Expr));
                if (after.type == TK_LIKE) {
                    Expr *pat = parse_expr(l, 5);
                    e->type=EXPR_BINOP; e->op=OP_NOT_LIKE; e->left=lhs; e->right=pat;
                } else if (after.type == TK_ILIKE) {
                    Expr *pat = parse_expr(l, 5);
                    e->type=EXPR_BINOP; e->op=OP_NOT_ILIKE; e->left=lhs; e->right=pat;
                } else if (after.type == TK_BETWEEN) {
                    Expr *low = parse_expr(l, 6);
                    lex_eat(l, TK_AND);
                    Expr *high = parse_expr(l, 6);
                    Expr *rng = arena_calloc(l->a, sizeof(Expr));
                    rng->type=EXPR_BINOP; rng->op=OP_AND; rng->left=low; rng->right=high;
                    e->type=EXPR_BINOP; e->op=OP_NOT_BETWEEN; e->left=lhs; e->right=rng;
                } else { /* IN */
                    lex_eat(l, TK_LPAREN);
                    Expr *list = arena_calloc(l->a, sizeof(Expr));
                    list->type = EXPR_LIST;
                    int cap=4; list->items=arena_alloc(l->a,cap*sizeof(Expr*)); list->nitems=0;
                    /* could be subquery */
                    if (lex_peek_is(l, TK_SELECT) || lex_peek_is(l, TK_WITH)) {
                        Stmt *sub = parse_stmt(l);
                        list->type = EXPR_SUBQUERY; list->subq = sub;
                    } else if (!lex_peek_is(l, TK_RPAREN)) {
                        do {
                            if (list->nitems==cap){cap*=2;Expr**nb=arena_alloc(l->a,cap*sizeof(Expr*));memcpy(nb,list->items,list->nitems*sizeof(Expr*));list->items=nb;}
                            list->items[list->nitems++]=parse_expr(l,0);
                        } while (lex_eat(l,TK_COMMA));
                    }
                    lex_eat(l, TK_RPAREN);
                    e->type=EXPR_BINOP; e->op=OP_NOT_IN; e->left=lhs; e->right=list;
                }
                lhs = e; continue;
            }
            /* put NOT back as unary on next expr — re-parse as unary */
            Expr *e = arena_calloc(l->a, sizeof(Expr));
            Expr *rhs = parse_primary(l);
            e->type=EXPR_UNOP; e->op=OP_NOT; e->left=rhs;
            /* wrap: lhs AND NOT rhs? no — this was standalone NOT */
            /* Actually bare NOT consumed ahead of operator — treat as short-circuit */
            lhs = e; continue;
        }

        /* BETWEEN */
        if (pt.type == TK_BETWEEN) {
            lex_consume(l);
            Expr *low  = parse_expr(l, 6);
            lex_eat(l, TK_AND);
            Expr *high = parse_expr(l, 6);
            Expr *e = arena_calloc(l->a, sizeof(Expr));
            Expr *rng = arena_calloc(l->a, sizeof(Expr));
            rng->type=EXPR_BINOP; rng->op=OP_AND; rng->left=low; rng->right=high;
            e->type=EXPR_BINOP; e->op=OP_BETWEEN; e->left=lhs; e->right=rng;
            lhs = e; continue;
        }

        /* IN (list | subquery) */
        if (pt.type == TK_IN) {
            lex_consume(l);
            lex_eat(l, TK_LPAREN);
            Expr *list = arena_calloc(l->a, sizeof(Expr));
            if (lex_peek_is(l, TK_SELECT) || lex_peek_is(l, TK_WITH)) {
                Stmt *sub = parse_stmt(l);
                list->type = EXPR_SUBQUERY; list->subq = sub;
            } else {
                list->type = EXPR_LIST;
                int cap=4; list->items=arena_alloc(l->a,cap*sizeof(Expr*)); list->nitems=0;
                if (!lex_peek_is(l, TK_RPAREN)) {
                    do {
                        if (list->nitems==cap){cap*=2;Expr**nb=arena_alloc(l->a,cap*sizeof(Expr*));memcpy(nb,list->items,list->nitems*sizeof(Expr*));list->items=nb;}
                        list->items[list->nitems++]=parse_expr(l,0);
                    } while (lex_eat(l,TK_COMMA));
                }
            }
            lex_eat(l, TK_RPAREN);
            Expr *e = arena_calloc(l->a, sizeof(Expr));
            e->type=EXPR_BINOP; e->op=OP_IN; e->left=lhs; e->right=list;
            lhs = e; continue;
        }

        int prec = binop_prec(pt.type);
        if (prec < 0 || prec < min_prec) break;
        lex_consume(l);
        Expr *rhs = parse_expr(l, prec + 1);
        Expr *e = arena_calloc(l->a, sizeof(Expr));
        e->type = EXPR_BINOP; e->op = tktype_to_op(pt.type);
        e->left = lhs; e->right = rhs;
        lhs = e;
    }
    return lhs;
}

/* Поглощает идентификатор и возвращает его копию в Arena; "" при несовпадении типа. */
static const char *consume_ident(Lexer *l) {
    Token t = lex_consume(l);
    if (t.type != TK_IDENT) return "";
    return arena_strndup(l->a, t.start, t.len);
}

/* Разбирает один элемент списка SELECT: *, table.*, либо выражение с необязательным алиасом (AS). */
static Expr *parse_select_expr(Lexer *l) {
    if (lex_peek_is(l, TK_STAR)) {
        lex_consume(l);
        Expr *e=arena_calloc(l->a,sizeof(Expr)); e->type=EXPR_STAR; return e;
    }
    /* table.* */
    if (lex_peek_is(l, TK_IDENT)) {
        Lexer save = *l;
        Token id = lex_consume(l);
        if (lex_peek_is(l, TK_DOT)) {
            lex_consume(l);
            if (lex_peek_is(l, TK_STAR)) {
                lex_consume(l);
                Expr *e=arena_calloc(l->a,sizeof(Expr)); e->type=EXPR_STAR;
                e->table = arena_strndup(l->a, id.start, id.len);
                return e;
            }
        }
        *l = save; /* put back */
    }
    Expr *base = parse_expr(l, 0);
    /* optional alias — отсекаем ключевые слова-разделители, чтобы не принять их за алиас */
    TkType pk = lex_peek(l).type;
    if (pk == TK_AS || (pk == TK_IDENT &&
        pk != TK_FROM && pk != TK_WHERE && pk != TK_GROUP &&
        pk != TK_HAVING && pk != TK_ORDER && pk != TK_LIMIT &&
        pk != TK_OFFSET && pk != TK_UNION && pk != TK_INTERSECT &&
        pk != TK_EXCEPT && pk != TK_SEMICOLON)) {
        bool explicit_as = (pk == TK_AS);
        if (explicit_as || pk == TK_IDENT) {
            if (explicit_as) lex_consume(l);
            if (!explicit_as && lex_peek(l).type != TK_IDENT) goto no_alias;
            Token alias = lex_consume(l);
            if (alias.type != TK_IDENT && alias.type != TK_STRING) goto no_alias;
            Expr *e = arena_calloc(l->a, sizeof(Expr));
            e->type = EXPR_ALIAS; e->expr = base;
            e->alias = arena_strndup(l->a, alias.start, alias.len);
            return e;
        }
    }
no_alias:
    return base;
}

static SelectStmt parse_select(Lexer *l);

/* Разбирает тело SELECT (после ключевого слова SELECT): список выборки, FROM/JOIN,
   WHERE, GROUP BY, HAVING, ORDER BY и LIMIT/OFFSET. limit=-1 означает «без лимита». */
static SelectStmt parse_select(Lexer *l) {
    SelectStmt s = {0}; s.limit = -1; s.offset = 0;
    if (lex_eat(l, TK_DISTINCT)) s.distinct = true;
    else lex_eat(l, TK_ALL);

    /* select list */
    int cap=8;
    s.select_list = arena_alloc(l->a, cap*sizeof(Expr*));
    do {
        if (s.nselect==cap){cap*=2;Expr**nb=arena_alloc(l->a,cap*sizeof(Expr*));memcpy(nb,s.select_list,s.nselect*sizeof(Expr*));s.select_list=nb;}
        s.select_list[s.nselect++] = parse_select_expr(l);
    } while (lex_eat(l, TK_COMMA));

    /* FROM */
    if (lex_eat(l, TK_FROM)) {
        int fcap=4; s.from = arena_alloc(l->a, fcap*sizeof(FromItem));

        /* parse first table or derived table */
        auto_from_item: ;
        if (s.nfrom==fcap){fcap*=2;FromItem*nb=arena_alloc(l->a,fcap*sizeof(FromItem));memcpy(nb,s.from,s.nfrom*sizeof(FromItem));s.from=nb;}
        FromItem *fi = &s.from[s.nfrom++];
        memset(fi, 0, sizeof(*fi)); fi->join_type = JOIN_INNER;

        if (lex_peek_is(l, TK_LPAREN)) {
            lex_consume(l);
            if (lex_peek_is(l, TK_SELECT) || lex_peek_is(l, TK_WITH)) {
                fi->subquery = parse_stmt(l);
            } else {
                /* VALUES or other — skip */
                while (!lex_peek_is(l, TK_RPAREN) && !lex_peek_is(l, TK_EOF)) lex_consume(l);
            }
            if (!lex_eat(l, TK_RPAREN)) lex_set_error(l, "syntax error: expected ')' in FROM");
        } else {
            if (!lex_peek_is(l, TK_IDENT)) lex_set_error(l, "syntax error: expected table name in FROM");
            fi->table = consume_ident(l);
        }
        if (lex_eat(l, TK_AS)) fi->alias = consume_ident(l);
        else if (lex_peek_is(l, TK_IDENT)) {
            TkType pk = lex_peek(l).type;
            if (pk == TK_IDENT) fi->alias = consume_ident(l);
        }

        /* comma-separated additional tables (implicit cross join) */
        while (lex_eat(l, TK_COMMA)) {
            goto auto_from_item;
        }
    }

    /* JOINs */
    for (;;) {
        JoinType jt = JOIN_INNER;
        Token pk = lex_peek(l);
        if (pk.type==TK_LEFT){lex_consume(l);jt=JOIN_LEFT;lex_eat(l,TK_OUTER);}
        else if (pk.type==TK_RIGHT){lex_consume(l);jt=JOIN_RIGHT;lex_eat(l,TK_OUTER);}
        else if (pk.type==TK_FULL){lex_consume(l);jt=JOIN_FULL;lex_eat(l,TK_OUTER);}
        else if (pk.type==TK_CROSS){lex_consume(l);jt=JOIN_CROSS;}
        else if (pk.type==TK_INNER){lex_consume(l);}
        else if (pk.type!=TK_JOIN) break;
        if (!lex_eat(l,TK_JOIN)) break;

        int fcap=s.nfrom+1;
        FromItem *nb=arena_alloc(l->a,fcap*sizeof(FromItem));
        memcpy(nb,s.from,s.nfrom*sizeof(FromItem));
        s.from=nb;
        FromItem *fi=&s.from[s.nfrom++]; memset(fi,0,sizeof(*fi));
        fi->join_type=jt;

        if (lex_peek_is(l, TK_LPAREN)) {
            lex_consume(l);
            if (lex_peek_is(l, TK_SELECT)||lex_peek_is(l, TK_WITH)) {
                fi->subquery = parse_stmt(l);
            } else {
                while (!lex_peek_is(l, TK_RPAREN)&&!lex_peek_is(l,TK_EOF)) lex_consume(l);
            }
            lex_eat(l, TK_RPAREN);
        } else {
            fi->table = consume_ident(l);
        }
        if (lex_eat(l,TK_AS)) fi->alias=consume_ident(l);
        else if (lex_peek_is(l,TK_IDENT)) fi->alias=consume_ident(l);

        if (lex_eat(l,TK_ON)) fi->on=parse_expr(l,0);
        /* USING (...) — simplified: ignore */
    }

    /* WHERE */
    if (lex_eat(l, TK_WHERE)) s.where=parse_expr(l,0);

    /* GROUP BY */
    if (lex_peek_is(l,TK_GROUP)){
        lex_consume(l); lex_eat(l,TK_BY);
        int gcap=4; s.group_by=arena_alloc(l->a,gcap*sizeof(Expr*));
        do {
            if(s.ngroup==gcap){gcap*=2;Expr**nb=arena_alloc(l->a,gcap*sizeof(Expr*));memcpy(nb,s.group_by,s.ngroup*sizeof(Expr*));s.group_by=nb;}
            s.group_by[s.ngroup++]=parse_expr(l,0);
        } while(lex_eat(l,TK_COMMA));
    }

    /* HAVING */
    if (lex_eat(l,TK_HAVING)) s.having=parse_expr(l,0);

    /* ORDER BY */
    if (lex_peek_is(l,TK_ORDER)){
        lex_consume(l); lex_eat(l,TK_BY);
        int ocap=4; s.order_by=arena_alloc(l->a,ocap*sizeof(OrderItem));
        do {
            if(s.norder==ocap){ocap*=2;OrderItem*nb=arena_alloc(l->a,ocap*sizeof(OrderItem));memcpy(nb,s.order_by,s.norder*sizeof(OrderItem));s.order_by=nb;}
            OrderItem *oi=&s.order_by[s.norder++]; memset(oi,0,sizeof(*oi));
            oi->expr=parse_expr(l,0);
            oi->nulls_last = true; /* default */
            if(lex_peek_is(l,TK_DESC)){lex_consume(l);oi->desc=true;}
            else lex_eat(l,TK_ASC);
            if(lex_eat(l,TK_NULLS)){
                if(lex_eat(l,TK_FIRST)) oi->nulls_last=false;
                else lex_eat(l,TK_LAST);
            }
        } while(lex_eat(l,TK_COMMA));
    }

    /* LIMIT / OFFSET */
    if (lex_eat(l,TK_LIMIT)){
        Token t=lex_consume(l);
        if(t.type==TK_NUM_INT) s.limit=t.ival;
        else if(t.type==TK_IDENT && strcmp(t.start,"ALL")==0) s.limit=-1; /* LIMIT ALL */
    }
    if (lex_eat(l,TK_OFFSET)){Token t=lex_consume(l);if(t.type==TK_NUM_INT)s.offset=t.ival;}
    /* also accept: OFFSET n ROWS / FETCH FIRST|NEXT m ROWS ONLY —
       поглощаем «пагинационный» хвост (ROW[S]/FIRST/NEXT/FETCH/ONLY и числа
       между ними), чтобы стандартная пагинация не считалась мусором. */
    {
        bool saw_pgkw = false;  /* видели хотя бы одно пагинационное ключевое слово */
        for (;;) {
            Token pt = lex_peek(l);
            bool kw = (pt.type == TK_ROWS) || (pt.type == TK_ROW) ||
                      (pt.type == TK_FIRST) ||
                      (pt.type == TK_IDENT && pt.len == 5 &&
                       (pt.start[0]=='F'||pt.start[0]=='f') &&
                       (pt.start[1]=='E'||pt.start[1]=='e')) /* FETCH */ ||
                      (pt.type == TK_IDENT && pt.len == 4 &&
                       (pt.start[0]=='N'||pt.start[0]=='n')) /* NEXT */ ||
                      (pt.type == TK_IDENT && pt.len == 4 &&
                       (pt.start[0]=='O'||pt.start[0]=='o')) /* ONLY */;
            if (kw) { saw_pgkw = true; lex_consume(l); continue; }
            /* число допускаем только как часть FETCH FIRST n ROWS */
            if (saw_pgkw && pt.type == TK_NUM_INT) { lex_consume(l); continue; }
            break;
        }
    }

    return s;
}

/* Parse WITH CTEs then SELECT
   Разбирает список общих табличных выражений (CTE), затем основной SELECT,
   к которому прикрепляются собранные CTE. RECURSIVE распознаётся, но не обрабатывается особо. */
static Stmt *parse_with(Lexer *l) {
    Stmt *s = arena_calloc(l->a, sizeof(Stmt));
    lex_eat(l, TK_RECURSIVE); /* optional */

    SelectStmt *sel = &s->select;
    int ctecap = 4;
    sel->ctes = arena_alloc(l->a, ctecap * sizeof(CTE));
    sel->nctes = 0;

    do {
        if (sel->nctes == ctecap) {
            ctecap *= 2;
            CTE *nb = arena_alloc(l->a, ctecap * sizeof(CTE));
            memcpy(nb, sel->ctes, sel->nctes * sizeof(CTE));
            sel->ctes = nb;
        }
        CTE *cte = &sel->ctes[sel->nctes++];
        cte->name = consume_ident(l);
        /* optional column list */
        if (lex_eat(l, TK_LPAREN)) {
            while (!lex_peek_is(l,TK_RPAREN)&&!lex_peek_is(l,TK_EOF)) lex_consume(l);
            lex_eat(l, TK_RPAREN);
        }
        lex_eat(l, TK_AS);
        lex_eat(l, TK_LPAREN);
        Stmt *body = parse_stmt(l);
        lex_eat(l, TK_RPAREN);
        cte->body = (body->type == STMT_SELECT) ? &body->select : NULL;
    } while (lex_eat(l, TK_COMMA));

    /* Now parse the main SELECT */
    if (!lex_eat(l, TK_SELECT)) { s->type=STMT_UNKNOWN; s->error="expected SELECT after WITH"; return s; }
    s->type = STMT_SELECT;
    SelectStmt main_sel = parse_select(l);
    /* copy ctes into main_sel */
    main_sel.ctes = sel->ctes;
    main_sel.nctes = sel->nctes;
    s->select = main_sel;
    return s;
}

/* Разбирает UPDATE tbl SET col=val, ... [WHERE ...]; значения нормализуются в текст в s->dml. */
static void parse_update(Lexer *l, Stmt *s) {
    s->type = STMT_UPDATE;
    Token tbl = lex_consume(l);
    if (tbl.type != TK_IDENT) { s->type=STMT_UNKNOWN; s->error="UPDATE: expected table name"; return; }
    size_t tl = tbl.len < 127 ? tbl.len : 127;
    memcpy(s->dml.table, tbl.start, tl); s->dml.table[tl] = '\0';

    if (!lex_eat(l, TK_SET)) { s->type=STMT_UNKNOWN; s->error="UPDATE: expected SET"; return; }

    s->dml.nset = 0;
    while (s->dml.nset < 64) {
        Token col = lex_consume(l);
        if (col.type != TK_IDENT) { s->type=STMT_UNKNOWN; s->error="UPDATE SET: expected column"; return; }
        if (!lex_eat(l, TK_EQ)) { s->type=STMT_UNKNOWN; s->error="UPDATE SET: expected ="; return; }

        int neg = 0;
        if (lex_peek_is(l, TK_MINUS)) { lex_consume(l); neg = 1; }
        Token val = lex_consume(l);

        int i = s->dml.nset;
        size_t cl = col.len < 63 ? col.len : 63;
        memcpy(s->dml.set_cols[i], col.start, cl); s->dml.set_cols[i][cl] = '\0';

        if (val.type == TK_STRING) {
            size_t vl = val.len < 255 ? val.len : 255;
            memcpy(s->dml.set_vals[i], val.start, vl); s->dml.set_vals[i][vl] = '\0';
        } else if (val.type == TK_NUM_INT) {
            snprintf(s->dml.set_vals[i], 256, "%s%lld", neg?"-":"", (long long)val.ival);
        } else if (val.type == TK_NUM_FLOAT) {
            { /* значение уходит в UPDATE ... SET — путь данных, нужен точный формат */
              char _b[64]; json_fmt_double(_b, sizeof _b, val.fval);
              snprintf(s->dml.set_vals[i], 256, "%s%s", neg?"-":"", _b); }
        } else if (val.type == TK_NULL) {
            snprintf(s->dml.set_vals[i], 256, "NULL");
        } else if (val.type == TK_TRUE) {
            snprintf(s->dml.set_vals[i], 256, "true");
        } else if (val.type == TK_FALSE) {
            snprintf(s->dml.set_vals[i], 256, "false");
        } else {
            size_t vl = val.len < 255 ? val.len : 255;
            memcpy(s->dml.set_vals[i], val.start, vl); s->dml.set_vals[i][vl] = '\0';
        }
        s->dml.nset++;
        if (!lex_peek_is(l, TK_COMMA)) break;
        lex_consume(l);
    }

    s->dml.where = NULL;
    if (lex_eat(l, TK_WHERE)) s->dml.where = parse_expr(l, 0);
}

/* Разбирает DELETE [FROM] tbl [WHERE ...]. */
static void parse_delete(Lexer *l, Stmt *s) {
    s->type = STMT_DELETE;
    lex_eat(l, TK_FROM);
    Token tbl = lex_consume(l);
    if (tbl.type != TK_IDENT) { s->type=STMT_UNKNOWN; s->error="DELETE: expected table name"; return; }
    size_t tl = tbl.len < 127 ? tbl.len : 127;
    memcpy(s->dml.table, tbl.start, tl); s->dml.table[tl] = '\0';
    s->dml.where = NULL;
    if (lex_eat(l, TK_WHERE)) s->dml.where = parse_expr(l, 0);
}

/* Точка входа парсера: распознаёт вид инструкции (WITH/SELECT/UPDATE/DELETE/
   BEGIN/COMMIT/ROLLBACK) и для SELECT достраивает цепочку UNION/INTERSECT/EXCEPT. */
static Stmt *parse_stmt(Lexer *l) {
    Stmt *s = arena_calloc(l->a, sizeof(Stmt));
    lex_eat(l, TK_SEMICOLON);
    Token first = lex_peek(l);

    if (first.type == TK_WITH) {
        lex_consume(l);
        return parse_with(l);
    }

    if (first.type == TK_SELECT) {
        lex_consume(l);
        s->type = STMT_SELECT;
        s->select = parse_select(l);
    } else if (first.type == TK_UPDATE) {
        lex_consume(l);
        parse_update(l, s);
        return s;
    } else if (first.type == TK_DELETE) {
        lex_consume(l);
        parse_delete(l, s);
        return s;
    } else if (first.type == TK_BEGIN) {
        lex_consume(l);
        lex_eat(l, TK_TRANSACTION); /* optional TRANSACTION keyword */
        s->type = STMT_BEGIN;
        return s;
    } else if (first.type == TK_COMMIT) {
        lex_consume(l);
        s->type = STMT_COMMIT;
        return s;
    } else if (first.type == TK_ROLLBACK) {
        lex_consume(l);
        s->type = STMT_ROLLBACK;
        return s;
    } else {
        s->type = STMT_UNKNOWN;
        s->error = "unsupported statement";
        return s;
    }

    /* UNION / INTERSECT / EXCEPT */
    for (;;) {
        Token pt = lex_peek(l);
        if (pt.type != TK_UNION && pt.type != TK_INTERSECT && pt.type != TK_EXCEPT) break;
        lex_consume(l);
        SetOpType op;
        if (pt.type == TK_UNION) {
            op = lex_eat(l, TK_ALL) ? SET_UNION_ALL : SET_UNION;
        } else if (pt.type == TK_INTERSECT) {
            op = SET_INTERSECT;
        } else {
            op = SET_EXCEPT;
        }
        lex_eat(l, TK_SELECT);
        Stmt *rhs = arena_calloc(l->a, sizeof(Stmt));
        rhs->type = STMT_SELECT;
        rhs->select = parse_select(l);

        Stmt *combined = arena_calloc(l->a, sizeof(Stmt));
        combined->type = STMT_SET_OP;
        combined->set_op = op;
        combined->set_left = s;
        combined->set_right = rhs;
        s = combined;
    }

    /* Прокинуть наверх первую обнаруженную лексическую/структурную ошибку
       (в т.ч. из подзапросов и выражений), если она ещё не зафиксирована. */
    if (l->error && !s->error) s->error = l->error;

    return s;
}

/* Публичный API: разобрать SQL-запрос длиной len в AST (Stmt), размещённый в Arena a. */
Stmt *sql_parse(Arena *a, const char *query, size_t len) {
    Lexer l = {query, 0, len, {0}, false, a};
    Stmt *s = parse_stmt(&l);

    /* На верхнем уровне после оператора допустимы только необязательные ';' и EOF.
       Любые «висячие» токены (мусор, оборванное выражение, лишние скобки) — ошибка. */
    while (lex_eat(&l, TK_SEMICOLON)) { /* поглотить разделители операторов */ }
    if (!lex_peek_is(&l, TK_EOF)) lex_set_error(&l, "syntax error: unexpected trailing tokens");

    if (l.error && (!s || !s->error)) {
        if (!s) { s = arena_calloc(a, sizeof(Stmt)); s->type = STMT_UNKNOWN; }
        s->error = l.error;
    }
    return s;
}

/* ── Planner ── */
/* Строит дерево плана исполнения из AST: scan/join → filter → aggregate →
   having → distinct → project → window → sort → limit (узлы навешиваются снизу вверх). */
PlanNode *sql_plan(Arena *a, const Stmt *s) {
    if (!s) return NULL;
    if (s->type == STMT_SET_OP) {
        PlanNode *p = arena_calloc(a, sizeof(PlanNode));
        p->type = PLAN_SET_OP;
        p->set_op = s->set_op;
        p->left  = sql_plan(a, s->set_left);
        p->right = sql_plan(a, s->set_right);
        return p;
    }
    if (s->type != STMT_SELECT) return NULL;
    const SelectStmt *sel = &s->select;

    PlanNode *root = NULL;
    for (int i = 0; i < sel->nfrom; i++) {
        PlanNode *scan = arena_calloc(a, sizeof(PlanNode));
        scan->type  = PLAN_SCAN;
        scan->table = sel->from[i].table ? sel->from[i].table : "";
        if (!root) { root = scan; continue; }
        PlanNode *join = arena_calloc(a, sizeof(PlanNode));
        join->type      = PLAN_JOIN;
        join->left      = root;
        join->right     = scan;
        join->join_type = sel->from[i].join_type;
        join->join_on   = sel->from[i].on;
        root = join;
    }
    if (!root) { root = arena_calloc(a,sizeof(PlanNode)); root->type=PLAN_SCAN; root->table=""; }

    if (sel->where) {
        PlanNode *f = arena_calloc(a, sizeof(PlanNode));
        f->type = PLAN_FILTER; f->predicate = sel->where; f->left = root; root = f;
    }
    if (sel->ngroup > 0) {
        PlanNode *agg = arena_calloc(a, sizeof(PlanNode));
        agg->type=PLAN_AGG; agg->left=root;
        agg->group_keys=sel->group_by; agg->ngroup_keys=sel->ngroup;
        agg->agg_exprs=sel->select_list; agg->nagg_exprs=sel->nselect;
        root=agg;
    }
    if (sel->having) {
        PlanNode *f=arena_calloc(a,sizeof(PlanNode));
        f->type=PLAN_FILTER;f->predicate=sel->having;f->left=root;root=f;
    }
    if (sel->distinct) {
        PlanNode *d=arena_calloc(a,sizeof(PlanNode));
        d->type=PLAN_DISTINCT;d->left=root;root=d;
    }
    {
        PlanNode *p=arena_calloc(a,sizeof(PlanNode));
        p->type=PLAN_PROJECT;p->exprs=sel->select_list;p->nexprs=sel->nselect;p->left=root;root=p;
    }
    {
        bool has_win = false;
        for (int i = 0; i < sel->nselect && !has_win; i++)
            has_win = has_window_expr(sel->select_list[i]);
        if (has_win) {
            PlanNode *wn = arena_calloc(a, sizeof(PlanNode));
            wn->type = PLAN_WINDOW;
            wn->window_exprs = sel->select_list;
            wn->nwindow_exprs = sel->nselect;
            wn->left = root; root = wn;
        }
    }
    if (sel->norder > 0) {
        PlanNode *srt=arena_calloc(a,sizeof(PlanNode));
        srt->type=PLAN_SORT;srt->order=sel->order_by;srt->norder=sel->norder;srt->left=root;root=srt;
    }
    if (sel->limit >= 0) {
        PlanNode *lim=arena_calloc(a,sizeof(PlanNode));
        lim->type=PLAN_LIMIT;lim->limit=sel->limit;lim->offset=sel->offset;lim->left=root;root=lim;
    }
    return root;
}

/* Отладочный вывод краткой сводки об инструкции (для диагностики/тестов). */
void stmt_dump(const Stmt *s) {
    if (!s) { puts("(null stmt)"); return; }
    if (s->error) { printf("ERROR: %s\n", s->error); return; }
    if (s->type == STMT_SELECT) {
        const SelectStmt *sel = &s->select;
        printf("SELECT %d exprs FROM %d tables WHERE=%s GROUP=%d ORDER=%d LIMIT=%lld\n",
               sel->nselect, sel->nfrom, sel->where?"yes":"no",
               sel->ngroup, sel->norder, (long long)sel->limit);
    } else if (s->type == STMT_SET_OP) {
        printf("SET_OP %d\n", s->set_op);
    }
}
