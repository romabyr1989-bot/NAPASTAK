#include "../../lib/sql_parser/sql.h"
#include "../../lib/qengine/qengine.h"
#include "../../lib/core/arena.h"
#include <stdio.h>
#include <string.h>
#include <math.h>

static int plan=0,pass=0,fail=0;
#define ok(c,...) do{plan++;if(c){pass++;printf("ok %d — " __VA_ARGS__);puts("");}else{fail++;printf("not ok %d — " __VA_ARGS__);puts("");}}while(0)

static Stmt *parse(Arena *a, const char *q){return sql_parse(a,q,strlen(q));}

/* Evaluate the single scalar select-expression of "SELECT <expr>". */
static Scalar evf(Arena *a, const char *expr) {
    char q[256]; snprintf(q,sizeof(q),"SELECT %s",expr);
    Stmt *s = sql_parse(a,q,strlen(q));
    return eval_expr(s->select.select_list[0], NULL, a);
}

int main(void) {
    puts("TAP version 14");
    Arena *a = arena_create(0);

    /* basic select */
    Stmt *s1=parse(a,"SELECT * FROM employees");
    ok(s1->type==STMT_SELECT,"basic select parsed");
    ok(s1->select.nfrom==1,"one from table");
    ok(strcmp(s1->select.from[0].table,"employees")==0,"table name correct");
    ok(s1->select.nselect==1,"one select expr (star)");

    /* with where */
    Stmt *s2=parse(a,"SELECT id, name FROM users WHERE age > 18 LIMIT 10");
    ok(s2->type==STMT_SELECT,"select with where");
    ok(s2->select.nselect==2,"two select cols");
    ok(s2->select.where!=NULL,"has where clause");
    ok(s2->select.limit==10,"limit=10");

    /* group by + having */
    Stmt *s3=parse(a,"SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5");
    ok(s3->type==STMT_SELECT,"group by parsed");
    ok(s3->select.ngroup==1,"one group key");
    ok(s3->select.having!=NULL,"has having");

    /* order by */
    Stmt *s4=parse(a,"SELECT name FROM t ORDER BY name DESC, age ASC");
    ok(s4->select.norder==2,"two order items");
    ok(s4->select.order_by[0].desc==true,"first order DESC");
    ok(s4->select.order_by[1].desc==false,"second order ASC");

    /* join */
    Stmt *s5=parse(a,"SELECT a.id FROM a JOIN b ON a.id=b.aid");
    ok(s5->type==STMT_SELECT,"join parsed");
    ok(s5->select.nfrom>=2,"two from items after join");

    /* planner */
    PlanNode *plan5 = sql_plan(a,s2);
    ok(plan5!=NULL,"planner produces plan");

    /* ── scalar string / fuzzy-match builtins (eval_expr) ── */
    Scalar r;
    r=evf(a,"trim('  hi  ')");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"hi")==0,"trim strips surrounding spaces");

    r=evf(a,"coalesce(NULL, 'x')");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"x")==0,"coalesce returns first non-NULL");
    r=evf(a,"coalesce(NULL, NULL)");
    ok(r.type==SV_NULL,"coalesce of all NULL is NULL");
    r=evf(a,"coalesce(7, 9)");
    ok(r.type==SV_INT && r.val.ival==7,"coalesce keeps first arg type (int)");

    r=evf(a,"substr('hello', 2)");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"ello")==0,"substr from start to end");
    r=evf(a,"substr('hello', 2, 3)");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"ell")==0,"substr with length");
    r=evf(a,"substr('hello', 10)");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"")==0,"substr past end is empty");

    r=evf(a,"concat('a', 'b', 3)");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"ab3")==0,"concat coerces and joins");
    r=evf(a,"concat('a', NULL, 'b')");
    ok(r.type==SV_TEXT && strcmp(r.val.sval,"ab")==0,"concat treats NULL as empty");

    r=evf(a,"levenshtein('kitten', 'sitting')");
    ok(r.type==SV_INT && r.val.ival==3,"levenshtein kitten/sitting = 3");
    r=evf(a,"levenshtein('abc', 'abc')");
    ok(r.type==SV_INT && r.val.ival==0,"levenshtein equal strings = 0");

    r=evf(a,"jaro_winkler('abc', 'abc')");
    ok(r.type==SV_DOUBLE && fabs(r.val.fval-1.0)<1e-9,"jaro_winkler identical = 1.0");
    r=evf(a,"jaro_winkler('martha', 'marhta')");
    ok(r.type==SV_DOUBLE && fabs(r.val.fval-0.9611)<2e-3,"jaro_winkler martha/marhta ~ 0.961");
    r=evf(a,"jaro_winkler('abc', 'xyz')");
    ok(r.type==SV_DOUBLE && r.val.fval==0.0,"jaro_winkler disjoint = 0.0");

    arena_destroy(a);
    printf("1..%d\n# pass=%d fail=%d\n",plan,pass,fail);
    return fail>0?1:0;
}
