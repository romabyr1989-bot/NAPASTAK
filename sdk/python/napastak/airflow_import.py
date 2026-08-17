"""
Airflow DAG → NAPASTAK pipeline converter.

Parses Python DAG files using the stdlib `ast` module — does NOT require
Airflow itself to be installed. Outputs JSON files ready to POST to the
NAPASTAK gateway at `/api/pipelines`.

Usage:
    napastak-import-airflow ./dags/ --output ./dfo_pipelines/
    napastak-import-airflow ./dags/ --apply --gateway http://localhost:8080 \\
                                --api-key dfo_xxx

The goal is **80% coverage of typical DAGs**, not exact semantic equivalence.
Operators we don't recognize are emitted as `bash` steps with a TODO note,
so a human can fix them up before applying. Python callables can never be
auto-converted; the importer marks them clearly.
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
import time
from pathlib import Path
from typing import Any

# ── Operator → step mapping ──────────────────────────────────────
# Each entry tells the importer how to translate an Airflow operator.
#   "type"      — NAPASTAK step type (bash | sql | http | noop | unsupported)
#   "param"     — the kwarg of the operator that holds the payload
#   "field"     — the resulting step field that receives the payload
#   "connector" — optional: hint for SQL operators about which DB to use
OPERATOR_MAPPING: dict[str, dict[str, Any]] = {
    "BashOperator":              {"type": "bash",   "param": "bash_command",     "field": "command"},
    "PythonOperator":            {"type": "python", "param": "python_callable",  "field": "function"},
    "SQLExecuteQueryOperator":   {"type": "sql",    "param": "sql",              "field": "sql"},
    "PostgresOperator":          {"type": "sql",    "param": "sql",              "field": "sql", "connector": "postgresql"},
    "MySqlOperator":             {"type": "sql",    "param": "sql",              "field": "sql", "connector": "mysql"},
    "SnowflakeOperator":         {"type": "sql",    "param": "sql",              "field": "sql", "connector": "snowflake"},
    "BigQueryOperator":          {"type": "sql",    "param": "sql",              "field": "sql", "connector": "bigquery"},
    "BigQueryInsertJobOperator": {"type": "sql",    "param": "configuration",    "field": "sql", "connector": "bigquery"},
    "S3CopyObjectOperator":      {"type": "connector", "connector_type": "s3"},
    "S3ToRedshiftOperator":      {"type": "connector", "connector_type": "s3"},
    "SimpleHttpOperator":        {"type": "http",   "param": "endpoint",         "field": "url"},
    "HttpOperator":              {"type": "http",   "param": "endpoint",         "field": "url"},
    "EmailOperator":             {"type": "email",  "param": "to",               "field": "recipients"},
    "DummyOperator":             {"type": "noop"},
    "EmptyOperator":             {"type": "noop"},
}


# ── AST visitor ──────────────────────────────────────────────────
class DagVisitor(ast.NodeVisitor):
    """Walk a DAG file's AST and extract DAG metadata + tasks + dependencies.

    Recognizes:
      • Variable form:  ``dag = DAG(dag_id="x", schedule="...")``
      • With form:      ``with DAG(dag_id="x", ...) as dag: …``
      • Operator form:  ``task = FooOperator(task_id="t", ...)``
      • Bit-shift deps: ``a >> b >> c``  /  ``a << b``
      • Method deps:    ``a.set_downstream(b)`` / ``a.set_upstream(b)``
    """

    def __init__(self, source_file: str) -> None:
        self.source_file = source_file
        self.dag_id: str | None = None
        self.schedule: str | None = None
        self.start_date: str | None = None
        self.tasks: dict[str, dict[str, Any]] = {}        # task_id → {operator, kwargs}
        self.var_to_task: dict[str, str] = {}             # var name → canonical task_id
        self.deps: list[tuple[str, str]] = []             # (upstream, downstream) of task ids
        self._with_dag_active = False

    # — Visitors —

    def visit_Assign(self, node: ast.Assign) -> None:
        # dag = DAG(...)
        if isinstance(node.value, ast.Call) and self._call_name(node.value) == "DAG":
            self._extract_dag_kwargs(node.value)
            self.generic_visit(node)
            return
        # task = FooOperator(...)
        if isinstance(node.value, ast.Call):
            op_name = self._call_name(node.value)
            if op_name in OPERATOR_MAPPING:
                kwargs = self._extract_call_kwargs(node.value)
                task_id = kwargs.get("task_id")
                # Fall back to the variable name when task_id isn't a literal
                if not task_id and node.targets and isinstance(node.targets[0], ast.Name):
                    task_id = node.targets[0].id
                if task_id:
                    self.tasks[task_id] = {"operator": op_name, "kwargs": kwargs}
                    if node.targets and isinstance(node.targets[0], ast.Name):
                        self.var_to_task[node.targets[0].id] = task_id
        self.generic_visit(node)

    def visit_With(self, node: ast.With) -> None:
        # with DAG(...) as dag: ...
        for item in node.items:
            if isinstance(item.context_expr, ast.Call) and self._call_name(item.context_expr) == "DAG":
                self._with_dag_active = True
                self._extract_dag_kwargs(item.context_expr)
        self.generic_visit(node)
        self._with_dag_active = False

    def visit_BinOp(self, node: ast.BinOp) -> None:
        # a >> b   /   a << b   /   chains  a >> b >> c
        if isinstance(node.op, (ast.RShift, ast.LShift)):
            left_ids  = self._extract_task_refs(node.left)
            right_ids = self._extract_task_refs(node.right)
            if isinstance(node.op, ast.RShift):
                for ll in left_ids:
                    for rr in right_ids:
                        self.deps.append((ll, rr))
            else:  # LShift
                for ll in left_ids:
                    for rr in right_ids:
                        self.deps.append((rr, ll))
        self.generic_visit(node)

    def visit_Expr(self, node: ast.Expr) -> None:
        # a.set_downstream(b)  /  a.set_upstream(b)
        if isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Attribute):
            method = node.value.func.attr
            if method in ("set_downstream", "set_upstream") and node.value.args:
                anchor = self._task_id_from_attr(node.value.func.value)
                others = self._extract_task_refs(node.value.args[0])
                for o in others:
                    if method == "set_downstream":
                        self.deps.append((anchor, o))
                    else:
                        self.deps.append((o, anchor))
        self.generic_visit(node)

    # — Helpers —

    @staticmethod
    def _call_name(call: ast.Call) -> str:
        if isinstance(call.func, ast.Name):
            return call.func.id
        if isinstance(call.func, ast.Attribute):
            return call.func.attr
        return ""

    def _extract_dag_kwargs(self, call: ast.Call) -> None:
        for kw in call.keywords:
            if kw.arg == "dag_id" and isinstance(kw.value, ast.Constant):
                self.dag_id = kw.value.value
            elif kw.arg in ("schedule", "schedule_interval"):
                if isinstance(kw.value, ast.Constant):
                    self.schedule = kw.value.value
                else:
                    # timedelta(hours=1) etc — store source text for the converter
                    self.schedule = "__expr__:" + ast.unparse(kw.value)
            elif kw.arg == "start_date":
                self.start_date = ast.unparse(kw.value)

    @staticmethod
    def _extract_call_kwargs(call: ast.Call) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for kw in call.keywords:
            if kw.arg is None:
                continue  # **kwargs spread, can't represent
            try:
                out[kw.arg] = ast.literal_eval(kw.value)
            except (ValueError, SyntaxError):
                # Non-literal — preserve source text so a human can review
                out[kw.arg] = "__expr__:" + ast.unparse(kw.value)
        return out

    def _extract_task_refs(self, node: ast.AST) -> list[str]:
        """Return canonical task ids referenced by an expression.
        Resolves variable aliases via self.var_to_task."""
        refs: list[str] = []
        if isinstance(node, ast.Name):
            refs.append(self.var_to_task.get(node.id, node.id))
        elif isinstance(node, (ast.List, ast.Tuple)):
            for item in node.elts:
                refs.extend(self._extract_task_refs(item))
        elif isinstance(node, ast.BinOp) and isinstance(node.op, (ast.RShift, ast.LShift)):
            # In a >> b >> c, the middle operand is BOTH source and target.
            # visit_BinOp will record both edges; here we surface the boundary
            # nodes for the outer expression to chain correctly.
            if isinstance(node.op, ast.RShift):
                refs.extend(self._extract_task_refs(node.right))
            else:
                refs.extend(self._extract_task_refs(node.left))
        return refs

    def _task_id_from_attr(self, node: ast.AST) -> str:
        if isinstance(node, ast.Name):
            return self.var_to_task.get(node.id, node.id)
        return ""


# ── Schedule conversion ─────────────────────────────────────────
_SCHEDULE_PRESETS = {
    "@daily":   "0 0 * * *",
    "@hourly":  "0 * * * *",
    "@weekly":  "0 0 * * 0",
    "@monthly": "0 0 1 * *",
    "@yearly":  "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
}


def airflow_schedule_to_cron(schedule: Any) -> str:
    """Convert an Airflow `schedule` / `schedule_interval` value to a cron
    expression understood by NAPASTAK, or 'manual' if not periodic."""
    if schedule is None or schedule == "@once" or schedule == "None":
        return "manual"
    if isinstance(schedule, str):
        if schedule in _SCHEDULE_PRESETS:
            return _SCHEDULE_PRESETS[schedule]
        if schedule.startswith("__expr__:"):
            # timedelta(...) etc — we can't compute cron; fall back to manual
            return "manual"
        return schedule  # assume a literal cron expression
    return "manual"


# ── Step builder ────────────────────────────────────────────────
def operator_to_step(task_id: str, task: dict[str, Any]) -> dict[str, Any]:
    op_name = task["operator"]
    kwargs  = task["kwargs"]
    mapping = OPERATOR_MAPPING.get(op_name, {"type": "unsupported"})

    step: dict[str, Any] = {"id": task_id, "name": task_id, "type": mapping["type"]}

    t = mapping["type"]
    if t == "bash":
        step["command"] = kwargs.get("bash_command", "")
    elif t == "python":
        # We CAN'T execute arbitrary Python. Surface as bash with a clear TODO.
        step["type"]    = "bash"
        step["note"]    = f"TODO: rewrite Python callable: {kwargs.get('python_callable', '?')}"
        step["command"] = f"echo 'unconverted python task: {task_id}' && exit 1"
    elif t == "sql":
        step["sql"] = kwargs.get(mapping.get("param", "sql"), "")
        if "connector" in mapping:
            step["connector_type"] = mapping["connector"]
    elif t == "http":
        step["url"]    = kwargs.get("endpoint", "")
        step["method"] = kwargs.get("method", "GET")
    elif t == "connector":
        step["connector_type"]   = mapping["connector_type"]
        step["connector_config"] = {k: v for k, v in kwargs.items() if not str(k).startswith("_")}
    elif t == "noop":
        step["type"]    = "bash"
        step["command"] = "true"
    elif t == "email":
        step["type"]    = "bash"
        step["note"]    = f"TODO: send email to {kwargs.get('to', '?')}"
        step["command"] = f"echo 'TODO email: {kwargs.get('subject', '?')}'"
    elif t == "unsupported":
        step["type"]    = "bash"
        step["note"]    = f"TODO: unsupported operator {op_name}, manual conversion needed"
        step["command"] = f"echo 'UNSUPPORTED: {op_name}' && exit 1"

    # Common Airflow knobs
    if "retries" in kwargs and isinstance(kwargs["retries"], int):
        step["max_retries"] = kwargs["retries"]
    if "retry_delay" in kwargs:
        step["retry_delay_sec"] = 60  # Best-effort default; can't parse timedelta literally

    return step


def build_pipeline_json(visitor: DagVisitor) -> dict[str, Any]:
    if not visitor.dag_id:
        raise ValueError(f"No dag_id found in {visitor.source_file}")

    # Steps: keep only canonical task ids (deduplicate via var_to_task aliases)
    canonical_ids = list(visitor.tasks.keys())
    steps = [operator_to_step(tid, visitor.tasks[tid]) for tid in canonical_ids]

    # Resolve dependencies. Edges that reference an aliased var get rewritten
    # to canonical task ids (visit_BinOp / set_downstream already use that).
    deps_by_target: dict[str, list[str]] = {}
    for upstream, downstream in visitor.deps:
        if upstream == downstream:
            continue
        if upstream not in canonical_ids or downstream not in canonical_ids:
            continue
        deps_by_target.setdefault(downstream, []).append(upstream)
    # Dedup while keeping order
    for k, lst in deps_by_target.items():
        seen: set[str] = set()
        out: list[str] = []
        for x in lst:
            if x not in seen:
                seen.add(x)
                out.append(x)
        deps_by_target[k] = out

    for step in steps:
        if step["id"] in deps_by_target:
            step["depends_on"] = deps_by_target[step["id"]]

    return {
        "name":          visitor.dag_id,
        "cron":          airflow_schedule_to_cron(visitor.schedule),
        "description":   f"Imported from {visitor.source_file}",
        "imported_from": "airflow",
        "imported_at":   time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "steps":         steps,
    }


# ── File-level driver ───────────────────────────────────────────
def import_dag_file(path: Path) -> dict[str, Any] | None:
    """Parse a single .py DAG file. Returns pipeline dict or None on failure.
    Failures are logged via stderr; never raises (for batch import resilience)."""
    try:
        source = path.read_text(encoding="utf-8", errors="replace")
        tree = ast.parse(source, filename=str(path))
    except SyntaxError as e:
        print(f"  SKIP {path}: SyntaxError: {e}", file=sys.stderr)
        return None
    except OSError as e:
        print(f"  SKIP {path}: {e}", file=sys.stderr)
        return None

    visitor = DagVisitor(str(path))
    visitor.visit(tree)

    if not visitor.dag_id:
        print(f"  SKIP {path}: no DAG instantiation found", file=sys.stderr)
        return None
    return build_pipeline_json(visitor)


# ── CLI ─────────────────────────────────────────────────────────
def _summarize_todos(pipeline: dict[str, Any]) -> int:
    return sum(1 for s in pipeline["steps"] if "note" in s)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="napastak-import-airflow",
                                description="Convert Airflow DAGs to NAPASTAK pipelines")
    p.add_argument("dags_dir", type=Path,
                   help="Directory with Airflow .py DAG files (recursive)")
    p.add_argument("--output", type=Path, default=Path("./dfo_pipelines"),
                   help="Where to write JSON files (default: ./dfo_pipelines)")
    p.add_argument("--apply", action="store_true",
                   help="POST converted pipelines to a running gateway")
    p.add_argument("--gateway", default="http://localhost:8080",
                   help="Gateway URL (used with --apply)")
    p.add_argument("--api-key", default=None,
                   help="JWT/API key for the gateway (used with --apply)")
    args = p.parse_args(argv)

    if not args.dags_dir.exists():
        print(f"ERROR: directory not found: {args.dags_dir}", file=sys.stderr)
        return 2

    args.output.mkdir(parents=True, exist_ok=True)
    pipelines: list[dict[str, Any]] = []
    converted = skipped = 0
    todos = 0

    py_files = sorted(args.dags_dir.rglob("*.py"))
    for py in py_files:
        # Skip __init__.py and tests
        if py.name.startswith("__") or "/test" in str(py).lower():
            continue
        print(f"Reading {py}")
        pipeline = import_dag_file(py)
        if pipeline is None:
            skipped += 1
            continue
        out_file = args.output / f"{pipeline['name']}.json"
        out_file.write_text(json.dumps(pipeline, indent=2))
        pipelines.append(pipeline)
        converted += 1
        todos += _summarize_todos(pipeline)

    print()
    print("=== Summary ===")
    print(f"  Files scanned:    {len(py_files)}")
    print(f"  Converted:        {converted}")
    print(f"  Skipped:          {skipped}")
    print(f"  Steps with TODO:  {todos}")
    print(f"  Output dir:       {args.output}")

    if args.apply and pipelines:
        try:
            import requests  # type: ignore
        except ImportError:
            print("\nERROR: --apply requires `pip install requests`", file=sys.stderr)
            return 3
        ok = fail = 0
        headers = {"Content-Type": "application/json"}
        if args.api_key:
            headers["Authorization"] = f"Bearer {args.api_key}"
        for pipeline in pipelines:
            try:
                r = requests.post(f"{args.gateway}/api/pipelines",
                                  json=pipeline, headers=headers, timeout=30)
                if r.ok:
                    print(f"  ✓ Applied {pipeline['name']}")
                    ok += 1
                else:
                    print(f"  ✗ Failed {pipeline['name']}: HTTP {r.status_code} {r.text[:120]}",
                          file=sys.stderr)
                    fail += 1
            except Exception as e:
                print(f"  ✗ Failed {pipeline['name']}: {e}", file=sys.stderr)
                fail += 1
        print(f"\nApply: {ok} ok, {fail} failed")
        return 0 if fail == 0 else 4
    return 0


if __name__ == "__main__":
    sys.exit(main())
