"""Tests for the Airflow → NAPASTAK DAG importer.

Uses synthetic DAG strings parsed via stdlib `ast` — no Airflow needed.
"""
from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from napastak.airflow_import import (
    DagVisitor,
    OPERATOR_MAPPING,
    airflow_schedule_to_cron,
    build_pipeline_json,
    import_dag_file,
    operator_to_step,
)


# ── Fixtures ─────────────────────────────────────────────────────
SIMPLE_DAG = """
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(dag_id='my_etl', schedule='@daily', start_date=datetime(2024, 1, 1))

extract = BashOperator(task_id='extract', bash_command='psql -c "..."', dag=dag)
transform = BashOperator(task_id='transform', bash_command='python tx.py', dag=dag)
load = BashOperator(task_id='load', bash_command='aws s3 cp ...', dag=dag, retries=3)

extract >> transform >> load
"""

WITH_FORM_DAG = """
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='nightly_report', schedule_interval='0 2 * * *',
         start_date='2024-01-01') as dag:
    start = EmptyOperator(task_id='start')
    fetch = BashOperator(task_id='fetch', bash_command='./fetch.sh')
    publish = BashOperator(task_id='publish', bash_command='./publish.sh')

    start >> fetch >> publish
"""

SET_DOWNSTREAM_DAG = """
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(dag_id='legacy_syntax', schedule='@hourly', start_date=None)

a = BashOperator(task_id='a', bash_command='true', dag=dag)
b = BashOperator(task_id='b', bash_command='true', dag=dag)
c = BashOperator(task_id='c', bash_command='true', dag=dag)

a.set_downstream(b)
c.set_upstream(b)
"""

UNSUPPORTED_OP_DAG = """
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from datetime import datetime

dag = DAG(dag_id='exotic_ops', schedule=None, start_date=datetime(2024, 1, 1))

def my_callable():
    return 42

py = PythonOperator(task_id='py_step', python_callable=my_callable, dag=dag)
weird = S3ToRedshiftOperator(task_id='weird_step', schema='public', table='users',
                              s3_bucket='b', s3_key='k', dag=dag)
py >> weird
"""

LIST_DEPS_DAG = """
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(dag_id='fanout', schedule='@daily', start_date=None)

src = BashOperator(task_id='src', bash_command='true', dag=dag)
a = BashOperator(task_id='a', bash_command='true', dag=dag)
b = BashOperator(task_id='b', bash_command='true', dag=dag)
c = BashOperator(task_id='c', bash_command='true', dag=dag)

src >> [a, b, c]
"""

NO_DAG = """
def hello():
    print('not a dag')
"""


def _walk(source: str) -> DagVisitor:
    tree = ast.parse(source)
    v = DagVisitor("test.py")
    v.visit(tree)
    return v


# ── Schedule conversion ─────────────────────────────────────────
@pytest.mark.parametrize("inp,expected", [
    (None,        "manual"),
    ("@once",     "manual"),
    ("@daily",    "0 0 * * *"),
    ("@hourly",   "0 * * * *"),
    ("@weekly",   "0 0 * * 0"),
    ("@monthly",  "0 0 1 * *"),
    ("@yearly",   "0 0 1 1 *"),
    ("@annually", "0 0 1 1 *"),
    ("0 6 * * *", "0 6 * * *"),                 # passthrough
    ("__expr__:timedelta(hours=1)", "manual"),  # non-literal → manual
])
def test_schedule_conversion(inp, expected):
    assert airflow_schedule_to_cron(inp) == expected


# ── Basic DAG ────────────────────────────────────────────────────
def test_simple_dag_extracts_metadata():
    v = _walk(SIMPLE_DAG)
    assert v.dag_id == "my_etl"
    assert v.schedule == "@daily"


def test_simple_dag_extracts_three_tasks():
    v = _walk(SIMPLE_DAG)
    assert set(v.tasks.keys()) == {"extract", "transform", "load"}


def test_simple_dag_extracts_chain_dependencies():
    v = _walk(SIMPLE_DAG)
    pairs = set(v.deps)
    # extract → transform → load
    assert ("extract", "transform") in pairs
    assert ("transform", "load") in pairs


def test_simple_dag_pipeline_json_roundtrip():
    v = _walk(SIMPLE_DAG)
    p = build_pipeline_json(v)
    assert p["name"] == "my_etl"
    assert p["cron"] == "0 0 * * *"
    assert p["imported_from"] == "airflow"
    assert len(p["steps"]) == 3
    load = next(s for s in p["steps"] if s["id"] == "load")
    assert "transform" in load["depends_on"]
    assert load["max_retries"] == 3
    assert load["command"].startswith("aws s3")


# ── With-form DAG ────────────────────────────────────────────────
def test_with_form_dag():
    v = _walk(WITH_FORM_DAG)
    assert v.dag_id == "nightly_report"
    assert v.schedule == "0 2 * * *"
    assert {"start", "fetch", "publish"} <= set(v.tasks.keys())
    pairs = set(v.deps)
    assert ("start", "fetch") in pairs
    assert ("fetch", "publish") in pairs


def test_empty_operator_becomes_noop_bash():
    v = _walk(WITH_FORM_DAG)
    p = build_pipeline_json(v)
    start = next(s for s in p["steps"] if s["id"] == "start")
    assert start["type"] == "bash"
    assert start["command"] == "true"


# ── set_downstream / set_upstream ───────────────────────────────
def test_set_downstream_and_upstream_methods():
    v = _walk(SET_DOWNSTREAM_DAG)
    pairs = set(v.deps)
    assert ("a", "b") in pairs
    assert ("b", "c") in pairs


# ── Fanout list deps  src >> [a, b, c] ──────────────────────────
def test_list_dependencies():
    v = _walk(LIST_DEPS_DAG)
    pairs = set(v.deps)
    assert ("src", "a") in pairs
    assert ("src", "b") in pairs
    assert ("src", "c") in pairs


# ── Unsupported / Python operators ──────────────────────────────
def test_python_operator_emits_todo_bash():
    v = _walk(UNSUPPORTED_OP_DAG)
    p = build_pipeline_json(v)
    py_step = next(s for s in p["steps"] if s["id"] == "py_step")
    assert py_step["type"] == "bash"
    assert "TODO" in py_step["note"]
    # Must clearly fail rather than silently succeed
    assert "exit 1" in py_step["command"]


def test_unsupported_operator_keeps_pipeline_valid():
    v = _walk(UNSUPPORTED_OP_DAG)
    p = build_pipeline_json(v)
    # Pipeline still has both steps & dependency edge
    ids = {s["id"] for s in p["steps"]}
    assert ids == {"py_step", "weird_step"}
    weird = next(s for s in p["steps"] if s["id"] == "weird_step")
    # S3ToRedshiftOperator → connector step type
    assert weird["type"] == "connector"
    assert weird["connector_type"] == "s3"


# ── No DAG present ───────────────────────────────────────────────
def test_file_without_dag_returns_none(tmp_path):
    f = tmp_path / "not_a_dag.py"
    f.write_text(NO_DAG)
    assert import_dag_file(f) is None


def test_syntax_error_returns_none(tmp_path):
    f = tmp_path / "broken.py"
    f.write_text("def : <<<")
    assert import_dag_file(f) is None


# ── Operator → step (unit) ──────────────────────────────────────
def test_operator_to_step_sql_with_connector():
    s = operator_to_step("q", {"operator": "PostgresOperator",
                                "kwargs": {"sql": "SELECT 1"}})
    assert s["type"] == "sql"
    assert s["sql"] == "SELECT 1"
    assert s["connector_type"] == "postgresql"


def test_operator_to_step_unknown_marked_unsupported():
    s = operator_to_step("x", {"operator": "TotallyMadeUpOperator", "kwargs": {}})
    assert s["type"] == "bash"
    assert "UNSUPPORTED" in s["command"]


# ── Mapping coverage sanity ─────────────────────────────────────
def test_operator_mapping_has_core_operators():
    for op in ("BashOperator", "PythonOperator", "PostgresOperator",
              "BigQueryOperator", "SimpleHttpOperator", "EmptyOperator"):
        assert op in OPERATOR_MAPPING


# ── End-to-end via import_dag_file ──────────────────────────────
def test_import_dag_file_round_trip(tmp_path: Path):
    f = tmp_path / "etl.py"
    f.write_text(SIMPLE_DAG)
    pipeline = import_dag_file(f)
    assert pipeline is not None
    assert pipeline["name"] == "my_etl"
    # JSON must serialize cleanly
    json.dumps(pipeline)
