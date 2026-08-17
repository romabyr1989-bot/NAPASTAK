"""Step 3 Week 4: Extended Query Protocol via psycopg2.

Auto-skips when psycopg2 isn't installed or the gateway binary is missing.
Run as part of the broader integration suite or standalone:

    pip install psycopg2-binary
    pytest tests/integration/test_pgwire_extended.py -v
"""
from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GW   = ROOT / "build" / "release" / "bin" / "napastak_gateway"

HTTP_PORT = 18185
PG_PORT   = 15835

psycopg2 = pytest.importorskip("psycopg2")
if not GW.exists():
    pytest.skip(f"missing {GW}; run: make BUILD=release", allow_module_level=True)


def _wait_url(url, deadline_s=10):
    end = time.time() + deadline_s
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status < 500:
                    return True
        except Exception:
            time.sleep(0.2)
    return False


@pytest.fixture(scope="module")
def gateway(tmp_path_factory):
    data = tmp_path_factory.mktemp("dfo_pgw4")
    cfg = data / "cfg.json"
    cfg.write_text(json.dumps({
        "port": HTTP_PORT,
        "data_dir": str(data),
        "auth_enabled": True,
        "jwt_secret": "pgw4-test",
        "admin_password": "admin",
        "pgwire_enabled": 1,
        "pgwire_port": PG_PORT,
    }))
    proc = subprocess.Popen(
        [str(GW), "-c", str(cfg)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        assert _wait_url(f"http://localhost:{HTTP_PORT}/health")
        # Get token + ingest some test data via the JSON API
        req = urllib.request.Request(
            f"http://localhost:{HTTP_PORT}/api/auth/token",
            data=json.dumps({"username": "admin", "password": "admin"}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            tok = json.loads(r.read())["token"]
        csv = b"id,name,age\n1,alice,30\n2,bob,25\n3,carol,42\n4,dave,28\n5,eve,35\n"
        ingreq = urllib.request.Request(
            f"http://localhost:{HTTP_PORT}/api/ingest/csv?table=users",
            data=csv,
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "text/csv"},
            method="POST",
        )
        urllib.request.urlopen(ingreq, timeout=5).read()
        yield proc
    finally:
        proc.terminate()
        try: proc.wait(timeout=3)
        except subprocess.TimeoutExpired: proc.kill()


@pytest.fixture()
def conn(gateway):
    c = psycopg2.connect(
        host="localhost", port=PG_PORT,
        user="admin", password="admin", dbname="napastak",
    )
    yield c
    c.close()


def test_unparameterized_select(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM users ORDER BY id")
    rows = cur.fetchall()
    assert len(rows) == 5
    assert rows[0] == (1, "alice", 30)
    assert rows[-1] == (5, "eve", 35)


def test_parameterized_select_int_args(conn):
    cur = conn.cursor()
    cur.execute(
        "SELECT name, age FROM users WHERE age > %s AND age < %s ORDER BY age",
        (28, 40),
    )
    rows = cur.fetchall()
    # ages: 30 (alice), 35 (eve) — both > 28 and < 40
    names = [r[0] for r in rows]
    assert "alice" in names
    assert "eve" in names
    # 25 (bob), 28 (dave), 42 (carol) excluded
    for n in ("bob", "dave", "carol"):
        assert n not in names


def test_parameterized_select_text_arg(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE name = %s", ("alice",))
    row = cur.fetchone()
    assert row == (1, "alice", 30)


def test_text_arg_with_apostrophe_escaped(conn):
    """Single quote inside a parameter must be doubled, not break SQL."""
    cur = conn.cursor()
    # No row will match — what we're testing is that the SQL is well-formed
    cur.execute("SELECT * FROM users WHERE name = %s", ("o'malley",))
    assert cur.fetchall() == []


def test_compatibility_probes_via_extended(conn):
    """psycopg2 sends version() via Extended Query on first execute."""
    cur = conn.cursor()
    cur.execute("SELECT version()")
    (banner,) = cur.fetchone()
    assert "NAPASTAK" in banner


def test_information_schema_via_extended(conn):
    """Catalog handlers ignore projection (documented limitation):
    they always return the full row. Test verifies the table name
    appears somewhere in the result. SELECT * is the safe form."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM information_schema.tables")
    cells = [str(c) for row in cur.fetchall() for c in row]
    assert "users" in cells


def test_pg_class_via_extended(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM pg_class")
    cells = [str(c) for row in cur.fetchall() for c in row]
    assert "users" in cells


def test_two_connections_in_parallel(gateway):
    """Per-connection thread-isolation: two connections each prepare their
    own statement with the same name and don't collide."""
    a = psycopg2.connect(host="localhost", port=PG_PORT,
                          user="admin", password="admin", dbname="napastak")
    b = psycopg2.connect(host="localhost", port=PG_PORT,
                          user="admin", password="admin", dbname="napastak")
    try:
        ca, cb = a.cursor(), b.cursor()
        ca.execute("SELECT name FROM users WHERE id = %s", (1,))
        cb.execute("SELECT name FROM users WHERE id = %s", (5,))
        assert ca.fetchone() == ("alice",)
        assert cb.fetchone() == ("eve",)
    finally:
        a.close(); b.close()
