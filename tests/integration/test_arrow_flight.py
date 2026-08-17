"""Arrow Flight server smoke test.

Auto-skips when:
  - pyarrow is not installed
  - napastak_flight_server binary hasn't been built (Step 2 is opt-in)
  - the napastak_gateway binary is missing

When everything is in place: starts a gateway, starts the Flight server
pointing at it, and runs a basic ListFlights + DoGet exchange via PyArrow.
Run as: pytest tests/integration/test_arrow_flight.py
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
GATEWAY = ROOT / "build" / "release" / "bin" / "napastak_gateway"
FLIGHT  = ROOT / "build" / "release" / "bin" / "napastak_flight_server"

GATEWAY_PORT = 19290
FLIGHT_PORT  = 19291

pyarrow = pytest.importorskip("pyarrow")
pyarrow_flight = pytest.importorskip("pyarrow.flight")

if not GATEWAY.exists():
    pytest.skip(f"missing {GATEWAY}; run: make BUILD=release", allow_module_level=True)
if not FLIGHT.exists():
    pytest.skip(
        f"missing {FLIGHT}; run: make flight (requires apache-arrow + grpc deps)",
        allow_module_level=True,
    )


def _wait_url(url, deadline_s=10):
    end = time.time() + deadline_s
    while time.time() < end:
        try:
            with urllib.request.urlopen(url, timeout=1) as r:
                if r.status < 500: return True
        except Exception:
            time.sleep(0.2)
    return False


@pytest.fixture(scope="module")
def gateway_token(tmp_path_factory):
    data = tmp_path_factory.mktemp("dfo_flight_data")
    cfg = data / "cfg.json"
    cfg.write_text(json.dumps({
        "port": GATEWAY_PORT,
        "data_dir": str(data),
        "auth_enabled": True,
        "jwt_secret": "flight-test",
        "admin_password": "admin",
    }))
    proc = subprocess.Popen(
        [str(GATEWAY), "-c", str(cfg)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        assert _wait_url(f"http://localhost:{GATEWAY_PORT}/health"), \
            "gateway didn't come up"
        # Get token
        req = urllib.request.Request(
            f"http://localhost:{GATEWAY_PORT}/api/auth/token",
            data=json.dumps({"username": "admin", "password": "admin"}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5) as r:
            tok = json.loads(r.read())["token"]
        # Pre-create a small table by ingesting CSV
        csv = "id,name\n1,alice\n2,bob\n3,carol\n"
        ireq = urllib.request.Request(
            f"http://localhost:{GATEWAY_PORT}/api/ingest/csv?table=flight_smoke",
            data=csv.encode(),
            headers={
                "Content-Type": "text/csv",
                "Authorization": f"Bearer {tok}",
            },
            method="POST",
        )
        urllib.request.urlopen(ireq, timeout=5).read()
        yield tok
    finally:
        proc.terminate()
        try: proc.wait(timeout=3)
        except subprocess.TimeoutExpired: proc.kill()


@pytest.fixture(scope="module")
def flight_client(gateway_token):
    proc = subprocess.Popen(
        [str(FLIGHT),
         "--gateway", f"http://localhost:{GATEWAY_PORT}",
         "--api-key", gateway_token,
         "--port", str(FLIGHT_PORT)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )
    try:
        # Wait until the gRPC port is listening
        end = time.time() + 10
        client = None
        while time.time() < end:
            try:
                client = pyarrow_flight.FlightClient(f"grpc://localhost:{FLIGHT_PORT}")
                list(client.list_flights())
                break
            except Exception:
                time.sleep(0.3)
        assert client is not None, "Flight server didn't come up"
        yield client
    finally:
        proc.terminate()
        try: proc.wait(timeout=3)
        except subprocess.TimeoutExpired: proc.kill()


def test_list_flights_includes_smoke_table(flight_client):
    names = []
    for fi in flight_client.list_flights():
        # pyarrow renamed FlightDescriptor.PATH/CMD to DescriptorType.{PATH,CMD}
        # at some point; compare by name to be version-agnostic.
        if str(fi.descriptor.descriptor_type).endswith("PATH"):
            names.extend(p.decode() if isinstance(p, bytes) else p
                         for p in fi.descriptor.path)
    assert "flight_smoke" in names


def test_do_get_sql_returns_arrow_table(flight_client):
    ticket = pyarrow_flight.Ticket(b"sql:SELECT * FROM flight_smoke ORDER BY id")
    reader = flight_client.do_get(ticket)
    table = reader.read_all()
    assert table.num_rows == 3
    assert table.column_names == ["id", "name"]
    # PyArrow + pandas trip:
    df = table.to_pandas()
    assert df["name"].tolist() == ["alice", "bob", "carol"]


def test_do_get_table_ticket_works(flight_client):
    reader = flight_client.do_get(pyarrow_flight.Ticket(b"table:flight_smoke"))
    table = reader.read_all()
    assert table.num_rows == 3
