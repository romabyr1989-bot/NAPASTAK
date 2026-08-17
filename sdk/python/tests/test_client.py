"""Tests for the NAPASTAK Python SDK.

Run with:
    pip install -e ".[dev]"
    pytest tests/ -v
"""

import pytest
import responses as resp_lib

from napastak import Client
from napastak.exceptions import AuthError, PermissionError

BASE = "http://localhost:8080"


def make_client() -> Client:
    """Return a Client that skips authentication (api_key supplied)."""
    return Client(BASE, api_key="test-key")


# ---------------------------------------------------------------------------
# test_query_returns_dataframe
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_query_returns_dataframe():
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"columns": ["id", "name"], "rows": [[1, "alice"], [2, "bob"]]},
        status=200,
    )
    client = make_client()
    df = client.query("SELECT * FROM users")

    assert list(df.columns) == ["id", "name"]
    assert len(df) == 2
    assert df.iloc[0]["id"] == 1
    assert df.iloc[1]["name"] == "bob"


# ---------------------------------------------------------------------------
# test_ingest_sends_csv
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_ingest_sends_csv():
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/users/ingest",
        json={"inserted": 3},
        status=200,
    )
    import pandas as pd

    client = make_client()
    df = pd.DataFrame({"id": [1, 2, 3], "name": ["alice", "bob", "carol"]})
    result = client.ingest("users", df)

    assert result == {"inserted": 3}
    # Verify the request body contained CSV
    assert len(resp_lib.calls) == 1
    body = resp_lib.calls[0].request.body
    assert "alice" in body
    assert "bob" in body


# ---------------------------------------------------------------------------
# test_transaction_rollbacks_on_exception
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_transaction_rollbacks_on_exception():
    # BEGIN
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"txn_id": 42},
        status=200,
    )
    # ROLLBACK
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"ok": True},
        status=200,
    )

    client = make_client()

    with pytest.raises(RuntimeError, match="oops"):
        with client.transaction():
            raise RuntimeError("oops")

    # Check second request was ROLLBACK
    rollback_body = resp_lib.calls[1].request.body
    assert "ROLLBACK" in rollback_body


# ---------------------------------------------------------------------------
# test_transaction_commits_on_success
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_transaction_commits_on_success():
    # BEGIN
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"txn_id": 7},
        status=200,
    )
    # query inside transaction
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"columns": ["x"], "rows": [[99]]},
        status=200,
    )
    # COMMIT
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"ok": True},
        status=200,
    )

    client = make_client()
    with client.transaction() as txn:
        df = txn.query("SELECT 99 AS x")
        assert df.iloc[0]["x"] == 99

    commit_body = resp_lib.calls[2].request.body
    assert "COMMIT" in commit_body


# ---------------------------------------------------------------------------
# test_health
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_health():
    resp_lib.add(
        resp_lib.GET,
        f"{BASE}/health",
        json={"status": "ok"},
        status=200,
    )
    client = make_client()
    result = client.health()
    assert result == {"status": "ok"}


# ---------------------------------------------------------------------------
# test_auth_error_on_401
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_auth_error_on_401():
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"error": "Unauthorized"},
        status=401,
    )
    client = make_client()
    with pytest.raises(AuthError):
        client.query("SELECT 1")


# ---------------------------------------------------------------------------
# test_permission_error_on_403
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_permission_error_on_403():
    resp_lib.add(
        resp_lib.POST,
        f"{BASE}/api/tables/query",
        json={"error": "Forbidden"},
        status=403,
    )
    client = make_client()
    with pytest.raises(PermissionError):
        client.query("SELECT * FROM secret_table")
