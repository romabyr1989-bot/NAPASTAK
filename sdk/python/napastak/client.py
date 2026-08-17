from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

import pandas as pd
import requests

from .exceptions import AuthError, NapastakError, PermissionError, TableNotFoundError

if TYPE_CHECKING:
    from .table import Table
    from .pipeline import Pipeline


def _raise_for_status(resp: requests.Response) -> None:
    """Raise domain-specific exceptions based on HTTP status code."""
    if resp.status_code == 401:
        try:
            detail = resp.json().get("error", resp.text)
        except Exception:
            detail = resp.text
        raise AuthError(detail)
    if resp.status_code == 403:
        try:
            detail = resp.json().get("error", resp.text)
        except Exception:
            detail = resp.text
        raise PermissionError(detail)
    if resp.status_code == 404:
        try:
            detail = resp.json().get("error", resp.text)
        except Exception:
            detail = resp.text
        raise TableNotFoundError(detail)
    resp.raise_for_status()


def _parse_query_result(data: dict) -> pd.DataFrame:
    """Convert {"columns": [...], "rows": [[v1, v2], ...]} to a DataFrame."""
    columns = data.get("columns", [])
    rows = data.get("rows", [])
    return pd.DataFrame(rows, columns=columns)


class Transaction:
    """Context manager for server-side transactions."""

    def __init__(self, client: "Client") -> None:
        self._client = client
        self._txn_id: Optional[str] = None

    def __enter__(self) -> "Transaction":
        resp = self._client._session.post(
            f"{self._client._base_url}/api/tables/query",
            json={"sql": "BEGIN"},
            timeout=self._client._timeout,
        )
        _raise_for_status(resp)
        data = resp.json()
        # Server may return txn_id in several places
        self._txn_id = (
            data.get("txn_id")
            or data.get("transaction_id")
            or str(data.get("id", ""))
            or None
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        sql = "ROLLBACK" if exc_type is not None else "COMMIT"
        try:
            headers = {}
            if self._txn_id:
                headers["X-Txn-Id"] = str(self._txn_id)
            resp = self._client._session.post(
                f"{self._client._base_url}/api/tables/query",
                json={"sql": sql},
                headers=headers,
                timeout=self._client._timeout,
            )
            _raise_for_status(resp)
        except Exception:
            pass  # Don't mask the original exception
        return False  # Re-raise any original exception

    def query(self, sql: str) -> pd.DataFrame:
        """Execute SQL within this transaction."""
        headers = {}
        if self._txn_id:
            headers["X-Txn-Id"] = str(self._txn_id)
        resp = self._client._session.post(
            f"{self._client._base_url}/api/tables/query",
            json={"sql": sql},
            headers=headers,
            timeout=self._client._timeout,
        )
        _raise_for_status(resp)
        return _parse_query_result(resp.json())


class Client:
    """NAPASTAK HTTP client."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_ssl: bool = True,
        timeout: int = 30,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._session.verify = verify_ssl

        if api_key:
            self._session.headers["Authorization"] = f"Bearer {api_key}"
        elif username and password:
            self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        """Authenticate with username/password and store the Bearer token."""
        resp = self._session.post(
            f"{self._base_url}/api/auth/token",
            json={"username": username, "password": password},
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        data = resp.json()
        token = data.get("token") or data.get("access_token") or data.get("jwt")
        if not token:
            raise AuthError("No token returned by server")
        self._session.headers["Authorization"] = f"Bearer {token}"

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute arbitrary SQL and return a DataFrame."""
        resp = self._session.post(
            f"{self._base_url}/api/tables/query",
            json={"sql": sql},
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        return _parse_query_result(resp.json())

    def ingest(self, table: str, df: pd.DataFrame, if_exists: str = "append") -> dict:
        """Upload a DataFrame as CSV to a table."""
        csv_body = df.to_csv(index=False)
        resp = self._session.post(
            f"{self._base_url}/api/tables/{table}/ingest",
            data=csv_body,
            params={"if_exists": if_exists},
            headers={"Content-Type": "text/csv"},
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    def tables(self) -> list:
        """Return list of table info dicts."""
        resp = self._session.get(
            f"{self._base_url}/api/tables",
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("tables", [])

    def table(self, name: str) -> "Table":
        """Return a Table proxy for the given table name."""
        from .table import Table  # local import to avoid circular deps

        return Table(self, name)

    def transaction(self) -> Transaction:
        """Return a Transaction context manager."""
        return Transaction(self)

    def health(self) -> dict:
        """Check server health."""
        resp = self._session.get(
            f"{self._base_url}/health",
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    # ------------------------------------------------------------------
    # Pipeline operations
    # ------------------------------------------------------------------

    def pipelines(self) -> list:
        """List all pipelines."""
        resp = self._session.get(
            f"{self._base_url}/api/pipelines",
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("pipelines", [])

    def create_pipeline(self, config: dict) -> dict:
        """Create a new pipeline from a config dict."""
        resp = self._session.post(
            f"{self._base_url}/api/pipelines",
            json=config,
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    def run_pipeline(self, pipeline_id: Any) -> dict:
        """Trigger a pipeline run."""
        resp = self._session.post(
            f"{self._base_url}/api/pipelines/{pipeline_id}/run",
            timeout=self._timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    def pipeline(self, pipeline_id: Any) -> "Pipeline":
        """Return a Pipeline proxy."""
        from .pipeline import Pipeline  # local import

        return Pipeline(self, pipeline_id)
