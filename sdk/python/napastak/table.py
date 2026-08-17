from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .client import Client


class Table:
    """Proxy object for a NAPASTAK table."""

    def __init__(self, client: "Client", name: str) -> None:
        self._client = client
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def select(self, sql_fragment: str = "*") -> pd.DataFrame:
        """Execute SELECT {sql_fragment} FROM {table}.

        sql_fragment can be "*", a column list, or a full SQL expression.
        If it looks like a full SQL statement it is passed through unchanged.
        """
        fragment = sql_fragment.strip()
        if fragment.upper().startswith("SELECT"):
            sql = fragment
        else:
            sql = f"SELECT {fragment} FROM {self._name}"
        return self._client.query(sql)

    def insert(self, df: pd.DataFrame) -> dict:
        """Ingest a DataFrame into this table (append mode)."""
        return self._client.ingest(self._name, df, if_exists="append")

    def schema(self) -> dict:
        """Retrieve column schema for this table."""
        resp = self._client._session.get(
            f"{self._client._base_url}/api/tables/{self._name}/schema",
            timeout=self._client._timeout,
        )
        from .client import _raise_for_status

        _raise_for_status(resp)
        return resp.json()

    def count(self) -> int:
        """Return the number of rows in this table."""
        df = self._client.query(f"SELECT COUNT(*) AS n FROM {self._name}")
        if df.empty:
            return 0
        return int(df.iloc[0, 0])

    def head(self, n: int = 5) -> pd.DataFrame:
        """Return the first *n* rows of this table."""
        return self._client.query(f"SELECT * FROM {self._name} LIMIT {n}")
