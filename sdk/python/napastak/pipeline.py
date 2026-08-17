from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .client import Client


class Pipeline:
    """Proxy object for a NAPASTAK pipeline."""

    def __init__(self, client: "Client", pipeline_id: Any) -> None:
        self._client = client
        self._id = pipeline_id

    @property
    def id(self) -> Any:
        return self._id

    def run(self) -> dict:
        """Trigger this pipeline and return the run metadata."""
        return self._client.run_pipeline(self._id)

    def get(self) -> dict:
        """Fetch current pipeline configuration/status."""
        from .client import _raise_for_status

        resp = self._client._session.get(
            f"{self._client._base_url}/api/pipelines/{self._id}",
            timeout=self._client._timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    def delete(self) -> dict:
        """Delete this pipeline."""
        from .client import _raise_for_status

        resp = self._client._session.delete(
            f"{self._client._base_url}/api/pipelines/{self._id}",
            timeout=self._client._timeout,
        )
        _raise_for_status(resp)
        return resp.json()

    def runs(self) -> list:
        """List all runs for this pipeline."""
        from .client import _raise_for_status

        resp = self._client._session.get(
            f"{self._client._base_url}/api/pipelines/{self._id}/runs",
            timeout=self._client._timeout,
        )
        _raise_for_status(resp)
        data = resp.json()
        if isinstance(data, list):
            return data
        return data.get("runs", [])
