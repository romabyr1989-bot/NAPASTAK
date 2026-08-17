"""NAPASTAK Python SDK.

Top-level imports are lazy: importing the package does not pull in
optional dependencies (`requests`, `pandas`). Submodules that need
them raise an informative ImportError when accessed.

Examples:
    from napastak import Client                    # needs requests
    from napastak.airflow_import import main       # stdlib only
"""
from .exceptions import AuthError, PermissionError, TableNotFoundError, NapastakError

__all__ = [
    "Client",
    "Table",
    "Pipeline",
    "AuthError",
    "PermissionError",
    "TableNotFoundError",
    "NapastakError",
]
__version__ = "0.1.0"


def __getattr__(name):
    """Lazy attribute access — only imports submodule when used."""
    if name == "Client":
        from .client import Client
        return Client
    if name == "Table":
        from .table import Table
        return Table
    if name == "Pipeline":
        from .pipeline import Pipeline
        return Pipeline
    raise AttributeError(f"module 'napastak' has no attribute {name!r}")
