"""TSMDM Proxy configuration (all overridable via environment)."""
import os

DFO_REST_URL   = os.getenv("DFO_REST_URL",   "http://localhost:8080")
DFO_PGWIRE_DSN = os.getenv("DFO_PGWIRE_DSN", "host=localhost port=5432 user=admin password=admin dbname=dataflow")
DFO_TOKEN      = os.getenv("DFO_TOKEN",       "")   # JWT or API key for the REST API

SOAP_PORT      = int(os.getenv("SOAP_PORT",  "3000"))
REST_PORT      = int(os.getenv("REST_PORT",  "8443"))
CDI_PORT       = int(os.getenv("CDI_PORT",   "8444"))

DFO_TIMEOUT_SEC = int(os.getenv("DFO_TIMEOUT_SEC", "30"))
