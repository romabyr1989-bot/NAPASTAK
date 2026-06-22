"""
TSMDM Proxy — runs the three protocol servers in parallel:

  :3000 — SOAP (Siebel CRM)
  :8443 — REST (Flask, legacy endpoints)
  :8444 — CDI  (FastAPI, HFL/Joker)

Usage:
  python3 main.py
  DFO_REST_URL=http://dfo-gateway:8080 DFO_TOKEN=xxx python3 main.py
"""
import threading

import config
from dfo_client import ensure_tables
from soap_server import run_soap_server
from rest_server import run_rest_server
from cdi_server import run_cdi_server


def main():
    print("TSMDM Proxy starting")
    print(f"  DFO REST: {config.DFO_REST_URL}")
    print(f"  SOAP    : {config.SOAP_PORT}")
    print(f"  REST    : {config.REST_PORT}")
    print(f"  CDI     : {config.CDI_PORT}")

    ensure_tables()

    # SOAP + REST run in daemon threads; CDI (uvicorn) blocks the main thread.
    threading.Thread(target=run_soap_server, daemon=True).start()
    threading.Thread(target=run_rest_server, daemon=True).start()
    run_cdi_server()


if __name__ == "__main__":
    main()
