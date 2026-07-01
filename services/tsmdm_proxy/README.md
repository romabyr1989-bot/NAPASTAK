# TSMDM Proxy

Thin protocol-translation service between bank systems (Siebel CRM, CDI/Joker
Kafka consumers, internal REST clients) and NAPASTAK. It holds **no business
logic of its own** — every request becomes a SQL query against DFO via the REST
API (`POST /api/query/named`) or PgWire (`:5432`), and the result is returned in
the protocol the caller expects.

This keeps DFO a clean data store; the bank-specific SOAP/CDI surface lives here.

## Run

```bash
cd services/tsmdm_proxy
pip install -r requirements.txt
export DFO_REST_URL=http://localhost:8080
export DFO_TOKEN=<jwt_token>          # obtain via POST /api/auth/token
python3 main.py
```

Or with Docker (see the root `docker-compose.yml` for the `tsmdm-proxy` service):

```bash
docker build -t tsmdm-proxy services/tsmdm_proxy
```

## Ports

| Port | Protocol  | Caller             | Endpoints                                   |
|------|-----------|--------------------|---------------------------------------------|
| 3000 | SOAP 1.1  | Siebel CRM         | `get_agreements_by_party_id`                |
| 8443 | HTTP REST | Internal services  | `/tsmdm/getPersonInfo`, `/tsmdm/get_crm_id/<sys>/<key>`, `/tsmdm/get_agreements`, `/tsmdm/search_or_create_client_fl` |
| 8444 | HTTP REST | Kafka CDI consumer | `/cdi/soap/services/2_13/PartyRA/{getByHID,getSourceAttributes,saveAndMerge,getByRawID}` |

## Environment

| Variable        | Default                       | Description          |
|-----------------|-------------------------------|----------------------|
| `DFO_REST_URL`  | `http://localhost:8080`       | DFO gateway base URL |
| `DFO_PGWIRE_DSN`| `host=localhost port=5432 …`  | PgWire DSN (DDL)     |
| `DFO_TOKEN`     | (empty)                       | JWT / API key        |
| `SOAP_PORT`     | `3000`                        | SOAP server port     |
| `REST_PORT`     | `8443`                        | REST server port     |
| `CDI_PORT`      | `8444`                        | CDI server port      |
| `DFO_TIMEOUT_SEC`| `30`                         | Per-request timeout  |

## DFO tables (created idempotently at startup)

- `agreement2party` — client agreements (SOAP + REST read this)
- `crm_key_map` — CRM key mapping (analog of `S_CIF_CON_MAP`)
- `master_person` — golden person records
- `cdi_*` — CDI entities; normally produced by DFO pipelines (steps 1–3),
  created here too so the proxy starts against an empty DFO.

> **DFO null convention:** DFO stores blanks as the empty string `""`, not SQL
> `NULL`. The proxy's domain queries therefore filter with
> `(col IS NULL OR col = '')` for the deleted/expired flags.

## Notes

- `POST /api/query/named` is SELECT-only and RBAC-checked; the proxy never
  mutates DFO through it. DDL (`ensure_tables`) goes through PgWire.
- `search_or_create_client_fl` only *searches* the master — record creation in
  Siebel is out of the proxy's scope (returns `CrmID: null` when not found).
- `saveAndMerge` acknowledges the incoming record; the actual merge is performed
  by DFO match/SCD2 pipelines.
