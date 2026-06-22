"""
CDI server (:8444): replicates TSMDM hfl_simulator.py — the HFL/Joker endpoints
Kafka CDI consumers call. Translates each into a DFO SELECT.

  POST .../PartyRA/getByHID            — party by HID (primary key)
  POST .../PartyRA/getSourceAttributes — attributes by master HID
  POST .../PartyRA/saveAndMerge        — accept an incoming record (stub)
  POST .../PartyRA/getByRawID          — party by raw_id + source system
"""
import fastapi
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from dfo_client import dfo_query

app = fastapi.FastAPI(title="TSMDM CDI Proxy")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

BASE = "/cdi/soap/services/2_13/PartyRA"

# CDI entity type → DFO table
ENTITY_MAP = {
    "PHYSICAL": "cdi_physical_party",
    "LEGAL":    "cdi_party",
    "PHONE":    "cdi_phone",
    "DOCUMENT": "cdi_document",
    "ADDRESS":  "cdi_address",
}


class PartyRequest(BaseModel):
    hid:  int
    type: str = "PHYSICAL"


class PartyRequest2(BaseModel):
    hid:    str
    type:   str = "PHYSICAL"
    system: str = ""


class AttributeRequest(BaseModel):
    partyType:     str
    attributeType: str
    attributeHid:  int


@app.post(f"{BASE}/getByHID")
async def get_by_hid(request: PartyRequest):
    entity = ENTITY_MAP.get(request.type.upper(), "cdi_physical_party")
    rows = dfo_query(f"SELECT * FROM {entity} WHERE hid = :hid", {"hid": str(request.hid)})
    if not rows:
        return JSONResponse(status_code=404, content={"error": "not found"})
    row = rows[0]
    return {
        "party":        {"field": [{"name": k, "value": v} for k, v in row.items()]},
        "recordId":     request.hid,
        "sourceSystem": row.get("source_system", ""),
        "rawId":        row.get("raw_id", ""),
        "attributes":   [],
        "type":         request.type,
    }


@app.post(f"{BASE}/getSourceAttributes")
async def get_source_attributes(request: AttributeRequest):
    entity = ENTITY_MAP.get(request.attributeType.upper(), "cdi_document")
    rows = dfo_query(
        f"SELECT attr.* FROM cdi_merged_attribute ma "
        f"JOIN {entity} attr ON ma.original_hid = attr.hid "
        f"WHERE ma.final_hid = :hid AND ma.attribute_type = :atype",
        {"hid": str(request.attributeHid), "atype": request.attributeType})
    return {"attribute": [
        {"field": [{"name": k, "value": v} for k, v in row.items()],
         "type": request.attributeType, "hid": row.get("hid", ""),
         "rawId": row.get("raw_id", ""), "deleted": None}
        for row in rows]}


@app.post(f"{BASE}/saveAndMerge")
async def save_and_merge(request: fastapi.Request):
    """Accept an incoming CDI record. Persisting/merging is done by DFO pipelines
    (steps 1–3), so here we only validate the body and acknowledge — the real
    merge is out of the proxy's scope."""
    try:
        await request.json()
    except Exception:  # noqa: BLE001
        return JSONResponse(status_code=400, content={"error": "invalid JSON body"})
    return fastapi.Response(status_code=200)


@app.post(f"{BASE}/getByRawID")
async def get_by_raw_id(request: PartyRequest2):
    entity = ENTITY_MAP.get(request.type.upper(), "cdi_physical_party")
    rows = dfo_query(
        f"SELECT mst.* FROM {entity} attr "
        f"JOIN cdi_merged mrg ON mrg.hid = attr.hid "
        f"JOIN {entity} mst ON mst.hid = mrg.master_hid "
        f"WHERE attr.raw_id = :rid AND attr.source_system = :sys",
        {"rid": request.hid, "sys": request.system})
    if not rows:
        return JSONResponse(status_code=404, content={"error": "not found"})
    return rows[0]


def run_cdi_server():
    import uvicorn
    import config
    print(f"CDI server listening on :{config.CDI_PORT}")
    uvicorn.run(app, host="0.0.0.0", port=config.CDI_PORT)
