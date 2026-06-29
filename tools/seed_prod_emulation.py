#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Seed a realistic TSMDM/Joker prod emulation into DataFlow OS engine tables.

Generates a wide CDI golden table (cdi_physical_party) that serves all three
consumers — MDC matching (src_* cols), agr2party (owner_id/party_id JOIN), and
the CDI proxy (hid/crm_id lookup) — plus cdi_id_doc and consents_agreements,
with realistic cross-system duplication and field-format noise. Also produces
CDI JSON messages to the real prod topic name on the local Redpanda broker.
"""
import json, sys, subprocess, urllib.request

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
TOKEN = None

def auth():
    global TOKEN
    r = urllib.request.urlopen(urllib.request.Request(
        BASE + "/api/auth/token", data=json.dumps({"username":"admin","password":"admin"}).encode(),
        headers={"Content-Type":"application/json"}, method="POST"))
    TOKEN = json.load(r)["token"]

def ingest(table, rows, cols):
    csv = ",".join(cols) + "\n"
    for r in rows:
        csv += ",".join(str(r.get(c,"")).replace(",", " ").replace("\n"," ") for c in cols) + "\n"
    req = urllib.request.Request(BASE + f"/api/ingest/csv?table={table}",
        data=csv.encode("utf-8"), headers={"Authorization":"Bearer "+TOKEN}, method="POST")
    print(f"  {table}: {urllib.request.urlopen(req).read().decode()[:90]}")

# ── realistic Russian identities ────────────────────────────────────────────
LAST = ["Иванов","Петров","Сидоров","Смирнов","Кузнецов","Попов","Васильев",
        "Соколов","Михайлов","Новиков","Фёдоров","Морозов","Волков","Алексеев",
        "Лебедев","Семёнов","Егоров","Павлов","Козлов","Степанов"]
FIRST = ["Иван","Пётр","Мария","Анна","Сергей","Елена","Дмитрий","Ольга",
         "Алексей","Наталья","Андрей","Татьяна"]
MID  = ["Иванович","Петрович","Сергеевна","Алексеевна","Дмитриевич","Олегович"]
SYS  = ["gestdepo","siebel","way4"]

def fem(s): return s.endswith("а") or s in ("Мария","Анна","Елена","Ольга","Наталья","Татьяна")

people = []          # canonical persons (one per real human)
party_rows = []      # cdi_physical_party rows (a person may appear in 2-3 systems)
doc_rows = []
hid = 1000
master_grp = 0
for i in range(40):
    ln = LAST[i % len(LAST)]; fn = FIRST[(i*3) % len(FIRST)]; mn = MID[(i*5) % len(MID)]
    if fem(fn) and ln.endswith("ов"): ln += "а"
    inn = f"77{(10000000000 + i*137):011d}"[:12]
    bd = f"19{60 + (i % 39):02d}-{1+(i%12):02d}-{1+(i%27):02d}"
    docnum = f"{4500 + i:04d} {100000 + i*7:06d}"
    master_grp += 1
    crm = f"CRM{master_grp:05d}"
    n_sys = 1 + (i % 3)                       # 1..3 systems → ~33% are multi-system dupes
    for s in range(n_sys):
        sysname = SYS[s]
        # introduce realistic noise on duplicate copies
        L = ln.upper() if s == 1 else ln
        F = fn.upper() if s == 1 else fn
        innv = inn if s == 0 else ("-".join([inn[:2],inn[2:4],inn[4:10],inn[10:]]) if s==1 else inn)
        dn = docnum if s == 0 else docnum.replace(" ", "")
        ln2 = ln.replace("ё","е") if (s==2 and "ё" in ln) else L
        owner = f"OWN{i:04d}"
        pr = {
            "hid": hid, "crm_id": crm, "mdm_id": f"MDM{master_grp:05d}",
            "source_system": sysname, "raw_id": f"{sysname[:2].upper()}{i:04d}",
            "owner_id": owner, "party_id": str(hid),
            "source_id": f"{sysname[:2].upper()}{hid}", "system_name": sysname,
            "last_upd": f"2026-0{1+(s%6)}-{10+s:02d}",
            "src_last_name": ln2, "src_first_name": F, "src_middle_name": mn,
            "src_fullname": f"{ln2} {F} {mn}", "src_inn": innv,
            "last_name": ln, "first_name": fn, "middle_name": mn, "inn": inn,
            "birth_date": bd,
        }
        party_rows.append(pr)
        doc_rows.append({"source_id": pr["source_id"], "person_source_id": pr["source_id"],
                         "category": "21", "src_number": dn})
        hid += 1
    people.append({"owner_id": f"OWN{i:04d}", "crm_id": crm})

PARTY_COLS = ["hid","crm_id","mdm_id","source_system","raw_id","owner_id","party_id",
              "source_id","system_name","last_upd","src_last_name","src_first_name",
              "src_middle_name","src_fullname","src_inn","last_name","first_name",
              "middle_name","inn","birth_date"]
DOC_COLS = ["source_id","person_source_id","category","src_number"]

# ── consents / agreements (linked to owners) ────────────────────────────────
agr_rows = []
for a in range(100):
    owner = people[a % len(people)]["owner_id"]
    deleted = "2026-05-01" if a % 17 == 0 else ""
    expired = "2025-12-31" if a % 13 == 0 else ""
    agr_rows.append({
        "agreement_id": f"AGR{a:06d}", "abs_cd": f"ABS{a%50:03d}", "owner_id": owner,
        "agreement_type_cd": str(a % 12), "created_ts": f"2026-01-{1+(a%28):02d}",
        "expired_ts": expired, "last_upd_ts": f"2026-04-{1+(a%28):02d}",
        "deleted_ts": deleted, "scopes_ls": f"scope{a%5}", "agreement_file_link": f"doc/{a}.pdf",
    })
AGR_COLS = ["agreement_id","abs_cd","owner_id","agreement_type_cd","created_ts",
            "expired_ts","last_upd_ts","deleted_ts","scopes_ls","agreement_file_link"]

if __name__ == "__main__":
    auth()
    print(f"Сид: {len(party_rows)} party-строк ({len(people)} уникальных людей), "
          f"{len(doc_rows)} документов, {len(agr_rows)} согласий")
    ingest("cdi_physical_party", party_rows, PARTY_COLS)
    ingest("cdi_id_doc", doc_rows, DOC_COLS)
    ingest("consents_agreements", agr_rows, AGR_COLS)

    # ── Kafka: produce CDI messages to the REAL prod topic name ──
    msgs = []
    for pr in party_rows[:50]:
        msgs.append({"ows.CLIENT":[{
            "CCAT":"P","SERVICE_GROUP":"5","CLIENT_ID":pr["raw_id"],"HID":pr["hid"],
            "LAST_NAME":pr["src_last_name"],"FIRST_NAME":pr["src_first_name"],
            "MIDDLE_NAME":pr["src_middle_name"],"INN":pr["src_inn"],"BIRTH_DATE":pr["birth_date"],
            "CRM_ID":pr["crm_id"],"SOURCE_SYSTEM":pr["source_system"]}]})
    # a few LEGAL entities
    for j in range(5):
        msgs.append({"ows.CLIENT":[{"CCAT":"C","CLIENT_ID":f"ORG{j:03d}","NAME":f"ООО Компания{j}","INN":f"78{j:08d}"}]})
    payload = "\n".join(json.dumps(m, ensure_ascii=False) for m in msgs)
    subprocess.run(["docker","exec","dfo-kafka","rpk","topic","create","cdi.physical_party",
                    "--brokers","127.0.0.1:9092"], capture_output=True)
    p = subprocess.run(["docker","exec","-i","dfo-kafka","rpk","topic","produce",
                        "cdi.physical_party","--brokers","127.0.0.1:9092"],
                       input=payload.encode("utf-8"), capture_output=True)
    print(f"  Kafka cdi.physical_party: produced {len(msgs)} msgs "
          f"({p.stderr.decode()[-60:].strip() if p.returncode else 'OK'})")
