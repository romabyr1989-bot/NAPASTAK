"""
REST server (:8443): replicates TSMDM http_server.py legacy endpoints.

  POST /tsmdm/getPersonInfo               — search a person by name + birth date
  GET  /tsmdm/get_crm_id/<system>/<key>   — external key → CRM id mapping
  POST /tsmdm/get_agreements              — agreements by CRM_ID
  POST /tsmdm/search_or_create_client_fl  — search master (no Siebel create here)
"""
import json
import flask
import flask_cors

from dfo_client import dfo_query
import config

app = flask.Flask(__name__)
flask_cors.CORS(app)


def _body():
    b = flask.request.get_json(force=True, silent=True) or {}
    return json.loads(b) if isinstance(b, str) else b


@app.route("/tsmdm/getPersonInfo", methods=["POST"])
def get_person_info():
    data = _body().get("personSearchClientData", {})
    names = data.get("personNames", {})
    rows = dfo_query(
        "SELECT party_id, mdm_id, crm_id, last_name, first_name, middle_name, birth_date, inn "
        "FROM master_person "
        "WHERE last_name = :ln AND first_name = :fn AND birth_date = :bd",
        {"ln": names.get("personLastName", "").strip(),
         "fn": names.get("personFirstName", "").strip(),
         "bd": data.get("personBirthDate", "").strip()})
    return flask.jsonify(rows)


@app.route("/tsmdm/get_crm_id/<string:system>/<string:external_key>", methods=["GET"])
def get_crm_id(system, external_key):
    rows = dfo_query(
        "SELECT con_id, ext_cust_id FROM crm_key_map "
        "WHERE ext_cust_id = :key AND system_num = :sys",
        {"key": external_key, "sys": system.upper()})
    return flask.jsonify(rows)


@app.route("/tsmdm/get_agreements", methods=["POST", "GET"])
def get_agreements():
    body = _body()
    rows = dfo_query(
        "SELECT crm_id, agreement_id, abs_cd, owner_id, created_ts, "
        "expired_ts, scopes_ls, agreement_type_cd, agreement_file_link "
        "FROM agreement2party "
        "WHERE crm_id = :cid AND agreement_type_cd = :type "
        "AND (map__deleted IS NULL OR map__deleted = '') "
        "AND (deleted_ts IS NULL OR deleted_ts = '')",
        {"cid": body.get("CRM_ID", ""), "type": body.get("agr_type_cd", "BKI")})
    return flask.jsonify(rows)


@app.route("/tsmdm/search_or_create_client_fl", methods=["POST"])
def search_or_create():
    body = _body()
    rows = dfo_query(
        "SELECT crm_id FROM master_person "
        "WHERE last_name = :ln AND first_name = :fn AND birth_date = :bd",
        {"ln": body.get("LastName", "").strip(),
         "fn": body.get("Name", "").strip(),
         "bd": body.get("BirthDate", "").strip()})
    return flask.jsonify({"RequestId": body.get("RequestId", ""),
                          "CrmID": rows[0]["crm_id"] if rows else None})


def run_rest_server():
    print(f"REST server listening on :{config.REST_PORT}")
    app.run(host="0.0.0.0", port=config.REST_PORT)
