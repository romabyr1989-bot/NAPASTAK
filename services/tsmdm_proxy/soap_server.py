"""
SOAP server (:3000): translates Siebel CRM agreement requests into DFO SELECTs.

Contract:
  get_agreements_by_party_id(party_id, agreement_type_cd) -> [AgreementForPosition]

Logic (mirrors TSMDM agr2party_database.get_all_legal_agreements):
  1. collect every CRM key linked to party_id via crm_key_map (S_CIF_CON_MAP)
  2. select active agreements from agreement2party for those keys
"""
from spyne import Application, Iterable, ServiceBase, Unicode, rpc, ComplexModel
from spyne.protocol.soap import Soap11
from spyne.server.wsgi import WsgiApplication
from spyne.util.wsgi_wrapper import WsgiMounter
from wsgiref.simple_server import make_server

from dfo_client import dfo_query
import config


def _sql_lit(s: str) -> str:
    """Single-quoted SQL literal with internal quotes doubled (for the OR-list
    of ids, which can't use a single named param)."""
    return "'" + str(s).replace("'", "''") + "'"


class AgreementForPosition(ComplexModel):
    crm_id              = Unicode
    agreement_id        = Unicode
    abs_cd              = Unicode
    owner_id            = Unicode
    created_ts          = Unicode
    expired_ts          = Unicode
    scopes_ls           = Unicode
    agreement_type_cd   = Unicode
    agreement_file_link = Unicode


class AgreementService(ServiceBase):

    @rpc(Unicode, Unicode, _returns=Iterable(AgreementForPosition))
    def get_agreements_by_party_id(ctx, party_id, agreement_type_cd):
        if not party_id:
            yield AgreementForPosition(crm_id="", agreement_id="",
                                       agreement_file_link="ERR:no party_id")
            return

        # 1. all CRM keys for this client (_get_all_keys_by_known_party_id)
        key_rows = dfo_query(
            "SELECT ext_cust_id FROM crm_key_map "
            "WHERE (con_id = :pid OR ext_cust_id = :pid) AND system_num = 'CRM'",
            {"pid": party_id})
        all_ids = list({r["ext_cust_id"] for r in key_rows if r.get("ext_cust_id")} | {party_id})
        id_filter = " OR ".join(f"crm_id = {_sql_lit(i)}" for i in all_ids)

        # 2. active agreements (_get_agreements_by_collection). DFO stores blanks
        #    as "" not NULL, so test for both.
        agr_type = agreement_type_cd or "BKI"
        agreements = dfo_query(
            "SELECT crm_id, agreement_id, abs_cd, owner_id, created_ts, "
            "expired_ts, scopes_ls, agreement_type_cd, agreement_file_link "
            "FROM agreement2party "
            f"WHERE ({id_filter}) "
            "AND (map__deleted IS NULL OR map__deleted = '') "
            "AND (deleted_ts IS NULL OR deleted_ts = '') "
            "AND agreement_type_cd = :type",
            {"type": agr_type})

        if not agreements:
            yield AgreementForPosition()
            return

        for row in agreements:
            yield AgreementForPosition(
                crm_id              = str(row.get("crm_id", "")),
                agreement_id        = str(row.get("agreement_id", "")),
                abs_cd              = str(row.get("abs_cd", "")).replace("COMM_", ""),
                owner_id            = str(row.get("owner_id", "")),
                created_ts          = str(row.get("created_ts", "")),
                expired_ts          = str(row.get("expired_ts", "")),
                scopes_ls           = str(row.get("scopes_ls", "")),
                agreement_type_cd   = str(row.get("agreement_type_cd", "")),
                agreement_file_link = str(row.get("agreement_file_link", "")),
            )


def make_soap_app():
    return Application(
        services=[AgreementService],
        tns="abb.tsmdm.soap",
        in_protocol=Soap11(validator="lxml"),
        out_protocol=Soap11(polymorphic=True),
        name="tsmdm2sbl",
    )


def run_soap_server():
    wsgi = WsgiMounter({
        "get_agreements_by_party_id": WsgiApplication(make_soap_app()),
    })
    server = make_server("0.0.0.0", config.SOAP_PORT, wsgi)
    print(f"SOAP server listening on :{config.SOAP_PORT}")
    server.serve_forever()
