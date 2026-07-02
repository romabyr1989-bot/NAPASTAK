/* flight_service.cc — see flight_service.h
 *
 * Implementation notes:
 *   - DoGet builds the entire result set in memory then streams it. Fine
 *     for small/medium queries; large results would benefit from a
 *     pull-based reader. Future work.
 *   - GetFlightInfo for a SQL ticket cannot know the schema without
 *     executing — we run the query with LIMIT 0 to discover columns.
 *   - The auth token (Bearer JWT) flows through to every gateway call.
 */
#include "flight_service.h"

#include <arrow/api.h>
#include <arrow/flight/api.h>
#include <arrow/util/future.h>

#include <nlohmann/json.hpp>

#include <iostream>
#include <vector>

#include "http_client.h"

namespace dfo {

/* defined in colbatch_to_arrow.cc */
arrow::Result<std::shared_ptr<arrow::Table>> json_query_result_to_arrow(
    const nlohmann::json& result);

namespace flight = arrow::flight;
using nlohmann::json;

/* ── ctor / dtor ───────────────────────────────────────────────── */
DfoFlightService::DfoFlightService(std::string gateway_url, std::string api_key)
    : gateway_url_(std::move(gateway_url)), api_key_(std::move(api_key)) {
    http_ = std::make_unique<HttpClient>(gateway_url_, api_key_);
}

DfoFlightService::~DfoFlightService() = default;

/* ── helpers ───────────────────────────────────────────────────── */
namespace {

/* Извлекает SQL из полезной нагрузки Flight Ticket:
 *   "sql:…"   — берётся текст после префикса;
 *   "table:X" — превращается в SELECT * FROM X;
 *   иначе payload трактуется как сырой SQL. */
std::string ticket_to_sql(const flight::Ticket& t) {
    std::string s(reinterpret_cast<const char*>(t.ticket.data()), t.ticket.size());
    if (s.rfind("sql:", 0) == 0)   return s.substr(4);
    if (s.rfind("table:", 0) == 0) return "SELECT * FROM " + s.substr(6);
    return s; // assume raw SQL
}

/* Build the "sql:..." string used as a Flight ticket payload. */
std::string sql_ticket_str(const std::string& sql) { return "sql:" + sql; }

}  // namespace

/* ── ListFlights — every table is a flight ──────────────────── */
/* Перечисляет все таблицы gateway (GET /api/tables) как отдельные flights.
 * Для каждой таблицы схема выясняется отдельным запросом SELECT * LIMIT 0,
 * а Ticket формируется как "sql:SELECT * FROM <table>". Таблицы, схему
 * которых получить не удалось, молча пропускаются. */
arrow::Status DfoFlightService::ListFlights(
    const flight::ServerCallContext&,
    const flight::Criteria*,
    std::unique_ptr<flight::FlightListing>* listings) {

    auto resp = http_->get("/api/tables");
    if (resp.status < 0)
        return arrow::Status::IOError("gateway transport error: " + resp.error);
    if (resp.status != 200)
        return arrow::Status::IOError("gateway HTTP " + std::to_string(resp.status));

    json arr;
    try { arr = json::parse(resp.body); }
    catch (const std::exception& e) {
        return arrow::Status::Invalid(
            std::string("gateway returned invalid JSON: ") + e.what());
    }
    if (!arr.is_array())
        return arrow::Status::Invalid("expected array from /api/tables");

    std::vector<flight::FlightInfo> infos;
    infos.reserve(arr.size());
    for (const auto& t : arr) {
        std::string name = t.value("name", "");
        if (name.empty()) continue;

        /* Discover schema by running SELECT * LIMIT 0 — the gateway
         * still returns the column list. */
        std::string body = "{\"sql\":\"SELECT * FROM " + name + " LIMIT 0\"}";
        auto qresp = http_->post_json("/api/tables/query", body);
        if (qresp.status != 200) continue;
        json qjson;
        try { qjson = json::parse(qresp.body); } catch (...) { continue; }
        auto table_or = json_query_result_to_arrow(qjson);
        if (!table_or.ok()) continue;
        auto schema = (*table_or)->schema();

        auto descriptor = flight::FlightDescriptor::Path({name});
        flight::FlightEndpoint endpoint;
        endpoint.ticket = flight::Ticket{ sql_ticket_str("SELECT * FROM " + name) };
        auto info_or = flight::FlightInfo::Make(*schema, descriptor,
                                                {endpoint},
                                                static_cast<int64_t>(t.value("rows", 0)),
                                                /*total_bytes=*/-1);
        if (info_or.ok()) infos.push_back(std::move(*info_or));
    }

    *listings = std::make_unique<flight::SimpleFlightListing>(std::move(infos));
    return arrow::Status::OK();
}

/* ── GetFlightInfo — schema of one flight ───────────────────── */
/* Возвращает FlightInfo (в первую очередь схему) для одного flight.
 * Дескриптор PATH → "SELECT * FROM <path>"; дескриптор CMD → SQL из
 * команды (с префиксом "sql:" или без). Схема выясняется прогоном запроса
 * с добавленным LIMIT 0, число записей/байт неизвестно и отдаётся как -1. */
arrow::Status DfoFlightService::GetFlightInfo(
    const flight::ServerCallContext&,
    const flight::FlightDescriptor& descriptor,
    std::unique_ptr<flight::FlightInfo>* info) {

    std::string sql;
    if (descriptor.type == flight::FlightDescriptor::PATH && !descriptor.path.empty()) {
        sql = "SELECT * FROM " + descriptor.path[0] + " LIMIT 0";
    } else if (descriptor.type == flight::FlightDescriptor::CMD) {
        std::string cmd(descriptor.cmd.begin(), descriptor.cmd.end());
        if (cmd.rfind("sql:", 0) == 0) sql = cmd.substr(4) + " LIMIT 0";
        else                            sql = cmd + " LIMIT 0";
    } else {
        return arrow::Status::Invalid("descriptor must be PATH or CMD");
    }

    std::string body = "{\"sql\":\"" + sql + "\"}";
    auto qresp = http_->post_json("/api/tables/query", body);
    if (qresp.status != 200)
        return arrow::Status::IOError("gateway HTTP " + std::to_string(qresp.status));
    json qjson;
    try { qjson = json::parse(qresp.body); }
    catch (const std::exception& e) {
        return arrow::Status::Invalid(std::string("invalid JSON: ") + e.what());
    }
    ARROW_ASSIGN_OR_RAISE(auto table, json_query_result_to_arrow(qjson));

    flight::FlightEndpoint endpoint;
    endpoint.ticket = flight::Ticket{ sql_ticket_str(sql) };
    ARROW_ASSIGN_OR_RAISE(auto fi, flight::FlightInfo::Make(*table->schema(),
                                                             descriptor,
                                                             {endpoint},
                                                             /*total_records=*/-1,
                                                             /*total_bytes=*/-1));
    *info = std::make_unique<flight::FlightInfo>(std::move(fi));
    return arrow::Status::OK();
}

/* ── DoGet — execute SQL, stream Arrow ──────────────────────── */
/* Основной путь чтения: извлекает SQL из Ticket, экранирует его для JSON,
 * выполняет через POST /api/tables/query, конвертирует ответ в arrow::Table
 * и отдаёт клиенту потоком record batch'ей. Таблица целиком строится в
 * памяти (см. замечание о будущей pull-модели в шапке файла). */
arrow::Status DfoFlightService::DoGet(
    const flight::ServerCallContext&,
    const flight::Ticket& request,
    std::unique_ptr<flight::FlightDataStream>* stream) {

    std::string sql = ticket_to_sql(request);
    if (sql.empty())
        return arrow::Status::Invalid("ticket is empty");

    /* Escape quotes in SQL for the JSON wrapper */
    std::string esc;
    esc.reserve(sql.size() + 16);
    for (char c : sql) {
        if      (c == '"')  esc += "\\\"";
        else if (c == '\\') esc += "\\\\";
        else if (c == '\n') esc += "\\n";
        else if (c == '\r') esc += "\\r";
        else if (c == '\t') esc += "\\t";
        else                esc += c;
    }
    std::string body = "{\"sql\":\"" + esc + "\"}";
    auto resp = http_->post_json("/api/tables/query", body);
    if (resp.status < 0)
        return arrow::Status::IOError("gateway transport error: " + resp.error);
    if (resp.status != 200)
        return arrow::Status::IOError("gateway HTTP " + std::to_string(resp.status) +
                                      ": " + resp.body.substr(0, 200));

    json qjson;
    try { qjson = json::parse(resp.body); }
    catch (const std::exception& e) {
        return arrow::Status::Invalid(std::string("invalid JSON: ") + e.what());
    }
    ARROW_ASSIGN_OR_RAISE(auto table, json_query_result_to_arrow(qjson));

    /* TableBatchReader streams Table → record batches.
     * Pass the shared_ptr<Table> overload (NOT the Table& one) so the
     * table outlives the reader — DoGet returns immediately while the
     * stream is consumed asynchronously by gRPC. */
    auto reader = std::make_shared<arrow::TableBatchReader>(table);
    *stream = std::make_unique<flight::RecordBatchStream>(reader);
    return arrow::Status::OK();
}

/* ── DoPut — stub (Step 2 ships read-only) ───────────────────── */
/* Запись через Flight пока не поддержана: возвращает NotImplemented и
 * подсказывает обходной путь — POST /api/ingest/csv на gateway. */
arrow::Status DfoFlightService::DoPut(
    const flight::ServerCallContext&,
    std::unique_ptr<flight::FlightMessageReader>,
    std::unique_ptr<flight::FlightMetadataWriter>) {
    return arrow::Status::NotImplemented(
        "DoPut is not yet implemented. Use POST /api/ingest/csv on the gateway "
        "as a workaround for ingesting from clients.");
}

}  // namespace dfo
