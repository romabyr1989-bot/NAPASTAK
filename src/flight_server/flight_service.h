/* flight_service.h — Arrow Flight service that bridges to the NAPASTAK
 * gateway over HTTP. Implements a minimal subset of the Flight protocol:
 *
 *   ListFlights     — enumerate tables as flights
 *   GetFlightInfo   — schema of a specific flight (table or query)
 *   DoGet           — execute a SQL query, stream Arrow record batches
 *   DoPut           — TODO; currently returns Status::NotImplemented
 *   DoExchange      — not implemented
 *
 * Supported Ticket payload formats:
 *   "sql:SELECT …"      — execute SQL via /api/tables/query
 *   "table:my_table"    — equivalent to "sql:SELECT * FROM my_table"
 */
#pragma once
#include <arrow/flight/server.h>
#include <memory>
#include <string>

#include "http_client.h"

namespace dfo {

/* Реализация Arrow Flight-сервера, проксирующего запросы в HTTP-gateway
 * NAPASTAK. Каждый Flight-вызов транслируется в обращение к /api,
 * а ответ конвертируется в Arrow-структуры. */
class DfoFlightService : public arrow::flight::FlightServerBase {
public:
    /* gateway_url — базовый URL gateway; api_key — ключ для авторизации запросов. */
    DfoFlightService(std::string gateway_url, std::string api_key);
    ~DfoFlightService() override;

    /* Перечисляет доступные таблицы gateway как набор flights. */
    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::Criteria* criteria,
        std::unique_ptr<arrow::flight::FlightListing>* listings) override;

    /* Возвращает схему конкретного flight (таблицы или SQL-запроса). */
    arrow::Status GetFlightInfo(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::FlightDescriptor& descriptor,
        std::unique_ptr<arrow::flight::FlightInfo>* info) override;

    /* Исполняет SQL из Ticket и стримит результат как Arrow record batches. */
    arrow::Status DoGet(
        const arrow::flight::ServerCallContext& ctx,
        const arrow::flight::Ticket& request,
        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

    /* Приём данных от клиента. TODO: пока возвращает Status::NotImplemented. */
    arrow::Status DoPut(
        const arrow::flight::ServerCallContext& ctx,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

private:
    std::unique_ptr<HttpClient> http_;        /* HTTP-клиент для обращений к gateway */
    std::string                 gateway_url_;  /* базовый URL gateway */
    std::string                 api_key_;      /* ключ авторизации API */
};

}  // namespace dfo
