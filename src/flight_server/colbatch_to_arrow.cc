/* colbatch_to_arrow.cc — JSON query result → arrow::Table.
 *
 * The DataFlow OS gateway returns query results as:
 *   { "columns": ["a", "b", ...],
 *     "rows":    [[1, "x"], [2, "y"]],
 *     "duration_ms": 12 }
 *
 * No `types` field is included today, so we infer per-column types by
 * scanning the first non-null value. INT64 / DOUBLE / BOOL / STRING are
 * the only types we emit; complex values are stringified to STRING.
 *
 * For schemas larger than ~64 columns or rows >100k this approach builds
 * the entire table in memory — Step 2 ships that as a known limitation.
 */
#include <arrow/api.h>
#include <arrow/builder.h>
#include <nlohmann/json.hpp>

namespace dfo {

using nlohmann::json;

namespace {

enum class InferredType { Int64, Float64, Bool, Utf8 };

/* Определяет тип столбца по одному образцовому значению;
 * всё, что не bool/число, считается строкой (Utf8). */
InferredType infer_type(const json& sample) {
    if (sample.is_boolean())            return InferredType::Bool;
    if (sample.is_number_integer())     return InferredType::Int64;
    if (sample.is_number_unsigned())    return InferredType::Int64;
    if (sample.is_number_float())       return InferredType::Float64;
    return InferredType::Utf8;
}

/* Сопоставляет выведенный InferredType соответствующему arrow::DataType. */
std::shared_ptr<arrow::DataType> inferred_to_arrow(InferredType t) {
    switch (t) {
        case InferredType::Bool:    return arrow::boolean();
        case InferredType::Int64:   return arrow::int64();
        case InferredType::Float64: return arrow::float64();
        case InferredType::Utf8:
        default:                    return arrow::utf8();
    }
}

}  // namespace

/* Конвертирует JSON-результат запроса gateway ({columns,rows,...}) в
 * arrow::Table. Типы столбцов выводятся по первой строке; ячейки при
 * необходимости приводятся к целевому типу (строки парсятся в число,
 * несовместимые/null значения дают AppendNull). Возвращает ошибку, если
 * отсутствуют обязательные поля columns[]/rows[]. */
arrow::Result<std::shared_ptr<arrow::Table>> json_query_result_to_arrow(
    const json& result) {

    if (!result.is_object())
        return arrow::Status::Invalid("query result is not a JSON object");

    auto cols_it = result.find("columns");
    auto rows_it = result.find("rows");
    if (cols_it == result.end() || !cols_it->is_array())
        return arrow::Status::Invalid("missing columns[]");
    if (rows_it == result.end() || !rows_it->is_array())
        return arrow::Status::Invalid("missing rows[]");

    const auto& cols = *cols_it;
    const auto& rows = *rows_it;
    const size_t ncols = cols.size();

    /* Infer types from the first row. Empty results default to utf8. */
    std::vector<InferredType> types(ncols, InferredType::Utf8);
    if (!rows.empty() && rows[0].is_array()) {
        for (size_t i = 0; i < ncols && i < rows[0].size(); i++) {
            const auto& v = rows[0][i];
            if (!v.is_null()) types[i] = infer_type(v);
        }
    }

    /* Build the schema */
    arrow::FieldVector fields;
    fields.reserve(ncols);
    for (size_t i = 0; i < ncols; i++) {
        std::string name = cols[i].is_string() ? cols[i].get<std::string>()
                                               : "col" + std::to_string(i);
        fields.push_back(arrow::field(name, inferred_to_arrow(types[i])));
    }
    auto schema = arrow::schema(fields);

    /* Build per-column arrays */
    auto* pool = arrow::default_memory_pool();
    std::vector<std::shared_ptr<arrow::Array>> arrays(ncols);
    for (size_t c = 0; c < ncols; c++) {
        std::shared_ptr<arrow::ArrayBuilder> b;
        switch (types[c]) {
            case InferredType::Bool:
                b = std::make_shared<arrow::BooleanBuilder>(pool); break;
            case InferredType::Int64:
                b = std::make_shared<arrow::Int64Builder>(pool); break;
            case InferredType::Float64:
                b = std::make_shared<arrow::DoubleBuilder>(pool); break;
            default:
                b = std::make_shared<arrow::StringBuilder>(pool); break;
        }
        ARROW_RETURN_NOT_OK(b->Reserve(rows.size()));

        for (const auto& row : rows) {
            if (!row.is_array() || c >= row.size() || row[c].is_null()) {
                ARROW_RETURN_NOT_OK(b->AppendNull());
                continue;
            }
            const json& v = row[c];
            switch (types[c]) {
                case InferredType::Bool: {
                    auto* bb = static_cast<arrow::BooleanBuilder*>(b.get());
                    if (v.is_boolean())          ARROW_RETURN_NOT_OK(bb->Append(v.get<bool>()));
                    else if (v.is_number())      ARROW_RETURN_NOT_OK(bb->Append(v.get<double>() != 0));
                    else if (v.is_string()) {
                        std::string s = v.get<std::string>();
                        ARROW_RETURN_NOT_OK(bb->Append(s == "true" || s == "1"));
                    } else                        ARROW_RETURN_NOT_OK(bb->AppendNull());
                    break;
                }
                case InferredType::Int64: {
                    auto* ib = static_cast<arrow::Int64Builder*>(b.get());
                    if      (v.is_number_integer())  ARROW_RETURN_NOT_OK(ib->Append(v.get<int64_t>()));
                    else if (v.is_number_unsigned()) ARROW_RETURN_NOT_OK(ib->Append(static_cast<int64_t>(v.get<uint64_t>())));
                    else if (v.is_number_float())    ARROW_RETURN_NOT_OK(ib->Append(static_cast<int64_t>(v.get<double>())));
                    else if (v.is_string()) {
                        try { ARROW_RETURN_NOT_OK(ib->Append(std::stoll(v.get<std::string>()))); }
                        catch (...) { ARROW_RETURN_NOT_OK(ib->AppendNull()); }
                    } else                           ARROW_RETURN_NOT_OK(ib->AppendNull());
                    break;
                }
                case InferredType::Float64: {
                    auto* db = static_cast<arrow::DoubleBuilder*>(b.get());
                    if      (v.is_number())  ARROW_RETURN_NOT_OK(db->Append(v.get<double>()));
                    else if (v.is_string()) {
                        try { ARROW_RETURN_NOT_OK(db->Append(std::stod(v.get<std::string>()))); }
                        catch (...) { ARROW_RETURN_NOT_OK(db->AppendNull()); }
                    } else                   ARROW_RETURN_NOT_OK(db->AppendNull());
                    break;
                }
                default: {
                    auto* sb = static_cast<arrow::StringBuilder*>(b.get());
                    if (v.is_string())       ARROW_RETURN_NOT_OK(sb->Append(v.get<std::string>()));
                    else                     ARROW_RETURN_NOT_OK(sb->Append(v.dump()));
                    break;
                }
            }
        }

        ARROW_RETURN_NOT_OK(b->Finish(&arrays[c]));
    }

    return arrow::Table::Make(schema, arrays, static_cast<int64_t>(rows.size()));
}

}  // namespace dfo
