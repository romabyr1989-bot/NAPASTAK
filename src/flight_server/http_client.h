/* http_client.h — thin libcurl wrapper used by the Flight server to talk to
 * the DataFlow OS gateway. Synchronous; one client instance is fine to
 * share across requests since libcurl handles are per-call. */
#pragma once
#include <string>

namespace dfo {

/* Результат HTTP-вызова: код состояния, тело ответа и сообщение об ошибке. */
struct HttpResponse {
    int         status = 0;     // -1 on transport error
    std::string body;
    std::string error;
};

class HttpClient {
public:
    /* base_url — корень gateway; bearer — токен для заголовка Authorization. */
    HttpClient(std::string base_url, std::string bearer);
    ~HttpClient();

    /* GET по path относительно base_url. */
    HttpResponse get   (const std::string& path) const;
    /* POST с телом JSON (Content-Type: application/json). */
    HttpResponse post_json(const std::string& path,
                           const std::string& json_body) const;
    /* POST с произвольным телом и явно заданным content_type. */
    HttpResponse post_raw (const std::string& path,
                           const std::string& body,
                           const std::string& content_type) const;

private:
    std::string base_;
    std::string bearer_;
};

}  // namespace dfo
