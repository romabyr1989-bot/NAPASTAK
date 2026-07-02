/* http_client.cc — see http_client.h */
#include "http_client.h"

#include <curl/curl.h>
#include <iostream>

namespace dfo {

namespace {

/* libcurl-колбэк записи: дописывает порцию тела ответа в std::string. */
size_t write_cb(char* ptr, size_t sz, size_t nm, void* ud) {
    auto* out = static_cast<std::string*>(ud);
    out->append(ptr, sz * nm);
    return sz * nm;
}

/* Выполняет один синхронный HTTP-запрос: ставит заголовки Authorization
 * (если задан bearer) и Content-Type, шлёт body при непустом теле и
 * заполняет HttpResponse. status = -1 при транспортной ошибке curl. */
HttpResponse perform(const std::string& method,
                     const std::string& url,
                     const std::string& body,
                     const std::string& content_type,
                     const std::string& bearer) {
    HttpResponse out;
    CURL* c = curl_easy_init();
    if (!c) { out.status = -1; out.error = "curl_easy_init failed"; return out; }

    curl_slist* headers = nullptr;
    if (!bearer.empty()) {
        std::string h = "Authorization: Bearer " + bearer;
        headers = curl_slist_append(headers, h.c_str());
    }
    if (!content_type.empty()) {
        std::string h = "Content-Type: " + content_type;
        headers = curl_slist_append(headers, h.c_str());
    }

    curl_easy_setopt(c, CURLOPT_URL, url.c_str());
    curl_easy_setopt(c, CURLOPT_CUSTOMREQUEST, method.c_str());
    curl_easy_setopt(c, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_cb);
    curl_easy_setopt(c, CURLOPT_WRITEDATA, &out.body);
    curl_easy_setopt(c, CURLOPT_TIMEOUT, 60L);
    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
    if (!body.empty()) {
        curl_easy_setopt(c, CURLOPT_POSTFIELDS, body.c_str());
        curl_easy_setopt(c, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    }

    CURLcode rc = curl_easy_perform(c);
    long status = 0;
    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);

    if (rc != CURLE_OK) {
        out.status = -1;
        out.error  = curl_easy_strerror(rc);
    } else {
        out.status = static_cast<int>(status);
    }

    curl_slist_free_all(headers);
    curl_easy_cleanup(c);
    return out;
}

}  // namespace

HttpClient::HttpClient(std::string base_url, std::string bearer)
    : base_(std::move(base_url)), bearer_(std::move(bearer)) {
    /* curl_global_init is process-wide; calling here on each construction
     * is safe (curl tracks a refcount). One Flight server has one client. */
    curl_global_init(CURL_GLOBAL_DEFAULT);
}

HttpClient::~HttpClient() { curl_global_cleanup(); }

HttpResponse HttpClient::get(const std::string& path) const {
    return perform("GET", base_ + path, "", "", bearer_);
}

HttpResponse HttpClient::post_json(const std::string& path,
                                   const std::string& json_body) const {
    return perform("POST", base_ + path, json_body, "application/json", bearer_);
}

HttpResponse HttpClient::post_raw(const std::string& path,
                                  const std::string& body,
                                  const std::string& content_type) const {
    return perform("POST", base_ + path, body, content_type, bearer_);
}

}  // namespace dfo
