#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
NAPASTAK — демо-наполнение для презентации ("NovaShop", e-commerce аналитика).

Наполняет ВСЕ разделы UI:
  • Таблицы данных   — sales, customers, products, marketing, support_tickets
  • Конвейеры        — 4 шт. с разными триггерами (cron/webhook/file) + история запусков
  • Представления    — 3 материализованных представления (вкл. по расписанию)
  • Аналитика        — 6 сохранённых результатов под графики
  • Безопасность     — RBAC-политики + журнал аудита (наполняется активностью)
  • Настройки        — API-ключи, индексы

Запуск:
    python3 scripts/seed_demo.py [BASE_URL]
по умолчанию BASE_URL = http://localhost:8080
"""
import json, sys, time, random, urllib.request, urllib.error

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
random.seed(42)
TOKEN = None

# ── HTTP helpers ─────────────────────────────────────────────────────────────
def _req(method, path, body=None, ctype="application/json", raw=False):
    url = BASE + path
    data = None
    if body is not None:
        data = body.encode("utf-8") if raw else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", ctype)
    if TOKEN:
        req.add_header("Authorization", "Bearer " + TOKEN)
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            txt = r.read().decode("utf-8")
            try: return r.status, json.loads(txt)
            except Exception: return r.status, txt
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "replace")
    except Exception as e:
        return 0, str(e)

def post(path, body):           return _req("POST", path, body)
def get(path):                  return _req("GET", path)
def delete(path):               return _req("DELETE", path)
def ingest(table, csv_text):    return _req("POST", f"/api/ingest/csv?table={table}", csv_text,
                                            ctype="text/csv", raw=True)
def query(sql):                 return _req("POST", "/api/tables/query", {"sql": sql})

def step(msg): print(f"\033[36m→ {msg}\033[0m")
def ok(msg):   print(f"  \033[32m✓ {msg}\033[0m")
def warn(msg): print(f"  \033[33m! {msg}\033[0m")

# ── Auth ─────────────────────────────────────────────────────────────────────
def login():
    global TOKEN
    s, r = post("/api/auth/token", {"username": "admin", "password": "admin"})
    if s == 200 and isinstance(r, dict) and "token" in r:
        TOKEN = r["token"]; ok("авторизация admin")
    else:
        print("ОШИБКА авторизации:", s, r); sys.exit(1)

# ── CSV builder ───────────────────────────────────────────────────────────────
def to_csv(header, rows):
    out = [",".join(header)]
    for row in rows:
        out.append(",".join(str(c) for c in row))
    return "\n".join(out) + "\n"

# ── Reference data ─────────────────────────────────────────────────────────────
REGIONS   = ["Москва", "Санкт-Петербург", "Юг", "Урал", "Сибирь"]
CHANNELS  = ["Онлайн", "Розница", "Партнёры", "Мобильное"]
CATS      = ["Электроника", "Одежда", "Дом и сад", "Спорт", "Книги", "Красота"]
SEGMENTS  = ["B2C", "B2B", "VIP"]
COUNTRIES = ["Россия", "Казахстан", "Беларусь", "Армения"]
FNAMES = ["Иван","Мария","Алексей","Ольга","Дмитрий","Анна","Сергей","Елена",
          "Павел","Наталья","Артём","Юлия","Никита","Татьяна","Роман","Ксения"]
LNAMES = ["Иванов","Петрова","Смирнов","Кузнецова","Попов","Соколова","Лебедев",
          "Новикова","Морозов","Волкова","Орлов","Зайцева","Фёдоров","Михайлова"]
PRODUCTS = {
    "Электроника": ["Смартфон Nova X","Ноутбук AirBook","Наушники Pulse","Планшет Tab S","Умные часы Fit"],
    "Одежда":      ["Куртка Urban","Джинсы Classic","Кроссовки Run","Футболка Basic","Платье Aura"],
    "Дом и сад":   ["Кофемашина Barista","Пылесос Cyclone","Набор посуды Chef","Лампа Lumen","Кресло Relax"],
    "Спорт":       ["Велосипед Trail","Гантели Power","Коврик Yoga","Палатка Camp","Самокат Speed"],
    "Книги":       ["Атлас мира","Роман Бестселлер","Детектив Нуар","Учебник Python","Кулинария дома"],
    "Красота":     ["Крем Hydra","Парфюм Noir","Помада Velvet","Сыворотка Glow","Шампунь Pure"],
}

# ── 1. SALES (большая фактовая таблица с трендом) ──────────────────────────────
def build_sales(n=1800):
    header = ["order_id","order_date","region","category","product","channel",
              "units","revenue","cost","discount_pct"]
    rows = []
    start = time.mktime(time.strptime("2025-01-01", "%Y-%m-%d"))
    for i in range(1, n + 1):
        # дата: 166 дней (янв–15 июн), с уплотнением к концу (рост бизнеса)
        day = int(166 * (random.random() ** 0.85))
        ts = start + day * 86400
        date = time.strftime("%Y-%m-%d", time.localtime(ts))
        month_idx = int(time.strftime("%m", time.localtime(ts)))
        cat = random.choice(CATS)
        prod = random.choice(PRODUCTS[cat])
        units = random.randint(1, 12)
        base_price = {"Электроника":420,"Одежда":75,"Дом и сад":160,
                      "Спорт":130,"Книги":22,"Красота":35}[cat]
        # сезонный рост + шум
        growth = 1 + 0.06 * month_idx
        price = base_price * growth * random.uniform(0.8, 1.25)
        revenue = round(price * units, 2)
        cost = round(revenue * random.uniform(0.55, 0.72), 2)   # маржа 28–45%
        disc = random.choice([0,0,0,5,5,10,15,20])
        rows.append([i, date, random.choice(REGIONS), cat, prod,
                     random.choice(CHANNELS), units, revenue, cost, disc])
    return header, rows

# ── 2. CUSTOMERS (age ~ ltv корреляция для регрессии/scatter) ──────────────────
def build_customers(n=500):
    header = ["customer_id","name","segment","country","city","age",
              "signup_date","orders_count","ltv"]
    rows = []
    start = time.mktime(time.strptime("2023-01-01", "%Y-%m-%d"))
    for i in range(1, n + 1):
        age = random.randint(18, 70)
        seg = random.choices(SEGMENTS, weights=[70,22,8])[0]
        seg_mult = {"B2C":1.0,"B2B":2.4,"VIP":4.0}[seg]
        # ltv растёт с возрастом + сегмент + шум
        ltv = round((300 + age * 38 + random.gauss(0, 900)) * seg_mult, 2)
        ltv = max(120.0, ltv)
        orders = max(1, int(ltv / random.uniform(800, 1600)))
        sd = time.strftime("%Y-%m-%d", time.localtime(start + random.randint(0, 800) * 86400))
        name = f"{random.choice(FNAMES)} {random.choice(LNAMES)}"
        rows.append([i, name, seg, random.choice(COUNTRIES),
                     random.choice(["Москва","СПб","Казань","Сочи","Новосибирск","Екатеринбург"]),
                     age, sd, orders, ltv])
    return header, rows

# ── 3. PRODUCTS (каталог) ───────────────────────────────────────────────────
def build_products():
    header = ["product_id","name","category","price","cost","stock","rating"]
    rows = []; pid = 1
    for cat, names in PRODUCTS.items():
        base = {"Электроника":420,"Одежда":75,"Дом и сад":160,
                "Спорт":130,"Книги":22,"Красота":35}[cat]
        for nm in names:
            price = round(base * random.uniform(0.7, 1.6), 2)
            rows.append([pid, nm, cat, price, round(price*random.uniform(0.5,0.7),2),
                         random.randint(0, 500), round(random.uniform(3.5, 5.0), 1)])
            pid += 1
    return header, rows

# ── 4. MARKETING (воронка по каналам и месяцам) ───────────────────────────────
def build_marketing():
    header = ["month","channel","spend","impressions","clicks","leads","conversions","revenue"]
    rows = []
    months = ["2025-01","2025-02","2025-03","2025-04","2025-05","2025-06"]
    for m in months:
        for ch in CHANNELS:
            spend = random.randint(50000, 300000)
            impr = spend * random.randint(20, 40)
            clicks = int(impr * random.uniform(0.02, 0.06))
            leads = int(clicks * random.uniform(0.10, 0.22))
            conv = int(leads * random.uniform(0.12, 0.30))
            rev = int(conv * random.uniform(2500, 6000))
            rows.append([m, ch, spend, impr, clicks, leads, conv, rev])
    return header, rows

# ── 5. SUPPORT TICKETS ────────────────────────────────────────────────────────
def build_tickets(n=350):
    header = ["ticket_id","date","priority","category","status","resolution_hours","satisfaction"]
    rows = []
    start = time.mktime(time.strptime("2025-01-01", "%Y-%m-%d"))
    prio = ["Низкий","Средний","Высокий","Критичный"]
    tcat = ["Оплата","Доставка","Возврат","Технический","Аккаунт"]
    statuses = ["Открыт","В работе","Решён","Решён","Решён","Закрыт"]
    for i in range(1, n + 1):
        date = time.strftime("%Y-%m-%d", time.localtime(start + random.randint(0,165)*86400))
        p = random.choices(prio, weights=[40,35,18,7])[0]
        res = round(random.uniform(0.5, 72) * (1 + prio.index(p)*0.4), 1)
        sat = random.choices([5,4,3,2,1], weights=[40,30,15,9,6])[0]
        rows.append([i, date, p, random.choice(tcat), random.choice(statuses), res, sat])
    return header, rows

# ── Pipelines ──────────────────────────────────────────────────────────────────
def mk_pipeline(pid, name, cron, enabled, steps, triggers=None, webhook_url="",
                webhook_on="failure"):
    body = {"id": pid, "name": name, "cron": cron, "enabled": enabled,
            "steps": steps, "webhook_url": webhook_url, "webhook_on": webhook_on,
            "alert_cooldown": 300}
    if triggers: body["triggers"] = triggers
    return body

def sql_step(sid, name, sql, target, retries=1, delay=5):
    # connector_type="" → чистое SQL-преобразование: transform_sql выполняется
    # против каталога и результат пишется в target_table (api.c:4328).
    return {"id": sid, "name": name, "connector_type": "", "connector_config": "",
            "transform_sql": sql, "target_table": target,
            "max_retries": retries, "retry_delay_sec": delay, "deps": []}

# ── MAIN ───────────────────────────────────────────────────────────────────────
def main():
    print(f"\n=== NAPASTAK demo seed → {BASE} ===\n")
    login()

    # 1) Таблицы
    step("Загрузка таблиц данных")
    datasets = {
        "sales":           build_sales(),
        "customers":       build_customers(),
        "products":        build_products(),
        "marketing":       build_marketing(),
        "support_tickets": build_tickets(),
    }
    for name, (hdr, rows) in datasets.items():
        s, r = ingest(name, to_csv(hdr, rows))
        if s == 200: ok(f"{name}: {r.get('rows')} строк, {r.get('columns')} колонок")
        else: warn(f"{name}: {s} {r}")

    # 2) Конвейеры
    step("Создание конвейеров")
    pipelines = [
        mk_pipeline("p_revenue_by_region", "Витрина: выручка по регионам",
            "@daily", True,
            [sql_step("agg", "Агрегация выручки",
                "SELECT region, SUM(revenue) AS revenue, SUM(units) AS units, "
                "COUNT(*) AS orders FROM sales GROUP BY region ORDER BY revenue DESC",
                "rep_revenue_by_region")],
            triggers=[{"type":"cron","cron_expr":"@daily"},
                      {"type":"webhook","webhook_token":"rev-region-7f3a9c"}],
            webhook_url="https://hooks.slack.com/services/DEMO/REV/region"),
        mk_pipeline("p_customer_rfm", "Сегментация клиентов (RFM)",
            "", True,
            [sql_step("seg", "Расчёт сегментов",
                "SELECT segment, COUNT(*) AS clients, AVG(ltv) AS avg_ltv, "
                "SUM(orders_count) AS total_orders FROM customers "
                "GROUP BY segment ORDER BY avg_ltv DESC",
                "rep_customer_segments")],
            triggers=[{"type":"webhook","webhook_token":"rfm-seg-2b8e1d"},
                      {"type":"file_arrival","watch_dir":"./data/incoming","file_pattern":"customers_*.csv"}]),
        mk_pipeline("p_daily_sales", "Ежедневная агрегация продаж",
            "@daily", True,
            [sql_step("by_cat", "Выручка по категориям",
                "SELECT category, SUM(revenue) AS revenue, SUM(cost) AS cost, "
                "SUM(revenue)-SUM(cost) AS profit FROM sales GROUP BY category ORDER BY revenue DESC",
                "rep_sales_by_category"),
             sql_step("by_ch", "Выручка по каналам",
                "SELECT channel, SUM(revenue) AS revenue, COUNT(*) AS orders "
                "FROM sales GROUP BY channel ORDER BY revenue DESC",
                "rep_sales_by_channel")],
            triggers=[{"type":"cron","cron_expr":"0 6 * * *"}]),
        mk_pipeline("p_marketing_roi", "Маркетинг: ROI по каналам",
            "0 9 * * 1-5", True,
            [sql_step("roi", "Расчёт ROI",
                "SELECT channel, SUM(spend) AS spend, SUM(revenue) AS revenue, "
                "SUM(conversions) AS conversions FROM marketing GROUP BY channel ORDER BY revenue DESC",
                "rep_marketing_roi")],
            triggers=[{"type":"cron","cron_expr":"0 9 * * 1-5"}],
            webhook_url="https://hooks.slack.com/services/DEMO/MKT/roi", webhook_on="all"),
    ]
    created = []
    for pl in pipelines:
        s, r = post("/api/pipelines", pl)
        if s in (200, 201):
            created.append(pl["id"]); ok(f"{pl['name']}  ({len(pl['steps'])} шаг(ов))")
        else: warn(f"{pl['name']}: {s} {r}")

    # 3) Запуски конвейеров (история)
    step("Запуск конвейеров для истории")
    runs = {"p_revenue_by_region":3, "p_customer_rfm":2, "p_daily_sales":4, "p_marketing_roi":2}
    for pid, cnt in runs.items():
        if pid not in created: continue
        for _ in range(cnt):
            s, r = post(f"/api/pipelines/{pid}/run", {})
            time.sleep(0.4)
        ok(f"{pid}: {cnt} запуск(ов)")
    time.sleep(1.5)  # дать планировщику завершить

    # 4) Материализованные представления
    step("Создание материализованных представлений")
    mviews = [
        {"name":"mv_revenue_by_category",
         "definition_sql":"SELECT category, SUM(revenue) AS revenue, SUM(units) AS units "
                          "FROM sales GROUP BY category ORDER BY revenue DESC",
         "source_tables":["sales"], "refresh_mode":0},
        {"name":"mv_top_products",
         "definition_sql":"SELECT product, COUNT(*) AS orders, SUM(revenue) AS revenue "
                          "FROM sales GROUP BY product ORDER BY revenue DESC LIMIT 10",
         "source_tables":["sales"], "refresh_mode":0},
        {"name":"mv_monthly_sales",
         "definition_sql":"SELECT SUBSTR(order_date,1,7) AS month, SUM(revenue) AS revenue, "
                          "COUNT(*) AS orders FROM sales GROUP BY month ORDER BY month",
         "source_tables":["sales"], "refresh_mode":2, "refresh_cron":"@daily"},
    ]
    for mv in mviews:
        s, r = post("/api/matviews", mv)
        if s in (200,201):
            ok(f"{mv['name']}")
            post(f"/api/matviews/{mv['name']}/refresh", {})
        else: warn(f"{mv['name']}: {s} {r}")

    # 5) Аналитика — сохранённые результаты под графики
    step("Сохранение аналитических результатов")
    analytics = [
        ("Выручка по категориям",
         "SELECT category, SUM(revenue) AS revenue, SUM(revenue)-SUM(cost) AS profit "
         "FROM sales GROUP BY category ORDER BY revenue DESC"),
        ("Динамика выручки по месяцам",
         "SELECT SUBSTR(order_date,1,7) AS month, SUM(revenue) AS revenue, COUNT(*) AS orders "
         "FROM sales GROUP BY month ORDER BY month"),
        ("Доля каналов продаж",
         "SELECT channel, SUM(revenue) AS revenue FROM sales GROUP BY channel ORDER BY revenue DESC"),
        ("Возраст и LTV клиентов",
         "SELECT age, ltv, segment FROM customers ORDER BY age LIMIT 500"),
        ("Маркетинговая воронка",
         "SELECT SUM(impressions) AS impressions, SUM(clicks) AS clicks, "
         "SUM(leads) AS leads, SUM(conversions) AS conversions FROM marketing"),
        ("ТОП-10 клиентов по LTV",
         "SELECT name, segment, country, ltv, orders_count FROM customers "
         "ORDER BY ltv DESC LIMIT 10"),
    ]
    for name, sql in analytics:
        s, r = query(sql)
        if s == 200 and isinstance(r, dict):
            body = {"name": name, "sql": sql, "columns": r["columns"],
                    "rows": r["rows"], "row_count": r.get("row_count", len(r["rows"]))}
            s2, r2 = post("/api/analytics/results", body)
            if s2 in (200,201): ok(f"{name}  ({len(r['rows'])} строк)")
            else: warn(f"{name}: save {s2} {r2}")
        else: warn(f"{name}: query {s} {r}")

    # 6) RBAC-политики
    step("Настройка RBAC-политик")
    # role: 0=admin 1=analyst 2=viewer | actions bitmask: 1=read 2=write 4=create
    policies = [
        {"role":1, "table_pattern":"*",          "allowed_actions":7, "row_filter":""},
        {"role":2, "table_pattern":"rep_*",       "allowed_actions":1, "row_filter":""},
        {"role":2, "table_pattern":"sales",       "allowed_actions":1, "row_filter":"region = 'Москва'"},
        {"role":1, "table_pattern":"marketing*",  "allowed_actions":3, "row_filter":""},
        {"role":2, "table_pattern":"customers",   "allowed_actions":1, "row_filter":"segment = 'B2C'"},
    ]
    for p in policies:
        s, r = post("/api/rbac/policies", p)
        if s in (200,201): ok(f"role={p['role']} {p['table_pattern']} actions={p['allowed_actions']}")
        else: warn(f"policy {p['table_pattern']}: {s} {r}")

    # 7) API-ключи
    step("Создание API-ключей")
    for uid, role in [("svc-etl","analyst"), ("bi-dashboard","viewer"), ("ml-pipeline","analyst")]:
        s, r = post("/api/auth/apikeys", {"user_id": uid, "role": role})
        if s in (200,201): ok(f"{uid} ({role})")
        else: warn(f"apikey {uid}: {s} {r}")

    # 8) Индексы
    step("Создание индексов")
    for tbl, col in [("sales","order_id"), ("customers","customer_id")]:
        s, r = post(f"/api/tables/{tbl}/indexes", {"column": col})
        if s in (200,201): ok(f"{tbl}.{col}")
        else: warn(f"index {tbl}.{col}: {s} {r}")

    # Итог
    step("Готово. Текущее состояние:")
    for label, path in [("Таблицы","/api/tables"),("Конвейеры","/api/pipelines"),
                        ("Представления","/api/matviews"),("Аналитика","/api/analytics/results"),
                        ("RBAC","/api/rbac/policies")]:
        s, r = get(path)
        cnt = len(r) if isinstance(r, list) else "?"
        print(f"    {label}: {cnt}")
    s, r = get("/health")
    if isinstance(r, dict):
        m = r.get("metrics", {})
        print(f"    Метрики: строк={m.get('total_rows')} запусков={m.get('total_pipelines_run')} запросов={m.get('total_queries')}")
    print("\n\033[32mДемо-данные загружены. Открой http://localhost:8080 (admin/admin).\033[0m\n")

if __name__ == "__main__":
    main()
