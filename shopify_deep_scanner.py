"""
Shopify Subscription Extractor - Fixed Version
- JSON parse errors handle karo
- Homepage se app detect karo
- selling_plan_groups se products
- CloudScraper + 20 chunks
"""

import cloudscraper
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import random
import json
import os

INPUT_FILE  = os.getenv("INPUT_FILE", "SKU Subscription Data.csv")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "Shopify_Subscription_Deep_Analysis.xlsx")
THREADS     = int(os.getenv("THREADS", "5"))
TIMEOUT     = (5, 15)
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", "0"))
CHUNK_TOTAL = int(os.getenv("CHUNK_TOTAL", "1"))

APP_SIGNATURES = [
    ("Recharge",         ["rc_container", "/apps/recharge/", "data-recharge-provider", "rechargeapps.com"]),
    ("Bold",             ["bold-ro__product", "/apps/subscriptions/", "bold_recurring_id", "boldapps.com"]),
    ("Appstle",          ["appstle_init", "/apps/appstle-subscriptions/", "data-appstle-plan", "appstle.com"]),
    ("Seal",             ["seal-subs", "/apps/seal-subscriptions/", "data-seal-id"]),
    ("Skio",             ["skio-plan-picker", "/a/skio/", "data-skio-plan-id", "skio.com"]),
    ("Loop",             ["loop-subscription-widget", "/a/loop_subscriptions/", "data-loop-id"]),
    ("Stay AI",          ["stay-ai-widget", "/a/stay/", "data-stay-plan", "stayai.com"]),
    ("Ordergroove",      ["og-offer", "/apps/ordergroove/", "data-og-module", "ordergroove.com"]),
    ("Smartrr",          ["smartrr-widget", "/a/smartrr/", "data-smartrr-id", "smartrr.com"]),
    ("PayWhirl",         ["paywhirl-widget", "/apps/paywhirl/", "data-paywhirl-id"]),
    ("Ongoing",          ["ongoing-subscription-widget", "/apps/ongoing/", "ongoing_id"]),
    ("Subify",           ["subify-subscription-widget", "/apps/subify/", "data-subify-plan"]),
    ("Recurpay",         ["recurpay-widget", "/apps/recurpay/", "data-recurpay-id"]),
    ("Propel",           ["propel-widget", "/apps/propel/", "data-propel-plan"]),
    ("Monto",            ["monto-subscription-widget", "/apps/monto/", "data-monto-plan"]),
    ("Simple Sub",       ["simple-sub-widget", "/apps/simple-sub/", "data-simple-plan"]),
    ("CASA",             ["casa-widget", "/a/casa/", "data-casa-plan"]),
    ("Subbly",           ["subbly-checkout", "/a/subbly/", "data-subbly-id", "subbly.com"]),
    ("ChargeBee",        ["chargebee-widget", "/apps/chargebee/", "data-cb-plan-id", "chargebee.com"]),
    ("Recurly",          ["recurly-widget", "/apps/recurly/", "data-recurly-id", "recurly.com"]),
    ("Upscribe",         ["upscribe-widget", "/a/upscribe/", "data-upscribe-id"]),
    ("Growave",          ["growave-sub-widget", "/apps/growave/", "data-growave-id"]),
    ("Yotpo",            ["yotpo-sub-widget", "/apps/yotpo/", "data-yotpo-id"]),
    ("Rebuy",            ["rebuy-sub-widget", "/apps/rebuy/", "data-rebuy-id", "rebuyengine.com"]),
    ("Vitals",           ["vitals-sub-widget", "/apps/vitals/", "data-vitals-id"]),
    ("QPilot",           ["qpilot-widget", "/apps/qpilot/", "data-qpilot-id"]),
    ("Subflow",          ["subflow-widget", "/a/subflow/", "data-subflow-id"]),
    ("Kaching",          ["kaching-widget", "/a/kaching/", "data-kaching-id"]),
    ("Skio",             ["skio-plan-picker", "/a/skio/", "skio.com"]),
    ("EasySub",          ["easysub-widget", "/a/easysub/", "data-easysub-id"]),
    ("Native Shopify",   ["selling_plan_groups", "selling_plan_id"]),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
]

def make_scraper():
    s = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': random.choice(['windows','darwin','linux']), 'mobile': False}
    )
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    })
    return s

def detect_app(html: str) -> str:
    html_lower = html.lower()
    detected = []
    for app_name, keywords in APP_SIGNATURES:
        for kw in keywords:
            if kw.lower() in html_lower:
                detected.append(app_name)
                break
    if not detected:
        return "Unknown"
    named = [d for d in detected if d != "Native Shopify"]
    return " + ".join(named) if named else "Native Shopify"

def safe_fetch(scraper, url, retries=2):
    """
    URL fetch karo. 
    Returns: (text, status_code)
    JSON errors pe bhi text return karo — crash mat karo.
    """
    for attempt in range(retries + 1):
        try:
            resp = scraper.get(url, timeout=TIMEOUT, allow_redirects=True)
            if resp.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return resp.text, resp.status_code
        except Exception as e:
            if attempt == retries:
                return "", str(e)[:60]
            time.sleep(1)
    return "", "max_retries"

def safe_json(text):
    """
    Text ko JSON parse karo.
    Returns: dict ya None — crash nahi hoga.
    """
    try:
        return json.loads(text)
    except Exception:
        return None

def get_all_products(scraper, domain):
    """products.json paginate karo. JSON error = empty list, not crash."""
    all_products = []
    page = 1
    while True:
        text, status = safe_fetch(scraper, f"https://{domain}/products.json?limit=250&page={page}")
        if status != 200:
            return all_products, status
        data = safe_json(text)
        if data is None:
            # JSON parse failed = Cloudflare HTML aaya
            # Agar already kuch products hain toh return karo
            return all_products, "cf_blocked" if not all_products else 200
        products = data.get('products', [])
        if not products:
            return all_products, 200
        all_products.extend(products)
        if len(products) < 250:
            return all_products, 200
        page += 1
        time.sleep(random.uniform(0.5, 1.5))
    return all_products, 200

def check_product_js(scraper, domain, handle):
    text, status = safe_fetch(scraper, f"https://{domain}/products/{handle}.js")
    if status != 200:
        return False, None
    data = safe_json(text)
    if data is None:
        return False, None
    plans = data.get('selling_plan_groups', [])
    return bool(plans), data

def scrape_store(domain):
    domain = str(domain).strip().lower()
    domain = domain.replace("https://","").replace("http://","").split('/')[0]
    if not domain:
        return {"status": "skipped", "domain": domain, "rows": []}

    scraper = make_scraper()
    time.sleep(random.uniform(0.2, 0.8))

    # ── Step 1: Homepage — app detect + CF cookie ────────────
    detected_app = "Unknown"
    home_text, home_status = safe_fetch(scraper, f"https://{domain}")
    if not home_text:
        return {"status": f"blocked_{home_status}", "domain": domain, "rows": []}
    if home_status not in [200, 301, 302]:
        return {"status": f"blocked_{home_status}", "domain": domain, "rows": []}
    detected_app = detect_app(home_text)
    time.sleep(random.uniform(0.5, 1.5))

    # ── Step 2: Products fetch ───────────────────────────────
    products, api_status = get_all_products(scraper, domain)
    total_sku = len(products)

    # Agar products nahi aaye lekin app detect hua
    if total_sku == 0:
        if detected_app != "Unknown":
            return {
                "status": "app_detected_no_products",
                "domain": domain,
                "rows": [{
                    "Store":            domain,
                    "Subscription_App": detected_app,
                    "Total_SKUs":       0,
                    "Product_Title":    "",
                    "Price":            "",
                    "Sub_Plans":        "",
                    "Product_Link":     f"https://{domain}",
                    "Note":             f"App detected on homepage but products.json blocked ({api_status})"
                }]
            }
        return {"status": f"blocked_{api_status}", "domain": domain, "rows": []}

    # ── Step 3: Pre-check — pehle 3 products ────────────────
    has_sub = False
    for i in range(min(3, total_sku)):
        has_plans, _ = check_product_js(scraper, domain, products[i]['handle'])
        if has_plans:
            has_sub = True
            break

    if not has_sub:
        return {"status": "no_subscription", "domain": domain, "rows": []}

    # ── Step 4: Poora scan ───────────────────────────────────
    store_results = []
    for p in products:
        try:
            has_plans, data = check_product_js(scraper, domain, p['handle'])
            if has_plans and data:
                plans = data.get('selling_plan_groups', [])
                store_results.append({
                    "Store":            domain,
                    "Subscription_App": detected_app,
                    "Total_SKUs":       total_sku,
                    "Product_Title":    data['title'],
                    "Price":            data.get('price', 0) / 100,
                    "Sub_Plans":        ", ".join([plan['name'] for plan in plans]),
                    "Product_Link":     f"https://{domain}/products/{p['handle']}",
                    "Note":             ""
                })
        except Exception:
            continue

    status = "found" if store_results else "no_subscription"
    return {"status": status, "domain": domain, "rows": store_results}

def get_url_column(df):
    for col in df.columns:
        if any(w in col.lower() for w in ['url','domain','store','site','link','web','company']):
            print(f"✅ Column mila: '{col}'", flush=True)
            return col
    first_col = df.columns[0]
    print(f"⚠️ Pehli column use: '{first_col}'", flush=True)
    return first_col

def main():
    print(f"📥 Loading: {INPUT_FILE}", flush=True)
    try:
        df_input = pd.read_csv(INPUT_FILE)
        url_col  = get_url_column(df_input)
        domains  = df_input[url_col].dropna().tolist()
        print(f"Total domains: {len(domains)}", flush=True)
    except Exception as e:
        print(f"❌ Error: {e}", flush=True)
        return

    if CHUNK_TOTAL > 1:
        chunk_size = len(domains) // CHUNK_TOTAL
        start = CHUNK_INDEX * chunk_size
        end   = start + chunk_size if CHUNK_INDEX < CHUNK_TOTAL - 1 else len(domains)
        domains = domains[start:end]
        print(f"🔀 Chunk {CHUNK_INDEX+1}/{CHUNK_TOTAL}: {len(domains)} stores ({start}–{end})", flush=True)

    print(f"\n🚀 Scanning {len(domains)} stores | threads={THREADS}", flush=True)
    print(f"🛡️  CloudScraper | ⚡ Pre-check | 🔍 App Detection\n", flush=True)

    all_rows   = []
    status_log = []
    completed  = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(scrape_store, d): d for d in domains}
        with tqdm(total=len(domains), dynamic_ncols=True) as pbar:
            for future in as_completed(futures, timeout=36000):
                try:
                    r = future.result(timeout=90)
                    status_log.append({"Domain": r["domain"], "Status": r["status"]})
                    if r["rows"]:
                        all_rows.extend(r["rows"])
                except Exception:
                    domain = futures[future]
                    status_log.append({"Domain": str(domain), "Status": "timeout"})
                completed += 1
                pbar.update(1)
                if completed % 50 == 0:
                    found   = sum(1 for s in status_log if s["Status"] in ["found", "app_detected_no_products"])
                    blocked = sum(1 for s in status_log if "blocked" in s["Status"])
                    tqdm.write(f"[{completed}/{len(domains)}] ✅ Found: {found} | ❌ Blocked: {blocked}")

    df_log = pd.DataFrame(status_log)
    print("\n─── STATUS SUMMARY ───", flush=True)
    print(df_log["Status"].value_counts().to_string(), flush=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        if all_rows:
            df_detail = pd.DataFrame(all_rows)
            df_detail.to_excel(writer, sheet_name="Subscription_Products", index=False)

            summary = []
            for store, grp in df_detail.groupby("Store"):
                summary.append({
                    "Store":                 store,
                    "Subscription_App":      grp["Subscription_App"].iloc[0],
                    "Total_SKUs":            grp["Total_SKUs"].iloc[0],
                    "Subscription_Products": len(grp),
                    "Ratio":                 f"{len(grp)}/{grp['Total_SKUs'].iloc[0]}",
                    "Plan_Names":            " | ".join(str(x) for x in grp["Sub_Plans"].unique()[:5]),
                    "Product_Names":         " | ".join(grp["Product_Title"].tolist()[:10])
                })
            pd.DataFrame(summary).to_excel(writer, sheet_name="Store_Summary", index=False)

            app_counts = df_detail["Subscription_App"].value_counts().reset_index()
            app_counts.columns = ["App", "Count"]
            app_counts.to_excel(writer, sheet_name="App_Usage", index=False)

        df_log.to_excel(writer, sheet_name="Status_Log", index=False)

    print(f"\n✅ Saved: {OUTPUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
