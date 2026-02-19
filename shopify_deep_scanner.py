import cloudscraper
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import random
import json
import os

# ── CONFIG ──────────────────────────────────────────────────
INPUT_FILE  = os.getenv("INPUT_FILE", "SKU Subscription Data.csv")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "Shopify_Subscription_Deep_Analysis.xlsx")
THREADS     = int(os.getenv("THREADS", "5"))
TIMEOUT     = (5, 15)
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", "0"))
CHUNK_TOTAL = int(os.getenv("CHUNK_TOTAL", "1"))

# ── APP SIGNATURES ───────────────────────────────────────────
# (App Name, [keywords jo HTML mein dhundne hain])
APP_SIGNATURES = [
    ("Recharge",        ["rc_container", "/apps/recharge/", "data-recharge-provider", "rechargeapps.com"]),
    ("Bold",            ["bold-ro__product", "/apps/subscriptions/", "bold_recurring_id", "boldapps.com"]),
    ("Appstle",         ["appstle_init", "/apps/appstle-subscriptions/", "data-appstle-plan", "appstle.com"]),
    ("Seal",            ["seal-subs", "/apps/seal-subscriptions/", "data-seal-id"]),
    ("Skio",            ["skio-plan-picker", "/a/skio/", "data-skio-plan-id", "skio.com"]),
    ("Loop",            ["loop-subscription-widget", "/a/loop_subscriptions/", "data-loop-id", "loopwork.co"]),
    ("Stay AI",         ["stay-ai-widget", "/a/stay/", "data-stay-plan", "stayai.com"]),
    ("Ordergroove",     ["og-offer", "/apps/ordergroove/", "data-og-module", "ordergroove.com"]),
    ("Smartrr",         ["smartrr-widget", "/a/smartrr/", "data-smartrr-id", "smartrr.com"]),
    ("PayWhirl",        ["paywhirl-widget", "/apps/paywhirl/", "data-paywhirl-id", "paywhirl.com"]),
    ("Ongoing",         ["ongoing-subscription-widget", "/apps/ongoing/", "ongoing_id"]),
    ("Subify",          ["subify-subscription-widget", "/apps/subify/", "data-subify-plan", "subify.com"]),
    ("Recurpay",        ["recurpay-widget", "/apps/recurpay/", "data-recurpay-id"]),
    ("Propel",          ["propel-widget", "/apps/propel/", "data-propel-plan"]),
    ("Monto",           ["monto-subscription-widget", "/apps/monto/", "data-monto-plan"]),
    ("Simple Sub",      ["simple-sub-widget", "/apps/simple-sub/", "data-simple-plan"]),
    ("CASA",            ["casa-widget", "/a/casa/", "data-casa-plan"]),
    ("Gronos",          ["gronos-widget", "/apps/gronos/", "data-gronos-id"]),
    ("Subbly",          ["subbly-checkout", "/a/subbly/", "data-subbly-id", "subbly.com"]),
    ("ChargeBee",       ["chargebee-widget", "/apps/chargebee/", "data-cb-plan-id", "chargebee.com"]),
    ("Recurly",         ["recurly-widget", "/apps/recurly/", "data-recurly-id", "recurly.com"]),
    ("Spur",            ["spur-widget", "/a/spur/", "data-spur-id"]),
    ("Beboxed",         ["beboxed-widget", "/apps/beboxed/", "data-beboxed-id"]),
    ("Upscribe",        ["upscribe-widget", "/a/upscribe/", "data-upscribe-id", "upscribe.io"]),
    ("Zest",            ["zest-widget", "/a/zest/", "data-zest-id"]),
    ("Klaviyo Sub",     ["klaviyo-sub-widget", "/apps/klaviyo/", "data-klaviyo-id"]),
    ("ChargeZen",       ["chargezen-widget", "/a/chargezen/", "data-chargezen-id"]),
    ("Retentio",        ["retentio-widget", "/a/retentio/", "data-retentio-id"]),
    ("Subscrimo",       ["subscrimo-widget", "/a/subscrimo/", "data-subscrimo-id"]),
    ("Membership Sub",  ["membership-sub-widget", "/apps/membership/", "data-membership-id"]),
    ("Plobal",          ["plobal-sub-widget", "/apps/plobal/", "data-plobal-id"]),
    ("Tapcart",         ["tapcart-sub-widget", "/apps/tapcart/", "data-tapcart-id"]),
    ("Fulfillment Sub", ["fulfillment-sub-widget", "/apps/fulfillment/", "data-fulfillment-id"]),
    ("Growave",         ["growave-sub-widget", "/apps/growave/", "data-growave-id", "growave.io"]),
    ("Yotpo Sub",       ["yotpo-sub-widget", "/apps/yotpo/", "data-yotpo-id", "yotpo.com"]),
    ("Smile Sub",       ["smile-sub-widget", "/apps/smile/", "data-smile-id"]),
    ("Rivo",            ["rivo-sub-widget", "/a/rivo/", "data-rivo-id", "rivo.io"]),
    ("LoyaltyLion",     ["lion-sub-widget", "/apps/loyaltylion/", "data-lion-id"]),
    ("Rebuy",           ["rebuy-sub-widget", "/apps/rebuy/", "data-rebuy-id", "rebuyengine.com"]),
    ("Hulk",            ["hulk-sub-widget", "/apps/hulk-apps/", "data-hulk-id"]),
    ("Vitals",          ["vitals-sub-widget", "/apps/vitals/", "data-vitals-id", "getvitals.io"]),
    ("EasySub",         ["easysub-widget", "/a/easysub/", "data-easysub-id"]),
    ("Prime Sub",       ["prime-sub-widget", "/apps/prime/", "data-prime-id"]),
    ("Sub Plus",        ["plus-sub-widget", "/apps/subplus/", "data-plus-id"]),
    ("Ongoing Recurring",["recurring-ongoing", "/apps/ongoing-recurring/", "data-recurring-id"]),
    ("Kaching",         ["kaching-widget", "/a/kaching/", "data-kaching-id"]),
    ("Subflow",         ["subflow-widget", "/a/subflow/", "data-subflow-id"]),
    ("Simple Recurring",["simple-recurring-id", "/apps/simple-recurring/", "data-recurring-plan"]),
    ("QPilot",          ["qpilot-widget", "/apps/qpilot/", "data-qpilot-id", "qpilot.com"]),
    ("Native Shopify",  ["selling_plan_groups", "selling_plan_id", "selling_plan"]),
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def make_scraper():
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': random.choice(['windows','darwin','linux']), 'mobile': False}
    )
    scraper.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Cache-Control": "max-age=0",
    })
    return scraper

def human_delay(short=False):
    time.sleep(random.uniform(0.2, 0.8) if short else random.uniform(0.8, 2.5))
    if random.random() < 0.05:
        time.sleep(random.uniform(3, 6))

def detect_app(html: str) -> str:
    """HTML mein app signatures dhundo. Detected app name return karo."""
    html_lower = html.lower()
    detected = []
    for app_name, keywords in APP_SIGNATURES:
        for kw in keywords:
            if kw.lower() in html_lower:
                detected.append(app_name)
                break
    if not detected:
        return "Unknown"
    # Native Shopify ko priority mat do agar koi aur mila
    named = [d for d in detected if d != "Native Shopify"]
    if named:
        return " + ".join(named)
    return "Native Shopify"

def get_all_products(scraper, domain):
    all_products = []
    page = 1
    max_retries = 3
    while True:
        retries = 0
        while retries <= max_retries:
            try:
                resp = scraper.get(
                    f"https://{domain}/products.json?limit=250&page={page}",
                    timeout=TIMEOUT, allow_redirects=True
                )
                if resp.status_code == 200:
                    products = resp.json().get('products', [])
                    if not products:
                        return all_products, 200
                    all_products.extend(products)
                    if len(products) < 250:
                        return all_products, 200
                    page += 1
                    human_delay(short=True)
                    break
                elif resp.status_code == 429:
                    retries += 1
                    time.sleep(5 * retries)
                else:
                    return all_products, resp.status_code
            except Exception as e:
                return all_products, str(e)[:50]
        if retries > max_retries:
            return all_products, 429
    return all_products, 200

def check_product_js(scraper, domain, handle):
    try:
        human_delay(short=True)
        resp = scraper.get(f"https://{domain}/products/{handle}.js", timeout=TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            plans = data.get('selling_plan_groups', [])
            return bool(plans), data
    except Exception:
        pass
    return False, None

def scrape_store(domain):
    domain = str(domain).strip().lower()
    domain = domain.replace("https://","").replace("http://","").split('/')[0]
    if not domain:
        return {"status": "skipped", "domain": domain, "rows": []}

    scraper = make_scraper()
    human_delay(short=True)

    # ── Step 1: Homepage — Cloudflare cookie + app detect ────
    detected_app = "Unknown"
    try:
        home_resp = scraper.get(f"https://{domain}", timeout=TIMEOUT, allow_redirects=True)
        if home_resp.status_code not in [200, 301, 302]:
            return {"status": f"blocked_{home_resp.status_code}", "domain": domain, "rows": []}
        detected_app = detect_app(home_resp.text)
    except Exception as e:
        return {"status": f"blocked_{str(e)[:40]}", "domain": domain, "rows": []}

    human_delay()

    # ── Step 2: Products fetch ───────────────────────────────
    products, status_code = get_all_products(scraper, domain)
    total_sku = len(products)
    if total_sku == 0:
        return {"status": f"blocked_{status_code}", "domain": domain, "rows": []}

    # ── Step 3: Fast pre-check — pehle 3 products ───────────
    store_has_subscription = False
    for i in range(min(3, total_sku)):
        has_plans, _ = check_product_js(scraper, domain, products[i]['handle'])
        if has_plans:
            store_has_subscription = True
            break

    if not store_has_subscription:
        return {"status": "no_subscription", "domain": domain, "rows": []}

    # ── Step 4: Poora scan ───────────────────────────────────
    store_results = []
    for p in products:
        try:
            has_plans, data = check_product_js(scraper, domain, p['handle'])
            if has_plans and data:
                plans = data.get('selling_plan_groups', [])
                # Product page bhi scan karo app detect ke liye
                prod_resp = scraper.get(f"https://{domain}/products/{p['handle']}", timeout=TIMEOUT)
                if prod_resp.status_code == 200:
                    page_app = detect_app(prod_resp.text)
                    if page_app != "Unknown":
                        detected_app = page_app

                store_results.append({
                    "Store":            domain,
                    "Subscription_App": detected_app,
                    "Total_SKUs":       total_sku,
                    "Product_Title":    data['title'],
                    "Price":            data.get('price', 0) / 100,
                    "Sub_Plans":        ", ".join([plan['name'] for plan in plans]),
                    "Product_Link":     f"https://{domain}/products/{p['handle']}"
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
    print(f"🛡️  CloudScraper ON | ⚡ Pre-check ON | 🔍 App Detection ON\n", flush=True)

    all_product_rows = []
    status_log       = []
    completed        = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_domain = {executor.submit(scrape_store, d): d for d in domains}
        with tqdm(total=len(domains), dynamic_ncols=True) as pbar:
            for future in as_completed(future_to_domain, timeout=36000):
                try:
                    r = future.result(timeout=90)
                    status_log.append({"Domain": r["domain"], "Status": r["status"]})
                    if r["rows"]:
                        all_product_rows.extend(r["rows"])
                except Exception:
                    domain = future_to_domain[future]
                    status_log.append({"Domain": str(domain), "Status": "timeout_skipped"})
                completed += 1
                pbar.update(1)
                if completed % 50 == 0:
                    found   = sum(1 for s in status_log if s["Status"] == "found")
                    blocked = sum(1 for s in status_log if "blocked" in s["Status"])
                    tqdm.write(f"[{completed}/{len(domains)}] ✅ Found: {found} | ❌ Blocked: {blocked}")

    df_log = pd.DataFrame(status_log)
    print("\n─── STATUS SUMMARY ───", flush=True)
    print(df_log["Status"].value_counts().to_string(), flush=True)
    print(f"\n📊 Subscription stores: {df_log[df_log['Status']=='found'].shape[0]}", flush=True)
    print(f"📦 Total products: {len(all_product_rows)}", flush=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        if all_product_rows:
            df_detailed = pd.DataFrame(all_product_rows)
            df_detailed.to_excel(writer, sheet_name="Subscription_Products", index=False)

            # Store summary with app name
            summary = []
            for store, grp in df_detailed.groupby("Store"):
                summary.append({
                    "Store":                 store,
                    "Subscription_App":      grp["Subscription_App"].iloc[0],
                    "Total_SKUs":            grp["Total_SKUs"].iloc[0],
                    "Subscription_Products": len(grp),
                    "Ratio":                 f"{len(grp)}/{grp['Total_SKUs'].iloc[0]}",
                    "Plan_Names":            " | ".join(grp["Sub_Plans"].unique()[:5]),
                    "Product_Names":         " | ".join(grp["Product_Title"].tolist()[:10])
                })
            pd.DataFrame(summary).to_excel(writer, sheet_name="Store_Summary", index=False)

            # App frequency sheet
            app_counts = df_detailed["Subscription_App"].value_counts().reset_index()
            app_counts.columns = ["App", "Store_Count"]
            app_counts.to_excel(writer, sheet_name="App_Usage", index=False)

        df_log.to_excel(writer, sheet_name="Status_Log", index=False)

    print(f"\n✅ Saved: {OUTPUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
