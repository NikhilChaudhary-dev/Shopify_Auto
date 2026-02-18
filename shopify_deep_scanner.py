"""
Shopify Subscription Extractor - shopify_deep_scanner.py
- Pehle sirf 3 products pre-check karo
- Agar selling_plan_groups mile tabhi poora store scan karo
- GitHub Actions: 20 parallel chunks (330 stores each)
"""

import requests
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
TIMEOUT     = (5, 10)
CHUNK_INDEX = int(os.getenv("CHUNK_INDEX", "0"))
CHUNK_TOTAL = int(os.getenv("CHUNK_TOTAL", "1"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
    }

def get_all_products(domain):
    all_products = []
    page = 1
    max_retries = 3
    while True:
        retries = 0
        while retries <= max_retries:
            try:
                resp = requests.get(
                    f"https://{domain}/products.json?limit=250&page={page}",
                    headers=get_headers(), timeout=TIMEOUT, allow_redirects=True
                )
                if resp.status_code == 200:
                    products = resp.json().get('products', [])
                    if not products:
                        return all_products, 200
                    all_products.extend(products)
                    if len(products) < 250:
                        return all_products, 200
                    page += 1
                    time.sleep(random.uniform(0.5, 1.5))
                    break
                elif resp.status_code == 429:
                    retries += 1
                    time.sleep(3 * retries)
                    continue
                else:
                    return all_products, resp.status_code
            except Exception as e:
                return all_products, str(e)[:50]
        if retries > max_retries:
            return all_products, 429
    return all_products, 200

def check_product_js(domain, handle):
    try:
        time.sleep(random.uniform(0.1, 0.3))
        resp = requests.get(
            f"https://{domain}/products/{handle}.js",
            headers=get_headers(), timeout=TIMEOUT
        )
        if resp.status_code == 200:
            data = resp.json()
            plans = data.get('selling_plan_groups', [])
            return bool(plans), data
    except Exception:
        pass
    return False, None

def scrape_store(domain):
    domain = str(domain).strip().lower()
    domain = domain.replace("https://", "").replace("http://", "").split('/')[0]
    if not domain:
        return {"status": "skipped", "domain": domain, "rows": []}

    time.sleep(random.uniform(0.1, 0.5))

    products, status_code = get_all_products(domain)
    total_sku = len(products)

    if total_sku == 0:
        return {"status": f"blocked_{status_code}", "domain": domain, "rows": []}

    # Fast pre-check: sirf pehle 3 products
    store_has_subscription = False
    for i in range(min(3, total_sku)):
        has_plans, _ = check_product_js(domain, products[i]['handle'])
        if has_plans:
            store_has_subscription = True
            break

    if not store_has_subscription:
        return {"status": "no_subscription", "domain": domain, "rows": []}

    # Poora scan tabhi jab subscription mili
    store_results = []
    for p in products:
        try:
            has_plans, data = check_product_js(domain, p['handle'])
            if has_plans and data:
                plans = data.get('selling_plan_groups', [])
                store_results.append({
                    "Store":         domain,
                    "Total_SKUs":    total_sku,
                    "Product_Title": data['title'],
                    "Price":         data.get('price', 0) / 100,
                    "Sub_Plans":     ", ".join([plan['name'] for plan in plans]),
                    "Product_Link":  f"https://{domain}/products/{p['handle']}"
                })
        except Exception:
            continue

    status = "found" if store_results else "no_subscription"
    return {"status": status, "domain": domain, "rows": store_results}

def get_url_column(df):
    for col in df.columns:
        if any(w in col.lower() for w in ['url', 'domain', 'store', 'site', 'link', 'web', 'company']):
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
    print(f"⚡ Pre-check ON — sirf 3 products check, baaki skip\n", flush=True)

    all_product_rows = []
    status_log       = []
    completed        = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        future_to_domain = {executor.submit(scrape_store, d): d for d in domains}
        with tqdm(total=len(domains), dynamic_ncols=True) as pbar:
            for future in as_completed(future_to_domain, timeout=36000):
                try:
                    r = future.result(timeout=60)
                    status_log.append({"Domain": r["domain"], "Status": r["status"]})
                    if r["rows"]:
                        all_product_rows.extend(r["rows"])
                except Exception:
                    domain = future_to_domain[future]
                    status_log.append({"Domain": str(domain), "Status": "timeout_skipped"})
                completed += 1
                pbar.update(1)
                if completed % 100 == 0:
                    found   = sum(1 for s in status_log if s["Status"] == "found")
                    blocked = sum(1 for s in status_log if "429" in s["Status"])
                    tqdm.write(f"[{completed}/{len(domains)}] ✅ Found: {found} | ❌ Blocked: {blocked}")

    df_log = pd.DataFrame(status_log)
    print("\n─── STATUS SUMMARY ───", flush=True)
    print(df_log["Status"].value_counts().to_string(), flush=True)
    print(f"\n📊 Subscription stores: {df_log[df_log['Status']=='found'].shape[0]}", flush=True)
    print(f"📦 Total subscription products: {len(all_product_rows)}", flush=True)

    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        if all_product_rows:
            df_detailed = pd.DataFrame(all_product_rows)
            df_detailed.to_excel(writer, sheet_name="Subscription_Products", index=False)
            summary = []
            for store, grp in df_detailed.groupby("Store"):
                summary.append({
                    "Store":                 store,
                    "Total_SKUs":            grp["Total_SKUs"].iloc[0],
                    "Subscription_Products": len(grp),
                    "Ratio":                 f"{len(grp)}/{grp['Total_SKUs'].iloc[0]}",
                    "Plan_Names":            " | ".join(grp["Sub_Plans"].unique()[:5]),
                    "Product_Names":         " | ".join(grp["Product_Title"].tolist()[:10])
                })
            pd.DataFrame(summary).to_excel(writer, sheet_name="Store_Summary", index=False)
        df_log.to_excel(writer, sheet_name="Status_Log", index=False)

    print(f"\n✅ Saved: {OUTPUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
