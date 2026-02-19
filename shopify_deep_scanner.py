"""
Shopify Subscription Extractor - Playwright Version
- Real Chromium browser = Cloudflare 100% bypass
- Homepage se app detect
- selling_plan_groups se products  
- GitHub Actions: 20 chunks
"""

import asyncio
import re
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import time
import random
import json
import os

INPUT_FILE  = os.getenv("INPUT_FILE", "SKU Subscription Data.csv")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "Shopify_Subscription_Deep_Analysis.xlsx")
THREADS     = int(os.getenv("THREADS", "3"))
TIMEOUT     = 20000
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
    ("Rebuy",            ["rebuy-sub-widget", "/apps/rebuy/", "data-rebuy-id"]),
    ("Vitals",           ["vitals-sub-widget", "/apps/vitals/", "data-vitals-id"]),
    ("QPilot",           ["qpilot-widget", "/apps/qpilot/", "data-qpilot-id"]),
    ("Subflow",          ["subflow-widget", "/a/subflow/", "data-subflow-id"]),
    ("Kaching",          ["kaching-widget", "/a/kaching/", "data-kaching-id"]),
    ("EasySub",          ["easysub-widget", "/a/easysub/", "data-easysub-id"]),
    ("Ongoing Recurring",["recurring-ongoing", "/apps/ongoing-recurring/", "data-recurring-id"]),
    ("Simple Recurring", ["simple-recurring-id", "/apps/simple-recurring/", "data-recurring-plan"]),
    ("Native Shopify",   ["selling_plan_groups", "selling_plan_id"]),
]

def detect_app(html):
    h = html.lower()
    detected = []
    for name, kws in APP_SIGNATURES:
        for kw in kws:
            if kw.lower() in h:
                detected.append(name)
                break
    if not detected:
        return "Unknown"
    named = [d for d in detected if d != "Native Shopify"]
    return " + ".join(named) if named else "Native Shopify"

def safe_json(text):
    try:
        return json.loads(text)
    except Exception:
        return None

def extract_json_from_page(body):
    """Browser HTML wrapper se JSON nikalo."""
    pre = re.search(r'<pre[^>]*>(.*?)</pre>', body, re.DOTALL)
    txt = pre.group(1) if pre else body
    txt = txt.replace("&amp;","&").replace("&lt;","<").replace("&gt;",">").replace("&#39;","'")
    data = safe_json(txt)
    if data is None:
        data = safe_json(body)
    return data

async def scrape_store_async(domain, pw):
    domain = str(domain).strip().lower()
    domain = domain.replace("https://","").replace("http://","").split('/')[0]
    if not domain:
        return {"status":"skipped","domain":domain,"rows":[]}

    browser = await pw.chromium.launch(
        headless=True,
        args=["--no-sandbox","--disable-setuid-sandbox","--disable-blink-features=AutomationControlled"]
    )
    context = await browser.new_context(
        viewport={"width":1366,"height":768},
        user_agent=random.choice([
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        ]),
        locale="en-US",
        timezone_id="America/New_York",
    )
    await context.add_init_script("""
        Object.defineProperty(navigator,'webdriver',{get:()=>undefined});
        Object.defineProperty(navigator,'plugins',{get:()=>[1,2,3]});
        Object.defineProperty(navigator,'languages',{get:()=>['en-US','en']});
    """)
    page = await context.new_page()
    detected_app = "Unknown"
    store_results = []

    try:
        # Step 1: Homepage
        await asyncio.sleep(random.uniform(0.5,1.5))
        try:
            resp = await page.goto(f"https://{domain}", timeout=TIMEOUT, wait_until="domcontentloaded")
            status = resp.status if resp else 0
            if status not in [200,301,302]:
                return {"status":f"blocked_{status}","domain":domain,"rows":[]}
            detected_app = detect_app(await page.content())
        except PWTimeout:
            return {"status":"blocked_timeout","domain":domain,"rows":[]}
        except Exception as e:
            return {"status":f"blocked_{str(e)[:40]}","domain":domain,"rows":[]}

        await asyncio.sleep(random.uniform(0.8,1.5))

        # Step 2: products.json real browser se
        all_products = []
        page_num = 1
        while True:
            try:
                r = await page.goto(
                    f"https://{domain}/products.json?limit=250&page={page_num}",
                    timeout=TIMEOUT, wait_until="domcontentloaded"
                )
                if r and r.status == 200:
                    data = extract_json_from_page(await page.content())
                    if not data:
                        break
                    products = data.get('products', [])
                    if not products:
                        break
                    all_products.extend(products)
                    if len(products) < 250:
                        break
                    page_num += 1
                    await asyncio.sleep(random.uniform(0.5,1.0))
                elif r and r.status == 429:
                    await asyncio.sleep(5)
                else:
                    break
            except Exception:
                break

        total_sku = len(all_products)

        if total_sku == 0:
            if detected_app != "Unknown":
                return {
                    "status":"app_detected_no_products",
                    "domain":domain,
                    "rows":[{
                        "Store":domain, "Subscription_App":detected_app,
                        "Total_SKUs":0, "Product_Title":"", "Price":"",
                        "Sub_Plans":"", "Product_Link":f"https://{domain}",
                        "Note":"App on homepage, products.json blocked"
                    }]
                }
            return {"status":"no_products","domain":domain,"rows":[]}

        # Step 3: Pre-check 3 products
        has_sub = False
        for i in range(min(3, total_sku)):
            try:
                r = await page.goto(
                    f"https://{domain}/products/{all_products[i]['handle']}.js",
                    timeout=TIMEOUT, wait_until="domcontentloaded"
                )
                if r and r.status == 200:
                    data = extract_json_from_page(await page.content())
                    if data and data.get('selling_plan_groups'):
                        has_sub = True
                        break
            except Exception:
                continue

        if not has_sub:
            return {"status":"no_subscription","domain":domain,"rows":[]}

        # Step 4: Full scan
        for p in all_products:
            try:
                await asyncio.sleep(random.uniform(0.2,0.5))
                r = await page.goto(
                    f"https://{domain}/products/{p['handle']}.js",
                    timeout=TIMEOUT, wait_until="domcontentloaded"
                )
                if r and r.status == 200:
                    data = extract_json_from_page(await page.content())
                    if data:
                        plans = data.get('selling_plan_groups', [])
                        if plans:
                            store_results.append({
                                "Store":            domain,
                                "Subscription_App": detected_app,
                                "Total_SKUs":       total_sku,
                                "Product_Title":    data['title'],
                                "Price":            data.get('price',0)/100,
                                "Sub_Plans":        ", ".join([pl['name'] for pl in plans]),
                                "Product_Link":     f"https://{domain}/products/{p['handle']}",
                                "Note":             ""
                            })
            except Exception:
                continue

    finally:
        await browser.close()

    return {"status":"found" if store_results else "no_subscription","domain":domain,"rows":store_results}

def run_store(domain):
    async def _run():
        async with async_playwright() as pw:
            return await scrape_store_async(domain, pw)
    return asyncio.run(_run())

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
        print(f"🔀 Chunk {CHUNK_INDEX+1}/{CHUNK_TOTAL}: {len(domains)} stores ({start}-{end})", flush=True)

    print(f"\n🚀 Scanning {len(domains)} stores | threads={THREADS}", flush=True)
    print(f"🌐 Playwright real browser | 🔍 App Detection | ⚡ Pre-check\n", flush=True)

    all_rows   = []
    status_log = []
    completed  = 0

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {executor.submit(run_store, d): d for d in domains}
        with tqdm(total=len(domains), dynamic_ncols=True) as pbar:
            for future in as_completed(futures, timeout=72000):
                try:
                    r = future.result(timeout=120)
                    status_log.append({"Domain":r["domain"],"Status":r["status"]})
                    if r["rows"]:
                        all_rows.extend(r["rows"])
                except Exception:
                    domain = futures[future]
                    status_log.append({"Domain":str(domain),"Status":"timeout"})
                completed += 1
                pbar.update(1)
                if completed % 50 == 0:
                    found   = sum(1 for s in status_log if s["Status"] in ["found","app_detected_no_products"])
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
            app_counts.columns = ["App","Count"]
            app_counts.to_excel(writer, sheet_name="App_Usage", index=False)
        df_log.to_excel(writer, sheet_name="Status_Log", index=False)

    print(f"\n✅ Saved: {OUTPUT_FILE}", flush=True)

if __name__ == "__main__":
    main()
