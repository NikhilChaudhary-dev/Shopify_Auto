# Shopify Subscription Deep Scanner

## Files needed in your GitHub repo root:
```
shopify_deep_scanner.py        ← main scanner
merge_chunks.py                ← merges 10 outputs into 1
SKU Subscription Data.csv      ← your 6600 URLs
.github/workflows/shopify_scanner.yml
```

## How to run:
1. Push all files to your GitHub repo
2. Go to **Actions** tab → **Shopify Subscription Deep Scanner** → **Run workflow**
3. 10 runners start in parallel, each scanning ~660 stores
4. After all finish, a **Merge** job combines everything into `FINAL_Shopify_Deep_Analysis.xlsx`
5. Download from **Artifacts** section

## What it detects per store:
| Column | Description |
|--------|-------------|
| Subscription_App | Primary app detected |
| Apps_Detected | All apps found (pipe separated) |
| Page_Found_On | Homepage / /pages/ / /collections/ / /products/handle |
| Total_SKUs | Total products in store |
| Subscription_Products | Products with selling_plan_groups |
| Sub_Product_Names | Names of subscription products |
| Sub_Plan_Names | Plan names (Weekly, Monthly etc.) |
| Pages_Scanned | Total pages checked per store |
| Status | found / app_detected_no_product_api / no_subscription / blocked_XXX |

## Pages scanned per store:
- ✅ Homepage
- ✅ All /pages/ links found on homepage
- ✅ All /collections/ links
- ✅ Sample /products/ pages
- ✅ /products.json API (all SKUs)
- ✅ /products/{handle}.js for selling_plan_groups

## Apps detected: 50+
Recharge, Bold, Appstle, Seal, Skio, Loop, Stay AI, Ordergroove,
Smartrr, PayWhirl, Subify, Native Shopify, Recurpay, QPilot,
ChargeBee, Recurly, Yotpo, Growave, Rebuy, Vitals, + more

## Tips:
- THREADS=5 is safe. Can go up to 8 if you want faster (more 429 risk)
- Each runner gets a 6-hour limit (more than enough for 660 stores)
- If a chunk fails, re-run only that chunk manually by setting CHUNK_INDEX/CHUNK_TOTAL env vars
