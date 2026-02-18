"""
merge_chunks.py
Combines output_chunk_0.xlsx ... output_chunk_9.xlsx into one final Excel.
Run from repo root after all scan jobs finish.
"""

import os
import glob
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils.dataframe import dataframe_to_rows

CHUNKS_DIR  = "./chunks"
OUTPUT_FILE = "FINAL_Shopify_Deep_Analysis.xlsx"

def write_sheet(wb, df_data, sheet_name, header_color="1F4E79"):
    ws = wb.create_sheet(sheet_name)
    for r in dataframe_to_rows(df_data, index=False, header=True):
        ws.append(r)
    header_fill = PatternFill("solid", start_color=header_color)
    header_font = Font(bold=True, color="FFFFFF", name="Arial", size=10)
    for cell in ws[1]:
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
    for col in ws.columns:
        max_len = max((len(str(cell.value or "")) for cell in col), default=10)
        ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)
    for row in ws.iter_rows(min_row=2):
        for cell in row:
            cell.font = Font(name="Arial", size=9)

def main():
    files = glob.glob(os.path.join(CHUNKS_DIR, "output_chunk_*.xlsx"))
    if not files:
        # fallback: look in current dir
        files = glob.glob("output_chunk_*.xlsx")

    print(f"📦 Found {len(files)} chunk files: {sorted(files)}")

    dfs = []
    for f in sorted(files):
        try:
            df = pd.read_excel(f, sheet_name="All_Stores")
            dfs.append(df)
            print(f"  ✅ {f}: {len(df)} rows")
        except Exception as e:
            print(f"  ⚠️  {f}: could not read - {e}")

    if not dfs:
        print("❌ No valid chunks found. Exiting.")
        return

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\n📊 Total stores: {len(df_all)}")

    # ── Derived sheets ─────────────────────────────────────────
    df_found = df_all[df_all["Status"].isin(["found","app_detected_no_product_api"])].copy()
    print(f"✅ Subscription stores: {len(df_found)}")

    # App frequency
    app_counts = {}
    for apps_str in df_all.get("Apps_Detected", pd.Series()).dropna():
        for app in str(apps_str).split(" | "):
            if app.strip():
                app_counts[app.strip()] = app_counts.get(app.strip(), 0) + 1
    df_apps = pd.DataFrame(sorted(app_counts.items(), key=lambda x: -x[1]), columns=["App","Store_Count"])

    # Status summary
    df_status_summary = df_all["Status"].value_counts().reset_index()
    df_status_summary.columns = ["Status", "Count"]

    # ── Write final Excel ─────────────────────────────────────
    wb = Workbook()
    if "Sheet" in wb.sheetnames:
        del wb["Sheet"]

    write_sheet(wb, df_all,            "All_Stores",          "1F4E79")
    write_sheet(wb, df_found,          "Subscription_Stores", "375623")
    write_sheet(wb, df_apps,           "App_Usage_Stats",     "7030A0")
    write_sheet(wb, df_status_summary, "Status_Summary",      "843C0C")

    if "Status_Log" in pd.ExcelFile(sorted(files)[0]).sheet_names:
        log_dfs = []
        for f in sorted(files):
            try:
                log_dfs.append(pd.read_excel(f, sheet_name="Status_Log"))
            except:
                pass
        if log_dfs:
            write_sheet(wb, pd.concat(log_dfs, ignore_index=True), "Status_Log", "595959")

    wb.save(OUTPUT_FILE)
    print(f"\n🎉 Final file saved: {OUTPUT_FILE}")
    print(f"   Sheets: All_Stores, Subscription_Stores, App_Usage_Stats, Status_Summary")

if __name__ == "__main__":
    main()
