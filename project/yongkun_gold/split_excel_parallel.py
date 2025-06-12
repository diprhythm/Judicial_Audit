# -*- coding: utf-8 -*-
"""Excel splitting utility (parallel version)

This script reads specific sheets from a source Excel file and splits them
by company name into separate Excel files using multithreading.
"""

import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# ===== Configuration =====
SRC_FILE = r"C:\Users\Administrator\Desktop\租赁业务（0612）.xlsx"
OUT_DIR = r"C:\Users\Administrator\Desktop\租赁业务金行拆分"
SHEETS = [
    "金行信息表",
    "金行-投资人投资明细",
    "租赁业务—汇总表",
    "租赁业务—未兑付",
]
KEY_FIELD = "公司全称"


def sanitize_filename(name: str) -> str:
    """Sanitize filename for saving."""
    if not name or str(name) in {"nan", "None"}:
        return "未命名公司"
    invalid_chars = '\\/:*?"<>|'
    for char in invalid_chars:
        name = str(name).replace(char, "_")
    return name.strip()


def process_single_company(args):
    """Create an Excel file for a single company."""
    company, all_data, out_dir = args

    wb = Workbook()
    wb.remove(wb.active)

    for sheet_name, df in all_data.items():
        company_data = df[df[KEY_FIELD] == company]
        if not company_data.empty:
            ws = wb.create_sheet(sheet_name)
            for row in dataframe_to_rows(company_data, index=False, header=True):
                ws.append(row)

    safe_name = sanitize_filename(company)
    output_path = Path(out_dir) / f"{safe_name}.xlsx"
    wb.save(output_path)
    wb.close()

    return f"✅ {company}"


def split_excel_parallel():
    """Read source workbook and generate one workbook per company."""
    print("🚀 使用并行处理方案...")

    all_data = {}
    for sheet_name in SHEETS:
        try:
            df = pd.read_excel(SRC_FILE, sheet_name=sheet_name)
            if KEY_FIELD in df.columns:
                all_data[sheet_name] = df
        except Exception as exc:
            print(f"   ❌ {sheet_name}: {exc}")

    if not all_data:
        print("❌ 没有可处理的数据")
        return

    all_companies = set()
    for df in all_data.values():
        companies = df[KEY_FIELD].dropna().unique()
        all_companies.update([str(c) for c in companies if str(c) != 'nan'])

    all_companies = sorted(all_companies)
    print(f"📊 准备并行处理 {len(all_companies)} 个公司...")

    os.makedirs(OUT_DIR, exist_ok=True)

    with ThreadPoolExecutor(max_workers=4) as executor:
        tasks = [(company, all_data, OUT_DIR) for company in all_companies]
        results = executor.map(process_single_company, tasks)
        for result in results:
            print(result)

    print("✅ 并行处理完成！")


if __name__ == "__main__":
    split_excel_parallel()
