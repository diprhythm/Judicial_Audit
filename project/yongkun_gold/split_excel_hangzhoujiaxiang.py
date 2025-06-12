import os
import pandas as pd
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
import time

# ==== 配置 ====
SRC_FILE = r"C:\Users\Administrator\Desktop\杭州嘉祥珠宝有限公司.xlsx"
OUT_DIR = r"C:\Users\Administrator\Desktop\按签约金行拆分"
SHEETS = [
    "金行-投资人投资明细",
    "租赁业务—汇总表",
    "租赁业务—未兑付",
]
KEY_FIELD = "签约金行"  # 改为按签约金行拆分


def sanitize_filename(name):
    """清理文件名中的非法字符"""
    if not name or str(name) == "nan" or str(name) == "None":
        return "未命名金行"
    invalid_chars = "\\/:*?\"<>|"
    clean_name = str(name)
    for char in invalid_chars:
        clean_name = clean_name.replace(char, "_")
    return clean_name.strip()


def split_excel_by_jinghang():
    """按签约金行拆分Excel文件"""
    print("🚀 开始按签约金行拆分Excel文件...")
    start_time = time.time()

    if not os.path.exists(SRC_FILE):
        print(f"❌ 源文件不存在: {SRC_FILE}")
        return

    # 创建输出目录
    os.makedirs(OUT_DIR, exist_ok=True)

    # 读取所有工作表的数据
    all_data = {}
    print("📖 读取数据中...")

    for sheet_name in SHEETS:
        try:
            df = pd.read_excel(SRC_FILE, sheet_name=sheet_name)
            print(f"   📋 {sheet_name}: {len(df)} 行数据")

            # 检查是否存在目标列
            if KEY_FIELD in df.columns:
                all_data[sheet_name] = df
                print(f"   ✅ 找到 '{KEY_FIELD}' 列")
            else:
                print(f"   ⚠️ 工作表 '{sheet_name}' 中未找到 '{KEY_FIELD}' 列")
                print(f"   📝 可用列名: {list(df.columns)}")
                # 仍然保存数据，以防列名略有不同
                all_data[sheet_name] = df

        except Exception as e:
            print(f"   ❌ 读取工作表 '{sheet_name}' 失败: {e}")

    if not all_data:
        print("❌ 没有成功读取任何数据")
        return

    # 收集所有金行名称
    all_jinghang = set()
    jinghang_counts = {}

    for sheet_name, df in all_data.items():
        if KEY_FIELD in df.columns:
            # 获取非空的金行名称
            jinghang_series = df[KEY_FIELD].dropna()
            unique_jinghang = jinghang_series.unique()

            for jh in unique_jinghang:
                if str(jh) not in ['nan', 'None', '']:
                    all_jinghang.add(str(jh))
                    count = len(df[df[KEY_FIELD] == jh])
                    jinghang_counts[str(jh)] = jinghang_counts.get(str(jh), 0) + count

            print(f"   📊 {sheet_name} 中发现 {len(unique_jinghang)} 个不同的金行")

    all_jinghang = sorted(list(all_jinghang))
    print(f"\n🏪 总共发现 {len(all_jinghang)} 个签约金行:")
    for jh in all_jinghang:
        print(f"   • {jh} ({jinghang_counts.get(jh, 0)} 条记录)")

    if not all_jinghang:
        print("❌ 未找到任何有效的金行数据")
        return

    # 开始拆分
    print(f"\n🔄 开始拆分处理...")
    total_files_created = 0

    for i, jinghang in enumerate(all_jinghang, 1):
        print(f"\n[{i}/{len(all_jinghang)}] 处理金行: {jinghang}")

        # 创建新的工作簿
        wb = Workbook()
        wb.remove(wb.active)  # 删除默认工作表

        has_data = False

        for sheet_name, df in all_data.items():
            # 筛选当前金行的数据
            if KEY_FIELD in df.columns:
                jinghang_data = df[df[KEY_FIELD] == jinghang].copy()
            else:
                # 如果没有找到目标列，创建空的DataFrame但保持结构
                jinghang_data = pd.DataFrame(columns=df.columns)

            if not jinghang_data.empty or sheet_name in SHEETS:
                # 创建工作表
                ws = wb.create_sheet(sheet_name)

                if not jinghang_data.empty:
                    # 写入数据
                    for r in dataframe_to_rows(jinghang_data, index=False, header=True):
                        ws.append(r)
                    print(f"   └─ {sheet_name}: {len(jinghang_data)} 行数据")
                    has_data = True
                else:
                    # 即使没有数据，也写入表头
                    for r in dataframe_to_rows(df.head(0), index=False, header=True):
                        ws.append(r)
                    print(f"   └─ {sheet_name}: 0 行数据 (仅表头)")

        # 保存文件
        if has_data or len(wb.worksheets) > 0:
            safe_name = sanitize_filename(jinghang)
            output_path = os.path.join(OUT_DIR, f"{safe_name}.xlsx")

            try:
                wb.save(output_path)
                total_files_created += 1
                print(f"   ✅ 已保存: {safe_name}.xlsx")
            except Exception as e:
                print(f"   ❌ 保存失败: {e}")

        wb.close()

    # 完成统计
    elapsed = time.time() - start_time
    print(f"\n🎉 拆分完成!")
    print(f"📊 处理统计:")
    print(f"   • 处理金行数量: {len(all_jinghang)}")
    print(f"   • 创建文件数量: {total_files_created}")
    print(f"   • 总耗时: {elapsed:.2f} 秒")
    print(f"   • 输出目录: {OUT_DIR}")


def main():
    """主函数"""
    print("=" * 60)
    print("📊 Excel 按签约金行拆分工具")
    print("=" * 60)
    print(f"📁 源文件: {SRC_FILE}")
    print(f"📁 输出目录: {OUT_DIR}")
    print(f"🔑 拆分字段: {KEY_FIELD}")
    print(f"📋 工作表: {', '.join(SHEETS)}")
    print("=" * 60)

    # 确认执行
    response = input("🤔 确认开始拆分? (y/n): ").strip().lower()
    if response in ['y', 'yes', '是', '']:
        split_excel_by_jinghang()
    else:
        print("❌ 已取消操作")


if __name__ == "__main__":
    main()
