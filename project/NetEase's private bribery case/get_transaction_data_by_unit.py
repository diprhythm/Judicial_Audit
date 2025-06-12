import pandas as pd

# 1. 配置路径和相关参数
file_path = r"F:\2024\202408\网易非公受贿案\流水分析\网易非公受贿案银行流水整理表20250328.xlsx"
output_path = r"F:\2024\202408\网易非公受贿案\流水分析\输出数据_只筛选账户名222.xlsx"

# 2. 需要筛选的账户名列表（更新后的名单）
account_names = [
    "金晨",
    "许",
    "向",
]

print("=== 开始读取 Excel ===")
df1 = pd.read_excel(file_path, sheet_name="表1", dtype=str)
df2 = pd.read_excel(file_path, sheet_name="表2", dtype=str)

print("=== 合并两个表的数据 ===")
df = pd.concat([df1, df2], ignore_index=True)
print(f"合并后共有 {df.shape[0]} 行")

# 对账户名进行 strip()，去除前后空格
df["账户名"] = df["账户名"].astype(str).str.strip()

# 筛选账户名
filtered_df = df[df["账户名"].isin(account_names)].copy()
print(f"筛选后共有 {filtered_df.shape[0]} 行")

# 避免科学计数：将需要防止科学计数的列转成字符串
cols_to_string = ["交易卡号", "交易账号", "交易对手账卡号"]
for col in cols_to_string:
    if col in filtered_df.columns:
        filtered_df[col] = filtered_df[col].astype(str)

# 输出到新的 Excel 文件
filtered_df.to_excel(output_path, index=False)
print(f"处理完成，结果已导出至: {output_path}")
