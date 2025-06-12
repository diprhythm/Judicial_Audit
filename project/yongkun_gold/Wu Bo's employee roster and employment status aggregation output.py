import pandas as pd
from collections import Counter
from datetime import datetime
from dateutil.relativedelta import relativedelta

# —— 配置区域 ——
INPUT_FILE  = r"F:\2025\永坤非吸\吴波花名册合并\员工花名册数据合并_数据源-吴波.xlsx"
SHEET_NAME  = "员工花名册"
OUTPUT_FILE = r"C:\Users\Administrator\Desktop\员工花名册聚合输出.xlsx"

# 原表到输出表字段映射（统一“身份证号码”）
FIELD_MAP = {
    '姓名':       '姓名',
    '入职日期':   '入职时间',
    '公司':       '入职单位',
    '中心/部门':  '部门',
    '岗位':       '岗位',
    '期间':       '期间',
    '身份证号码': '身份证号码',
    '开户行':     '开户行',
    '银行卡号':   '银行卡号',
    '联系电话':   '联系电话',
}

# 最终输出列顺序
OUTPUT_COLS = [
    '姓名',
    '入职时间',
    '入职单位',
    '部门',
    '岗位',
    '期间',
    '身份证号码',
    '开户行',
    '银行卡号',
    '联系电话',
]

def most_common(vals):
    """取列表中出现次数最多的非空字符串，否则空串。"""
    clean = [v for v in vals if isinstance(v, str) and v.strip() and not pd.isna(v)]
    return Counter(clean).most_common(1)[0][0] if clean else ''

def merge_periods_dt(date_series):
    """
    接收一列 datetime，按月合并为连续区间，
    返回 'YYYY.MM-YYYY.MM;YYYY.MM;…'。
    """
    # 丢弃 NaT
    valid = date_series.dropna()
    if valid.empty:
        return ''
    # 转为 Period 并去重、排序
    periods = sorted(valid.dt.to_period('M').unique(), key=lambda p: (p.year, p.month))
    segments = []
    start = prev = periods[0]
    for p in periods[1:]:
        # Period 差一个月即连续
        if (p.year == prev.year and p.month == prev.month + 1) or \
           (p.year == prev.year + 1 and prev.month == 12 and p.month == 1):
            prev = p
        else:
            segments.append((start, prev))
            start = prev = p
    segments.append((start, prev))

    parts = []
    for s, e in segments:
        s_str = f"{s.year}.{s.month:02d}"
        e_str = f"{e.year}.{e.month:02d}"
        parts.append(s_str if s == e else f"{s_str}-{e_str}")
    return ";".join(parts)

# —— 主流程 ——
df = pd.read_excel(INPUT_FILE, sheet_name=SHEET_NAME, dtype=str)

# 重命名列 & 补齐目标列
rename_map = {orig: new for orig, new in FIELD_MAP.items() if orig in df.columns}
df = df.rename(columns=rename_map)
for tgt in FIELD_MAP.values():
    if tgt not in df.columns:
        df[tgt] = ''

# 1. 规范“入职时间”为 YYYY-MM-DD
df['入职时间'] = (
    pd.to_datetime(df['入职时间'], errors='coerce')
      .dt.strftime("%Y-%m-%d")
      .fillna('')
)

# 2. 规范“期间”为 datetime（你的“YYYY/MM/DD”格式）
df['期间'] = pd.to_datetime(df['期间'], errors='coerce')

# 检查数据
if df.empty:
    raise ValueError(f"工作表「{SHEET_NAME}」中未检索到任何数据行。")

# 分组聚合：按“身份证号码 + 岗位”
out = []
for (id_no, post), grp in df.groupby(['身份证号码','岗位'], dropna=False):
    rec = {
        '姓名':       most_common(grp['姓名']),
        '入职时间':   most_common(grp['入职时间']),
        '入职单位':   most_common(grp['入职单位']),
        '部门':       most_common(grp['部门']),
        '岗位':       post or '',
        '期间':       merge_periods_dt(grp['期间']),
        '身份证号码': id_no or '',
        '开户行':     most_common(grp['开户行']),
        '银行卡号':   most_common(grp['银行卡号']),
        '联系电话':   most_common(grp['联系电话']),
    }
    out.append(rec)

# 构建并输出
df_out = pd.DataFrame(out, columns=OUTPUT_COLS)
df_out.to_excel(OUTPUT_FILE, index=False)
print(f"✅ 完成！聚合结果已保存到：{OUTPUT_FILE}")
