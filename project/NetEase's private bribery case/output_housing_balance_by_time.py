import pandas as pd
import os

# 定义输入文件路径
input_file = r'F:\2024\202408\网易非公受贿案\流水分析\输出数据_只筛选账户名222.xlsx'
output_file = r'F:\2024\202408\网易非公受贿案\流水分析\还款相关时点账户余额.xlsx'

# 读取Excel数据
df = pd.read_excel(input_file)

# 分组数据
account_groups = df.groupby('交易账号')

# 定义时间点
jin_yuchen_dates = [
    ('2019-03-16', '_________________________'),
    ('2019-03-19', '_________________________'),
    ('2021-09-25', '_________________________'),
    ('2021-10-18', '_________________________'),
    ('2021-01-28', '_________________________'),
    ('2021-03-26', '_________________________'),
    ('2017-07-13', '_________________________'),
    ('2017-08-06', '_________________________'),
    ('2023-02-05', '_________________________'),
    ('2023-02-14', '_________________________'),
    ('2019-12-03', '_________________________'),
    ('2019-12-18', '_________________________'),
    ('2020-08-30', '_________________________'),
]


def create_sheet(account_groups, dates):
    """创建并填充Excel工作表"""
    data = []

    # 填充账户和账号
    for account, group in account_groups:
        # 确保'交易日期'是 datetime 类型
        group = group.copy()
        group['交易日期'] = pd.to_datetime(group['交易日期'], errors='coerce')

        row = [group['账户名'].iloc[0], account]
        for date, _ in dates:
            date = pd.to_datetime(date)
            seven_days_ago = date - pd.Timedelta(days=7)

            balance_row = group.loc[group['交易日期'] <= date].sort_values(by=['交易日期', '交易时间'],
                                                                           ascending=[False, False])
            if not balance_row.empty:
                balance1 = balance_row.iloc[0]['余额']
            else:
                balance1 = '已取流水内无该期间数据'

            balance_row_7 = group.loc[group['交易日期'] <= seven_days_ago].sort_values(by=['交易日期', '交易时间'],
                                                                                       ascending=[False, False])
            if not balance_row_7.empty:
                balance2 = balance_row_7.iloc[0]['余额']
            else:
                balance2 = '已取流水内无该期间数据'

            row.append(balance1)
            row.append(balance2)
        data.append(row)

    # 创建DataFrame
    columns = ['账户名', '账号']
    for date, event in dates:
        columns.append(f'{event} ({date})')
        columns.append(f'{event} ({pd.to_datetime(date) - pd.Timedelta(days=7)})')

    sheet_df = pd.DataFrame(data, columns=columns)
    return sheet_df


# 生成Excel文件
with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
    jin_yuchen_df = create_sheet(account_groups, jin_yuchen_dates)

    jin_yuchen_df.to_excel(writer, sheet_name='还款', index=False, startrow=2)

print("Excel 文件已生成！")
