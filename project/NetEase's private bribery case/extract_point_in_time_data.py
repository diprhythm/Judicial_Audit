import pandas as pd
import os

# 定义输入文件路径
input_file = r'F:\202408\网易非公受贿案\流水分析\输出数据_只筛选账户名.xlsx'
output_file = r'F:\202408\网易非公受贿案\流水分析\相关时点账户余额.xlsx'

# 读取Excel数据
df = pd.read_excel(input_file)

# 分组数据
account_groups = df.groupby('交易账号')

# 定义时间点
jin_yuchen_dates = [
    ('2021-10-11', '______'),
    ('2021-10-18', '______'),
    ('2021-10-29', '______'),
    ('2021-11-18', '______'),
    ('2021-11-19', '______'),
    ('2021-11-20', '______'),
    ('2021-11-21', '______'),
    ('2021-11-23', '______'),
    ('2021-11-26', '______'),
    ('2021-12-10', '______')
]

xu_ao_dates = [
    ('2021-01-28', '______'),
    ('2021-02-26', '______'),
    ('2021-03-05', '______'),
    ('2021-03-09', '______'),
    ('2021-03-10', '______'),
    ('2021-03-11', '______'),
    ('2021-03-12', '______'),
    ('2021-03-16', '______'),
    ('2021-03-17', '______'),
    ('2021-03-18', '______'),
    ('2021-03-19', '______'),
    ('2021-03-22', '______'),
    ('2021-03-23', '______'),
    ('2021-03-24', '______'),
    ('2021-03-25', '______'),
    ('2021-03-26', '______'),
    ('2021-03-28', '______'),
    ('2021-03-29', '______'),
    ('2021-04-15', '______'),
    ('2021-04-16', '______'),
    ('2021-04-19', '______')
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
    xu_ao_df = create_sheet(account_groups, xu_ao_dates)

    jin_yuchen_df.to_excel(writer, sheet_name='金雨晨', index=False, startrow=2)
    xu_ao_df.to_excel(writer, sheet_name='许骜', index=False, startrow=2)

print("Excel 文件已生成！")
