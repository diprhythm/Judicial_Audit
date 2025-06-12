import pandas as pd
import os

# 读取Excel文件
input_file = r'C:\Users\Administrator\Desktop\小善涉案公司及嫌疑人(1).xlsx'
df = pd.read_excel(input_file)

# 输出txt文件路径
output_file = r'C:\Users\Administrator\Desktop\小善涉案公司及嫌疑人信息汇总.txt'

# 保存所有内容
all_text = []

# 遍历每一行
for idx, row in df.iterrows():
    company_name = row['公司名称']
    establish_date = row['成立时间']
    shareholders = row['主要股东']
    legal_representative = row['法定代表人']

    # 把成立时间转成只保留年月日的字符串
    if pd.notna(establish_date):
        establish_date = pd.to_datetime(establish_date).strftime('%Y-%m-%d')
    else:
        establish_date = '未知时间'

    text = f"{idx + 1}、{company_name}\n" \
           f"成立于{establish_date}，主要股东：{shareholders}；法定代表人{legal_representative}；\n"

    all_text.append(text)

# 写入txt文件
with open(output_file, 'w', encoding='utf-8') as f:
    f.writelines(all_text)

print("生成完毕！文件路径：", output_file)
