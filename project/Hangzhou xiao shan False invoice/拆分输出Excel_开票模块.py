import os
import pymysql
import pandas as pd


def main():
    # 数据库连接参数
    conn_params = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '010203',
        'charset': 'utf8mb4'
    }

    # 连接 MySQL 数据库（这里不用指定数据库，后续SQL中会使用带库名的写法）
    connection = pymysql.connect(**conn_params)

    try:
        # 读取落地服务商清单
        # 该表位于数据库 `其它文件` 中，字段包含 序号、现用名_match、曾用名_match
        sql_units = "SELECT 序号, 现用名_match, 曾用名_match FROM `其它文件`.`落地服务商清单`"
        df_units = pd.read_sql(sql_units, connection)

        # 指定输出路径
        output_dir = r"C:\Users\H3C\Desktop\开票数据拆分"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # 循环遍历每个服务单位
        for _, row in df_units.iterrows():
            seq = row['序号']
            current_name = row['现用名_match']
            # 若曾用名字段为NaN则设为None
            former_name = row['曾用名_match'] if pd.notna(row['曾用名_match']) else None

            # 构造查询时需要使用的名称（当前名称始终加入，曾用名如果有且与当前名称不一样也加入）
            name_filters = []
            if current_name:
                name_filters.append(current_name)
            if former_name and former_name != current_name:
                name_filters.append(former_name)

            # --- 表1：平台数据 ---
            # 数据源来自数据库 `三平台合并数据` 表： '003_开票信息汇总表_20250409_字段清洗'
            # 筛选字段为 “服务公司名称_match”
            condition1 = " OR ".join([f"服务公司名称_match = '{name}'" for name in name_filters])
            sql1 = f"""
                SELECT *
                FROM `三平台合并数据`.`003_开票信息汇总表_20250409_字段清洗`
                WHERE {condition1}
            """
            df_platform = pd.read_sql(sql1, connection)

            # --- 表2：国税局数据 ---
            # 数据源来自数据库 `国税局数据` 表： '001_落地服务商国税局开票数据_finalversion'
            # 筛选字段为 “纳税人名称”
            condition2 = " OR ".join([f"纳税人名称 = '{name}'" for name in name_filters])
            sql2 = f"""
                SELECT *
                FROM `国税局数据`.`001_落地服务商国税局开票数据_finalversion`
                WHERE {condition2}
            """
            df_guoshuiju = pd.read_sql(sql2, connection)

            # --- 表3：平台与国税局数据核对 ---
            # 数据源来自数据库 `国税局数据` 表： '002_平台开票信息与国税局核对表_finalversion'
            # 筛选字段为 “服务公司名称”
            condition3 = " OR ".join([f"服务公司名称 = '{name}'" for name in name_filters])
            sql3 = f"""
                SELECT *
                FROM `国税局数据`.`002_平台开票信息与国税局核对表_finalversion`
                WHERE {condition3}
            """
            df_check = pd.read_sql(sql3, connection)

            # 判断三个表数据是否齐全
            missing_parts = []
            if df_platform.empty:
                missing_parts.append("无平台数据")
            if df_guoshuiju.empty:
                missing_parts.append("无国税局数据")
            if df_check.empty:
                missing_parts.append("无数据核对表")

            if not missing_parts:
                completeness = "三表齐全"
            else:
                completeness = "_".join(missing_parts)

            # 构造输出文件名称
            # 如果存在曾用名，则文件名使用格式： 序号、现用名(曾用名)_数据情况.xlsx
            if former_name and former_name != current_name:
                service_name = f"{current_name}({former_name})"
            else:
                service_name = current_name
            file_name = f"{seq}、{service_name}_{completeness}.xlsx"
            file_path = os.path.join(output_dir, file_name)

            # 将三个表分别写入 Excel 工作簿的三个工作表中
            with pd.ExcelWriter(file_path, engine='openpyxl') as writer:
                df_platform.to_excel(writer, sheet_name="1.平台数据", index=False)
                df_guoshuiju.to_excel(writer, sheet_name="2.国税局数据", index=False)
                df_check.to_excel(writer, sheet_name="3.平台与国税局数据核对", index=False)

            print(f"已保存工作簿：{file_path}")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
