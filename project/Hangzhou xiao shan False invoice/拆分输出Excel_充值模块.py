import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from concurrent.futures import ThreadPoolExecutor, as_completed
import gc

# ------------------------
# 日志设置
# ------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("脚本开始执行。")

# ------------------------
# 数据库连接配置
# ------------------------
HOST = 'localhost'
PORT = 3306
USER = 'root'
PASSWORD = '010203'
connection_string = f"mysql+pymysql://{USER}:{PASSWORD}@{HOST}:{PORT}/"

engine = create_engine(
    connection_string,
    poolclass=QueuePool,
    pool_size=20,
    max_overflow=30,
    pool_timeout=60,
    echo=False
)
logging.info("数据库连接创建成功。")

# ------------------------
# 读取基础表：平台充值数据 和 落地服务商清单
# ------------------------
try:
    sql_platform = "SELECT * FROM `三平台合并数据`.`001_充值汇总_20250408_字段清洗`;"
    logging.info("正在读取平台数据...")
    df_platform = pd.read_sql(sql_platform, engine)
    logging.info(f"平台数据读取完成，共 {len(df_platform)} 条。")

    sql_ld = "SELECT * FROM `其它文件`.`落地服务商清单`;"
    logging.info("正在读取落地服务商清单...")
    df_ld = pd.read_sql(sql_ld, engine)
    logging.info(f"落地服务商清单读取完成，共 {len(df_ld)} 条。")
except Exception as e:
    logging.error(f"读取数据出错：{e}")
    raise

# 去重服务商名称，防止重复处理
service_list = list(set(df_platform['服务公司名称_match'].dropna().tolist()))
logging.info(f"发现 {len(service_list)} 个唯一服务商需要处理。")

# ------------------------
# 设置输出目录（自动获取当前用户桌面）
# ------------------------
output_dir = os.path.join(os.path.expanduser("~"), "Desktop", "充值拆分")
os.makedirs(output_dir, exist_ok=True)

# 如果单个Sheet超过此行数，则拆分输出
MAX_EXCEL_ROWS = 1_000_000


# ------------------------
# 辅助函数：将列表分块
# ------------------------
def split_chunks(lst, size):
    """将列表lst按每块size大小切分"""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


# ------------------------
# 分块查询函数：严格切片查询
# ------------------------
def fetch_records_by_chunks(table, service, customer_list, chunk_size=20):
    """
    从指定银行流水表中查询数据：
      - 条件必须满足：账户名_match = 当前服务商，
      - 且 对手户名_match 属于给定的customer_list（分块查询，每次最多chunk_size个客户），
      - 针对“其它银行流水”额外增加过滤：日期 >= '2018-05-01'
    将所有块查询结果合并后返回，不会因IN条件太长而出错。
    """
    dfs = []
    if not customer_list:
        return pd.DataFrame()
    for chunk in split_chunks(customer_list, chunk_size):
        try:
            customers_str = ",".join(f"'{c}'" for c in chunk)
            date_condition = "AND `日期` >= '2018-05-01'" if table == "其它银行流水" else ""
            sql = f"""
                SELECT *
                FROM `银行流水`.`{table}`
                WHERE `账户名_match` = '{service}'
                  AND `对手户名_match` IN ({customers_str})
                  {date_condition}
            """
            logging.info(f"[{service}] 正在查询 {table} 客户块（数量：{len(chunk)}）")
            df_chunk = pd.read_sql(sql, engine)
            if not df_chunk.empty:
                dfs.append(df_chunk)
        except Exception as e:
            logging.error(f"[{service}] 查询 {table} 客户块出错：{e}")
    valid_dfs = [df for df in dfs if not df.empty]
    return pd.concat(valid_dfs, ignore_index=True).drop_duplicates() if valid_dfs else pd.DataFrame()


# ------------------------
# 单个服务商处理函数
# ------------------------
def process_service(service):
    """
    对某个服务商处理流程：
      1. 筛选平台充值数据，提取客户清单
      2. 分块查询“其它银行流水”数据：
         - 条件必须同时满足账户名_match为当前服务商，
           且对手户名_match在对应客户清单内；
         - 且仅查询日期 >= '2018-05-01'（剔除老数据）
      3. 分块查询“富民银行流水”数据，条件同上（不需要日期过滤）
      4. 对查询结果按对手户名及日期（或交易日期）排序
      5. 根据数据量自动拆分Sheet（超过MAX_EXCEL_ROWS拆分为part2）
      6. 根据落地服务商清单确定Excel文件名称，导出工作簿
    """
    try:
        logging.info(f"[{service}] 开始处理。")
        # 筛选平台充值数据和提取客户清单
        df_service = df_platform[df_platform['服务公司名称_match'] == service].copy()
        logging.info(f"[{service}] 平台充值数据记录数：{len(df_service)}")
        customer_list = df_service['公司名称_match'].dropna().unique().tolist()
        logging.info(f"[{service}] 客户公司数量：{len(customer_list)}")

        # 查询其它银行流水数据（分块查询，严格条件）
        df_other = fetch_records_by_chunks("其它银行流水", service, customer_list, chunk_size=20)
        if not df_other.empty and '日期' in df_other.columns:
            df_other.sort_values(by=['对手户名_match', '日期'], inplace=True)
        logging.info(f"[{service}] 其它银行流水查询结果记录数：{len(df_other)}")

        # 查询富民银行流水数据（分块查询）
        df_fumin = fetch_records_by_chunks("富民银行流水", service, customer_list, chunk_size=20)
        if not df_fumin.empty and '交易日期' in df_fumin.columns:
            df_fumin.sort_values(by=['对手户名_match', '交易日期'], inplace=True)
        logging.info(f"[{service}] 富民银行流水查询结果记录数：{len(df_fumin)}")

        # 判断匹配情况标识
        if not df_other.empty and not df_fumin.empty:
            match_flag = "三表齐全"
        else:
            flag = []
            if df_other.empty:
                flag.append("其它银行流水无数据")
            if df_fumin.empty:
                flag.append("富民银行流水无数据")
            match_flag = "_".join(flag)
        logging.info(f"[{service}] 数据匹配情况：{match_flag}")

        # 根据落地服务商清单确定工作簿名称
        matched_ld = df_ld[(df_ld['现用名_match'] == service) | (df_ld['曾用名_match'] == service)]
        if not matched_ld.empty:
            row = matched_ld.iloc[0]
            num = str(row['序号']) if pd.notna(row['序号']) else ''
            now = row['现用名'] if pd.notna(row['现用名']) else service
            old = row['曾用名'] if pd.notna(row['曾用名']) else ''
            filename = f"{num}、{now}({old})_{match_flag}.xlsx" if old else f"{num}、{now}_{match_flag}.xlsx"
        else:
            filename = f"{service}_{match_flag}.xlsx"
        logging.info(f"[{service}] 工作簿名称：{filename}")
        path = os.path.join(output_dir, filename)

        # 导出Excel：3个Sheet，根据数据行数自动拆分
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df_service.to_excel(writer, sheet_name="1.平台充值数据", index=False)
            logging.info(f"[{service}] 1.平台充值数据写入完成。")

            if len(df_other) > MAX_EXCEL_ROWS:
                df_other.iloc[:MAX_EXCEL_ROWS].to_excel(writer, sheet_name="2.其它银行流水", index=False)
                df_other.iloc[MAX_EXCEL_ROWS:].to_excel(writer, sheet_name="2.其它银行流水part2", index=False)
                logging.info(f"[{service}] 其它银行流水拆分写入完成，共 {len(df_other)} 条记录。")
            else:
                df_other.to_excel(writer, sheet_name="2.其它银行流水", index=False)
                logging.info(f"[{service}] 其它银行流水写入完成，共 {len(df_other)} 条记录。")

            if len(df_fumin) > MAX_EXCEL_ROWS:
                df_fumin.iloc[:MAX_EXCEL_ROWS].to_excel(writer, sheet_name="3.富民银行流水", index=False)
                df_fumin.iloc[MAX_EXCEL_ROWS:].to_excel(writer, sheet_name="3.富民银行流水part2", index=False)
                logging.info(f"[{service}] 富民银行流水拆分写入完成，共 {len(df_fumin)} 条记录。")
            else:
                df_fumin.to_excel(writer, sheet_name="3.富民银行流水", index=False)
                logging.info(f"[{service}] 富民银行流水写入完成，共 {len(df_fumin)} 条记录。")

        logging.info(f"[{service}] 工作簿导出成功：{filename}")
        return f"{service} 处理成功"
    except Exception as e:
        logging.error(f"[{service}] 出错：{e}")
        return f"{service} 处理失败：{e}"
    finally:
        gc.collect()


# ------------------------
# 多线程并发处理所有服务商
# ------------------------
results = []
max_workers = 20
with ThreadPoolExecutor(max_workers=max_workers) as executor:
    future_to_service = {executor.submit(process_service, s): s for s in service_list}
    for future in as_completed(future_to_service):
        service = future_to_service[future]
        try:
            result = future.result()
            logging.info(result)
            results.append({"服务商": service, "处理结果": result})
        except Exception as e:
            logging.error(f"[{service}] 处理异常：{e}")
            results.append({"服务商": service, "处理结果": f"异常：{e}"})

summary = pd.DataFrame(results)
summary.to_excel(os.path.join(output_dir, "服务商处理汇总.xlsx"), index=False)
logging.info("所有服务商处理完毕。汇总文件已生成。")
