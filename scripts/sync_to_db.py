"""
股票/ETF 数据一键同步到线上数据库
用法:
  python sync_to_db.py [文件夹路径或文件路径] [--config db_config.json] [--table 表名] [--dry-run]

支持的文件格式:
  - CSV (.csv)
  - Excel (.xlsx / .xls)

支持的数据库:
  - MySQL    mysql+pymysql://user:pass@host:port/db
  - PostgreSQL  postgresql+psycopg2://user:pass@host:port/db
  - SQLite   sqlite:///path/to/file.db

自动识别的表类型:
  - stock_daily    股票日线 (含: 股票代码,日期,开盘,收盘,最高,最低,成交量...)
  - trades         交易记录 (含: 买入时间,卖出时间,买入价格,卖出价格...)
  - etf_daily      ETF/指数日线 (含: date,open,close,high,low,amount)
"""

import os
import sys
import json
import argparse
import glob
from pathlib import Path
from datetime import datetime

# ---------------------------------------------------------------------------
# 依赖检查
# ---------------------------------------------------------------------------
REQUIRED = {"pandas": "pandas", "sqlalchemy": "sqlalchemy"}
OPTIONAL = {"pymysql": "pymysql", "psycopg2": "psycopg2-binary", "openpyxl": "openpyxl"}

def check_deps():
    missing = []
    for mod, pkg in {**REQUIRED, **OPTIONAL}.items():
        try:
            __import__(mod)
        except ImportError:
            if mod in REQUIRED:
                missing.append(pkg)
            else:
                print(f"[WARN] 可选依赖 {pkg} 未安装，部分功能受限")
    if missing:
        print(f"[ERROR] 缺少必要依赖，请先安装:\n  pip install {' '.join(missing)}")
        sys.exit(1)

check_deps()

import pandas as pd
from sqlalchemy import create_engine, text, inspect

# ---------------------------------------------------------------------------
# 列名标准化映射
# ---------------------------------------------------------------------------
STOCK_DAILY_COLS = {
    "股票名称": "stock_name",
    "股票代码": "code",
    "日期": "trade_date",
    "开盘": "open",
    "收盘": "close",
    "最高": "high",
    "最低": "low",
    "成交量": "volume",
    "成交额": "amount",
    "振幅": "amplitude",
    "涨跌幅": "pct_change",
    "涨跌额": "price_change",
    "换手率": "turnover_rate",
}

TRADE_COLS = {
    "股票代码": "code",
    "股票名称": "stock_name",
    "买入时间": "buy_time_raw",
    "买入价格": "buy_price",
    "卖出时间": "sell_time_raw",
    "卖出价格": "sell_price",
    "收益率": "return_rate",
    "code": "code_clean",
    "买入时间_dt": "buy_time",
    "卖出时间_dt": "sell_time",
    "buy_date": "buy_date",
    "sell_date": "sell_date",
}

ETF_DAILY_COLS = {
    "date": "trade_date",
    "open": "open",
    "close": "close",
    "high": "high",
    "low": "low",
    "amount": "amount",
}

# ---------------------------------------------------------------------------
# 数据类型自动识别
# ---------------------------------------------------------------------------
def detect_table_type(df: pd.DataFrame, filename: str) -> tuple[str, dict]:
    """
    返回 (表名, 列名映射)
    """
    cols = set(df.columns.str.strip())

    # 股票日线
    if "股票代码" in cols and "日期" in cols and "开盘" in cols:
        return "stock_daily", STOCK_DAILY_COLS

    # 交易记录
    if "买入时间" in cols and "卖出时间" in cols:
        return "trades", TRADE_COLS

    # ETF/指数日线（英文列）
    if {"date", "open", "close", "high", "low"}.issubset(cols):
        # 从文件名提取标的代码
        code = Path(filename).stem.split("_")[0]
        return f"etf_daily", ETF_DAILY_COLS

    # 未识别 → 用文件名作表名
    safe_name = Path(filename).stem.lower()
    safe_name = "".join(c if c.isalnum() or c == "_" else "_" for c in safe_name)
    return safe_name, {}


# ---------------------------------------------------------------------------
# 读取文件
# ---------------------------------------------------------------------------
def read_file(path: str) -> pd.DataFrame:
    ext = Path(path).suffix.lower()
    try:
        if ext == ".csv":
            df = pd.read_csv(path, encoding="utf-8-sig")
        elif ext in (".xlsx", ".xls"):
            df = pd.read_excel(path, engine="openpyxl")
        else:
            raise ValueError(f"不支持的文件格式: {ext}")
        # 去除列名空白
        df.columns = df.columns.str.strip()
        return df
    except Exception as e:
        raise RuntimeError(f"读取文件失败 [{path}]: {e}")


# ---------------------------------------------------------------------------
# 数据清洗
# ---------------------------------------------------------------------------
def clean_df(df: pd.DataFrame, col_map: dict) -> pd.DataFrame:
    if col_map:
        # 只保留映射中存在的列
        existing = {k: v for k, v in col_map.items() if k in df.columns}
        df = df[list(existing.keys())].rename(columns=existing)

    # 去除全空行
    df = df.dropna(how="all")

    # 日期列统一格式
    for col in ["trade_date", "buy_date", "sell_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    # 数值列去除非数字
    numeric_cols = ["open", "close", "high", "low", "volume", "amount",
                    "amplitude", "pct_change", "price_change", "turnover_rate",
                    "buy_price", "sell_price", "return_rate"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


# ---------------------------------------------------------------------------
# 同步到数据库
# ---------------------------------------------------------------------------
def sync_to_db(df: pd.DataFrame, table: str, engine, if_exists: str = "append",
               unique_keys: list = None, dry_run: bool = False):
    """
    same: append / replace
    unique_keys: 用于 upsert 去重的列（可选）
    """
    row_count = len(df)
    if dry_run:
        print(f"  [DRY-RUN] 将写入表 {table}，共 {row_count} 行")
        print(df.head(3).to_string())
        return row_count

    with engine.connect() as conn:
        insp = inspect(engine)
        table_exists = insp.has_table(table)

        if table_exists and unique_keys and all(k in df.columns for k in unique_keys):
            # Upsert: 删除已有的重复行再插入
            for _, row in df.iterrows():
                conds = " AND ".join(f"{k} = :{k}" for k in unique_keys)
                conn.execute(text(f"DELETE FROM {table} WHERE {conds}"),
                             {k: row[k] for k in unique_keys})
            conn.commit()
            df.to_sql(table, engine, if_exists="append", index=False)
        else:
            df.to_sql(table, engine, if_exists=if_exists, index=False)

    print(f"  [OK] 写入表 {table}，共 {row_count} 行")
    return row_count


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="股票/ETF 数据同步到数据库")
    parser.add_argument("path", nargs="?", default=".", help="文件或文件夹路径")
    parser.add_argument("--config", default="db_config.json", help="数据库配置文件")
    parser.add_argument("--table", default=None, help="强制指定目标表名")
    parser.add_argument("--if-exists", default="append",
                        choices=["append", "replace", "fail"], help="表已存在时的处理方式")
    parser.add_argument("--dry-run", action="store_true", help="只预览，不写入数据库")
    parser.add_argument("--url", default=None, help="直接传入数据库 URL（优先于配置文件）")
    args = parser.parse_args()

    # 读取数据库配置
    db_url = args.url
    if not db_url:
        config_path = Path(args.config)
        if not config_path.exists():
            # 尝试脚本同目录
            config_path = Path(__file__).parent.parent / "db_config.json"
        if config_path.exists():
            with open(config_path, encoding="utf-8") as f:
                cfg = json.load(f)
            db_url = cfg.get("url") or cfg.get("db_url")
            if not db_url and "host" in cfg:
                driver = cfg.get("driver", "mysql+pymysql")
                user = cfg["user"]
                pwd = cfg["password"]
                host = cfg["host"]
                port = cfg.get("port", 3306)
                db = cfg["database"]
                db_url = f"{driver}://{user}:{pwd}@{host}:{port}/{db}"
        else:
            print("[ERROR] 未找到数据库配置文件 db_config.json")
            print("请创建 db_config.json，格式如下：")
            print(json.dumps({
                "host": "your-host",
                "port": 3306,
                "user": "your-user",
                "password": "your-password",
                "database": "your-database",
                "driver": "mysql+pymysql"
            }, ensure_ascii=False, indent=2))
            sys.exit(1)

    if args.dry_run:
        print("[DRY-RUN 模式] 不会实际写入数据库\n")
        engine = None
    else:
        print(f"[连接] 数据库: {db_url.split('@')[-1]}")  # 不打印密码
        engine = create_engine(db_url, pool_pre_ping=True)
        # 测试连接
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("[连接] 成功\n")
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            sys.exit(1)

    # 收集文件列表
    target = Path(args.path)
    if target.is_file():
        files = [str(target)]
    elif target.is_dir():
        files = (
            glob.glob(str(target / "**/*.csv"), recursive=True) +
            glob.glob(str(target / "**/*.xlsx"), recursive=True) +
            glob.glob(str(target / "**/*.xls"), recursive=True)
        )
        files.sort()
    else:
        print(f"[ERROR] 路径不存在: {args.path}")
        sys.exit(1)

    if not files:
        print("[WARN] 未找到任何 CSV/Excel 文件")
        sys.exit(0)

    print(f"[扫描] 找到 {len(files)} 个文件\n")

    total_rows = 0
    errors = []

    for fp in files:
        print(f"[处理] {fp}")
        try:
            df = read_file(fp)
            table_name, col_map = detect_table_type(df, fp)
            if args.table:
                table_name = args.table

            df = clean_df(df, col_map)

            # ETF 日线追加标的代码列
            if table_name == "etf_daily":
                code = Path(fp).stem.split("_")[0]
                df.insert(0, "code", code)

            # upsert key
            ukeys = None
            if table_name == "stock_daily":
                ukeys = ["code", "trade_date"]
            elif table_name == "etf_daily":
                ukeys = ["code", "trade_date"]
            elif table_name == "trades":
                ukeys = ["code", "buy_time", "sell_time"] if "buy_time" in df.columns else None

            rows = sync_to_db(df, table_name, engine,
                              if_exists=args.if_exists,
                              unique_keys=ukeys,
                              dry_run=args.dry_run)
            total_rows += rows
        except Exception as e:
            print(f"  [ERROR] {e}")
            errors.append((fp, str(e)))

    print(f"\n{'='*50}")
    print(f"完成！共同步 {total_rows} 行数据，失败 {len(errors)} 个文件")
    if errors:
        print("\n失败文件：")
        for fp, err in errors:
            print(f"  {fp}: {err}")


if __name__ == "__main__":
    main()
