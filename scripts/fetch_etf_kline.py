"""
ETF 分钟 K 线批量抓取 & 增量入库
支持两种数据源：
  --source tdx  通达信协议（默认，约1.5年，无需代理）
  --source em   东方财富API（约5年，前复权，需VPN代理可用）

用法:
  python scripts/fetch_etf_kline.py                   # TDX源，全部ETF，增量更新
  python scripts/fetch_etf_kline.py --source em       # EM源，需VPN，5年历史
  python scripts/fetch_etf_kline.py --klt 1           # 1分钟线
  python scripts/fetch_etf_kline.py --pool etf12      # 只拉ETF12池
  python scripts/fetch_etf_kline.py --pool etf23      # 只拉ETF23池
  python scripts/fetch_etf_kline.py --etf 518880.SS   # 只拉单只
  python scripts/fetch_etf_kline.py --full            # 强制全量
  python scripts/fetch_etf_kline.py --dry-run         # 预览不写库

表命名规则: etf_{6位代码}_{周期}  例: etf_518880_5 / etf_518880_1

TDX字段: open/close/high/low/volume/amount（不复权）
EM字段:  open/close/high/low/volume/amount/pct_change/amplitude/turnover（前复权）
"""

import argparse
import json
import socket
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# ─────────────────────────────────────────────
# 依赖检查
# ─────────────────────────────────────────────
def _check_deps(source: str):
    base = [("pandas", "pandas"), ("sqlalchemy", "sqlalchemy"), ("pymysql", "pymysql")]
    extra = {
        "tdx": [("pytdx", "pytdx")],
        "em":  [("requests", "requests")],
    }
    missing = []
    for mod, pkg in base + extra.get(source, []):
        try:
            __import__(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"[ERROR] 缺少依赖，请先安装:\n  pip install {' '.join(missing)}")
        sys.exit(1)


# ─────────────────────────────────────────────
# ETF 池（ETF12 + ETF23 合并去重，共24只）
# ─────────────────────────────────────────────
ETF12 = [
    {"code": "563300.SS", "name": "中证2000ETF"},
    {"code": "159611.SZ", "name": "电力ETF"},
    {"code": "159681.SZ", "name": "算力ETF"},
    {"code": "588000.SS", "name": "科创50ETF"},
    {"code": "513100.SS", "name": "纳指ETF"},
    {"code": "513180.SS", "name": "恒生科技ETF"},
    {"code": "515980.SS", "name": "人工智能ETF"},
    {"code": "518880.SS", "name": "黄金ETF"},
    {"code": "162411.SZ", "name": "华宝油气"},
    {"code": "512890.SS", "name": "红利低波ETF"},
    {"code": "515880.SS", "name": "通信ETF"},
    {"code": "159992.SZ", "name": "创新药ETF"},
]

ETF23_EXTRA = [
    # ETF23新增，ETF12没有的
    {"code": "159845.SZ", "name": "中证1000ETF"},
    {"code": "159566.SZ", "name": "储能电池ETF"},
    {"code": "516180.SS", "name": "光伏ETF"},
    {"code": "512480.SS", "name": "半导体ETF"},
    {"code": "562500.SS", "name": "机器人ETF"},
    {"code": "515170.SS", "name": "食品饮料ETF"},
    {"code": "159699.SZ", "name": "恒生消费ETF"},
    {"code": "512880.SS", "name": "证券ETF"},
    {"code": "159883.SZ", "name": "医疗器械ETF"},
    {"code": "512660.SS", "name": "军工ETF"},
    {"code": "515220.SS", "name": "煤炭ETF"},
    {"code": "159880.SZ", "name": "有色ETF"},
]

ETF_ALL = ETF12 + ETF23_EXTRA   # 24只，全部池子

POOLS = {"etf12": ETF12, "etf23": ETF12 + ETF23_EXTRA, "all": ETF_ALL}

# TDX 频率: klt → category
KLT_TO_TDX = {1: 7, 5: 0, 15: 1, 30: 2, 60: 3}

# 备用 TDX 服务器
FALLBACK_TDX = [
    ("110.41.147.114", 7709), ("47.92.127.181", 7709),
    ("120.79.212.229", 7709), ("47.107.75.159", 7709),
]

# 默认 VPN 代理
DEFAULT_PROXY = "http://127.0.0.1:7897"


# ─────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────
def _proxy_alive(host="127.0.0.1", port=7897, timeout=1.0) -> bool:
    """检测代理端口是否在监听（VPN是否运行）"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        s.connect((host, port))
        s.close()
        return True
    except Exception:
        return False


def _em_secid(symbol: str) -> str:
    code, suffix = symbol.split(".")
    return f"0.{code}" if suffix.upper() == "SZ" else f"1.{code}"


def _market(symbol: str) -> int:
    return 1 if symbol.upper().endswith(".SS") else 0


# ─────────────────────────────────────────────
# 东方财富数据源
# ─────────────────────────────────────────────
def _em_fetch_segment(symbol: str, klt: int, start: str, end: str,
                      proxies: dict) -> "pd.DataFrame":
    import requests, pandas as pd
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "ut":    "7eea3edcaed734bea9cbfc24409ed989",
        "klt":   str(klt),
        "fqt":   "1",        # 前复权
        "beg":   start,
        "end":   end,
        "secid": _em_secid(symbol),
        "lmt":   "1500000",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer":    "https://quote.eastmoney.com/",
    }
    s = requests.Session()
    s.trust_env = False
    resp = s.get(url, params=params, headers=headers, proxies=proxies, timeout=30)
    resp.raise_for_status()
    klines = (resp.json().get("data") or {}).get("klines") or []
    if not klines:
        return pd.DataFrame()
    rows = []
    for item in klines:
        p = item.split(",")
        if len(p) < 11:
            continue
        rows.append({
            "dt": p[0], "open": p[1], "close": p[2], "high": p[3], "low": p[4],
            "volume": p[5], "amount": p[6], "amplitude_pct": p[7],
            "pct_change_pct": p[8], "change": p[9], "turnover_pct": p[10],
        })
    df = pd.DataFrame(rows)
    for col in [c for c in df.columns if c != "dt"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["dt"] = pd.to_datetime(df["dt"])
    return df


def fetch_em(symbol: str, klt: int, start_date: str, proxies: dict) -> "pd.DataFrame":
    """按年分段拉取东方财富历史K线（前复权）"""
    import pandas as pd
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt   = datetime.today() - timedelta(days=1)
    frames   = []
    cur      = start_dt
    while cur <= end_dt:
        seg_end = min(datetime(cur.year, 12, 31), end_dt)
        seg = _em_fetch_segment(symbol, klt,
                                cur.strftime("%Y%m%d"), seg_end.strftime("%Y%m%d"),
                                proxies)
        if not seg.empty:
            frames.append(seg)
        cur = datetime(cur.year + 1, 1, 1)
        time.sleep(0.4)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return df.drop_duplicates("dt").sort_values("dt").reset_index(drop=True)


# ─────────────────────────────────────────────
# 通达信数据源
# ─────────────────────────────────────────────
def _load_tdx_servers() -> list:
    cfg_path = Path.home() / ".mootdx" / "config.json"
    if cfg_path.exists():
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            hq = cfg.get("SERVER", {}).get("HQ", [])
            if hq:
                return [(s[1], s[2]) for s in hq[:5]]
        except Exception:
            pass
    return FALLBACK_TDX


def connect_tdx():
    from pytdx.hq import TdxHq_API
    api = TdxHq_API()
    for ip, port in _load_tdx_servers():
        try:
            if api.connect(ip, port):
                return api
        except Exception:
            continue
    raise RuntimeError("无法连接 TDX 服务器，请运行: python -m mootdx bestip")


def fetch_tdx(api, symbol: str, klt: int, stop_after=None) -> "pd.DataFrame":
    """分页拉取通达信历史K线（不复权，约1.5年）"""
    import pandas as pd
    category = KLT_TO_TDX.get(klt)
    if category is None:
        raise ValueError(f"klt 不支持: {klt}，可选: {list(KLT_TO_TDX)}")
    code6 = symbol.split(".")[0]
    market = _market(symbol)
    frames, offset, batch = [], 0, 800
    while True:
        raw = api.get_security_bars(category, market, code6, offset, batch)
        if not raw:
            break
        df = api.to_df(raw)
        if df is None or len(df) == 0:
            break
        frames.append(df)
        if stop_after is not None:
            import pandas as _pd
            if _pd.to_datetime(df["datetime"]).min() <= _pd.Timestamp(stop_after):
                break
        if len(df) < batch:
            break
        offset += batch
        time.sleep(0.1)
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    return result.drop_duplicates("datetime").sort_values("datetime").reset_index(drop=True)


# ─────────────────────────────────────────────
# 数据库工具
# ─────────────────────────────────────────────
def load_engine(config_path="db_config.json"):
    import pandas as pd
    from sqlalchemy import create_engine, text
    cfg_file = Path(config_path)
    if not cfg_file.exists():
        cfg_file = Path(__file__).parent.parent / "db_config.json"
    if not cfg_file.exists():
        print("[ERROR] 未找到 db_config.json")
        sys.exit(1)
    cfg = json.loads(cfg_file.read_text(encoding="utf-8"))
    url = cfg.get("url") or cfg.get("db_url")
    if not url:
        driver = cfg.get("driver", "mysql+pymysql")
        url = f"{driver}://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg.get('port',3306)}/{cfg['database']}"
    engine = create_engine(url, pool_pre_ping=True)
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))
    return engine


def _get_dt(engine, table: str, func="MAX"):
    from sqlalchemy import inspect as sa_inspect, text
    if not sa_inspect(engine).has_table(table):
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(text(f"SELECT {func}(dt) FROM `{table}`")).fetchone()
            return row[0] if (row and row[0]) else None
    except Exception:
        return None


def _delete_from(engine, table: str, code: str, from_dt):
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(f"DELETE FROM `{table}` WHERE code=:c AND dt>=:d"),
                     {"c": code, "d": from_dt})
        conn.commit()


# ─────────────────────────────────────────────
# 单只 ETF 同步（TDX 源）
# ─────────────────────────────────────────────
def sync_tdx(etf: dict, klt: int, api, engine,
             full=False, dry_run=False) -> int:
    import pandas as pd
    symbol, name = etf["code"], etf["name"]
    tname = f"etf_{symbol.split('.')[0]}_{klt}"
    last_dt  = None if (full or dry_run) else _get_dt(engine, tname, "MAX")
    first_dt = None if (full or dry_run) else _get_dt(engine, tname, "MIN")
    label = f"增量(库内至:{last_dt})" if last_dt else "全量"
    print(f"  [{symbol}] {name}  {klt}min [TDX] {label}...", end=" ", flush=True)
    stop = first_dt if (last_dt and not full) else None
    raw_df = fetch_tdx(api, symbol, klt, stop_after=stop)
    if raw_df.empty:
        print("无数据")
        return 0
    # 标准化列
    df = raw_df.rename(columns={"datetime": "dt", "vol": "volume"})[
        ["dt", "open", "close", "high", "low", "volume", "amount"]
    ].copy()
    df["code"]  = symbol
    df["code6"] = symbol.split(".")[0]
    df["name"]  = name
    for col in ["open", "close", "high", "low", "volume", "amount"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["dt"] = pd.to_datetime(df["dt"])
    if last_dt and not full:
        _delete_from(engine, tname, symbol, last_dt)
        df = df[df["dt"] >= pd.Timestamp(last_dt)]
    print(f"{len(df)} rows", end="")
    if dry_run:
        print(f"  [DRY-RUN] -> {tname}")
        print(df[["dt","open","close","high","low"]].head(2).to_string())
        return len(df)
    df.to_sql(tname, engine, if_exists="append", index=False, chunksize=5000)
    print(f" -> {tname} [OK]")
    return len(df)


# ─────────────────────────────────────────────
# 单只 ETF 同步（东方财富源）
# ─────────────────────────────────────────────
def sync_em(etf: dict, klt: int, engine, proxies: dict,
            full=False, dry_run=False, years=5) -> int:
    import pandas as pd
    symbol, name = etf["code"], etf["name"]
    tname = f"etf_{symbol.split('.')[0]}_{klt}"
    # 增量：从已有最新日期的前一天开始
    last_dt = None if (full or dry_run) else _get_dt(engine, tname, "MAX")
    if last_dt and not full:
        start = (pd.Timestamp(last_dt) - timedelta(days=1)).strftime("%Y%m%d")
        label = f"增量(库内至:{last_dt})"
    else:
        start_year = datetime.today().year - years
        start = f"{start_year}0101"
        label = f"全量({start_year}-至今)"
    print(f"  [{symbol}] {name}  {klt}min [EM] {label}...", end=" ", flush=True)
    df = fetch_em(symbol, klt, start, proxies)
    if df.empty:
        print("无数据")
        return 0
    df["code"]  = symbol
    df["code6"] = symbol.split(".")[0]
    df["name"]  = name
    print(f"{len(df)} rows", end="")
    if dry_run:
        print(f"  [DRY-RUN] -> {tname}")
        print(df[["dt","open","close","high","low"]].head(2).to_string())
        return len(df)
    if last_dt and not full:
        _delete_from(engine, tname, symbol,
                     pd.Timestamp(last_dt) - timedelta(days=1))
    df.to_sql(tname, engine, if_exists="append", index=False, chunksize=5000)
    print(f" -> {tname} [OK]")
    return len(df)


# ─────────────────────────────────────────────
# 主入口
# ─────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="ETF 分钟K线抓取入库")
    parser.add_argument("--source", default="tdx", choices=["tdx", "em"],
                        help="数据源: tdx=通达信(默认,1.5年), em=东方财富(5年,需VPN)")
    parser.add_argument("--klt",    type=int, default=5, choices=[1, 5, 15, 30, 60])
    parser.add_argument("--pool",   default="all", choices=["etf12", "etf23", "all"],
                        help="ETF池: etf12/etf23/all(默认)")
    parser.add_argument("--etf",    default=None, help="只处理单只ETF，如 518880.SS")
    parser.add_argument("--full",   action="store_true", help="强制全量（覆盖增量）")
    parser.add_argument("--proxy",  default=DEFAULT_PROXY,
                        help=f"代理地址，默认 {DEFAULT_PROXY}（--source em 时用）")
    parser.add_argument("--years",  type=int, default=5,
                        help="EM全量模式往前几年，默认5年")
    parser.add_argument("--config", default="db_config.json")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    _check_deps(args.source)

    # 选择 ETF 列表
    if args.etf:
        etf_list = [e for e in ETF_ALL if e["code"] == args.etf]
        if not etf_list:
            print(f"[ERROR] 未找到 {args.etf}，可用: {[e['code'] for e in ETF_ALL]}")
            sys.exit(1)
    else:
        etf_list = POOLS[args.pool]

    # 数据库连接
    engine = None
    if not args.dry_run:
        print(f"[DB] 连接...", end=" ")
        engine = load_engine(args.config)
        print("成功")

    total, errors = 0, []

    if args.source == "em":
        # 东方财富：检查代理
        proxy_host = args.proxy.split("//")[-1].split(":")[0]
        proxy_port = int(args.proxy.split(":")[-1])
        if not _proxy_alive(proxy_host, proxy_port):
            print(f"\n[ERROR] 代理 {args.proxy} 不可达！")
            print("       请先启动 VPN/代理软件（Clash/v2ray 等），再运行此命令。")
            print("       无需代理可改用 pytdx: python scripts/fetch_etf_kline.py --source tdx")
            sys.exit(1)
        proxies = {"http": args.proxy, "https": args.proxy}
        print(f"[EM] 代理 {args.proxy} 可用，开始抓取（最多{args.years}年）\n")
        for etf in etf_list:
            try:
                n = sync_em(etf, args.klt, engine, proxies,
                            full=args.full, dry_run=args.dry_run, years=args.years)
                total += n
            except Exception as e:
                print(f"  [ERROR] {etf['code']}: {e}")
                errors.append((etf["code"], str(e)))
            time.sleep(0.5)
    else:
        # 通达信
        print("[TDX] 连接行情服务器...", end=" ")
        api = connect_tdx()
        print(f"成功\n")
        for etf in etf_list:
            try:
                n = sync_tdx(etf, args.klt, api, engine,
                             full=args.full, dry_run=args.dry_run)
                total += n
            except Exception as e:
                print(f"  [ERROR] {etf['code']}: {e}")
                errors.append((etf["code"], str(e)))
            time.sleep(0.3)
        api.disconnect()

    print(f"\n{'='*50}")
    print(f"完成！共写入 {total} 行，失败 {len(errors)} 个")
    if errors:
        for code, err in errors:
            print(f"  {code}: {err}")


if __name__ == "__main__":
    main()
