"""
ETF T+0 做T策略 回测引擎
================================================
策略核心（对应 etf_t0_做T策略.py）:
  - 基于 VWAP 的日内做 T
  - 当价格低于当日 VWAP 1% 且价格企稳时买入
  - 止盈：+1% 卖 1/3 → +1.5% 再卖 1/3 → +2% 全清
  - 止损：-1% 立即全清
  - 14:55 强制清仓，绝不留隔夜仓
  - 趋势过滤：近 5 日跌幅 > 3% 的标的今日跳过
  - 每日最多 3 次交易，同时最多持 2 只
  - 大盘暴跌(-1.5%) 时暂停买入

数据要求（5 分钟 K 线）:
  必须列: dt (datetime), code, open, high, low, close, volume, amount
  来源1: MySQL 数据库（etf_518880_5 等表，由 fetch_etf_kline.py 写入）
  来源2: CSV 文件（同样格式）

用法:
  # 从数据库读取（需要 db_config.json）
  python tools/backtest_t0.py --source db --start 2024-01-01
  # 从 CSV 读取
  python tools/backtest_t0.py --source csv --data data/etf_t0_5min.csv

默认初始资金: 100,000 元
输出目录: validator_output/backtest_t0/
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ── T0 策略标的池 ─────────────────────────────────────────────────────────────
# 主力池：跨境 ETF（真正支持 T+0）
PRIMARY_POOL = {
    "513180": "恒生科技ETF",
    "513330": "恒生互联网ETF",
    "513050": "中概互联ETF",
    "162411": "华宝油气",
    "513100": "纳斯达克ETF",
    "159518": "日经ETF",
    "513310": "港股科技30",
    "513900": "港股红利ETF",
    "513030": "港股通ETF",
    "513000": "日经225ETF",
    "513080": "法国CAC40ETF",
    "159502": "中韩半导体ETF",
    "159980": "有色金属ETF",
}
# 备选池
SECONDARY_POOL = {
    "518880": "黄金ETF",
    "520500": "恒生创新药ETF",
    "159608": "稀有金属ETF",
    "159985": "豆粕ETF",
    "159981": "能源化工ETF",
    "159286": "碳中和ETF",
}
ALL_POOL = {**PRIMARY_POOL, **SECONDARY_POOL}

MARKET_BENCHMARK = "510300"   # 沪深300ETF，作大盘参考（用A股ETF近似）

# 交易时间常量（5分钟K线时间标签，格式 HH:MM）
START_TRADE = "09:50"
CLEAR_TIME  = "14:55"


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def norm_code(c: str) -> str:
    c = str(c).strip()
    return c.split(".")[0].zfill(6) if "." in c else c.zfill(6)


def load_config(path: str) -> dict:
    defaults = {
        "策略分配资金(元)": 100000,
        "买入低于均线(%)":  1.0,
        "第一档卖出(%)":    1.0,
        "第二档卖出(%)":    1.5,
        "卖出止盈(%)":      2.0,
        "止损(%)":          1.0,
        "每日最大交易次数": 3,
        "同时最大持仓ETF数": 2,
        "趋势判断跌幅阈值(%)": 3.0,
        "大盘放量暴跌暂停阈值(%)": -1.5,
    }
    for f in (path, "etf_t0_config.json"):
        if f and Path(f).exists():
            with open(f, encoding="utf-8") as fp:
                defaults.update(json.load(fp))
            break
    return defaults


def load_from_csv(csv_path: str) -> pd.DataFrame:
    """从 CSV 读取 5 分钟 K 线，格式兼容通达信导出"""
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    rename = {
        "时间": "dt", "date": "dt", "datetime": "dt",
        "代码": "code", "stock_code": "code",
        "开盘": "open",  "开盘价": "open",
        "收盘": "close", "收盘价": "close",
        "最高": "high",  "最高价": "high",
        "最低": "low",   "最低价": "low",
        "成交量": "volume", "vol": "volume",
        "成交额": "amount", "money": "amount",
    }
    df.rename(columns=rename, inplace=True)
    for c in ("dt", "code", "close"):
        if c not in df.columns:
            raise ValueError(f"CSV 缺少必要列: {c}  现有: {list(df.columns)}")
    df["dt"]     = pd.to_datetime(df["dt"])
    df["code"]   = df["code"].apply(norm_code)
    df["close"]  = pd.to_numeric(df["close"],  errors="coerce")
    df["open"]   = pd.to_numeric(df.get("open",  df["close"]), errors="coerce")
    df["high"]   = pd.to_numeric(df.get("high",  df["close"]), errors="coerce")
    df["low"]    = pd.to_numeric(df.get("low",   df["close"]), errors="coerce")
    df["volume"] = pd.to_numeric(df.get("volume", 0),          errors="coerce").fillna(0)
    df["amount"] = pd.to_numeric(df.get("amount", 0),          errors="coerce").fillna(0)
    df = df.dropna(subset=["close"]).sort_values(["code", "dt"]).reset_index(drop=True)
    return df


def load_from_db(db_config_path: str, codes: list, start: str = None, end: str = None) -> pd.DataFrame:
    """从 MySQL 数据库读取 5 分钟 K 线（etf_XXXXXX_5 表）"""
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        raise ImportError("请先安装: pip install sqlalchemy pymysql")

    with open(db_config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    url = cfg.get("url") or (
        f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg.get('port',3306)}/{cfg['database']}"
    )
    engine = create_engine(url)
    frames = []
    for code in codes:
        table = f"etf_{code}_5"
        try:
            q = f"SELECT dt, open, high, low, close, volume, amount FROM `{table}`"
            filters = []
            if start:
                filters.append(f"dt >= '{start}'")
            if end:
                filters.append(f"dt <= '{end} 23:59:59'")
            if filters:
                q += " WHERE " + " AND ".join(filters)
            q += " ORDER BY dt"
            tmp = pd.read_sql(text(q), engine)
            tmp["code"] = code
            frames.append(tmp)
        except Exception as e:
            print(f"  [WARN] 读取 {table} 失败: {e}")
    if not frames:
        raise ValueError("未读取到任何 ETF 分钟数据，请先运行 fetch_etf_kline.py")
    df = pd.concat(frames, ignore_index=True)
    df["dt"] = pd.to_datetime(df["dt"])
    return df.sort_values(["code", "dt"]).reset_index(drop=True)


# ── 核心回测 ──────────────────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, params: dict) -> dict:
    buy_dip      = params["buy_dip_pct"]         # 低于VWAP的百分比（如 1.0）
    sell1        = params["sell_target_1"]        # 第一档止盈（如 1.0%）
    sell2        = params["sell_target_2"]
    sell3        = params["sell_target_3"]
    stop_loss    = params["stop_loss_pct"]
    max_trades   = params["max_trades_per_day"]
    max_conc     = params["max_concurrent"]
    trend_drop   = params["trend_drop_pct"]       # 近5日跌幅阈值
    market_drop  = params["market_crash_pct"]     # 大盘暴跌阈值
    capital      = params["capital"]

    pool_codes = list(ALL_POOL.keys())
    df_pool = df[df["code"].isin(pool_codes)].copy()
    if df_pool.empty:
        print("[WARN] 未匹配到 T0 池代码，使用全部数据")
        df_pool = df.copy()

    if params.get("start"):
        df_pool = df_pool[df_pool["dt"] >= pd.Timestamp(params["start"])]
    if params.get("end"):
        df_pool = df_pool[df_pool["dt"] <= pd.Timestamp(params["end"]) + pd.Timedelta(days=1)]

    # 提取所有交易日
    df_pool["date"] = df_pool["dt"].dt.date
    all_dates = sorted(df_pool["date"].unique())

    # 计算日线收盘价（用于趋势过滤）
    daily_close = (
        df_pool.groupby(["code", "date"])["close"]
        .last()
        .unstack("code")
        .sort_index()
    )

    run_capital = capital
    trades      = []
    daily_vals  = []

    for trade_date in all_dates:
        # ── 趋势过滤：近 5 日跌幅 > trend_drop% 的标的今日跳过 ──
        date_idx = list(daily_close.index).index(trade_date)
        blocked  = set()
        if date_idx >= 5:
            for c in daily_close.columns:
                prev5 = daily_close.iloc[date_idx - 5].get(c, np.nan)
                today_c = daily_close.iloc[date_idx].get(c, np.nan)
                if pd.notna(prev5) and pd.notna(today_c) and prev5 > 0:
                    if (today_c / prev5 - 1) * 100 < -trend_drop:
                        blocked.add(c)

        # ── 今日 5 分钟 K 线 ──
        day_df = df_pool[df_pool["date"] == trade_date].copy()

        # ── 日内状态 ──
        today_trades   = 0
        positions      = {}   # {code: {"shares": int, "buy_px": float, "stage": set()}}
        day_pnl        = 0.0
        day_val_start  = run_capital

        # 按时间顺序处理每根 K 线
        times = sorted(day_df["dt"].unique())

        # 累计 VWAP（每日重置）
        cum_amount = {}   # {code: 累计成交额}
        cum_volume = {}   # {code: 累计成交量}
        prev_close_bar = {}  # {code: 上一根K线收盘价}

        # 大盘今日开盘价（用第一根 K 线近似）
        market_open = None

        for dt in times:
            hm = dt.strftime("%H:%M")
            if hm < START_TRADE:
                continue

            bar_all = day_df[day_df["dt"] == dt]

            # ── 更新大盘情绪 ──
            mkt_bar = bar_all[bar_all["code"] == MARKET_BENCHMARK]
            mkt_paused = False
            if not mkt_bar.empty:
                mkt_px = float(mkt_bar.iloc[0]["close"])
                if market_open is None:
                    market_open = mkt_px
                if market_open and market_open > 0:
                    mkt_chg = (mkt_px / market_open - 1) * 100
                    if mkt_chg <= market_drop:
                        mkt_paused = True

            for _, bar in bar_all.iterrows():
                code = bar["code"]
                if code == MARKET_BENCHMARK:
                    continue
                if code in blocked:
                    continue
                if pd.isna(bar["close"]) or bar["close"] <= 0:
                    continue

                px   = float(bar["close"])
                vol  = float(bar["volume"]) if bar["volume"] > 0 else 0
                amt  = float(bar["amount"]) if bar["amount"] > 0 else 0

                # ── 更新 VWAP ──
                cum_amount[code] = cum_amount.get(code, 0) + amt
                cum_volume[code] = cum_volume.get(code, 0) + vol
                if cum_volume[code] > 0:
                    vwap = cum_amount[code] / cum_volume[code]
                else:
                    vwap = px
                prev_px = prev_close_bar.get(code, px)

                # ══════════════════════════════
                # 有持仓 → 止盈/止损
                # ══════════════════════════════
                if code in positions:
                    pos    = positions[code]
                    buy_px = pos["buy_px"]
                    shares = pos["shares"]
                    stages = pos["stage"]
                    pnl_pct = (px - buy_px) / buy_px * 100

                    # 强制清仓时间
                    if hm >= CLEAR_TIME:
                        day_pnl += shares * (px - buy_px)
                        trades.append({
                            "日期":      str(trade_date),
                            "买入时间":  pos["buy_time"],
                            "卖出时间":  hm,
                            "标的":      ALL_POOL.get(code, code),
                            "代码":      code,
                            "买入价":    round(buy_px, 4),
                            "卖出价":    round(px, 4),
                            "数量":      shares,
                            "收益率(%)": round(pnl_pct, 2),
                            "卖出原因":  "14:55强制清仓",
                        })
                        del positions[code]
                        continue

                    # 止损 -1%
                    if pnl_pct <= -stop_loss:
                        day_pnl += shares * (px - buy_px)
                        trades.append({
                            "日期":      str(trade_date),
                            "买入时间":  pos["buy_time"],
                            "卖出时间":  hm,
                            "标的":      ALL_POOL.get(code, code),
                            "代码":      code,
                            "买入价":    round(buy_px, 4),
                            "卖出价":    round(px, 4),
                            "数量":      shares,
                            "收益率(%)": round(pnl_pct, 2),
                            "卖出原因":  f"止损 {pnl_pct:.2f}%",
                        })
                        del positions[code]
                        continue

                    # 分档止盈
                    if pnl_pct >= sell3 and 3 not in stages:
                        day_pnl += shares * (px - buy_px)
                        stages.add(3)
                        trades.append({
                            "日期":      str(trade_date),
                            "买入时间":  pos["buy_time"],
                            "卖出时间":  hm,
                            "标的":      ALL_POOL.get(code, code),
                            "代码":      code,
                            "买入价":    round(buy_px, 4),
                            "卖出价":    round(px, 4),
                            "数量":      shares,
                            "收益率(%)": round(pnl_pct, 2),
                            "卖出原因":  f"三档全清 +{pnl_pct:.2f}%",
                        })
                        del positions[code]
                        continue

                    if pnl_pct >= sell2 and 2 not in stages:
                        sell_amt = shares // 3
                        if sell_amt >= 100:
                            day_pnl  += sell_amt * (px - buy_px)
                            pos["shares"] -= sell_amt
                            stages.add(2)

                    if pnl_pct >= sell1 and 1 not in stages:
                        sell_amt = max(shares // 3, 100)
                        if sell_amt >= 100:
                            day_pnl  += sell_amt * (px - buy_px)
                            pos["shares"] -= sell_amt
                            stages.add(1)

                # ══════════════════════════════
                # 无持仓 → 寻找买入机会
                # ══════════════════════════════
                elif (
                    today_trades < max_trades
                    and len(positions) < max_conc
                    and not mkt_paused
                    and hm < CLEAR_TIME
                ):
                    buy_thresh = vwap * (1 - buy_dip / 100)
                    # 买入条件：低于VWAP + 价格企稳（当前收盘 >= 上一根收盘）
                    if px <= buy_thresh and px >= prev_px:
                        # 计算买入金额（分配资金 / 最大每日交易次数）
                        trade_val = capital / max_trades
                        shares    = int(trade_val / px // 100) * 100
                        if shares >= 100:
                            positions[code] = {
                                "shares":   shares,
                                "buy_px":   px,
                                "buy_time": hm,
                                "stage":    set(),
                            }
                            today_trades += 1

                prev_close_bar[code] = px

        # ── 日内剩余持仓（超时未触发清仓逻辑的兜底）──
        for code, pos in list(positions.items()):
            last_bars = day_df[day_df["code"] == code]
            if not last_bars.empty:
                last_px = float(last_bars.iloc[-1]["close"])
                pnl_pct = (last_px - pos["buy_px"]) / pos["buy_px"] * 100
                day_pnl += pos["shares"] * (last_px - pos["buy_px"])
                trades.append({
                    "日期":      str(trade_date),
                    "买入时间":  pos["buy_time"],
                    "卖出时间":  "14:59",
                    "标的":      ALL_POOL.get(code, code),
                    "代码":      code,
                    "买入价":    round(pos["buy_px"], 4),
                    "卖出价":    round(last_px, 4),
                    "数量":      pos["shares"],
                    "收益率(%)": round(pnl_pct, 2),
                    "卖出原因":  "收盘兜底清仓",
                })

        # ── 更新资金 ──
        run_capital += day_pnl
        prev_val     = daily_vals[-1]["value"] if daily_vals else capital
        daily_ret    = (run_capital / prev_val - 1) * 100 if prev_val > 0 else 0.0

        daily_vals.append({
            "date":       pd.Timestamp(trade_date),
            "value":      run_capital,
            "day_trades": today_trades,
            "day_pnl":    round(day_pnl, 2),
            "daily_ret":  round(daily_ret, 4),
        })

    return {
        "daily":  pd.DataFrame(daily_vals),
        "trades": pd.DataFrame(trades),
        "params": params,
    }


# ── 绩效指标 ──────────────────────────────────────────────────────────────────

def calc_metrics(daily_df: pd.DataFrame, trades_df: pd.DataFrame) -> dict:
    if daily_df.empty:
        return {}
    v0, v1 = daily_df.iloc[0]["value"], daily_df.iloc[-1]["value"]
    total   = (v1 / v0 - 1) * 100
    days    = max((daily_df.iloc[-1]["date"] - daily_df.iloc[0]["date"]).days, 1)
    ann     = ((v1 / v0) ** (365 / days) - 1) * 100
    rets    = daily_df["daily_ret"].values / 100
    sharpe  = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0
    vals    = daily_df["value"].values
    dd_arr  = (vals - np.maximum.accumulate(vals)) / np.maximum.accumulate(vals) * 100
    max_dd  = dd_arr.min()

    if not trades_df.empty:
        wins     = trades_df[trades_df["收益率(%)"] > 0]
        losses   = trades_df[trades_df["收益率(%)"] <= 0]
        win_rate = len(wins) / len(trades_df) * 100
        avg_win  = wins["收益率(%)"].mean()   if not wins.empty   else 0
        avg_loss = losses["收益率(%)"].mean() if not losses.empty else 0
        avg_trades_per_day = len(trades_df) / max(len(daily_df), 1)
    else:
        win_rate = avg_win = avg_loss = avg_trades_per_day = 0

    return {
        "总收益(%)":        round(total,   2),
        "年化收益(%)":      round(ann,     2),
        "夏普比率":         round(sharpe,  3),
        "最大回撤(%)":      round(max_dd,  2),
        "总交易次数":       len(trades_df),
        "胜率(%)":          round(win_rate, 1),
        "平均盈利(%)":      round(avg_win,  2),
        "平均亏损(%)":      round(avg_loss, 2),
        "日均交易次数":     round(avg_trades_per_day, 1),
        "回测日历天数":     days,
    }


def calc_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    df = daily_df.copy()
    df["month"] = df["date"].dt.to_period("M")
    rows = []
    for mo, g in df.groupby("month"):
        v0, v1 = g.iloc[0]["value"], g.iloc[-1]["value"]
        rows.append({"月份": str(mo), "月收益(%)": round((v1 / v0 - 1) * 100, 2)})
    return pd.DataFrame(rows)


# ── 报告生成 ──────────────────────────────────────────────────────────────────

def _pct_bar(pct: float, width: int = 20) -> str:
    filled = min(abs(int(pct / 0.3)), width)
    if pct >= 0:
        return "+" + "█" * filled + f" {pct:+.2f}%"
    return "-" + "▓" * filled + f" {pct:+.2f}%"


def generate_report(result: dict, metrics: dict, monthly: pd.DataFrame,
                    init_capital: float) -> str:
    p   = result["params"]
    dd  = result["daily"]
    td  = result["trades"]
    s   = dd["date"].min().strftime("%Y-%m-%d")
    e   = dd["date"].max().strftime("%Y-%m-%d")
    fv  = dd.iloc[-1]["value"]

    lines = [
        "# ETF T+0 做T策略 — 回测报告",
        "",
        "```",
        f"  策略名称   ETF T+0 日内做T（VWAP低吸 + 分档止盈）",
        f"  回测区间   {s}  →  {e}    ({metrics.get('回测日历天数','-')} 天)",
        f"  初始资金   {init_capital:,.0f} 元",
        f"  期末资金   {fv:,.2f} 元",
        "```",
        "",
        "---",
        "",
        "## ① 核心绩效",
        "",
        "```",
        f"  总        收  益    {metrics.get('总收益(%)','-'):>8}%",
        f"  年  化  收  益    {metrics.get('年化收益(%)','-'):>8}%",
        f"  最  大  回  撤    {metrics.get('最大回撤(%)','-'):>8}%",
        f"  夏  普  比  率    {metrics.get('夏普比率','-'):>8}",
        f"  ────────────────────────────────",
        f"  总  交  易  次    {metrics.get('总交易次数','-'):>8} 次",
        f"  日  均  交  易    {metrics.get('日均交易次数','-'):>8} 次/日",
        f"  胜          率    {metrics.get('胜率(%)','-'):>8}%",
        f"  平  均  盈  利    {metrics.get('平均盈利(%)','-'):>8}%",
        f"  平  均  亏  损    {metrics.get('平均亏损(%)','-'):>8}%",
        "```",
        "",
        "---",
        "",
        "## ② 策略参数",
        "",
        f"| 参数 | 值 |",
        f"|------|-----|",
        f"| 标的池 | {len(ALL_POOL)} 只跨境 ETF |",
        f"| 买入条件 | 价格低于 VWAP {p['buy_dip_pct']}% 且企稳 |",
        f"| 止盈一档 | 买入价 +{p['sell_target_1']}% 卖 1/3 |",
        f"| 止盈二档 | 买入价 +{p['sell_target_2']}% 再卖 1/3 |",
        f"| 止盈三档 | 买入价 +{p['sell_target_3']}% 全清 |",
        f"| 止损 | 买入价 -{p['stop_loss_pct']}% 立即全清 |",
        f"| 强制清仓 | {CLEAR_TIME} 清仓，不留隔夜仓 |",
        f"| 趋势过滤 | 近 5 日跌幅 > {p['trend_drop_pct']}% 的标的不参与 |",
        f"| 每日上限 | {p['max_trades_per_day']} 次 / 同时 {p['max_concurrent']} 只 |",
        "",
        "---",
        "",
        "## ③ 月度收益",
        "",
    ]

    if not monthly.empty:
        lines.append("```")
        pos_cnt = (monthly["月收益(%)"] > 0).sum()
        neg_cnt = (monthly["月收益(%)"] < 0).sum()
        for _, row in monthly.iterrows():
            lines.append(f"  {row['月份']}   {_pct_bar(row['月收益(%)'])}")
        lines += [
            "```",
            "",
            f"> 正收益月份 **{pos_cnt}** / 共 {len(monthly)} 月  ·  "
            f"负收益月份 {neg_cnt}  ·  "
            f"月均 {monthly['月收益(%)'].mean():.2f}%  ·  "
            f"最大月亏 {monthly['月收益(%)'].min():.2f}%",
        ]
    lines += ["", "---", ""]

    # ── 每日盈亏（最近 40 日）──
    lines += ["## ④ 近期每日盈亏（最近 40 交易日）", "", "```"]
    recent = dd.tail(40)
    v_base = dd.iloc[0]["value"]
    for _, row in recent.iterrows():
        pct   = (row["value"] / v_base - 1) * 100
        dpnl  = row.get("day_pnl", 0)
        ntrd  = int(row.get("day_trades", 0))
        sign  = "+" if pct >= 0 else "-"
        bar   = "█" * max(0, int(abs(pct) / 1))
        lines.append(
            f"  {row['date'].strftime('%Y-%m-%d')} | "
            f"今日P&L: {dpnl:+8.1f}元 | 交易{ntrd}次 | "
            f"累计{sign}{abs(pct):.1f}% {bar}"
        )
    lines += ["```", "", "---", ""]

    # ── 交易明细（最近 100 笔）──
    lines += ["## ⑤ 交易明细（最近 100 笔）", ""]
    if not td.empty:
        recent_td = td.tail(100)
        lines += [
            "| 日期 | 买入 | 卖出 | 标的 | 买入价 | 卖出价 | 数量 | 收益率 | 原因 |",
            "|------|------|------|------|--------|--------|------|--------|------|",
        ]
        for _, row in recent_td.iterrows():
            flag = "✓" if row["收益率(%)"] > 0 else "✗"
            lines.append(
                f"| {row['日期']} | {row['买入时间']} | {row['卖出时间']} "
                f"| {row['标的']} | {row['买入价']} | {row['卖出价']} "
                f"| {row['数量']} | {flag} {row['收益率(%)']}% | {row['卖出原因']} |"
            )
    else:
        lines.append("> 回测期间无交易（可能数据中无分钟数据或参数过严）")
    lines.append("")

    lines += [
        "---",
        "",
        "> **回测说明**",
        "> - 使用 5 分钟 K 线，VWAP = 当日累计成交额 / 累计成交量",
        "> - 量能分析（量比、内外盘）已简化，实盘会有额外过滤",
        "> - 大盘情绪过滤使用 510300（沪深300ETF）近似",
        "> - 成交按当根 K 线收盘价成交（实际有滑点 0.2%）",
        "> - 不含手续费；T+0 跨境 ETF 无印花税，手续费约 0.01%-0.03%",
        "",
    ]

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="ETF T+0 做T策略回测")
    p.add_argument("--source",           choices=["csv", "db"], default="csv",
                   help="数据来源: csv 或 db (MySQL)")
    p.add_argument("--data",             default=None,
                   help="CSV 文件路径（--source csv 时必填）")
    p.add_argument("--db-config",        default="db_config.json",
                   help="数据库配置 JSON（--source db 时使用）")
    p.add_argument("--config",           default=None,    help="策略参数 JSON 文件")
    p.add_argument("--start",            default=None,    help="回测开始日期 YYYY-MM-DD")
    p.add_argument("--end",              default=None,    help="回测结束日期 YYYY-MM-DD")
    p.add_argument("--buy-dip",          type=float,      help="低于VWAP买入阈值 % (默认 1.0)")
    p.add_argument("--sell1",            type=float,      help="第一档止盈 % (默认 1.0)")
    p.add_argument("--sell2",            type=float,      help="第二档止盈 % (默认 1.5)")
    p.add_argument("--sell3",            type=float,      help="第三档止盈 % (默认 2.0)")
    p.add_argument("--stop-loss",        type=float,      help="止损 % (默认 1.0)")
    p.add_argument("--capital",          type=float,      help="初始资金 (默认 100000)")
    p.add_argument("--output",           default="validator_output/backtest_t0",
                   help="输出目录")
    return p.parse_args()


def main():
    args = parse_args()
    cfg  = load_config(args.config)

    init_capital = args.capital or cfg.get("策略分配资金(元)", 100000)

    params = {
        "buy_dip_pct":        args.buy_dip    or cfg.get("买入低于均线(%)",   1.0),
        "sell_target_1":      args.sell1      or cfg.get("第一档卖出(%)",     1.0),
        "sell_target_2":      args.sell2      or cfg.get("第二档卖出(%)",     1.5),
        "sell_target_3":      args.sell3      or cfg.get("卖出止盈(%)",       2.0),
        "stop_loss_pct":      args.stop_loss  or cfg.get("止损(%)",           1.0),
        "max_trades_per_day": cfg.get("每日最大交易次数",  3),
        "max_concurrent":     cfg.get("同时最大持仓ETF数", 2),
        "trend_drop_pct":     cfg.get("趋势判断跌幅阈值(%)", 3.0),
        "market_crash_pct":   cfg.get("大盘放量暴跌暂停阈值(%)", -1.5),
        "capital":            init_capital,
        "start":              args.start,
        "end":                args.end,
    }

    print("[1/5] 读取分钟数据...")
    if args.source == "db":
        df = load_from_db(args.db_config, list(ALL_POOL.keys()),
                          start=args.start, end=args.end)
    else:
        if not args.data:
            print("[ERROR] --source csv 时需要提供 --data 参数")
            return 1
        df = load_from_csv(args.data)

    print(f"      数据: {df['dt'].min()} ~ {df['dt'].max()}"
          f"  标的数: {df['code'].nunique()}  共 {len(df):,} 条")

    print("[2/5] 运行回测...")
    result = run_backtest(df, params)

    print("[3/5] 计算绩效指标")
    metrics = calc_metrics(result["daily"], result["trades"])
    monthly = calc_monthly(result["daily"])

    print("\n" + "=" * 52)
    print("  ETF T+0 策略回测结果")
    print("=" * 52)
    for k, v in metrics.items():
        print(f"  {k:14s}: {v}")
    print("=" * 52 + "\n")

    print(f"[4/5] 生成报告: {args.output}")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    report = generate_report(result, metrics, monthly, init_capital)
    (out / "report.md").write_text(report, encoding="utf-8")
    result["trades"].to_csv(out / "trades.csv",  index=False, encoding="utf-8-sig")
    result["daily"].to_csv(out  / "daily.csv",   index=False, encoding="utf-8-sig")
    monthly.to_csv(out          / "monthly.csv", index=False, encoding="utf-8-sig")

    print(f"[5/5] 完成 → {out}/report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
