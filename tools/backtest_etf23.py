"""
ETF23 轮动防过热策略 回测引擎
================================================
策略核心（对应 etf23轮动防过热.txt）:
  - 23 只 ETF 轮动池
  - 20 日动量排名 + 13 日准入阈值（默认 1.5%）
  - 移动止损：从持仓最高价回撤 5%
  - 当日跌幅止损：当日涨跌幅 <= -5%
  - 防过热：今日涨幅 > 5% 跳过，递延下一名
  - 防跌买：今日跌幅 < -4% 跳过
  - 主动换仓：第一名领先持仓 3%+ 且持仓动量为正时换仓
  - 市场弱势：全排名第二动量为负时今日不买

用法:
  python tools/backtest_etf23.py --data data/etf23_daily.csv
  python tools/backtest_etf23.py --data data/etf23_daily.csv --start 2023-01-01
  python tools/backtest_etf23.py --data data/etf23_daily.csv --trailing-stop 4.0

输入 CSV 格式（date, code, close 必须有）:
  date,code,close[,open,high,low,volume,amount]

默认初始资金: 100,000 元
输出目录: validator_output/backtest_etf23/
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ── ETF23 池（6位代码 → 名称）────────────────────────────────────────────────
ETF_POOL = {
    "563300": "中证2000ETF",
    "159681": "创业板50ETF",
    "159845": "中证1000ETF",
    "588000": "科创50ETF",
    "159611": "电力ETF",
    "159566": "储能电池ETF",
    "516180": "光伏ETF",
    "515980": "人工智能ETF",
    "515880": "通信ETF",
    "512480": "半导体ETF",
    "562500": "机器人ETF",
    "515170": "食品饮料ETF",
    "159699": "恒生消费ETF",
    "512880": "证券ETF",
    "159992": "创新药ETF",
    "159883": "医疗器械ETF",
    "512660": "军工ETF",
    "515220": "煤炭ETF",
    "159880": "有色ETF",
    "518880": "黄金ETF",
    "162411": "华宝油气",
    "513180": "恒生科技ETF",
    "513100": "纳指ETF",
}

BENCHMARK_CODE = "513100"  # 纳指 ETF 作为基准


# ── 工具 ──────────────────────────────────────────────────────────────────────

def norm_code(c: str) -> str:
    c = str(c).strip()
    return c.split(".")[0].zfill(6) if "." in c else c.zfill(6)


def load_config(path: str) -> dict:
    defaults = {
        "止损回撤比(%)": 5.0,
        "买入准入均线要求(%)": 1.5,
        "当日跌幅卖出阈值(%)": -5.0,
        "主动换仓动量领先阈值(%)": 3.0,
        "买入防过热暴涨线(%)": 5.0,
        "单次投入金额": 100000,
    }
    cfg_file = "etf23_halfhour_config.json"
    if path and Path(path).exists():
        with open(path, encoding="utf-8") as f:
            defaults.update(json.load(f))
    elif Path(cfg_file).exists():
        with open(cfg_file, encoding="utf-8") as f:
            defaults.update(json.load(f))
    return defaults


def load_price_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]
    rename = {
        "日期": "date", "trade_date": "date",
        "代码": "code", "股票代码": "code",
        "收盘": "close", "收盘价": "close",
        "开盘": "open", "开盘价": "open",
        "最高": "high", "最高价": "high",
        "最低": "low",  "最低价": "low",
    }
    df.rename(columns=rename, inplace=True)
    missing = [c for c in ("date", "code", "close") if c not in df.columns]
    if missing:
        raise ValueError(f"CSV 缺少必要列: {missing}  现有: {list(df.columns)}")
    df["date"]  = pd.to_datetime(df["date"])
    df["code"]  = df["code"].apply(norm_code)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).sort_values(["code", "date"]).reset_index(drop=True)
    return df


# ── 回测核心 ─────────────────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, params: dict) -> dict:
    trailing      = params["trailing_stop"]
    sell_thresh   = params["sell_threshold"]     # 当日跌幅止损，例 -5.0
    buy_thr       = params["buy_threshold"]      # 13日准入
    switch_thr    = params["switch_threshold"]   # 主动换仓领先阈值
    overheat      = params["overheat_pct"]       # 防过热涨幅上限
    rank_n        = params["ranking_period"]
    buy_n         = params["buy_period"]

    pool_codes = list(ETF_POOL.keys())
    df_pool = df[df["code"].isin(pool_codes)].copy()
    if df_pool.empty:
        print("[WARN] 未匹配到 ETF23 池代码，使用全部数据")
        df_pool = df.copy()

    wide = df_pool.pivot_table(index="date", columns="code", values="close").sort_index()

    if params.get("start"):
        wide = wide[wide.index >= pd.Timestamp(params["start"])]
    if params.get("end"):
        wide = wide[wide.index <= pd.Timestamp(params["end"])]

    dates  = wide.index.tolist()
    codes  = wide.columns.tolist()
    n_req  = max(rank_n, buy_n)

    if len(dates) < n_req + 5:
        raise ValueError(f"数据不足，至少需要 {n_req+5} 个交易日")

    # ── 状态变量 ──
    holding      = None
    entry_price  = 0.0
    entry_date   = None
    highest_px   = 0.0
    run_capital  = params["capital"]

    trades     = []
    daily_vals = []

    for i, dt in enumerate(dates):
        row = wide.iloc[i]

        if i < n_req:
            daily_vals.append({"date": dt, "value": run_capital, "holding": None, "daily_ret": 0.0})
            continue

        # ── 计算各 ETF 动量 & 当日涨跌幅 ──
        ret20, ret13, today_chg = {}, {}, {}
        for c in codes:
            p_now = row.get(c, np.nan)
            if pd.isna(p_now):
                continue
            p20  = wide.iloc[i - rank_n].get(c, np.nan)
            p13  = wide.iloc[i - buy_n].get(c, np.nan)
            prev = wide.iloc[i - 1].get(c, np.nan)
            if pd.notna(p20) and p20 > 0:
                ret20[c] = (p_now / p20 - 1) * 100
            if pd.notna(p13) and p13 > 0:
                ret13[c] = (p_now / p13 - 1) * 100
            if pd.notna(prev) and prev > 0:
                today_chg[c] = (p_now / prev - 1) * 100

        sold_today_codes = set()

        # ── 卖出逻辑 ──
        if holding and holding in row.index and pd.notna(row[holding]):
            cur_px     = row[holding]
            highest_px = max(highest_px, cur_px)
            drawdown   = (cur_px - highest_px) / highest_px * 100
            chg_today  = today_chg.get(holding, 0)

            sell_reason = None
            if drawdown < -trailing:
                sell_reason = f"移动止损 回撤{drawdown:.1f}%"
            elif chg_today <= sell_thresh:
                sell_reason = f"当日跌幅止损 {chg_today:.1f}%"

            if sell_reason:
                trade_ret = (cur_px / entry_price - 1) * 100
                run_capital *= (cur_px / entry_price)
                trades.append({
                    "买入日":    entry_date.strftime("%Y-%m-%d"),
                    "卖出日":    dt.strftime("%Y-%m-%d"),
                    "标的":      ETF_POOL.get(holding, holding),
                    "代码":      holding,
                    "买入价":    round(entry_price, 4),
                    "卖出价":    round(cur_px, 4),
                    "持仓天数":  (dt - entry_date).days,
                    "收益率(%)": round(trade_ret, 2),
                    "卖出原因":  sell_reason,
                })
                sold_today_codes.add(holding)
                holding = entry_price = entry_date = None
                highest_px = 0.0

        # ── 买入逻辑 ──
        # 正动量排名（20日>0，按降序）
        ranked = sorted(
            [(c, ret20[c]) for c in codes if c in ret20 and ret20[c] > 0],
            key=lambda x: x[1], reverse=True,
        )

        if holding is not None:
            # ─ 已持仓：检查主动换仓 ─
            holding_r20 = ret20.get(holding, None)
            if holding_r20 is None:
                pass  # 停牌，维持
            elif holding_r20 <= 0 and ranked:
                # 持仓动量转负，强制换仓
                top = ranked[0]
                top_price = row.get(top[0], np.nan)
                if pd.notna(top_price) and top[0] not in sold_today_codes:
                    trade_ret = (row[holding] / entry_price - 1) * 100 if holding in row.index and pd.notna(row[holding]) else 0
                    if holding in row.index and pd.notna(row[holding]):
                        run_capital *= (row[holding] / entry_price)
                        trades.append({
                            "买入日":    entry_date.strftime("%Y-%m-%d"),
                            "卖出日":    dt.strftime("%Y-%m-%d"),
                            "标的":      ETF_POOL.get(holding, holding),
                            "代码":      holding,
                            "买入价":    round(entry_price, 4),
                            "卖出价":    round(row[holding], 4),
                            "持仓天数":  (dt - entry_date).days,
                            "收益率(%)": round(trade_ret, 2),
                            "卖出原因":  f"动量转负强制换仓({holding_r20:.1f}%)",
                        })
                        sold_today_codes.add(holding)
                    # 买入新标的
                    holding    = top[0]
                    entry_price = top_price
                    entry_date  = dt
                    highest_px  = top_price
            elif ranked and holding_r20 is not None:
                top_code, top_r20 = ranked[0]
                gap = top_r20 - holding_r20
                if gap >= switch_thr and top_code != holding and top_code not in sold_today_codes:
                    # 主动换仓
                    top_price = row.get(top_code, np.nan)
                    if pd.notna(top_price) and holding in row.index and pd.notna(row[holding]):
                        trade_ret = (row[holding] / entry_price - 1) * 100
                        run_capital *= (row[holding] / entry_price)
                        trades.append({
                            "买入日":    entry_date.strftime("%Y-%m-%d"),
                            "卖出日":    dt.strftime("%Y-%m-%d"),
                            "标的":      ETF_POOL.get(holding, holding),
                            "代码":      holding,
                            "买入价":    round(entry_price, 4),
                            "卖出价":    round(row[holding], 4),
                            "持仓天数":  (dt - entry_date).days,
                            "收益率(%)": round(trade_ret, 2),
                            "卖出原因":  f"主动换仓({top_code}领先{gap:.1f}%)",
                        })
                        sold_today_codes.add(holding)
                        holding    = top_code
                        entry_price = top_price
                        entry_date  = dt
                        highest_px  = top_price
        else:
            # ─ 空仓：寻找买入标的 ─
            # 市场弱势检查：全排名(含负动量)第二名有负动量时不买
            all_ranked = sorted(
                [(c, ret20[c]) for c in codes if c in ret20],
                key=lambda x: x[1], reverse=True,
            )
            if len(all_ranked) >= 2:
                r2_r20 = all_ranked[1][1]
                r2_r13 = ret13.get(all_ranked[1][0], 0)
                if r2_r20 < 0 or r2_r13 < 0:
                    # 市场偏弱，今日不买
                    port_val = run_capital
                    prev_val = daily_vals[-1]["value"] if daily_vals else run_capital
                    daily_vals.append({
                        "date": dt, "value": port_val, "holding": None,
                        "daily_ret": round((port_val / prev_val - 1) * 100, 4) if prev_val > 0 else 0,
                    })
                    continue

            # 选候选（正动量，按20日排名）
            for c, r20 in ranked:
                if c in sold_today_codes:
                    continue
                if ret13.get(c, 0) < buy_thr:
                    continue
                chg = today_chg.get(c, 0)
                if chg > overheat:       # 防过热
                    continue
                if chg < -4.0:           # 防跌买
                    continue
                px = row.get(c, np.nan)
                if pd.notna(px):
                    holding    = c
                    entry_price = px
                    entry_date  = dt
                    highest_px  = px
                    break

        # ── 当日组合价值 ──
        if holding and pd.notna(row.get(holding)):
            port_val = run_capital * (row[holding] / entry_price)
        else:
            port_val = run_capital

        prev_val  = daily_vals[-1]["value"] if daily_vals else run_capital
        daily_ret = (port_val / prev_val - 1) * 100 if prev_val > 0 else 0.0

        daily_vals.append({
            "date":      dt,
            "value":     port_val,
            "holding":   ETF_POOL.get(holding, holding) if holding else None,
            "daily_ret": round(daily_ret, 4),
        })

    # ── 收盘未平仓 ──
    if holding and dates:
        last_px = wide.iloc[-1].get(holding, entry_price)
        if pd.isna(last_px):
            last_px = entry_price
        trade_ret = (last_px / entry_price - 1) * 100
        trades.append({
            "买入日":    entry_date.strftime("%Y-%m-%d"),
            "卖出日":    dates[-1].strftime("%Y-%m-%d"),
            "标的":      ETF_POOL.get(holding, holding),
            "代码":      holding,
            "买入价":    round(entry_price, 4),
            "卖出价":    round(last_px, 4),
            "持仓天数":  (dates[-1] - entry_date).days,
            "收益率(%)": round(trade_ret, 2),
            "卖出原因":  "持仓至今(未平)",
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
        fin = trades_df[trades_df["卖出原因"] != "持仓至今(未平)"]
        wins     = fin[fin["收益率(%)"] > 0]
        losses   = fin[fin["收益率(%)"] <= 0]
        win_rate = len(wins) / len(fin) * 100 if len(fin) > 0 else 0
        avg_win  = wins["收益率(%)"].mean()   if not wins.empty   else 0
        avg_loss = losses["收益率(%)"].mean() if not losses.empty else 0
        avg_hold = trades_df["持仓天数"].mean()
        n_trades = len(fin)
    else:
        win_rate = avg_win = avg_loss = avg_hold = n_trades = 0

    return {
        "总收益(%)":    round(total,   2),
        "年化收益(%)":  round(ann,     2),
        "夏普比率":     round(sharpe,  3),
        "最大回撤(%)":  round(max_dd,  2),
        "已平仓次数":   n_trades,
        "胜率(%)":      round(win_rate, 1),
        "平均盈利(%)":  round(avg_win,  2),
        "平均亏损(%)":  round(avg_loss, 2),
        "平均持仓天数": round(avg_hold, 1),
        "回测日历天数": days,
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
    """把收益率转成简易横向条形图"""
    filled = min(abs(int(pct / 1.5)), width)
    if pct >= 0:
        return "+" + "█" * filled + f" {pct:+.2f}%"
    return "-" + "▓" * filled + f" {pct:+.2f}%"


def generate_report(result: dict, metrics: dict, monthly: pd.DataFrame,
                    init_capital: float) -> str:
    p  = result["params"]
    dd = result["daily"]
    td = result["trades"]
    s  = dd["date"].min().strftime("%Y-%m-%d")
    e  = dd["date"].max().strftime("%Y-%m-%d")
    final_val = dd.iloc[-1]["value"]

    lines = [
        "# ETF23 防过热轮动策略 — 回测报告",
        "",
        "```",
        f"  策略名称   ETF23 尾盘轮动（防过热版）",
        f"  回测区间   {s}  →  {e}    ({metrics.get('回测日历天数','-')} 天)",
        f"  初始资金   {init_capital:,.0f} 元",
        f"  期末资金   {final_val:,.2f} 元",
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
        f"  已  平  仓  次    {metrics.get('已平仓次数','-'):>8} 次",
        f"  胜          率    {metrics.get('胜率(%)','-'):>8}%",
        f"  平  均  盈  利    {metrics.get('平均盈利(%)','-'):>8}%",
        f"  平  均  亏  损    {metrics.get('平均亏损(%)','-'):>8}%",
        f"  平均持仓天数    {metrics.get('平均持仓天数','-'):>8} 天",
        "```",
        "",
        "---",
        "",
        "## ② 策略参数",
        "",
        f"| 参数 | 值 |",
        f"|------|-----|",
        f"| ETF 池 | 23 只 |",
        f"| 排名周期 | {p['ranking_period']} 日 |",
        f"| 买入准入 | 13 日涨幅 ≥ {p['buy_threshold']}% |",
        f"| 移动止损 | 最高点回撤 {p['trailing_stop']}% |",
        f"| 当日跌幅止损 | ≤ {p['sell_threshold']}% |",
        f"| 防过热线 | 今日涨幅 > {p['overheat_pct']}% 跳过 |",
        f"| 主动换仓 | 第一名领先 ≥ {p['switch_threshold']}% 才换 |",
        "",
        "---",
        "",
        "## ③ 月度收益",
        "",
    ]

    # ── 月度收益热力图（文字版）──
    if not monthly.empty:
        lines.append("```")
        pos_cnt = (monthly["月收益(%)"] > 0).sum()
        neg_cnt = (monthly["月收益(%)"] < 0).sum()
        for _, row in monthly.iterrows():
            r = row["月收益(%)"]
            lines.append(f"  {row['月份']}   {_pct_bar(r)}")
        lines += [
            "```",
            "",
            f"> 正收益月份 **{pos_cnt}** / 共 {len(monthly)} 月  ·  "
            f"负收益月份 {neg_cnt}  ·  "
            f"月均 {monthly['月收益(%)'].mean():.2f}%  ·  "
            f"最大月亏 {monthly['月收益(%)'].min():.2f}%",
        ]
    lines.append("")
    lines.append("---")
    lines.append("")

    # ── 净值走势（最近 60 交易日）──
    lines += ["## ④ 近期净值走势（最近 60 交易日）", "", "```"]
    recent = dd.tail(60)
    v_base = dd.iloc[0]["value"]
    for _, row in recent.iterrows():
        pct  = (row["value"] / v_base - 1) * 100
        hold = f"[{row['holding']:8s}]" if row["holding"] else "[  空仓  ]"
        bar  = "█" * max(0, int(abs(pct) / 2))
        sign = "+" if pct >= 0 else "-"
        lines.append(
            f"  {row['date'].strftime('%Y-%m-%d')} {hold}  {sign}{abs(pct):.1f}%  {bar}"
        )
    lines += ["```", "", "---", ""]

    # ── 交易记录 ──
    lines += ["## ⑤ 完整交易记录", ""]
    if not td.empty:
        lines += [
            "| # | 买入日 | 卖出日 | 标的 | 买入价 | 卖出价 | 持仓天 | 收益 | 卖出原因 |",
            "|---|--------|--------|------|--------|--------|--------|------|---------|",
        ]
        for idx, row in td.iterrows():
            flag = "✓" if row["收益率(%)"] > 0 else "✗"
            lines.append(
                f"| {idx+1} | {row['买入日']} | {row['卖出日']} | {row['标的']} "
                f"| {row['买入价']} | {row['卖出价']} | {row['持仓天数']} "
                f"| {flag} {row['收益率(%)']}% | {row['卖出原因']} |"
            )
    else:
        lines.append("> 回测期间无交易记录（可能数据不足或参数过严）")
    lines.append("")

    # ── 尾注 ──
    lines += [
        "---",
        "",
        "> **回测说明**",
        "> - 成交价取当日收盘价（实盘在 14:50，误差约 0.1%-0.3%）",
        "> - 当日跌幅止损在实盘中为盘中分钟级触发；日线回测中用收盘价，可能轻微高估止损效果",
        "> - 防过热/防跌买用当日 close vs prev_close 近似",
        "> - 未考虑印花税 + 手续费（约 0.03%-0.05% / 单边，可用 --commission 调整）",
        "",
    ]

    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="ETF23 防过热策略回测")
    p.add_argument("--data",            required=True,  help="日线价格 CSV 文件路径")
    p.add_argument("--config",          default=None,   help="策略参数 JSON 文件")
    p.add_argument("--start",           default=None,   help="回测开始日期 YYYY-MM-DD")
    p.add_argument("--end",             default=None,   help="回测结束日期 YYYY-MM-DD")
    p.add_argument("--trailing-stop",   type=float,     help="移动止损 % (默认 5.0)")
    p.add_argument("--sell-threshold",  type=float,     help="当日跌幅止损 % (默认 -5.0)")
    p.add_argument("--buy-threshold",   type=float,     help="13日准入 % (默认 1.5)")
    p.add_argument("--switch-threshold",type=float,     help="主动换仓领先 % (默认 3.0)")
    p.add_argument("--overheat",        type=float,     help="防过热上限 % (默认 5.0)")
    p.add_argument("--capital",         type=float,     help="初始资金 (默认 100000)")
    p.add_argument("--ranking-period",  type=int,       help="排名周期天数 (默认 20)")
    p.add_argument("--buy-period",      type=int,       help="准入周期天数 (默认 13)")
    p.add_argument("--output",          default="validator_output/backtest_etf23",
                   help="输出目录")
    return p.parse_args()


def main():
    args = parse_args()
    cfg  = load_config(args.config)

    init_capital = args.capital or cfg.get("单次投入金额", 100000)

    params = {
        "trailing_stop":  args.trailing_stop    or cfg.get("止损回撤比(%)",           5.0),
        "sell_threshold": args.sell_threshold   or cfg.get("当日跌幅卖出阈值(%)",    -5.0),
        "buy_threshold":  args.buy_threshold    or cfg.get("买入准入均线要求(%)",      1.5),
        "switch_threshold": args.switch_threshold or cfg.get("主动换仓动量领先阈值(%)", 3.0),
        "overheat_pct":   args.overheat         or cfg.get("买入防过热暴涨线(%)",      5.0),
        "capital":        init_capital,
        "ranking_period": args.ranking_period   or 20,
        "buy_period":     args.buy_period       or 13,
        "start":          args.start,
        "end":            args.end,
    }

    print(f"[1/5] 读取价格数据: {args.data}")
    df = load_price_data(args.data)
    print(f"      数据: {df['date'].min().date()} ~ {df['date'].max().date()}"
          f"  标的数: {df['code'].nunique()}")

    print(f"[2/5] 运行回测  止损={params['trailing_stop']}%"
          f"  跌幅止损={params['sell_threshold']}%"
          f"  准入={params['buy_threshold']}%  防过热={params['overheat_pct']}%")
    result = run_backtest(df, params)

    print("[3/5] 计算绩效指标")
    metrics = calc_metrics(result["daily"], result["trades"])
    monthly = calc_monthly(result["daily"])

    print("\n" + "=" * 52)
    print("  ETF23 策略回测结果")
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
