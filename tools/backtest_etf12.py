"""
ETF12 策略回测引擎
================================================
用法:
  python tools/backtest_etf12.py --data <CSV路径>
  python tools/backtest_etf12.py --data data/etf_daily.csv --start 2023-01-01
  python tools/backtest_etf12.py --data data/etf_daily.csv --trailing-stop 3.0 --buy-threshold 1.0

输入 CSV 必须包含列: date, code, close
  code 格式: 518880.SS 或 518880（6位代码均可）
  date 格式: 2024-01-02

输出目录: validator_output/backtest_etf12/
  report.md   —— 汇总报告（总收益、月收益、交易记录）
  trades.csv  —— 逐笔交易明细
  monthly.csv —— 月度收益表
  daily.csv   —— 每日净值
"""

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ── ETF12 池（6位代码） ────────────────────────────────────────────────────────
ETF_POOL = {
    "563300": "中证2000ETF",
    "159611": "电力ETF",
    "159681": "算力ETF",
    "588000": "科创50ETF",
    "513100": "纳指ETF",
    "513180": "恒生科技ETF",
    "515980": "人工智能ETF",
    "518880": "黄金ETF",
    "162411": "华宝油气",
    "512890": "红利低波ETF",
    "515880": "通信ETF",
    "159992": "创新药ETF",
}


# ── 工具函数 ───────────────────────────────────────────────────────────────────

def norm_code(c: str) -> str:
    """统一转成6位纯数字代码。"""
    c = str(c).strip()
    return c.split(".")[0].zfill(6) if "." in c else c.zfill(6)


def load_config(path: str) -> dict:
    defaults = {
        "移动止损回撤比(%)": 4.0,
        "买入准入均线要求(%)": 1.5,
        "持仓死亡被动割肉线(%)": -2.5,
        "单次投入金额": 150000,
    }
    if path and Path(path).exists():
        with open(path, encoding="utf-8") as f:
            defaults.update(json.load(f))
    return defaults


def load_price_data(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path, encoding="utf-8-sig")
    df.columns = [c.strip() for c in df.columns]

    # 兼容多种列名
    rename = {
        "日期": "date", "trade_date": "date",
        "代码": "code", "股票代码": "code", "etf代码": "code",
        "收盘": "close", "收盘价": "close",
        "开盘": "open", "开盘价": "open",
        "最高": "high", "最高价": "high",
        "最低": "low",  "最低价": "low",
    }
    df.rename(columns=rename, inplace=True)

    missing = [c for c in ("date", "code", "close") if c not in df.columns]
    if missing:
        raise ValueError(f"CSV 缺少必要列: {missing}  现有列: {list(df.columns)}")

    df["date"]  = pd.to_datetime(df["date"])
    df["code"]  = df["code"].apply(norm_code)
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).sort_values(["code", "date"]).reset_index(drop=True)
    return df


# ── 核心回测 ───────────────────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, params: dict) -> dict:
    trailing  = params["trailing_stop"]
    buy_thr   = params["buy_threshold"]
    sell_low  = params["sell_low"]
    capital   = params["capital"]
    rank_n    = params["ranking_period"]
    buy_n     = params["buy_period"]

    # 只保留 ETF 池里的标的
    pool_codes = list(ETF_POOL.keys())
    df_pool = df[df["code"].isin(pool_codes)].copy()
    if df_pool.empty:
        print("[WARN] 未在数据中找到 ETF 池代码，使用全部数据")
        df_pool = df.copy()

    # 宽表：行=日期，列=code
    wide = df_pool.pivot_table(index="date", columns="code", values="close").sort_index()

    # 日期范围裁剪
    if params.get("start"):
        wide = wide[wide.index >= pd.Timestamp(params["start"])]
    if params.get("end"):
        wide = wide[wide.index <= pd.Timestamp(params["end"])]

    dates = wide.index.tolist()
    codes = wide.columns.tolist()
    n_dates = len(dates)

    if n_dates < max(rank_n, buy_n) + 5:
        raise ValueError(f"数据不足，需至少 {max(rank_n, buy_n)+5} 个交易日，当前只有 {n_dates} 天")

    # ── 状态变量 ──
    holding      = None   # 当前持仓 code
    entry_price  = 0.0
    entry_date   = None
    highest_px   = 0.0
    run_capital  = capital  # 随交易盈亏滚动更新

    trades      = []
    daily_vals  = []

    for i, dt in enumerate(dates):
        row = wide.iloc[i]

        # ── 预热期：不执行任何交易 ──
        if i < max(rank_n, buy_n):
            daily_vals.append({"date": dt, "value": run_capital, "holding": None, "daily_ret": 0.0})
            continue

        # ── 计算各 ETF 的动量 ──
        ret20, ret13 = {}, {}
        for c in codes:
            p_now = row.get(c, np.nan)
            if pd.isna(p_now):
                continue
            p20 = wide.iloc[i - rank_n].get(c, np.nan)
            p13 = wide.iloc[i - buy_n].get(c, np.nan)
            if pd.notna(p20) and p20 > 0:
                ret20[c] = (p_now / p20 - 1) * 100
            if pd.notna(p13) and p13 > 0:
                ret13[c] = (p_now / p13 - 1) * 100

        # ── 卖出逻辑（优先于买入） ──
        if holding and holding in row.index and pd.notna(row[holding]):
            cur_px     = row[holding]
            highest_px = max(highest_px, cur_px)
            drawdown   = (cur_px - highest_px) / highest_px * 100

            sell_reason = None
            if drawdown < -trailing:
                sell_reason = f"移动止损 回撤{drawdown:.1f}%"
            elif ret20.get(holding, 0) < sell_low:
                sell_reason = f"割肉线 20日{ret20.get(holding, 0):.1f}%"

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
                holding = entry_price = entry_date = None
                highest_px = 0.0

        # ── 买入逻辑 ──
        if holding is None:
            # 候选：20日和13日动量都为正，按20日排名
            candidates = sorted(
                [(c, ret20[c]) for c in codes
                 if c in ret20 and c in ret13
                 and ret20[c] > 0 and ret13[c] > 0],
                key=lambda x: x[1], reverse=True,
            )
            for c, _ in candidates:
                if ret13.get(c, 0) >= buy_thr and pd.notna(row.get(c)):
                    holding     = c
                    entry_price = row[c]
                    entry_date  = dt
                    highest_px  = entry_price
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

    # ── 收盘未平仓记录 ──
    if holding and dates:
        last_px    = wide.iloc[-1].get(holding, entry_price)
        trade_ret  = (last_px / entry_price - 1) * 100
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

    v0, v1  = daily_df.iloc[0]["value"], daily_df.iloc[-1]["value"]
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
        avg_win  = wins["收益率(%)"].mean()  if not wins.empty  else 0
        avg_loss = losses["收益率(%)"].mean() if not losses.empty else 0
        avg_hold = trades_df["持仓天数"].mean()
        n_trades = len(trades_df)
    else:
        win_rate = avg_win = avg_loss = avg_hold = n_trades = 0

    return {
        "总收益(%)":      round(total,   2),
        "年化收益(%)":    round(ann,     2),
        "夏普比率":       round(sharpe,  3),
        "最大回撤(%)":    round(max_dd,  2),
        "总交易次数":     n_trades,
        "胜率(%)":        round(win_rate,1),
        "平均盈利(%)":    round(avg_win, 2),
        "平均亏损(%)":    round(avg_loss,2),
        "平均持仓天数":   round(avg_hold,1),
        "回测日历天数":   days,
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

def generate_report(result: dict, metrics: dict, monthly: pd.DataFrame) -> str:
    p  = result["params"]
    dd = result["daily"]
    td = result["trades"]
    s  = dd["date"].min().strftime("%Y-%m-%d")
    e  = dd["date"].max().strftime("%Y-%m-%d")

    lines = [
        "# ETF12 策略回测报告",
        "",
        "## 一、回测概览",
        "",
        f"| 项目 | 数值 |",
        f"|------|------|",
        f"| 回测区间 | {s} ~ {e} ({metrics.get('回测日历天数','-')} 天) |",
        f"| **总收益** | **{metrics.get('总收益(%)','-')}%** |",
        f"| 年化收益 | {metrics.get('年化收益(%)','-')}% |",
        f"| 夏普比率 | {metrics.get('夏普比率','-')} |",
        f"| **最大回撤** | **{metrics.get('最大回撤(%)','-')}%** |",
        f"| 总交易次数 | {metrics.get('总交易次数','-')} 次 |",
        f"| 胜率 | {metrics.get('胜率(%)','-')}% |",
        f"| 平均盈利 | {metrics.get('平均盈利(%)','-')}% |",
        f"| 平均亏损 | {metrics.get('平均亏损(%)','-')}% |",
        f"| 平均持仓天数 | {metrics.get('平均持仓天数','-')} 天 |",
        "",
        "## 二、参数配置",
        "",
        f"| 参数 | 值 |",
        f"|------|----|",
        f"| 排名周期 | {p['ranking_period']} 日 |",
        f"| 买入准入 | 13日涨幅 ≥ {p['buy_threshold']}% |",
        f"| 移动止损 | 最高点回撤 {p['trailing_stop']}% |",
        f"| 割肉线 | 20日动量 < {p['sell_low']}% |",
        "",
    ]

    # ── 月度收益 ──
    lines += ["## 三、月度收益", ""]
    if not monthly.empty:
        lines += ["| 月份 | 月收益 | 方向 |", "|------|--------|------|"]
        for _, row in monthly.iterrows():
            r  = row["月收益(%)"]
            flag = "▲" if r > 0 else ("▼" if r < 0 else "—")
            lines.append(f"| {row['月份']} | {r}% | {flag} |")
    lines.append("")

    # 月度收益分布小结
    if not monthly.empty:
        pos   = (monthly["月收益(%)"] > 0).sum()
        neg   = (monthly["月收益(%)"] < 0).sum()
        total = len(monthly)
        lines += [
            f"> 正收益月份: {pos}/{total}，负收益月份: {neg}/{total}",
            f"> 月均收益: {monthly['月收益(%)'].mean():.2f}%  最大月亏损: {monthly['月收益(%)'].min():.2f}%",
            "",
        ]

    # ── 每日净值尾部（最近 40 个交易日） ──
    lines += ["## 四、近期每日净值（最近40交易日）", ""]
    recent = dd.tail(40)
    v_base = dd.iloc[0]["value"]
    for _, row in recent.iterrows():
        pct = (row["value"] / v_base - 1) * 100
        bar = "█" * max(0, int(abs(pct) / 2))
        sign = "+" if pct >= 0 else "-"
        hold_str = f"[{row['holding']}]" if row["holding"] else "[空仓]"
        lines.append(f"{row['date'].strftime('%Y-%m-%d')} {hold_str:16s} {sign}{abs(pct):.1f}% {bar}")
    lines.append("")

    # ── 交易记录 ──
    lines += ["## 五、交易记录", ""]
    if not td.empty:
        lines += [
            "| 买入日 | 卖出日 | 标的 | 买入价 | 卖出价 | 持仓天 | 收益(%) | 卖出原因 |",
            "|--------|--------|------|--------|--------|--------|---------|---------|",
        ]
        for _, row in td.iterrows():
            flag = "✓" if row["收益率(%)"] > 0 else "✗"
            lines.append(
                f"| {row['买入日']} | {row['卖出日']} | {row['标的']} "
                f"| {row['买入价']} | {row['卖出价']} "
                f"| {row['持仓天数']} | {flag} {row['收益率(%)']}% | {row['卖出原因']} |"
            )
    lines.append("")

    return "\n".join(lines)


# ── 主程序 ────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="ETF12 策略回测引擎")
    p.add_argument("--data",           required=True,  help="价格数据 CSV 文件路径")
    p.add_argument("--config",         default="etf12_config.json", help="策略参数 JSON 文件")
    p.add_argument("--start",          default=None,   help="回测开始日期 YYYY-MM-DD")
    p.add_argument("--end",            default=None,   help="回测结束日期 YYYY-MM-DD")
    p.add_argument("--trailing-stop",  type=float,     help="移动止损 %（覆盖配置）")
    p.add_argument("--buy-threshold",  type=float,     help="买入准入 %（覆盖配置）")
    p.add_argument("--sell-low",       type=float,     help="割肉线 %（覆盖配置）")
    p.add_argument("--capital",        type=float,     help="起始资金（覆盖配置）")
    p.add_argument("--ranking-period", type=int,       help="排名周期天数（默认20）")
    p.add_argument("--buy-period",     type=int,       help="准入计算周期（默认13）")
    p.add_argument("--output",         default="validator_output/backtest_etf12", help="输出目录")
    return p.parse_args()


def main():
    args   = parse_args()
    cfg    = load_config(args.config)

    params = {
        "trailing_stop":  args.trailing_stop  or cfg.get("移动止损回撤比(%)",       4.0),
        "buy_threshold":  args.buy_threshold  or cfg.get("买入准入均线要求(%)",      1.5),
        "sell_low":       args.sell_low       or cfg.get("持仓死亡被动割肉线(%)",   -2.5),
        "capital":        args.capital        or cfg.get("单次投入金额",         150000),
        "ranking_period": args.ranking_period or 20,
        "buy_period":     args.buy_period     or 13,
        "start":          args.start,
        "end":            args.end,
    }

    print(f"[1/5] 读取价格数据: {args.data}")
    df = load_price_data(args.data)
    print(f"      数据: {df['date'].min().date()} ~ {df['date'].max().date()}"
          f"  标的数: {df['code'].nunique()}")

    print(f"[2/5] 运行回测  trailing={params['trailing_stop']}%"
          f"  buy_thr={params['buy_threshold']}%  sell_low={params['sell_low']}%")
    result = run_backtest(df, params)

    print("[3/5] 计算绩效指标")
    metrics  = calc_metrics(result["daily"], result["trades"])
    monthly  = calc_monthly(result["daily"])

    print("\n" + "=" * 50)
    for k, v in metrics.items():
        print(f"  {k:16s}: {v}")
    print("=" * 50 + "\n")

    print(f"[4/5] 生成报告: {args.output}")
    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)

    report = generate_report(result, metrics, monthly)
    (out / "report.md").write_text(report, encoding="utf-8")
    result["trades"].to_csv(out / "trades.csv",  index=False, encoding="utf-8-sig")
    result["daily"].to_csv(out  / "daily.csv",   index=False, encoding="utf-8-sig")
    monthly.to_csv(out          / "monthly.csv", index=False, encoding="utf-8-sig")

    print(f"[5/5] 完成  报告: {out}/report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
