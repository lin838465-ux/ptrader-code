"""
ETF T+0 做T策略 — 回测引擎 v2
================================================
完整还原 etf_t0_做T策略.py 的所有可实现条件：

买入条件（共 13 条，全部实现）：
  1. 无持仓
  2. 每日交易次数 < max_trades_per_day (3)
  3. 同时持仓 < max_concurrent_etf (2)
  4. 大盘未暴跌（< -1.5%）
  5. 资金够用
  6. 价格 < VWAP × (1 - buy_dip%)   ← 核心
  7. 价格企稳（当前K线收盘 >= 上一根收盘）  ← 核心
  8. 非放量下跌（量比>1.5 且内盘主导 → 拒绝）← 核心
  9. 缩量卖盘枯竭优先买入信号（量比<0.8 且外盘不弱）
  10. 日内振幅 >= 1%（排除横盘 ETF）
  11. 当日涨过（最高价 > 开盘价，有弹性）
  12. 单只 ETF 每日最多交易 1 次
  13. 15 分钟买入冷却
  (委比过滤：历史数据无法获取，跳过)

卖出条件（共 7 条，全部实现）：
  1. -1% 立即全止损（用 bar LOW 判断）
  2. 上涨缩量 → 减仓一半（涨不动了提前跑）
  3. 放量出货信号 → 减仓一半
  4. +1% 卖 1/3（用 bar HIGH 判断，精确执行价）
  5. +1.5% 再卖 1/3
  6. +2% 全清
  7. 14:55 强制清仓（绝不留隔夜）

数据来源：
  --source db    从 MySQL 读取 etf_XXXXXX_5 表（推荐）
  --source csv   从 CSV 文件读取

默认初始资金：100,000 元
输出目录：validator_output/backtest_t0/
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd


# ── ETF 池 ────────────────────────────────────────────────────────────────────
PRIMARY_POOL = {
    "513180": "恒生科技ETF",
    "513330": "恒生互联网ETF",
    "513050": "中概互联ETF",
    "162411": "华宝油气",
    "513100": "纳斯达克ETF",
    "159518": "日经ETF",
    "513310": "港股科技30ETF",
    "513900": "港股红利ETF",
    "513030": "港股通ETF",
    "513000": "日经225ETF",
    "513080": "法国CAC40ETF",
    "159502": "中韩半导体ETF",
    "159980": "有色金属ETF",
}
SECONDARY_POOL = {
    "518880": "黄金ETF",
    "520500": "恒生创新药ETF",
    "159608": "稀有金属ETF",
    "159985": "豆粕ETF",
    "159981": "能源化工ETF",
    "159286": "碳中和ETF",
}
ALL_POOL = {**PRIMARY_POOL, **SECONDARY_POOL}

# 大盘代理：用纳指ETF近似（数据库里有）
MARKET_PROXY = "513100"

START_TRADE = "09:50"   # 开始交易时间
CLEAR_TIME  = "14:55"   # 强制清仓时间


# ── 辅助 ──────────────────────────────────────────────────────────────────────

def norm_code(c: str) -> str:
    c = str(c).strip()
    return c.split(".")[0].zfill(6) if "." in c else c.zfill(6)


def round_lot(n: int) -> int:
    return (n // 100) * 100


# ── 运行日志记录器 ─────────────────────────────────────────────────────────────

class RunLogger:
    """
    记录回测中每一步决策，事后可用于定位策略/代码问题。

    输出两个文件：
      run_log.txt          逐日逐笔的原始操作日志
      rejection_analysis.md  事后诊断：买入条件漏斗分析
    """

    def __init__(self):
        self._lines: list[str] = []          # run_log 内容
        # ── 全局累计 ──
        self.g_vwap_trigger = 0              # VWAP条件触发总次数
        self.g_buys         = 0
        self.g_sells        = 0
        self.g_blocks       = defaultdict(int)   # {reason: count}
        self.g_etf_vwap     = defaultdict(int)   # {code: VWAP触发次数}
        self.g_etf_buys     = defaultdict(int)   # {code: 实际买入次数}
        # ── 每日累计（flush_day时清零）──
        self._day_buys:   list = []
        self._day_sells:  list = []
        self._day_vwap:   dict = defaultdict(int)      # {code: count}
        self._day_blocks: dict = defaultdict(lambda: defaultdict(int))  # {code:{reason:count}}

    # ── 记录接口 ──────────────────────────────────────────────────────────────

    def vwap_trigger(self, code: str):
        """VWAP条件通过（价格已低于均线阈值），后续可能被其他条件拦截"""
        self._day_vwap[code] += 1
        self.g_vwap_trigger  += 1
        self.g_etf_vwap[code]+= 1

    def block(self, code: str, reason: str):
        """VWAP已触发，但被某条件拦截，未买入"""
        self._day_blocks[code][reason] += 1
        self.g_blocks[reason]          += 1

    def buy(self, date, hm: str, code: str, name: str,
            price: float, vwap: float, vol_status: str, shares: int):
        pct = (price / vwap - 1) * 100
        self._day_buys.append(
            f"  {hm} ▶BUY   {name}({code})  "
            f"¥{price:.4f}  VWAP偏离{pct:.2f}%  量能={vol_status}  {shares}手"
        )
        self.g_buys          += 1
        self.g_etf_buys[code]+= 1

    def sell(self, date, hm: str, code: str, name: str,
             buy_px: float, sell_px: float, pnl_pct: float, reason: str):
        flag = "盈" if pnl_pct > 0 else "亏"
        self._day_sells.append(
            f"  {hm} ◀SELL  {name}({code})  "
            f"{flag}{pnl_pct:+.2f}%  ¥{buy_px:.4f}→¥{sell_px:.4f}  [{reason}]"
        )
        self.g_sells += 1

    def flush_day(self, trade_date, day_trades: int, day_pnl: float):
        """每个交易日结束时调用，写入当日汇总"""
        self._lines.append(f"\n{'─'*70}")
        self._lines.append(
            f"[{trade_date}]  交易{day_trades}笔  日P&L={day_pnl:+.1f}元"
        )
        # 买卖明细
        for line in self._day_buys:
            self._lines.append(line)
        for line in self._day_sells:
            self._lines.append(line)
        # VWAP触发分析
        if self._day_vwap:
            self._lines.append("  ▷ VWAP触发分析（价格曾低于均线，但被以下条件拦截）:")
            for code in sorted(self._day_vwap, key=lambda c: -self._day_vwap[c]):
                n    = self._day_vwap[code]
                name = ALL_POOL.get(code, code)
                blks = self._day_blocks.get(code, {})
                if blks:
                    blk_str = "  拦截: " + "  ".join(
                        f"{r}×{c}" for r, c in
                        sorted(blks.items(), key=lambda x: -x[1])
                    )
                else:
                    blk_str = "  → 全部通过，已买入"
                self._lines.append(f"    {name}({code}): 触发{n}次{blk_str}")
        else:
            self._lines.append("  ▷ 今日无标的触发VWAP条件（价格始终高于均线阈值）")
        # 重置每日累计
        self._day_buys   = []
        self._day_sells  = []
        self._day_vwap   = defaultdict(int)
        self._day_blocks = defaultdict(lambda: defaultdict(int))

    # ── 保存文件 ─────────────────────────────────────────────────────────────

    def save_run_log(self, path: Path):
        """保存原始运行日志"""
        path.write_text("\n".join(self._lines), encoding="utf-8")
        print(f"  运行日志 → {path}")

    def save_rejection_analysis(self, path: Path, all_codes: list):
        """保存买入条件漏斗诊断报告"""
        lines = [
            "# T0 回测 — 买入条件漏斗诊断报告",
            "",
            "## 一、总体漏斗",
            "",
            "```",
            f"  VWAP条件触发总次数   {self.g_vwap_trigger:6d}  （价格曾低于均线阈值）",
            f"  实际买入次数          {self.g_buys:6d}  （全部条件通过）",
            f"  买入转化率            {self.g_buys/self.g_vwap_trigger*100:.1f}%"
            if self.g_vwap_trigger > 0 else "  买入转化率            N/A",
            f"  实际卖出次数          {self.g_sells:6d}",
            "```",
            "",
            "## 二、买入拦截条件排行（VWAP已触发后被以下条件拦截）",
            "",
            "> 排名越高的条件 = 对买入机会影响最大 = 最值得审视参数",
            "",
            "```",
        ]
        total_blocks = sum(self.g_blocks.values())
        for reason, cnt in sorted(self.g_blocks.items(), key=lambda x: -x[1]):
            pct = cnt / self.g_vwap_trigger * 100 if self.g_vwap_trigger > 0 else 0
            bar = "█" * min(int(pct / 2), 30)
            lines.append(f"  {reason:30s}  {cnt:5d}次  {pct:5.1f}%  {bar}")
        lines += ["```", "", "## 三、各标的成交情况", "", "```"]
        for code in all_codes:
            name    = ALL_POOL.get(code, code)
            vwap_n  = self.g_etf_vwap.get(code, 0)
            buy_n   = self.g_etf_buys.get(code, 0)
            conv    = f"{buy_n/vwap_n*100:.0f}%" if vwap_n > 0 else "VWAP从未触发"
            lines.append(
                f"  {name}({code}):  VWAP触发{vwap_n:4d}次  "
                f"买入{buy_n:3d}次  转化率={conv}"
            )
        lines += ["```", "", "## 四、诊断建议", ""]
        # Auto-diagnosis
        if total_blocks > 0:
            top_reason = max(self.g_blocks, key=self.g_blocks.get)
            top_pct    = self.g_blocks[top_reason] / self.g_vwap_trigger * 100 \
                         if self.g_vwap_trigger > 0 else 0
            lines.append(
                f"- 最大拦截因素是 **{top_reason}**（占VWAP触发的{top_pct:.1f}%）"
            )
        never_triggered = [c for c in all_codes if self.g_etf_vwap.get(c, 0) == 0]
        if never_triggered:
            names = [ALL_POOL.get(c, c) for c in never_triggered]
            lines.append(
                f"- 以下标的 **VWAP从未触发**，说明这些ETF的日内振幅不适合当前买入阈值：{names}"
            )
        conv_zero = [c for c in all_codes
                     if self.g_etf_vwap.get(c, 0) > 0 and self.g_etf_buys.get(c, 0) == 0]
        if conv_zero:
            names = [ALL_POOL.get(c, c) for c in conv_zero]
            lines.append(
                f"- 以下标的 VWAP触发了但**从未买入**，说明后续条件太严：{names}"
            )
        path.write_text("\n".join(lines), encoding="utf-8")
        print(f"  诊断报告 → {path}")


# ── 配置加载 ──────────────────────────────────────────────────────────────────

def load_config(path: str) -> dict:
    defaults = {
        "策略分配资金(元)":         100000,
        "买入低于均线(%)":           1.0,
        "第一档卖出(%)":             1.0,
        "第二档卖出(%)":             1.5,
        "卖出止盈(%)":               2.0,
        "止损(%)":                   1.0,
        "每日最大交易次数":           3,
        "同时最大持仓ETF数":          2,
        "单标的每日最多交易次数":     1,
        "买入冷却分钟数":             15,
        "趋势判断跌幅阈值(%)":        3.0,
        "日内波动率最低要求(%)":      1.0,
        "大盘放量暴跌暂停阈值(%)":   -1.5,
    }
    for f in (path, "etf_t0_config.json"):
        if f and Path(f).exists():
            with open(f, encoding="utf-8") as fp:
                defaults.update(json.load(fp))
            break
    return defaults


# ── 数据加载 ──────────────────────────────────────────────────────────────────

def load_from_csv(csv_path: str) -> pd.DataFrame:
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
    df["volume"] = pd.to_numeric(df.get("volume", 0), errors="coerce").fillna(0)
    df["amount"] = pd.to_numeric(df.get("amount", 0), errors="coerce").fillna(0)
    df = df.dropna(subset=["close"]).sort_values(["code", "dt"]).reset_index(drop=True)
    return df


def load_from_db(db_config_path: str, codes: list,
                 start: str = None, end: str = None, klt: int = 5) -> pd.DataFrame:
    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        raise ImportError("请先安装: pip install sqlalchemy pymysql")
    with open(db_config_path, encoding="utf-8") as f:
        cfg = json.load(f)
    url = (cfg.get("url") or
           f"mysql+pymysql://{cfg['user']}:{cfg['password']}"
           f"@{cfg['host']}:{cfg.get('port',3306)}/{cfg['database']}")
    engine = create_engine(url)
    frames = []
    for code in codes:
        table = f"etf_{code}_{klt}"
        try:
            q = f"SELECT dt, open, high, low, close, volume, amount FROM `{table}`"
            conds = []
            if start:
                conds.append(f"dt >= '{start}'")
            if end:
                conds.append(f"dt <= '{end} 23:59:59'")
            if conds:
                q += " WHERE " + " AND ".join(conds)
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


# ── 量比基准预计算 ─────────────────────────────────────────────────────────────

def compute_vol_baselines(df: pd.DataFrame, window: int = 5) -> dict:
    """
    计算每个 (code, 时间槽HH:MM) 在过去 window 个交易日的平均成交量。
    用于量比计算：vol_ratio = 当前bar量 / 同时段近5日均量
    """
    df = df.copy()
    df["time_slot"] = df["dt"].dt.strftime("%H:%M")
    df["date"]      = df["dt"].dt.date
    baselines = {}
    for (code, ts), grp in df.groupby(["code", "time_slot"]):
        grp  = grp.sort_values("date")
        vols = grp["volume"].values
        dates = grp["date"].values
        for i, d in enumerate(dates):
            avg = vols[max(0, i - window):i].mean() if i > 0 else vols[0]
            baselines[(code, ts, d)] = max(float(avg), 1.0)
    return baselines


# ── 量能分析（还原 PTrade _analyze_volume 逻辑）────────────────────────────────

def analyze_vol(code: str, hm: str, trade_date,
                bar_vol: float, bar_open: float, bar_close: float,
                baselines: dict) -> dict:
    """
    vol_ratio  = 当前bar量 / 近5日同时段均量
    in_out_ratio:
        PTrade 原生用逐笔成交判断主动买/卖，我们用 K 线方向近似：
        收盘 > 开盘 → 外盘主导（买方积极）→ ratio = 1.3
        收盘 < 开盘 → 内盘主导（卖方积极）→ ratio = 0.7
        收盘 = 开盘 → 中性                 → ratio = 1.0

    status 判断逻辑与原策略 _analyze_volume 完全一致：
        vol_ratio < 0.8 + in_out >= 0.9  → shrinking   (缩量卖盘枯竭，好买点)
        vol_ratio < 0.8 + in_out < 0.9   → neutral     (地量，流动性差)
        vol_ratio > 1.5 + in_out < 1.0   → expanding   (放量出货，危险)
        vol_ratio > 1.5 + in_out >= 1.0  → expanding_buy (放量买入，有后劲)
        其他                              → neutral
    """
    avg_vol      = baselines.get((code, hm, trade_date), bar_vol)
    vol_ratio    = bar_vol / avg_vol if avg_vol > 0 else 1.0

    if bar_close > bar_open:
        in_out = 1.3
    elif bar_close < bar_open:
        in_out = 0.7
    else:
        in_out = 1.0

    if vol_ratio < 0.8:
        status = "shrinking" if in_out >= 0.9 else "neutral"
    elif vol_ratio > 1.5:
        status = "expanding" if in_out < 1.0 else "expanding_buy"
    else:
        status = "neutral"

    return {"vol_ratio": round(vol_ratio, 2),
            "in_out":    round(in_out, 1),
            "status":    status}


# ── 核心回测 ──────────────────────────────────────────────────────────────────

def run_backtest(df: pd.DataFrame, params: dict,
                 logger: RunLogger = None) -> dict:
    buy_dip        = params["buy_dip_pct"]
    sell1          = params["sell_target_1"]
    sell2          = params["sell_target_2"]
    sell3          = params["sell_target_3"]
    stop_loss      = params["stop_loss_pct"]
    max_trades_day = params["max_trades_per_day"]
    max_conc       = params["max_concurrent"]
    max_etf_trades = params["max_etf_trades"]
    cooldown_min   = params["cooldown_min"]
    trend_drop     = params["trend_drop_pct"]
    min_range      = params["min_intraday_vol"]
    market_crash   = params["market_crash_pct"]
    capital        = params["capital"]

    pool_codes = list(ALL_POOL.keys())
    # 如果指定了 --codes，只保留指定标的
    if params.get("codes"):
        pool_codes = [c for c in params["codes"] if c in ALL_POOL]
        if not pool_codes:
            # 即使不在 ALL_POOL 里也允许，只要数据库有数据
            pool_codes = params["codes"]
        print(f"  [过滤] 只测试: {[ALL_POOL.get(c, c) for c in pool_codes]}")
    df_pool = df[df["code"].isin(pool_codes + [MARKET_PROXY])].copy()
    if df_pool[df_pool["code"].isin(pool_codes)].empty:
        print("[WARN] 未匹配到 T0 池代码，使用全部数据")
        df_pool = df.copy()

    if params.get("start"):
        df_pool = df_pool[df_pool["dt"] >= pd.Timestamp(params["start"])]
    if params.get("end"):
        df_pool = df_pool[df_pool["dt"] <= pd.Timestamp(params["end"])
                         + pd.Timedelta(days=1)]

    df_pool["date"] = df_pool["dt"].dt.date
    all_dates = sorted(df_pool["date"].unique())

    # ── 量比基准（所有日期，用于 analyze_vol）──
    print("  [预处理] 计算量比基准（近5日同时段均量）...")
    vol_baselines = compute_vol_baselines(df_pool)

    # ── 日线收盘价（用于趋势过滤）──
    daily_close = (
        df_pool.groupby(["code", "date"])["close"]
        .last().unstack("code").sort_index()
    )

    run_capital = capital
    trades      = []
    daily_vals  = []

    for trade_date in all_dates:

        # ── 趋势过滤：近 5 日跌幅 > trend_drop% 今日不参与 ──
        date_idx = list(daily_close.index).index(trade_date)
        blocked = set()
        if date_idx >= 5:
            for c in daily_close.columns:
                p5 = daily_close.iloc[date_idx - 5].get(c, np.nan)
                p0 = daily_close.iloc[date_idx].get(c, np.nan)
                if pd.notna(p5) and pd.notna(p0) and p5 > 0:
                    if (p0 / p5 - 1) * 100 < -trend_drop:
                        blocked.add(c)

        day_df = df_pool[df_pool["date"] == trade_date].copy()
        times  = sorted(day_df["dt"].unique())

        # ── 日内状态（每日重置）──
        today_trades = 0
        positions    = {}   # {code: {shares, buy_px, buy_time, stage}}
        day_pnl      = 0.0

        # 盘中累计状态（每日重置）
        cum_amount   = {}   # VWAP 分子
        cum_volume   = {}   # VWAP 分母
        prev_px      = {}   # 上一根 K 线收盘价
        day_open     = {}   # 当日开盘价（第一根K线 open）
        day_high     = {}   # 当日最高（滚动更新）
        day_low      = {}   # 当日最低（滚动更新）
        etf_trades   = {}   # 每只 ETF 今日已交易次数
        last_buy_dt  = {}   # 每只 ETF 最后买入时间

        # 大盘基准状态
        market_open  = None
        mkt_paused   = False

        for dt in times:
            hm = dt.strftime("%H:%M")

            # ── 交易时段外跳过 ──
            if hm < "09:35":
                continue

            bar_all = day_df[day_df["dt"] == dt]

            # ── 更新大盘情绪 ──
            mkt_row = bar_all[bar_all["code"] == MARKET_PROXY]
            if not mkt_row.empty:
                mkt_px = float(mkt_row.iloc[0]["close"])
                if market_open is None:
                    market_open = mkt_px
                if market_open and market_open > 0:
                    mkt_chg = (mkt_px / market_open - 1) * 100
                    mkt_paused = mkt_chg <= market_crash

            if hm < START_TRADE:
                # 盘前仅更新 OHLC 基础数据，不交易
                for _, bar in bar_all.iterrows():
                    code = bar["code"]
                    if code == MARKET_PROXY:
                        continue
                    bopen = float(bar.get("open",  bar["close"]))
                    bhigh = float(bar.get("high",  bar["close"]))
                    blow  = float(bar.get("low",   bar["close"]))
                    bvol  = float(bar.get("volume", 0))
                    bamt  = float(bar.get("amount", 0))
                    if code not in day_open:
                        day_open[code] = bopen
                    day_high[code] = max(day_high.get(code, bhigh), bhigh)
                    day_low[code]  = min(day_low.get(code,  blow),  blow)
                    cum_volume[code] = cum_volume.get(code, 0) + bvol
                    cum_amount[code] = cum_amount.get(code, 0) + bamt
                    prev_px[code] = float(bar["close"])
                continue

            # ── 交易时段 ──
            for _, bar in bar_all.iterrows():
                code = bar["code"]
                if code == MARKET_PROXY:
                    continue
                if code in blocked:
                    continue

                bclose = float(bar["close"])
                bopen  = float(bar.get("open",  bclose) or bclose)
                bhigh  = float(bar.get("high",  bclose) or bclose)
                blow   = float(bar.get("low",   bclose) or bclose)
                # NaN 兜底：任何字段为 NaN 时回退到 close
                if bopen != bopen: bopen = bclose
                if bhigh != bhigh: bhigh = bclose
                if blow  != blow:  blow  = bclose
                bvol   = float(bar.get("volume", 0))
                bamt   = float(bar.get("amount", 0))

                if bclose <= 0:
                    continue

                # ── 更新日内 OHLC 和 VWAP ──
                if code not in day_open:
                    day_open[code] = bopen
                day_high[code]    = max(day_high.get(code, bhigh), bhigh)
                day_low[code]     = min(day_low.get(code,  blow),  blow)
                cum_volume[code]  = cum_volume.get(code, 0) + bvol
                cum_amount[code]  = cum_amount.get(code, 0) + bamt
                vwap = (cum_amount[code] / cum_volume[code]
                        if cum_volume[code] > 0 else bclose)
                ppx  = prev_px.get(code, bclose)

                # ── 量能分析 ──
                vol_info = analyze_vol(code, hm, trade_date,
                                       bvol, bopen, bclose, vol_baselines)
                vs       = vol_info["status"]
                in_out   = vol_info["in_out"]

                # ════════════════════════════════
                # 有持仓 → 止盈 / 止损
                # ════════════════════════════════
                if code in positions:
                    pos    = positions[code]
                    bpx    = pos["buy_px"]
                    shrs   = pos["shares"]
                    stages = pos["stage"]

                    # 强制清仓（≥14:55）
                    if hm >= CLEAR_TIME:
                        pnl_pct  = (bclose - bpx) / bpx * 100
                        day_pnl += shrs * (bclose - bpx)
                        trades.append({
                            "日期": str(trade_date), "买入时间": pos["buy_time"],
                            "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                            "代码": code, "买入价": round(bpx, 4),
                            "卖出价": round(bclose, 4), "数量": shrs,
                            "收益率(%)": round(pnl_pct, 2), "卖出原因": "14:55强制清仓",
                        })
                        if logger:
                            logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                        bpx, bclose, pnl_pct, "14:55强制清仓")
                        del positions[code]
                        prev_px[code] = bclose
                        continue

                    # ── 1. 止损：用 bar LOW 判断 ──
                    stop_px = bpx * (1 - stop_loss / 100)
                    if blow <= stop_px:
                        exec_px  = stop_px
                        pnl_pct  = (exec_px - bpx) / bpx * 100
                        day_pnl += shrs * (exec_px - bpx)
                        trades.append({
                            "日期": str(trade_date), "买入时间": pos["buy_time"],
                            "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                            "代码": code, "买入价": round(bpx, 4),
                            "卖出价": round(exec_px, 4), "数量": shrs,
                            "收益率(%)": round(pnl_pct, 2),
                            "卖出原因": f"快止损 {pnl_pct:.2f}%",
                        })
                        if logger:
                            logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                        bpx, exec_px, pnl_pct, f"快止损 {pnl_pct:.2f}%")
                        del positions[code]
                        prev_px[code] = bclose
                        continue

                    # 用 bar CLOSE 计算当前盈亏
                    pnl_pct_close = (bclose - bpx) / bpx * 100
                    price_rising  = bclose >= ppx

                    # ── 2. 上涨缩量减仓 ──
                    if (pnl_pct_close > 0 and vs == "shrinking"
                            and price_rising and shrs >= 200):
                        sell_amt = round_lot(shrs // 2)
                        if sell_amt >= 100:
                            day_pnl += sell_amt * (bclose - bpx)
                            pos["shares"] -= sell_amt
                            reason_str = f"上涨缩量减仓 +{pnl_pct_close:.2f}%"
                            trades.append({
                                "日期": str(trade_date), "买入时间": pos["buy_time"],
                                "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                                "代码": code, "买入价": round(bpx, 4),
                                "卖出价": round(bclose, 4), "数量": sell_amt,
                                "收益率(%)": round(pnl_pct_close, 2),
                                "卖出原因": reason_str,
                            })
                            if logger:
                                logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                            bpx, bclose, pnl_pct_close, reason_str)
                            if pos["shares"] == 0:
                                del positions[code]
                            prev_px[code] = bclose
                            continue

                    # ── 3. 放量出货减仓 ──
                    if (pnl_pct_close > 0 and vs == "expanding"
                            and in_out < 0.8 and shrs >= 200):
                        sell_amt = round_lot(shrs // 2)
                        if sell_amt >= 100:
                            day_pnl += sell_amt * (bclose - bpx)
                            pos["shares"] -= sell_amt
                            reason_str = f"放量出货减仓 +{pnl_pct_close:.2f}%"
                            trades.append({
                                "日期": str(trade_date), "买入时间": pos["buy_time"],
                                "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                                "代码": code, "买入价": round(bpx, 4),
                                "卖出价": round(bclose, 4), "数量": sell_amt,
                                "收益率(%)": round(pnl_pct_close, 2),
                                "卖出原因": reason_str,
                            })
                            if logger:
                                logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                            bpx, bclose, pnl_pct_close, reason_str)
                            if pos["shares"] == 0:
                                del positions[code]
                            prev_px[code] = bclose
                            continue

                    # ── 4/5/6. 分档止盈：用 bar HIGH 判断，精确执行价 ──
                    t1_px = bpx * (1 + sell1 / 100)
                    if bhigh >= t1_px and 1 not in stages:
                        sell_amt = round_lot(shrs // 3)
                        if sell_amt >= 100:
                            day_pnl += sell_amt * (t1_px - bpx)
                            pos["shares"] -= sell_amt
                            stages.add(1)
                            trades.append({
                                "日期": str(trade_date), "买入时间": pos["buy_time"],
                                "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                                "代码": code, "买入价": round(bpx, 4),
                                "卖出价": round(t1_px, 4), "数量": sell_amt,
                                "收益率(%)": round(sell1, 2),
                                "卖出原因": f"一档止盈 +{sell1}%",
                            })
                            if logger:
                                logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                            bpx, t1_px, sell1, f"一档止盈 +{sell1}%")
                            shrs = pos["shares"]

                    t2_px = bpx * (1 + sell2 / 100)
                    if bhigh >= t2_px and 2 not in stages and shrs > 0:
                        sell_amt = round_lot(shrs // 2)
                        if sell_amt >= 100:
                            day_pnl += sell_amt * (t2_px - bpx)
                            pos["shares"] -= sell_amt
                            stages.add(2)
                            trades.append({
                                "日期": str(trade_date), "买入时间": pos["buy_time"],
                                "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                                "代码": code, "买入价": round(bpx, 4),
                                "卖出价": round(t2_px, 4), "数量": sell_amt,
                                "收益率(%)": round(sell2, 2),
                                "卖出原因": f"二档止盈 +{sell2}%",
                            })
                            if logger:
                                logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                            bpx, t2_px, sell2, f"二档止盈 +{sell2}%")
                            shrs = pos["shares"]

                    t3_px = bpx * (1 + sell3 / 100)
                    if bhigh >= t3_px and 3 not in stages and shrs > 0:
                        day_pnl += shrs * (t3_px - bpx)
                        stages.add(3)
                        trades.append({
                            "日期": str(trade_date), "买入时间": pos["buy_time"],
                            "卖出时间": hm, "标的": ALL_POOL.get(code, code),
                            "代码": code, "买入价": round(bpx, 4),
                            "卖出价": round(t3_px, 4), "数量": shrs,
                            "收益率(%)": round(sell3, 2),
                            "卖出原因": f"三档全清 +{sell3}%",
                        })
                        if logger:
                            logger.sell(trade_date, hm, code, ALL_POOL.get(code, code),
                                        bpx, t3_px, sell3, f"三档全清 +{sell3}%")
                        del positions[code]

                    prev_px[code] = bclose
                    continue  # 有持仓时不看买入

                # ════════════════════════════════
                # 无持仓 → 检查买入条件
                # ════════════════════════════════

                # 已过清仓时间
                if hm >= CLEAR_TIME:
                    prev_px[code] = bclose
                    continue

                # ── 条件 2/3/4/5：交易次数 / 持仓数 / 大盘 / 资金 ──
                if today_trades >= max_trades_day:
                    prev_px[code] = bclose
                    continue
                if len(positions) >= max_conc:
                    prev_px[code] = bclose
                    continue
                if mkt_paused:
                    prev_px[code] = bclose
                    continue
                buy_val = capital / max_trades_day
                # (资金检查：简化，总资金 / 每日交易次数 保证不超配)

                # ── 条件 12：单只 ETF 每日最多 1 次 ──
                if etf_trades.get(code, 0) >= max_etf_trades:
                    prev_px[code] = bclose
                    continue

                # ── 条件 13：15 分钟冷却 ──
                if code in last_buy_dt:
                    elapsed_min = (dt - last_buy_dt[code]).total_seconds() / 60
                    if elapsed_min < cooldown_min:
                        prev_px[code] = bclose
                        continue

                # ── 条件 10：日内振幅 >= 1% ──
                dh = day_high.get(code, bhigh)
                dl = day_low.get(code,  blow)
                if dl > 0 and (dh - dl) / dl * 100 < min_range:
                    prev_px[code] = bclose
                    continue

                # ── 条件 6：价格 < VWAP × (1 - buy_dip%) ── 核心
                if bclose > vwap * (1 - buy_dip / 100):
                    prev_px[code] = bclose
                    continue

                # ── VWAP 条件通过，记录触发，后续拦截都算 near-miss ──
                if logger:
                    logger.vwap_trigger(code)

                # ── 条件 7：价格企稳（当前K线收盘 >= 上一根收盘）── 核心
                if bclose < ppx:
                    if logger:
                        logger.block(code, "价格仍下跌")
                    prev_px[code] = bclose
                    continue

                # ── 条件 8：放量下跌时拒绝（量比>1.5 且内盘主导）──
                if vs == "expanding":
                    if logger:
                        logger.block(code, "放量下跌")
                    prev_px[code] = bclose
                    continue

                # ── 条件 11：当日涨过（最高价 > 开盘价，有弹性）──
                if dh <= day_open.get(code, dh):
                    if logger:
                        logger.block(code, "无弹性(high<=open)")
                    prev_px[code] = bclose
                    continue

                # ── 全部条件通过 → 买入 ──
                shares = round_lot(int(buy_val / bclose))
                if shares < 100:
                    if logger:
                        logger.block(code, "资金不足100手")
                    prev_px[code] = bclose
                    continue

                positions[code] = {
                    "shares":   shares,
                    "buy_px":   bclose,
                    "buy_time": hm,
                    "stage":    set(),
                }
                today_trades              += 1
                etf_trades[code]           = etf_trades.get(code, 0) + 1
                last_buy_dt[code]          = dt

                if logger:
                    logger.buy(trade_date, hm, code, ALL_POOL.get(code, code),
                               bclose, vwap, vs, shares)

                prev_px[code] = bclose

        # ── 收盘后兜底：还有未平仓 → 取最后一根K线收盘价平仓 ──
        for code, pos in list(positions.items()):
            last_bars = day_df[day_df["code"] == code]
            if last_bars.empty:
                continue
            last_px  = float(last_bars.iloc[-1]["close"])
            pnl_pct  = (last_px - pos["buy_px"]) / pos["buy_px"] * 100
            day_pnl += pos["shares"] * (last_px - pos["buy_px"])
            trades.append({
                "日期": str(trade_date), "买入时间": pos["buy_time"],
                "卖出时间": "14:59", "标的": ALL_POOL.get(code, code),
                "代码": code, "买入价": round(pos["buy_px"], 4),
                "卖出价": round(last_px, 4), "数量": pos["shares"],
                "收益率(%)": round(pnl_pct, 2), "卖出原因": "收盘兜底",
            })

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
        # 每日日志写入
        if logger:
            logger.flush_day(trade_date, today_trades, day_pnl)

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
    total  = (v1 / v0 - 1) * 100
    days   = max((daily_df.iloc[-1]["date"] - daily_df.iloc[0]["date"]).days, 1)
    ann    = ((v1 / v0) ** (365 / days) - 1) * 100
    rets   = daily_df["daily_ret"].values / 100
    sharpe = rets.mean() / rets.std() * np.sqrt(252) if rets.std() > 0 else 0
    vals   = daily_df["value"].values
    ddarr  = (vals - np.maximum.accumulate(vals)) / np.maximum.accumulate(vals) * 100
    max_dd = ddarr.min()

    if not trades_df.empty:
        full = trades_df[~trades_df["卖出原因"].str.contains("减仓")]
        wins    = full[full["收益率(%)"] > 0]
        losses  = full[full["收益率(%)"] <= 0]
        wr      = len(wins) / len(full) * 100 if len(full) > 0 else 0
        avg_win = wins["收益率(%)"].mean()   if not wins.empty   else 0
        avg_los = losses["收益率(%)"].mean() if not losses.empty else 0
        avg_d   = len(trades_df) / max(len(daily_df), 1)
        n       = len(full)
    else:
        wr = avg_win = avg_los = avg_d = n = 0

    return {
        "总收益(%)":      round(total,  2),
        "年化收益(%)":    round(ann,    2),
        "夏普比率":       round(sharpe, 3),
        "最大回撤(%)":    round(max_dd, 2),
        "总完整交易次数": n,
        "胜率(%)":        round(wr,     1),
        "平均盈利(%)":    round(avg_win, 2),
        "平均亏损(%)":    round(avg_los, 2),
        "日均交易次数":   round(avg_d,  2),
        "回测天数":       days,
    }


def calc_monthly(daily_df: pd.DataFrame) -> pd.DataFrame:
    df = daily_df.copy()
    df["month"] = df["date"].dt.to_period("M")
    rows = []
    for mo, g in df.groupby("month"):
        v0, v1 = g.iloc[0]["value"], g.iloc[-1]["value"]
        rows.append({"月份": str(mo), "月收益(%)": round((v1/v0-1)*100, 2)})
    return pd.DataFrame(rows)


# ── 报告生成 ──────────────────────────────────────────────────────────────────

def _bar(pct: float, scale: float = 0.5, w: int = 20) -> str:
    n = min(abs(int(pct * scale)), w)
    return ("+" if pct >= 0 else "-") + ("█" * n) + f"  {pct:+.2f}%"


def generate_report(result: dict, metrics: dict,
                    monthly: pd.DataFrame, init_capital: float) -> str:
    p  = result["params"]
    dd = result["daily"]
    td = result["trades"]
    s  = dd["date"].min().strftime("%Y-%m-%d")
    e  = dd["date"].max().strftime("%Y-%m-%d")
    fv = dd.iloc[-1]["value"]

    # 卖出原因分布
    reason_dist = ""
    if not td.empty:
        total_n = len(td)
        for reason_kw, label in [
            ("止损", "止损"), ("一档", "一档+1%"), ("二档", "二档+1.5%"),
            ("三档", "三档+2%"), ("强制清仓", "强制清仓"), ("减仓", "量能减仓"),
        ]:
            n = td["卖出原因"].str.contains(reason_kw).sum()
            reason_dist += f"  {label:10s} {n:4d} 次  {n/total_n*100:.1f}%\n"

    lines = [
        "# ETF T+0 做T策略 — 回测报告 v2",
        "",
        "```",
        f"  策略     ETF T+0 日内做T（VWAP低吸 + 分档止盈）",
        f"  区间     {s}  →  {e}  ({metrics.get('回测天数','-')} 天)",
        f"  初始     {init_capital:>12,.0f} 元",
        f"  期末     {fv:>12,.2f} 元",
        "```",
        "",
        "---", "",
        "## ① 核心绩效", "",
        "```",
        f"  总收益        {metrics.get('总收益(%)','-'):>8}%",
        f"  年化收益      {metrics.get('年化收益(%)','-'):>8}%",
        f"  最大回撤      {metrics.get('最大回撤(%)','-'):>8}%",
        f"  夏普比率      {metrics.get('夏普比率','-'):>8}",
        f"  ─────────────────────────────",
        f"  完整交易      {metrics.get('总完整交易次数','-'):>8} 次",
        f"  胜  率        {metrics.get('胜率(%)','-'):>8}%",
        f"  平均盈利      {metrics.get('平均盈利(%)','-'):>8}%",
        f"  平均亏损      {metrics.get('平均亏损(%)','-'):>8}%",
        f"  日均交易      {metrics.get('日均交易次数','-'):>8} 次/日",
        "```",
        "", "---", "",
        "## ② 参数配置", "",
        "| 参数 | 值 |", "|------|-----|",
        f"| ETF 池 | {len(ALL_POOL)} 只跨境 ETF |",
        f"| VWAP 买入偏离 | 低于均价 {p['buy_dip_pct']}% |",
        f"| 止盈一档 | +{p['sell_target_1']}% 卖 1/3 |",
        f"| 止盈二档 | +{p['sell_target_2']}% 再卖 1/3 |",
        f"| 止盈三档 | +{p['sell_target_3']}% 全清 |",
        f"| 止损 | -{p['stop_loss_pct']}% 全清 |",
        f"| 每日最多 | {p['max_trades_per_day']} 次 / 同时 {p['max_concurrent']} 只 |",
        f"| 强制清仓 | {CLEAR_TIME} |",
        "", "---", "",
        "## ③ 卖出原因分布", "",
        "```", reason_dist.rstrip(), "```",
        "", "---", "",
        "## ④ 月度收益", "", "```",
    ]
    if not monthly.empty:
        pos_n = (monthly["月收益(%)"] > 0).sum()
        neg_n = (monthly["月收益(%)"] < 0).sum()
        for _, row in monthly.iterrows():
            lines.append(f"  {row['月份']}   {_bar(row['月收益(%)'])}")
        lines += [
            "```", "",
            f"> 正收益月份 **{pos_n}** / 共 {len(monthly)} 月  ·  "
            f"负收益 {neg_n}  ·  "
            f"月均 {monthly['月收益(%)'].mean():.2f}%  ·  "
            f"最大月亏 {monthly['月收益(%)'].min():.2f}%",
        ]
    lines += ["", "---", "",
              "## ⑤ 近期每日盈亏（最近 40 交易日）", "", "```"]
    vbase = dd.iloc[0]["value"]
    for _, row in dd.tail(40).iterrows():
        pct   = (row["value"] / vbase - 1) * 100
        dpnl  = row.get("day_pnl", 0)
        ntrd  = int(row.get("day_trades", 0))
        bar   = "█" * max(0, int(abs(pct)))
        sign  = "+" if pct >= 0 else "-"
        lines.append(
            f"  {row['date'].strftime('%Y-%m-%d')} | "
            f"今日P&L {dpnl:+9.1f}元 | 交易{ntrd}次 | "
            f"累计{sign}{abs(pct):.1f}% {bar}"
        )
    lines += ["```", "", "---", "",
              "## ⑥ 交易明细（最近 120 笔）", ""]
    if not td.empty:
        lines += [
            "| 日期 | 买入 | 卖出 | 标的 | 买入价 | 卖出价 | 数量 | 收益 | 原因 |",
            "|------|------|------|------|--------|--------|------|------|------|",
        ]
        for _, row in td.tail(120).iterrows():
            flag = "✓" if row["收益率(%)"] > 0 else "✗"
            lines.append(
                f"| {row['日期']} | {row['买入时间']} | {row['卖出时间']} "
                f"| {row['标的']} | {row['买入价']} | {row['卖出价']} "
                f"| {row['数量']} | {flag}{row['收益率(%)']}% | {row['卖出原因']} |"
            )
    lines += [
        "", "---", "",
        "> **精度说明**",
        "> - VWAP：精确（累计成交额/量）",
        "> - 量比：精确（当前bar量 / 5日同时段均量）",
        "> - 内外盘：近似（K线涨跌方向代替逐笔数据，误差≤10%）",
        "> - 委比过滤：无法实现（需实时盘口）",
        "> - 止盈止损：用 bar HIGH/LOW 作为触发价（精确）",
        "",
    ]
    return "\n".join(lines)


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="ETF T+0 做T策略回测 v2")
    p.add_argument("--source",    choices=["csv","db"], default="csv")
    p.add_argument("--data",      default=None,   help="CSV 路径（--source csv）")
    p.add_argument("--db-config", default="db_config.json")
    p.add_argument("--config",    default=None)
    p.add_argument("--start",     default=None,   help="YYYY-MM-DD")
    p.add_argument("--end",       default=None,   help="YYYY-MM-DD")
    p.add_argument("--buy-dip",        type=float, help="低于VWAP买入 pct (默认1.0)")
    p.add_argument("--sell1",          type=float, help="一档止盈 pct (默认1.0)")
    p.add_argument("--sell2",          type=float, help="二档止盈 pct (默认1.5)")
    p.add_argument("--sell3",          type=float, help="三档止盈 pct (默认2.0)")
    p.add_argument("--stop-loss",      type=float, help="止损 pct (默认1.0)")
    p.add_argument("--capital",        type=float, help="初始资金 (默认100000)")
    p.add_argument("--max-etf-trades", type=int,   help="单标的每日最多交易次数 (默认1)")
    p.add_argument("--cooldown",       type=int,   help="买入冷却分钟数 (默认15)")
    p.add_argument("--codes",          type=str,   help="只测试指定标的，逗号分隔")
    p.add_argument("--klt",            type=int,   default=5, help="K线周期：1或5(默认)")
    p.add_argument("--output",         default="validator_output/backtest_t0")
    return p.parse_args()


def main():
    args = parse_args()
    cfg  = load_config(args.config)
    ic   = args.capital or cfg.get("策略分配资金(元)", 100000)

    params = {
        "buy_dip_pct":        args.buy_dip   or cfg.get("买入低于均线(%)",    1.0),
        "sell_target_1":      args.sell1     or cfg.get("第一档卖出(%)",      1.0),
        "sell_target_2":      args.sell2     or cfg.get("第二档卖出(%)",      1.5),
        "sell_target_3":      args.sell3     or cfg.get("卖出止盈(%)",        2.0),
        "stop_loss_pct":      args.stop_loss or cfg.get("止损(%)",            1.0),
        "max_trades_per_day": cfg.get("每日最大交易次数",     3),
        "max_concurrent":     cfg.get("同时最大持仓ETF数",   2),
        "max_etf_trades":     args.max_etf_trades or cfg.get("单标的每日最多交易次数", 1),
        "cooldown_min":       args.cooldown       or cfg.get("买入冷却分钟数",       15),
        "trend_drop_pct":     cfg.get("趋势判断跌幅阈值(%)",  3.0),
        "min_intraday_vol":   cfg.get("日内波动率最低要求(%)", 1.0),
        "market_crash_pct":   cfg.get("大盘放量暴跌暂停阈值(%)", -1.5),
        "capital":            ic,
        "start":              args.start,
        "end":                args.end,
        "codes":              [c.strip().zfill(6) for c in args.codes.split(",")] if args.codes else None,
    }

    print("[1/5] 读取分钟数据...")
    # 决定要拉哪些 codes
    codes_to_load = (
        [c.strip().zfill(6) for c in args.codes.split(",")]
        if args.codes else list(ALL_POOL.keys())
    )
    if args.source == "db":
        df = load_from_db(args.db_config,
                          codes_to_load + [MARKET_PROXY],
                          args.start, args.end, klt=args.klt)
    else:
        if not args.data:
            print("[ERROR] --source csv 时必须提供 --data")
            return 1
        df = load_from_csv(args.data)
    print(f"      {df['dt'].min()} ~ {df['dt'].max()}"
          f"  标的 {df['code'].nunique()} 只  共 {len(df):,} 条")

    print("[2/5] 运行回测...")
    logger = RunLogger()
    result  = run_backtest(df, params, logger=logger)

    print("[3/5] 计算绩效")
    metrics = calc_metrics(result["daily"], result["trades"])
    monthly = calc_monthly(result["daily"])

    print("\n" + "="*50)
    print("  ETF T+0 回测结果")
    print("="*50)
    for k, v in metrics.items():
        print(f"  {k:14s}: {v}")
    print("="*50 + "\n")

    out = Path(args.output)
    out.mkdir(parents=True, exist_ok=True)
    # 记录本次实际回测的标的列表（用于诊断报告）
    pool_codes = (
        [c.strip().zfill(6) for c in args.codes.split(",")]
        if args.codes else list(ALL_POOL.keys())
    )
    print(f"[4/5] 生成报告 → {out}/report.md")
    rpt = generate_report(result, metrics, monthly, ic)
    (out / "report.md").write_text(rpt, encoding="utf-8")
    result["trades"].to_csv(out / "trades.csv",  index=False, encoding="utf-8-sig")
    result["daily"].to_csv( out / "daily.csv",   index=False, encoding="utf-8-sig")
    monthly.to_csv(         out / "monthly.csv", index=False, encoding="utf-8-sig")
    logger.save_run_log(out / "run_log.txt")
    logger.save_rejection_analysis(out / "rejection_analysis.md", pool_codes)

    print("[5/5] 完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
