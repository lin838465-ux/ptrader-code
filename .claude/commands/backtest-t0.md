---
description: ETF T+0 做T策略回测 - 用5分钟K线模拟VWAP低吸+分档止盈，输出聚宽风格完整绩效报告（总收益/日收益/月收益），并给出优化建议
---

# ETF T+0 做T策略回测 Skill

对 **ETF T+0 日内做T策略（VWAP低吸 + 分档止盈）** 跑历史回测，使用 5 分钟 K 线模拟。输出类聚宽格式的完整绩效报告，并分析弱点给出优化建议。

默认初始资金：**100,000 元**

> **注意**：T+0 策略必须用分钟级数据，日线数据无法回测。

---

## 用法

```
/backtest-t0 [选项]
```

**示例：**
```
# 从数据库读取（需先用 fetch_etf_kline.py 同步数据）
/backtest-t0 --source db --start 2024-01-01

# 从 CSV 读取
/backtest-t0 --source csv --data data/etf_t0_5min.csv

# 调整参数
/backtest-t0 --source db --buy-dip 1.5 --stop-loss 1.5
```

---

## 执行步骤

### 第一步：确认数据来源

**方式 A：从数据库读取（推荐）**

先确认 5 分钟 K 线数据已同步到数据库：
```bash
# 检查数据库配置
cat db_config.json

# 下载/更新 5 分钟 K 线（T0 池是跨境ETF，代码不在 ETF12 里）
# 需要先在 fetch_etf_kline.py 里临时修改 ETF 池为 T0 池的代码
python scripts/fetch_etf_kline.py
```

T0 池主力 ETF（跨境 T+0）：
- 513180（恒生科技）、513330（恒生互联网）、513050（中概互联）
- 162411（华宝油气）、513100（纳指）、159518（日经）
- 159980（有色金属）、518880（黄金）等

**方式 B：从 CSV 读取**

CSV 必须包含：`dt（datetime）, code, open, high, low, close, volume, amount`

### 第二步：运行回测

**从数据库：**
```bash
python tools/backtest_t0.py --source db --start $START_DATE
```

**从 CSV：**
```bash
python tools/backtest_t0.py --source csv --data $CSV_PATH
```

如果 $ARGUMENTS 中有额外参数，原样追加。

### 第三步：展示回测报告

读取 `validator_output/backtest_t0/report.md`，重点展示：
- 总收益和年化收益
- 每日 P&L 分布（有多少天亏损？最大单日亏损？）
- 月度收益热力图
- 最近 100 笔交易记录
- 止盈/止损/清仓原因分布统计

对交易记录做统计，输出：
```
止损比例:    X%
一档止盈:    X%
二档止盈:    X%
三档全清:    X%
强制清仓:    X%
```

### 第四步：分析弱点并给出优化建议

1. **买入频率是否合理**
   - 日均交易次数 < 1：买入条件过严（VWAP偏离要求太高），建议降低 `--buy-dip`（如从1.0%到0.8%）
   - 日均交易次数 > 2.5：买入过频，止损次数会累积，检查是否需要提高偏离要求

2. **止损率是否过高**
   - 止损次数/总次数 > 40%：策略亏多赢少，说明入场信号质量差
   - 可能原因：买入时未真正企稳，建议加强趋势过滤（降低 `--trend-drop-pct` 为2%）

3. **强制清仓占比**
   - 强制清仓次数/总次数 > 30%：说明持仓时间内涨幅不够，+1%/+1.5%/+2%阈值过高
   - 建议降低 `--sell1` 到 0.8% 或 `--sell3` 到 1.5%

4. **月度亏损集中**
   - 连续亏损月：可能是整体市场行情差，跨境ETF普遍弱势
   - 建议检查当时市场环境（港股/美股）

5. **单日最大亏损**
   - 最大单日亏损 > 0.5%：说明极端行情下止损被放大，检查是否可以加大盘情绪过滤

### 第五步：给出参数对比命令

```bash
# 方案A：更保守（更高VWAP偏离要求，更快止盈）
python tools/backtest_t0.py --source db --buy-dip 1.5 --sell3 1.5

# 方案B：降低止损敏感度（拉开止损空间）
python tools/backtest_t0.py --source db --stop-loss 1.5 --buy-dip 0.8

# 方案C：不同时间段对比
python tools/backtest_t0.py --source db --start 2025-01-01
```

---

## 参数参考

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--source` | 数据来源: `csv` 或 `db` | `csv` |
| `--data` | CSV 文件路径 (`--source csv` 时必填) | — |
| `--db-config` | 数据库配置 JSON | `db_config.json` |
| `--config` | 策略参数 JSON | `etf_t0_config.json` |
| `--start` | 回测开始日期 | 数据最早日期 |
| `--end` | 回测结束日期 | 数据最新日期 |
| `--buy-dip` | 低于 VWAP 的买入阈值 % | 1.0 |
| `--sell1` | 第一档止盈 % | 1.0 |
| `--sell2` | 第二档止盈 % | 1.5 |
| `--sell3` | 第三档全清 % | 2.0 |
| `--stop-loss` | 止损 % | 1.0 |
| `--capital` | 初始资金（元） | 100000 |
| `--output` | 输出目录 | `validator_output/backtest_t0` |

---

## T+0 回测精度说明

| 项目 | 实盘 | 回测近似 |
|------|------|---------|
| VWAP | 逐笔计算 | 5分钟累计成交额/成交量 |
| 量比/内外盘 | PTrade原生 | 已简化（不过滤） |
| 买入信号 | 每分钟 | 每5分钟K线收盘时判断 |
| 成交价 | 挂单成交 | 当根K线收盘价 |
| 滑点 | 约0.2% | 未计入 |

> T+0 回测的精度低于日线策略，实际表现可能比回测更好（有量能过滤）也可能更差（有滑点）。回测结论应结合实际盘中观察。
