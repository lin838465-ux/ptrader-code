---
description: ETF23 防过热轮动策略回测 - 传入日线 CSV，输出聚宽风格完整绩效报告（总收益/月收益/交易记录），并给出参数优化建议
---

# ETF23 防过热轮动策略回测 Skill

对 **ETF23 尾盘轮动（防过热版）** 跑历史回测，输出类聚宽格式的完整绩效报告，并分析弱点给出参数调优方向。

默认初始资金：**100,000 元**

---

## 用法

```
/backtest-etf23 <数据CSV路径> [选项]
```

**示例：**
```
/backtest-etf23 data/etf23_daily.csv
/backtest-etf23 data/etf23_daily.csv --start 2023-01-01 --end 2026-04-01
/backtest-etf23 data/etf23_daily.csv --trailing-stop 4.0 --overheat 4.0
```

---

## 执行步骤

### 第一步：确认数据文件

读取用户提供的 CSV 路径（$ARGUMENTS 中第一个参数）。

如果用户没有数据文件，提示：
```bash
# 先用东方财富接口下载 ETF23 池的日线历史（输出到 data/etf23_daily.csv）
python tools/download_eastmoney_etf_history.py
```
注意：需要在 `download_eastmoney_etf_history.py` 里把 ETF 列表改成 ETF23 池的 23 只。

### 第二步：运行回测

```bash
python tools/backtest_etf23.py --data $ARGUMENTS
```

如果 $ARGUMENTS 中包含额外选项（如 `--trailing-stop`、`--start`），原样追加。

### 第三步：展示回测报告

读取 `validator_output/backtest_etf23/report.md`，完整展示给用户，重点标注：
- **总收益 vs 同期纳指ETF基准**
- **最大回撤发生在哪个月**
- **最大单笔亏损是什么标的/时间**
- **正收益月份占比**

### 第四步：分析弱点并给出优化建议

按以下维度逐项分析：

1. **防过热是否起作用**
   - 看交易记录里有多少次买入当天涨幅>3%的标的
   - 如果过热跳过导致大量空仓机会被错过，建议提高 `--overheat` 值

2. **当日跌幅止损与移动止损重叠**
   - 如果大量卖出原因是"移动止损"但持仓天数很短（<3天），说明移动止损过紧
   - 如果大量是"当日跌幅止损"，说明-5%阈值可能可以放宽

3. **主动换仓频率是否合理**
   - 看交易记录中"主动换仓"原因的次数
   - 换仓后新标的当笔收益是否优于旧标的
   - 如果换仓后立即亏损：建议提高 `--switch-threshold`（如从3%到5%）

4. **市场弱势过滤**
   - 看空仓期是否与市场下行期吻合
   - 如果空仓期太长（超过1个月），说明弱势过滤过于严格

5. **月度收益分布**
   - 连续3月以上亏损：趋势策略在震荡市无效，考虑加黄金ETF防守
   - 盈利过于集中在1-2个月：策略依赖性过强，需要评估池子多样性

### 第五步：给出 2-3 个参数对比命令

根据分析给出具体调优方案，例如：
```bash
# 方案A：更保守（收紧止损，降低准入）
python tools/backtest_etf23.py --data <文件> --trailing-stop 4.0 --buy-threshold 1.0

# 方案B：减少换仓频率（提高换仓阈值）
python tools/backtest_etf23.py --data <文件> --switch-threshold 5.0

# 方案C：不同时间段对比
python tools/backtest_etf23.py --data <文件> --start 2024-01-01
```

---

## 参数参考

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `--data` | 日线 CSV 文件路径（必填） | — |
| `--config` | 策略参数 JSON 文件 | `etf23_halfhour_config.json` |
| `--start` | 回测开始日期 | 数据最早日期 |
| `--end` | 回测结束日期 | 数据最新日期 |
| `--trailing-stop` | 移动止损回撤 % | 5.0 |
| `--sell-threshold` | 当日跌幅止损 % | -5.0 |
| `--buy-threshold` | 13 日准入涨幅 % | 1.5 |
| `--switch-threshold` | 主动换仓领先 % | 3.0 |
| `--overheat` | 防过热今日涨幅上限 % | 5.0 |
| `--capital` | 初始资金（元） | 100000 |
| `--ranking-period` | 排名周期（天） | 20 |
| `--output` | 输出目录 | `validator_output/backtest_etf23` |
