---
description: ETF12 轮动策略回测 - 传入日线 CSV，输出总收益/月收益/交易记录，并分析弱点给出改进建议
---

# ETF12 策略回测 Skill

对 ETF12 轮动策略跑历史回测，输出完整绩效报告，再结合策略规则分析弱点并给出参数调整建议。

## 用法

```
/backtest-etf12 <数据CSV路径> [选项]
```

**示例：**
```
/backtest-etf12 data/etf_daily.csv
/backtest-etf12 data/etf_daily.csv --start 2023-01-01 --end 2026-04-01
/backtest-etf12 data/etf_daily.csv --trailing-stop 3.0 --buy-threshold 1.0
```

**数据 CSV 格式（最少需要这三列）：**
```
date,code,close
2024-01-02,518880.SS,5.123
2024-01-02,513100.SS,3.456
...
```

---

## 执行步骤

### 第一步：检查数据文件

读取用户提供的 CSV 路径（即 `$ARGUMENTS` 中第一个参数），确认文件存在且包含必要列。
如果用户没有指定数据文件，提示：
> 请提供 ETF 日线 CSV 文件路径。可以用 `tools/download_eastmoney_etf_history.py` 先下载数据。

### 第二步：运行回测

```bash
python tools/backtest_etf12.py --data $ARGUMENTS
```

如果 `$ARGUMENTS` 中包含额外参数（如 `--trailing-stop`、`--start`），原样追加到命令后面。

### 第三步：读取并展示报告

读取 `validator_output/backtest_etf12/report.md`，向用户完整展示：
- 总收益、年化收益、最大回撤、夏普比率
- 月度收益表（正/负月份数量）
- 近期每日净值
- 交易记录（按收益排序，标注最大盈利和最大亏损的交易）

### 第四步：分析弱点并给出改进建议

结合报告数据，按以下维度分析，并给出 **具体的参数调整方向**：

1. **止损是否过松/过紧**
   - 如果最大回撤 > 15%，建议收紧 `trailing_stop`（如从 4% 降到 3%）
   - 如果胜率 < 40% 且平均亏损 > 平均盈利，建议检查 `sell_low` 阈值

2. **准入门槛是否合适**
   - 如果空仓期过长（看交易记录中无买入的时间段），建议降低 `buy_threshold`
   - 如果频繁买入后立刻止损，建议提高 `buy_threshold`

3. **月度分布是否集中**
   - 如果盈利集中在少数几个月，说明策略依赖行情，需考虑加防御 ETF
   - 如果连续 3 个月以上亏损，说明趋势跟随策略在震荡市无效

4. **持仓时间是否过短**
   - 平均持仓天数 < 3 天：过于敏感，建议放宽 `trailing_stop`
   - 平均持仓天数 > 20 天：过于迟钝，建议收紧止损

5. **与基准对比**
   - 对比纳指ETF（513100）同期收益
   - 如果跑输基准，说明轮动选股没有价值，需要重新审视排名逻辑

### 第五步：提供优化命令

根据以上分析，给出 **2-3 个具体的参数组合** 供用户尝试，格式：

```bash
# 组合1：收紧止损，降低准入（更保守）
python tools/backtest_etf12.py --data <文件> --trailing-stop 3.0 --buy-threshold 1.0

# 组合2：放宽准入，加大周期（更趋势）
python tools/backtest_etf12.py --data <文件> --ranking-period 25 --buy-threshold 2.0

# 组合3：当前参数加时间范围对比
python tools/backtest_etf12.py --data <文件> --start 2024-01-01
```

---

## 全部命令行参数参考

| 参数 | 说明 | 默认值（来自配置文件） |
|------|------|----------------------|
| `--data` | 价格 CSV 文件路径（必填） | — |
| `--config` | 策略参数 JSON 文件 | `etf12_config.json` |
| `--start` | 回测开始日期 | 数据最早日期 |
| `--end` | 回测结束日期 | 数据最新日期 |
| `--trailing-stop` | 移动止损回撤 % | 4.0 |
| `--buy-threshold` | 13日准入涨幅 % | 1.5 |
| `--sell-low` | 20日动量割肉线 % | -2.5 |
| `--capital` | 起始资金 | 150000 |
| `--ranking-period` | 排名周期（天） | 20 |
| `--buy-period` | 准入计算周期（天） | 13 |
| `--output` | 输出目录 | `validator_output/backtest_etf12` |

---

## 数据来源

没有 CSV？用项目内已有工具下载：

```bash
# 用东方财富接口下载 ETF12 日线历史（输出 data/etf12_daily.csv）
python tools/download_eastmoney_etf_history.py
```
