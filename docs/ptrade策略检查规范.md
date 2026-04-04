# PTrade 策略检查规范

> **基准参考**: `etf23轮动防过热.txt`（唯一验证通过回测+实盘下单均正常的策略）
>
> **Scope**: 股票、ETF 量化策略（不含期货/期权/可转债）
>
> **数据来源**: ptradeapi.com 官方 API 文档

---

## 工作流概览

```
第一步：strategy_spec_XXX.md（纯文本规格文档）
              ↓
第二步：checklist_XXX.md（逐条打钩，100%通过才继续）
              ↓
第三步：生成完整代码 → git 推送
              ↓
第四步：PTrade 回测 → 实盘
```

---

## 15条核心检查清单

每个策略都必须生成 checklist 并逐条打钩：

| # | 检查项 | 状态 | 位置/说明 |
|---|--------|------|-----------|
| 1 | initialize 中有 `set_parameters(server_restart_not_do_before="1", receive_cancel_response="1")` | ✅/❌ | |
| 2 | 无 `set_option('use_real_price',...)` / `set_option('avoid_future_data',...)` | ✅/❌ | |
| 3 | 无 `log.set_level(...)` | ✅/❌ | |
| 4 | 时间全部用 `context.blotter.current_dt`，无 `context.current_dt` / `context.previous_date` | ✅/❌ | |
| 5 | 卖出前检查 `pos.enable_amount`（非 `closeable_amount`），> 0 才下单 | ✅/❌ | |
| 6 | 买卖单均有 `g.pending_order` 防重复下单 | ✅/❌ | |
| 7 | initialize 中调用了 `set_universe(codes)` | ✅/❌ | |
| 8 | 回测设置包在 `if not is_trade():` 里 | ✅/❌ | |
| 9 | `run_daily` 传了 context，用具名函数，总数 ≤ 5 | ✅/❌ | |
| 10 | `get_snapshot` 只在 `is_trade()` 内调用 | ✅/❌ | |
| 11 | 实盘下单有 `limit_price`，且价格小数位正确（股票2位/ETF3位） | ✅/❌ | |
| 12 | 含实盘逻辑时实现了 `on_order_response` / `on_trade_response` | ✅/❌ | |
| 13 | 文件读写通过 `get_research_path()` 拼接路径 | ✅/❌ | |
| 14 | `g.*` 全局变量在 `initialize` 中全部初始化 | ✅/❌ | |
| 15 | 回测有效时间段内无超出 09:31~15:00 的定时任务（或已做兼容） | ✅/❌ | |

---

## 问题严重性分级

### 高危（直接报错/下错单）

| 问题 | 影响 |
|------|------|
| `set_option('use_real_price',...)` / `set_option('avoid_future_data',...)` | PTrade 不支持，initialize 直接报错 |
| `context.previous_date` / `context.current_dt` | PTrade 不存在，运行时报错 |
| `pos.closeable_amount` | PTrade 用 `enable_amount`，T+1 当日买入错误尝试卖出 |
| 卖出前未检查 `enable_amount` | T+1 当日买入触发卖出委托，柜台拒绝，无限重试 |
| limit_price 小数位错误 | 股票超过2位/ETF超过3位，委托失败 |

### 中危（实盘会出问题）

| 问题 | 影响 |
|------|------|
| 缺 `set_parameters(...)` | 不接收撤单/成交回调推送 |
| 缺 `on_order_response` / `on_trade_response` | 实盘委托状态无法同步 |
| 缺 `g.pending_order` 防重复 | 每分钟触发一次重复下单 |
| 下单无 `limit_price` | 部分柜台拒绝市价单 |
| `before_trading_start` 中调用实时行情 | 9:10前行情未更新，数据有误 |

### 低危（影响一致性）

| 问题 | 影响 |
|------|------|
| 缺 `set_universe()` | 部分接口可能无默认股票池 |
| 缺回测 `commission/slippage/benchmark` 设置 | 回测手续费/滑点/基准不准确 |
| g变量包含不可序列化对象 | 服务器重启后状态丢失 |

---

## 官方文档核心要点（ptradeapi.com）

### 策略引擎四大事件

| 事件 | 函数 | 触发时机 | 必须？ |
|------|------|---------|--------|
| 初始化 | `initialize(context)` | 回测/交易启动时执行一次 | **必选** |
| 盘前 | `before_trading_start(context, data)` | 每日交易前 | 可选 |
| 盘中 | `handle_data(context, data)` | 按周期频率执行 | **必选** |
| 盘后 | `after_trading_end(context, data)` | 每日交易结束后 | 可选 |

### handle_data 执行时间

| 频率 | 回测执行时间 | 交易执行时间 |
|------|------------|------------|
| 日线 | 15:00 | 券商配置（默认14:50） |
| 分钟线 | 9:31 ~ 15:00 | 9:30 ~ 14:59 |

### 委托状态（status）

| status | 含义 |
|--------|------|
| 0 | 报单 | 1 | 待报 | 2 | 已报 | 3 | 部分成交 |
| 4 | 待确认 | 5 | 全部成交 | 6 | 撤单 | 7 | 改单 |
| 8 | 成交部分撤单 | 9 | 废单 |

### 成交方向（entrust_bs）

| entrust_bs | 含义 |
|------------|------|
| 1 | 买入 |
| 2 | 卖出 |

### 价格小数位要求

| 品种 | 价格小数位 |
|------|-----------|
| 股票 | **2位**（如 12.34） |
| ETF/LOF/可转债 | **3位**（如 1.234） |

### g变量序列化规则

- 全局变量 `g` 会被框架持久化
- 服务器重启恢复交易时，先执行 `initialize`，再恢复持久化变量（**持久化变量会覆盖 initialize 中同名的初始值**）
- `g` 中以 `__` 开头的变量为**私有变量，持久化时不保存**

### before_trading_start 注意事项

- 交易中默认 9:10 执行；回测中 8:30 执行
- 在 9:10 前开启交易时，行情未更新，此时调用实时行情接口会导致数据有误
- 解决方案：在函数内 `sleep` 至 9:10，或把实时行情调用改到 `run_daily` 中

### get_history 注意事项

- `get_history` 与 `get_price` **不支持多线程同时调用**（即在 `run_daily`/`run_interval` 中不要与 `handle_data` 同一时刻调用）
- 推荐 `is_dict=True`（速度快）

---

## JoinQuant 转 PTrade 对照表

| JoinQuant | PTrade |
|-----------|--------|
| `context.current_dt` | `context.blotter.current_dt` |
| `context.previous_date` | `get_trading_day(-1)` |
| `set_option('use_real_price', True)` | **删除** |
| `set_option('avoid_future_data', True)` | **删除** |
| `pos.closeable_amount` | `pos.enable_amount` |
| `order_value(code, cash)` | `order(code, shares, limit_price=price)` |
| `order_target_value(code, 0)` | 先检查 enable_amount，再 `order(code, -amount, limit_price=price)` |
| `log.set_level('system', 'error')` | **删除** |
| `get_stock_name([code])` | `g.code_name.get(code, code)` 或缓存 |

---

## etf23 核心模式（开发模板）

### 全局变量初始化
```python
g.current_holding = None
g.highest_price = 0.0
g.sold_today_codes = set()
g.pending_order = {'code': None, 'side': None, 'order_id': None}
g.switch_target = None
```

### 防重复下单
```python
pending = g.pending_order
if pending.get('side') == 'buy' and _same_security(pending.get('code'), code):
    return  # 已有未完成买单
if pending.get('side') == 'sell' and _same_security(pending.get('code'), code):
    return  # 已有未完成卖单
```

### T+1 卖出检查
```python
sell_amount = int(float(positions[actual_code].enable_amount))
if sell_amount <= 0:
    log.info('T+1限制，跳过')
    return
```

### 涨跌停检查
```python
result = check_limit(code)
if isinstance(result, dict):
    flag = result.get(code, 0)
else:
    flag = result
# 1=涨停，-1=跌停，0=正常
```

### set_parameters 必开项
```python
set_parameters(server_restart_not_do_before="1", receive_cancel_response="1")
```
