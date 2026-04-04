---
name: "ptrade-strategy-developer"
description: "Complete PTrade stock/ETF strategy development workflow. Invoke when developing new strategies, converting JoinQuant strategies to PTrade, or before deploying strategies to production."
---

# PTrade 股票/ETF 策略开发规范

> **基准参考**: `etf23轮动防过热.txt`（唯一验证通过回测+实盘下单均正常的策略）
>
> **Scope**: 股票、ETF 量化策略（不含期货/期权/可转债）

---

## 完整工作流

```
┌─────────────────────────────────────────────────────────────┐
│  第一步：需求 → 输出 strategy_spec_XXX.md（纯文本，不写代码）    │
│           包含：逻辑描述、信号规则、参数配置、文件说明           │
├─────────────────────────────────────────────────────────────┤
│  第二步：对照 etf23轮动防过热.txt + 15条自查清单逐项打钩       │
│           填写 checklist_XXX.md，每一条都必须过               │
├─────────────────────────────────────────────────────────────┤
│  第三步：通过后生成完整策略代码（由本Skill执行）                │
│           代码放入 docs/ 目录，同步到 git 仓库               │
├─────────────────────────────────────────────────────────────┤
│  第四步：PTrade 回测 → 验证 → 实盘模拟 → 实盘                 │
└─────────────────────────────────────────────────────────────┘
```

---

## 第一步：输出策略规格文档（strategy_spec_XXX.md）

每次开发新策略或转化 JoinQuant 策略，先写纯文本规格文档，内容包括：

```markdown
# 策略名称 - PTrade开发规格

## 一、策略概述
[交易品种 / 周期频率 / 核心逻辑一句话描述]

## 二、信号规则

### 买入信号
- 条件1：...
- 条件2：...

### 卖出信号
- 止损：...
- 止盈：...

## 三、参数配置
| 参数名 | 默认值 | 说明 |
|--------|--------|------|

## 四、时间轴
| 时间 | 动作 |
|------|------|
| 09:30 | 买入检查 |
| 14:50 | 卖出检查 |

## 五、参考对比（对照 etf23）
- 与 etf23 相同点：...
- 与 etf23 差异点：...

## 六、PTrade API 差异点（JoinQuant → PTrade）
| JoinQuant | PTrade |
|-----------|--------|

## 七、文件结构
- 状态持久化：...
- 配置读取：...
```

---

## 第二步：对照检查（checklist_XXX.md）

每个策略都必须生成并填写此清单，**逐条打钩确认**：

```markdown
# PTrade 策略上线前检查清单

> 基准参考：`etf23轮动防过热.txt`

## 15条核心检查项

| # | 检查项 | 状态 | 位置/说明 |
|---|--------|------|-----------|
| 1 | initialize 中有 set_parameters(server_restart_not_do_before="1", receive_cancel_response="1") | ✅/❌ | |
| 2 | 无 set_option('use_real_price',...) / set_option('avoid_future_data',...) | ✅/❌ | |
| 3 | 无 log.set_level(...) | ✅/❌ | |
| 4 | 时间全部用 context.blotter.current_dt，无 context.current_dt / context.previous_date | ✅/❌ | |
| 5 | 卖出前检查 pos.enable_amount（非 closeable_amount），> 0 才下单 | ✅/❌ | |
| 6 | 买卖单均有 g.pending_order 防重复下单 | ✅/❌ | |
| 7 | initialize 中调用了 set_universe(codes) | ✅/❌ | |
| 8 | 回测设置包在 if not is_trade(): 里 | ✅/❌ | |
| 9 | run_daily 传了 context，用具名函数，总数 ≤ 5 | ✅/❌ | |
| 10 | get_snapshot 只在 is_trade() 内调用 | ✅/❌ | |
| 11 | 实盘下单有 limit_price，且价格小数位正确（股票2位/ETF3位） | ✅/❌ | |
| 12 | 含实盘逻辑时实现了 on_order_response / on_trade_response | ✅/❌ | |
| 13 | 文件读写通过 get_research_path() 拼接路径 | ✅/❌ | |
| 14 | g.* 全局变量在 initialize 中全部初始化 | ✅/❌ | |
| 15 | 回测有效时间段内无超出 09:31~15:00 的定时任务（或已做兼容） | ✅/❌ | |

## 问题修复记录
[列出所有发现的问题及修复位置]

## 最终结论
✅ 全部通过 → 可进入 PTrade 回测
❌ 未通过 → 修复后重新检查
```

---

## 第三步：etf23轮动防过热.txt 核心模式（开发对照基准）

### 全局变量初始化模板

```python
g.current_holding = None
g.highest_price = 0.0
g.sold_today_codes = set()
g.pending_order = {'code': None, 'side': None, 'order_id': None}
g.switch_target = None
```

### 防重复下单检查模板

```python
# 下单前
pending = g.pending_order
if pending.get('side') == 'buy' and _same_security(pending.get('code'), code):
    log.info('[买入] 已有未完成买单，等待')
    return
if pending.get('side') == 'sell' and _same_security(pending.get('code'), code):
    log.info('[卖出] 已有未完成卖单，等待')
    return

# 下单后
if oid is not None:
    g.pending_order = {'code': code, 'side': 'buy', 'order_id': oid}
```

### 卖出执行模板（含T+1检查）

```python
def _do_sell(context, code, reason):
    positions = context.portfolio.positions
    actual_code = None
    for c in positions:
        if _same_security(c, code) and positions[c].amount > 0:
            actual_code = c
            break
    if actual_code is None:
        return

    sell_amount = int(float(positions[actual_code].enable_amount))
    if sell_amount <= 0:
        log.info('[卖出] T+1限制，跳过')
        return

    limit_flag = _get_limit_flag(actual_code)
    if limit_flag == -1:
        log.warning('[卖出] 跌停板，无法卖出')
        return

    if is_trade():
        sell_price = _get_live_price(actual_code, direction='sell')
        oid = order(actual_code, -sell_amount, limit_price=sell_price)
    else:
        oid = order(actual_code, -sell_amount)

    if oid is not None:
        g.pending_order = {'code': actual_code, 'side': 'sell', 'order_id': oid}
```

### 状态同步模板

```python
def _sync_holding_state(context):
    positions = context.portfolio.positions
    actual = None
    for code in positions:
        if positions[code].amount > 0 and _is_strategy_security(code):
            actual = code
            break
    g.current_holding = actual
    if actual is None:
        if g.pending_order.get('side') == 'buy':
            return actual
        g.highest_price = 0.0
        g.pending_order = {'code': None, 'side': None, 'order_id': None}
    return actual
```

### 回调处理模板

```python
def _normalize_payload(payload):
    if not payload:
        return []
    if isinstance(payload, (list, tuple)):
        return list(payload)
    return [payload]

def _refresh_pending_order():
    open_orders = _get_strategy_open_orders()
    if not open_orders:
        g.pending_order = {'code': None, 'side': None, 'order_id': None}
        return
    pending = open_orders[0]
    symbol = _extract_symbol(pending)
    amount = _extract_amount(pending)
    side = 'sell' if float(amount) < 0 else 'buy'
    g.pending_order = {'code': symbol, 'side': side, 'order_id': getattr(pending, 'id', None)}
```

---

## 第四步：Git 推送时机与规范

```
策略开发 → strategy_spec_XXX.md（第一步）
              ↓
        对照检查 → checklist_XXX.md（第二步）
              ↓
        全部 ✅ 通过？
           ↓ 否    ↓ 是
        修复    生成完整代码
           ↓         ↓
        重新检查   推送 git
                      ↓
                 PTrade回测
                      ↓
                 实盘模拟 → 实盘
```

**Git 推送规范**：
- 规格文档和检查清单先推送（纯文本，变更清晰）
- 代码推送前 checklist 必须 100% 通过
- 提交信息格式：`feat: 完成 [策略名] PTrade转化，checklist全部通过`

---

## 工具函数参考（开发对照）

### 时间获取
```python
def _get_previous_trading_day(context):
    try:
        return get_trading_day(-1)
    except Exception:
        return get_trading_day(context.blotter.current_dt)
```

### 实时价格（股票2位/ETF3位）
```python
def _get_live_price(code, direction='buy'):
    snap = get_snapshot(code)
    info = snap.get(code)
    if info is None:
        for k in snap:
            if k[:6] == code[:6]:
                info = snap[k]
                break
    last_px = float(info.get('last_px', 0))
    if direction == 'buy':
        return round(last_px * 1.002, 3)  # ETF 3位
    else:
        return round(last_px * 0.998, 3)
```

### 文件读写
```python
def _get_file_path(filename):
    try:
        return get_research_path() + '/' + filename
    except Exception:
        return filename

def _load_json(filename):
    try:
        with open(_get_file_path(filename), 'r', encoding='utf-8') as f:
            return json.loads(f.read()) or {}
    except Exception as e:
        log.warning('[文件读取失败] %s | %s' % (filename, str(e)))
        return None

def _save_json(filename, data):
    import os
    path = _get_file_path(filename)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, ensure_ascii=False, indent=2))
    os.rename(tmp, path)
```

---

## 调用场景

**Invoke this skill when:**
- Developing a new PTrade strategy from scratch（先写 spec）
- Converting a JoinQuant strategy to PTrade（先写 spec + checklist）
- Before deploying any strategy to production（run checklist）
- Reviewing another trader's PTrade code before deployment
