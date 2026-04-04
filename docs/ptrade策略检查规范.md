# PTrade 策略上线前检查规范

> 基准参考：`etf23轮动防过热.txt`（已验证回测+实盘下单均正常）  
> 所有新策略写完后，必须逐项对照本规范过一遍。

---

## 一、初始化（initialize）

### ✅ 必须有
```python
def initialize(context):
    # 1. 开启服务端重启保护 + 接收撤单回调
    set_parameters(server_restart_not_do_before="1", receive_cancel_response="1")

    # 2. 全局状态变量，必须在此初始化（防止 before_trading_start 访问时报 AttributeError）
    g.pending_order = {'code': None, 'side': None, 'order_id': None}
    g.current_holding = None
    # ... 其他 g.* 变量

    # 3. 注册定时任务（最多5个）
    run_daily(context, func_name, time='HH:MM')   # 必须传 context，必须用具名函数

    # 4. 设置标的池
    set_universe(g.codes)

    # 5. 回测专用设置（必须包在 if not is_trade(): 里）
    if not is_trade():
        set_commission(commission_ratio=0.0003)
        set_slippage(slippage=0.001)
        set_benchmark('513100.SS')
```

### ❌ 禁止出现（JoinQuant 专属，PTrade 不支持）
```python
set_option('use_real_price', True)       # ❌ 删除
set_option('avoid_future_data', True)    # ❌ 删除
log.set_level('system', 'error')         # ❌ 删除
```

---

## 二、时间获取

| 场景 | ✅ PTrade 写法 | ❌ JoinQuant 写法（禁用）|
|------|--------------|------------------------|
| 当前时间 | `context.blotter.current_dt` | `context.current_dt` |
| 今日日期字符串 | `context.blotter.current_dt.strftime('%Y-%m-%d')` | `context.current_dt.date()` |
| 昨日日期 | 手动偏移或用 `get_all_trades_days()` 查前一个交易日 | `context.previous_date` |

> `context.previous_date` 在 PTrade 中不存在，会直接报错。

---

## 三、持仓判断

### ✅ 正确写法（enable_amount = T+1后可卖数量）
```python
pos = context.portfolio.positions.get(code)
sell_amount = int(float(pos.enable_amount))   # PTrade 专用字段
if sell_amount <= 0:
    log.info('T+1限制，当日买入无法卖出，跳过')
    return
```

### ❌ 错误写法
```python
pos.closeable_amount    # ❌ JoinQuant 字段，PTrade 不支持
```

---

## 四、下单

### ✅ 推荐写法（限价单，更可靠）
```python
# 买入：取快照价 * 1.002 上浮，确保能成交
if is_trade():
    price = round(float(get_snapshot(code)[code]['last_px']) * 1.002, 3)
else:
    # 回测无快照，用 get_history 取昨收近似
    price = ...
shares = (int(available_cash / price) // 100) * 100   # 整百股
oid = order(code, shares, limit_price=price)

# 卖出：取快照价 * 0.998 下浮
sell_price = round(float(get_snapshot(code)[code]['last_px']) * 0.998, 3)
oid = order(code, -enable_amount, limit_price=sell_price)
```

### ⚠️ 注意事项
- `order_value(code, value)` / `order_target_value(code, 0)` 本身可用，但须先检查 `enable_amount`
- 实盘必须有 `limit_price`，否则部分柜台拒绝市价单
- 下单后必须记录 `g.pending_order`，防止每分钟重复触发

---

## 五、防重复下单（g.pending_order）

```python
# initialize 中初始化
g.pending_order = {'code': None, 'side': None, 'order_id': None}

# 买入前检查
if g.pending_order.get('side') == 'buy' and code[:6] == g.pending_order.get('code', '')[:6]:
    log.info('已有未成交买单，等待')
    return

# 卖出前检查
if g.pending_order.get('side') == 'sell' and code[:6] == g.pending_order.get('code', '')[:6]:
    log.info('已有未成交卖单，等待')
    return

# 下单后记录
oid = order(code, shares, limit_price=price)
if oid is not None:
    g.pending_order = {'code': code, 'side': 'buy', 'order_id': oid}
```

---

## 六、文件读取（config / state）

```python
import json

def _get_file_path(filename):
    try:
        return get_research_path() + '/' + filename
    except Exception:
        return filename

def _load_json(filename):
    try:
        path = _get_file_path(filename)
        with open(path, 'r', encoding='utf-8') as f:
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
    os.rename(tmp, path)   # 原子替换，防崩溃导致文件损坏
```

> 不使用原始的 `open(filename)` 或相对路径；必须走 `get_research_path()` 拼接绝对路径。

---

## 七、实盘回调（on_order_response / on_trade_response）

> 纯回测策略可省略，但含实盘逻辑的策略**必须实现**。

```python
def on_order_response(context, order_list):
    """委托状态主推回调"""
    if not is_trade():
        return
    # 终结态（全成5/撤单6/废单9）刷新 pending_order
    for item in _normalize_payload(order_list):
        status = str(item.get('status', ''))
        if status in ['5', '6', '8', '9']:
            _refresh_pending_order()

def on_trade_response(context, trade_list):
    """成交明细主推回调"""
    if not is_trade():
        return
    for item in _normalize_payload(trade_list):
        real_type = str(item.get('real_type', '0'))
        if real_type == '2':          # 撤单，不是真实成交
            _refresh_pending_order()
            continue
        entrust_bs = str(item.get('entrust_bs', ''))
        if entrust_bs == '1':         # 买入成交
            g.current_holding = ...
            g.pending_order = {'code': None, 'side': None, 'order_id': None}
        elif entrust_bs == '2':       # 卖出成交
            g.pending_order = {'code': None, 'side': None, 'order_id': None}
```

---

## 八、get_snapshot 用法

```python
snap = get_snapshot(code)         # 传单个 code 或 list
info = snap.get(code)
if info is None:
    # 兜底：按前6位匹配（不同后缀格式问题）
    for k in snap:
        if k[:6] == code[:6]:
            info = snap[k]
            break
last_px    = float(info.get('last_px', 0))
preclose   = float(info.get('preclose_px', 0))
open_px    = float(info.get('open_px', 0))
high_limit = float(info.get('up_px', 0))     # 涨停价字段名是 up_px
```

> `get_snapshot` 仅在 `is_trade()` 下可用；回测中调用会报错，必须用 `if is_trade():` 包裹。

---

## 九、run_daily 注册

```python
# ✅ 正确：必须传 context，必须用具名函数（不能用 lambda）
run_daily(context, my_func, time='14:50')

# ❌ 错误写法
run_daily(my_func, time='14:50')                    # 少了 context
run_daily(context, lambda ctx: do_something(ctx), time='14:50')   # lambda 序列化风险
```

PTrade run_daily 上限为 **5个**，超出会报错。

---

## 十、check_limit / filter_stock_by_status

```python
# 检查单只标的涨跌停
result = check_limit(code)
# result 可能是 dict 或 int，需做兼容处理：
if isinstance(result, dict):
    flag = result.get(code, 0)
else:
    flag = result
# 1=涨停，-1=跌停，0=正常

# 批量过滤停牌/退市
tradable = filter_stock_by_status(codes, filter_type=['HALT', 'DELISTING'])
```

---

## 十一、回测时间范围

PTrade 回测有效时间范围为 `09:31 ~ 15:00`：
- `15:05` 的任务在回测中**不执行**，实盘正常执行
- 解决方案：`run_daily(context, func, time='14:59' if not is_trade() else '15:05')`

---

## 十二、上线前逐项自查清单

```
[ ] 1.  initialize 中有 set_parameters(...)
[ ] 2.  无 set_option('use_real_price',...) / set_option('avoid_future_data',...)
[ ] 3.  无 log.set_level(...)
[ ] 4.  时间全部用 context.blotter.current_dt，无 context.current_dt / context.previous_date
[ ] 5.  卖出前检查 pos.enable_amount（非 closeable_amount），> 0 才下单
[ ] 6.  买卖单均有 g.pending_order 防重复下单
[ ] 7.  initialize 中调用了 set_universe(codes)
[ ] 8.  回测设置包在 if not is_trade(): 里
[ ] 9.  run_daily 传了 context，用具名函数，总数 ≤ 5
[ ] 10. get_snapshot 只在 is_trade() 内调用
[ ] 11. 实盘下单有 limit_price
[ ] 12. 含实盘逻辑时实现了 on_order_response / on_trade_response
[ ] 13. 文件读写通过 get_research_path() 拼接路径
[ ] 14. g.* 全局变量在 initialize 中全部初始化
[ ] 15. 回测有效时间段内无超出 09:31~15:00 的定时任务（或已做兼容）
```

---

## 附：两个转换文件当前已知问题

### 首板低开5止损_ptrade.py
| 行号 | 问题 | 修改方案 |
|------|------|---------|
| 27 | `set_option('use_real_price', True)` | 删除 |
| 28 | `set_option('avoid_future_data', True)` | 删除 |
| 29 | `log.set_level('system', 'error')` | 删除 |
| initialize | 缺 `set_parameters(...)` | 补加 |
| initialize | 缺 `set_universe` / 回测设置 | 补加 |
| initialize | 缺 `g.pending_order` 初始化 | 补加 |
| 48,49 | `context.previous_date` / `context.current_dt` | 改用 `context.blotter.current_dt` |
| 149 | `order_value(code, cash)` 无 limit_price，无 T+1 检查 | 改为检查 enable_amount + limit 单 |
| 189,203,209,215 | `order_target_value(code, 0)` 未检查 enable_amount | 先检查再下单 |
| 195 | `pos.closeable_amount` | 改为 `pos.enable_amount` |
| 150 | `get_stock_name([code])` | 验证 Ptrade API 名称 |
| 全文 | 缺 `on_order_response` / `on_trade_response` | 补加（实盘用） |
| 全文 | 缺 `g.pending_order` 防重复下单 | 补加 |

### 集合竞价三合一_ptrade.py
| 行号 | 问题 | 修改方案 |
|------|------|---------|
| 39-41 | `set_option` / `log.set_level` | 删除 |
| initialize | 缺 `set_parameters(...)` | 补加 |
| initialize | 缺 `set_universe` / 回测设置 | 补加 |
| initialize | `g.target_list` / `g.target_list2` 未初始化 | 补 `g.target_list = []` |
| 87-89 | `context.previous_date` / `context.current_dt` | 改用 `context.blotter.current_dt` |
| 265 | `order_value(s, value)` 无 limit_price，无 T+1 检查 | 改为 enable_amount + limit 单 |
| 303,311,333,335 | `pos.closeable_amount` / `order_target_value` 未检 T+1 | 同上 |
| 全文 | 缺 `on_order_response` / `on_trade_response` | 补加 |
| 全文 | 缺 `g.pending_order` 防重复 | 补加 |
