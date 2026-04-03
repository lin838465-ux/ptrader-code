# PTrade 策略开发规则

本文件是基于 ETF轮动防过热策略 三天实战开发中踩过的所有坑，提炼出的强制性规则。
所有 PTrade 策略的生成、修改、审查都必须严格遵守。

---

## 一、get_history 使用铁律

### 1. 回测模式 handle_data 中获取"当前价格"必须用分钟线

```python
# ✅ 正确：回测中 handle_data 每分钟执行，必须用 '1m' 拿实时价格
get_history(1, '1m', 'close', code, fq='pre', include=True, is_dict=True)

# ❌ 错误：日线在盘中只返回一个固定收盘价，整天不变，无法追踪盘中价格变化
get_history(2, '1d', 'close', code, fq='pre', include=True, is_dict=True)
```

**原因**：日线 `'1d'` 在回测中返回的是当天的日收盘价，整个交易日内不会变。
用它做分钟级扫描，最高价永远不会更新，止损永远不会触发。

### 2. 获取"昨日收盘价"（preclose）可以用日线

```python
# ✅ 正确：preclose 就是要昨天的收盘价，用日线没问题
raw = get_history(2, '1d', 'close', code, fq='pre', include=True, is_dict=True)
preclose = cl[-2]  # 倒数第二根 = 昨收
```

### 3. 获取"历史 N 日数据"用于排名计算，用 include=False

```python
# ✅ 正确：排名需要过去 N 天的历史（不含今天），今天价格单独从分钟线取
raw = get_history(count, '1d', 'close', code, fq='pre', include=False, is_dict=True)
today_price = _get_current_price(context, code)  # 分钟线
```

### 4. is_dict=True 时返回完整 8 列，不受 field 参数影响

```python
# is_dict=True 时，不管 field 传 'close' 还是 'high'，返回的都是：
# (timestamp, open, high, low, close, volume, money, price)
# 索引:  0        1     2     3     4       5       6      7
#
# ✅ 一次 API 调用可以同时拿到 close 和 high：
row = arr[-1]
close_val = float(row[4])  # close
high_val = float(row[2])   # high
```

### 5. 拿不到分钟数据不要回退到日线

```python
# ❌ 错误：回退日线会拿到错误的价格，导致最高价跟踪失败
if 分钟线失败:
    raw = get_history(2, '1d', 'close', ...)  # 千万别这样！

# ✅ 正确：拿不到就跳过这一轮扫描，等下一分钟
if 分钟线失败:
    return None
```

### 6. get_history 不支持多线程同时调用

run_daily 和 handle_data 在同一分钟触发时（如 14:50），两者同时调 get_history
可能偶尔返回空数据。代码必须处理 None 返回值，不能假设一定有数据。

---

## 二、最高价追踪规则

### 1. 回测模式必须同时用 close 和 high 更新最高价

```python
# ✅ 正确：close 是分钟收盘价，high 是分钟内最高价（可能更高）
_update_highest_price(code, current_price)   # close
if minute_high > 0:
    _update_highest_price(code, minute_high)  # high
```

**原因**：如果只看 close，分钟K线内的尖峰（high > close）会被漏掉，
导致实际最高价 3.50 但代码记录的是 3.48（close），回撤计算不准确。

### 2. 最高价只在涨的时候更新

```python
if price > g.highest_price:
    g.highest_price = price  # 只有新价格更高才更新
```

跌的时候不更新是正确行为 —— 这就是"从最高点回撤"止损的核心逻辑。

### 3. 最高价必须持久化到文件

每次更新最高价都要立刻写入 state 文件，防止盘中重启后最高价丢失。

### 4. 跨日恢复最高价时必须校验持仓代码

```python
# ✅ 正确：持仓代码匹配才恢复，不匹配说明已换仓，必须清零
if saved_code[:6] == current_holding[:6]:
    g.highest_price = saved_price
else:
    g.highest_price = 0.0  # 换仓了，从头开始跟踪
```

---

## 三、API 使用约束

### 1. get_snapshot() 只能在实盘使用

```python
# ✅ 正确
if is_trade():
    snap = get_snapshot(code)

# ❌ 错误：回测中 get_snapshot 不可用
snap = get_snapshot(code)  # 回测会返回 None 或报错
```

### 2. order() 返回值必须检查

```python
oid = order(code, shares, limit_price=price)
if oid is not None:
    # 委托成功
else:
    # 委托失败，必须处理
```

### 3. check_limit() 返回值含义

```
 2 = 触板涨停（有卖盘但价格封顶）
 1 = 涨停
 0 = 正常
-1 = 跌停
-2 = 触板跌停（有买盘但价格封底）
```

- 买入时检查 `>= 1`（涨停和触板涨停都不买）
- 卖出时检查 `== -1`（跌停无法卖出，-2 触板跌停仍可能卖出）

### 4. run_daily() 限制

- 最多 5 个（run_daily + run_interval 合计）
- 回测时间范围：09:31~11:30, 13:00~15:00
- 实盘无时间限制

### 5. on_trade_response 回测中不执行

```python
# on_trade_response 只在实盘触发，回测不执行！
# 所以回测中的状态初始化（如最高价）必须在下单时就完成：
if not is_trade():
    if buy_price > g.highest_price:
        g.highest_price = buy_price
```

### 6. context.portfolio.positions 字段

- `amount`：持仓总量
- `enable_amount`：可卖数量（T+1 下当日买入的不可卖）
- 卖出时必须用 `enable_amount`，不能用 `amount`

---

## 四、代码结构规范

### 1. 代码提取复用字典键名匹配

PTrade 返回的代码后缀可能不一致（.SS/.XSHG, .SZ/.XSHE），
所有取数据的地方都必须做后缀兼容：

```python
# ✅ 必须处理后缀映射
if code in raw:
    arr = raw[code]
else:
    for old, new in [('.SS', '.XSHG'), ('.SZ', '.XSHE'),
                     ('.XSHG', '.SS'), ('.XSHE', '.SZ')]:
        alt = code.replace(old, new)
        if alt in raw:
            arr = raw[alt]
            break
    if arr is None:
        # 最后兜底：用前6位匹配
        for k in raw:
            if k[:6] == code[:6]:
                arr = raw[k]
                break
```

### 2. 同一标的判断用前6位

```python
def _same_security(code_a, code_b):
    return str(code_a)[:6] == str(code_b)[:6]
```

### 3. 状态文件写入用原子操作

```python
# ✅ 先写 .tmp 再 rename，防止写入过程中崩溃导致文件损坏
with open(tmp_path, 'w') as f:
    f.write(json.dumps(data))
os.rename(tmp_path, path)
```

### 4. 防止重复下单

每次下单前必须检查是否已有同方向的未完成委托：

```python
if pending.get('side') == 'sell' and _same_security(pending.get('code'), code):
    return  # 已有卖单在途，不重复下
```

### 5. 卖出后当日禁止回买

```python
g.sold_today_codes.add(code[:6])  # 卖出时立刻标记
# 买入时检查
if code[:6] in g.sold_today_codes:
    return  # 今日已卖出，不买回
```

---

## 五、回测 vs 实盘 分支检查清单

每个涉及"获取价格"或"下单"的函数，都必须区分 `is_trade()` 分支：

| 功能 | 实盘 (is_trade=True) | 回测 (is_trade=False) |
|------|---------------------|----------------------|
| 获取当前价格 | `get_snapshot()` → `last_px` | `get_history(1, '1m', ...)` |
| 获取昨收 | `get_snapshot()` → `preclose_px` | `get_history(2, '1d', ...)` → `cl[-2]` |
| 买入报价 | `get_snapshot` + 上浮 0.2% | `get_history(1m)` + 上浮 0.2% |
| 卖出报价 | `get_snapshot` + 下浮 0.2% | 市价单（无 limit_price） |
| 成交回调 | `on_trade_response` 处理 | 不执行，下单时直接初始化状态 |
| 最高价追踪 | `get_snapshot` → `last_px` | `get_history(1m)` → close + high |

---

## 六、常见 Bug 模式（开发时必须自查）

1. **日线当分钟用**：回测中用 `'1d'` 获取"当前价格" → 整天不变 → 最高价不更新
2. **忘记处理 None**：get_history/get_snapshot 可能返回 None，必须检查
3. **回退到错误数据源**：分钟线拿不到就回退日线 → 拿到错误价格 → 不如不拿
4. **重复 API 调用**：is_dict=True 返回全部字段，不需要分别取 close 和 high
5. **on_trade_response 回测不触发**：回测中依赖成交回调初始化状态 → 状态永远为空
6. **enable_amount vs amount**：T+1 下用 amount 卖出会包含当日买入无法卖出的部分
7. **代码后缀不一致**：.SS vs .XSHG, .SZ vs .XSHE，不做映射会取不到数据
