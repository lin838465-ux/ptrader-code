# 任务报告：PTrade ETF激进轮动策略 Bug 修复

> **时间**: 2026-03-31
> **策略**: `etf激进轮动.txt`

## 🛠️ 修改清单

1. **日志方法修复**
   - **问题**：`log.warn` 在 PTrade LogEngine 并不存在。
   - **修复**：文档确认为 `log.warning`，已全局替换策略中散落的 9 处 `log.warn` 调用。

2. **numpy ndarray 比较兼容**
   - **问题**：`回测不支持 ndarray 和 int 做比较`。这是因为 `get_history(is_dict=True)` 返回的是包含多个字段的结构化二维数组，其元素是一个 tuple / string 或 numpy array `[datetime, open, high, low, close, volume, money, price]`。
   - **修复**：已重写 `_extract_close_list` 工具函数。首先判断返回值类型，智能提取索引为 4 的 `close` 价格并强制转换为 `float`，解决了后面的比较逻辑崩溃。

3. **回测配置方法调整**
   - **问题**：`NameError: name 'PerOrder' is not defined`。
   - **修复**：PTrade 的 `set_commission` 直接接收 `commission_ratio` 浮点数，不支持 `PerOrder` 对象，已修改；同理，`set_slippage` 修改为接收 `slippage=0.001`，抛弃了原来多余的 `PriceSlippage` 对象。

4. **盘前回调函数传参调整**
   - **问题**：`before_trading_start() takes 1 positional argument but 2 were given`。
   - **修复**：PTrade 最新的引擎机制会给 `before_trading_start` 带上 `data` 入参，已将原先的 `def before_trading_start(context):` 增加了 `data` 签名。

## 🧪 验证建议
所有代码现已按照最新分析过的《ptraderAi文档》修复完毕。
请您复制代码重新在 PTrade 中进行回测，如果仍有报错，可以直接将新的日志反馈给我进一步跟进！
