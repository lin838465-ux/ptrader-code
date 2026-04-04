---
description: 批量抓取 ETF 分钟K线并增量写入数据库（通达信协议，默认5分钟，支持1/5/15/30/60分钟）
---

# ETF 分钟K线抓取 Skill

通过**通达信 TCP 协议**批量下载 ETF12 分钟K线数据，增量写入线上 MySQL（quant_data）。

**每只 ETF 单独一张表**，命名规则：`etf_{6位代码}_{周期}`
- 5分钟：`etf_518880_5`
- 1分钟：`etf_518880_1`

**历史深度**：约 1.5～2 年（通达信服务器保存约 25000 条 5 分钟历史）

---

## 用法

```
/fetch-etf-kline
```

---

## 执行步骤

**第一步：确认数据库配置**

```bash
cat db_config.json
```

**第二步：安装依赖**

```bash
pip install pandas sqlalchemy pymysql pytdx mootdx
python -m mootdx bestip   # 自动选择最快的 TDX 行情服务器
```

**第三步：预览（dry-run）**

```bash
python scripts/fetch_etf_kline.py --dry-run
```

**第四步：正式抓取 5 分钟线（增量）**

```bash
python scripts/fetch_etf_kline.py
```

**第五步（可选）：抓取 1 分钟线**

```bash
python scripts/fetch_etf_kline.py --klt 1
```

---

## 常用选项

```bash
# 默认：5分钟线，ETF12 全部，增量更新
python scripts/fetch_etf_kline.py

# 1分钟线
python scripts/fetch_etf_kline.py --klt 1

# 只拉某只 ETF（调试用）
python scripts/fetch_etf_kline.py --etf 518880.SS

# 强制全量（忽略库内已有数据，全部重新拉）
python scripts/fetch_etf_kline.py --full

# 组合使用
python scripts/fetch_etf_kline.py --klt 1 --etf 518880.SS --full
```

---

## 数据字段说明

| 字段 | 含义 | 备注 |
|------|------|------|
| `dt` | K线时间 | 如 2024-03-01 09:35:00 |
| `open/close/high/low` | OHLC 价格 | 通达信原始价（不复权） |
| `volume` | 成交量（股） | |
| `amount` | 成交额（元） | |

---

## ETF12 池 & 目标表名

| 代码 | 名称 | 5分钟表 | 1分钟表 |
|------|------|---------|---------|
| 563300.SS | 中证2000ETF | etf_563300_5 | etf_563300_1 |
| 159611.SZ | 电力ETF | etf_159611_5 | etf_159611_1 |
| 159681.SZ | 算力ETF | etf_159681_5 | etf_159681_1 |
| 588000.SS | 科创50ETF | etf_588000_5 | etf_588000_1 |
| 513100.SS | 纳指ETF | etf_513100_5 | etf_513100_1 |
| 513180.SS | 恒生科技ETF | etf_513180_5 | etf_513180_1 |
| 515980.SS | 人工智能ETF | etf_515980_5 | etf_515980_1 |
| 518880.SS | 黄金ETF | etf_518880_5 | etf_518880_1 |
| 162411.SZ | 华宝油气 | etf_162411_5 | etf_162411_1 |
| 512890.SS | 红利低波ETF | etf_512890_5 | etf_512890_1 |
| 515880.SS | 通信ETF | etf_515880_5 | etf_515880_1 |
| 159992.SZ | 创新药ETF | etf_159992_5 | etf_159992_1 |
