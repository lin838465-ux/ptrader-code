---
description: 一键将 Excel/CSV 股票或 ETF 数据同步到线上数据库
---

# 数据库同步 Skill

将指定文件夹（或单个文件）中的 CSV/Excel 股票、ETF 数据一键解析并同步到线上数据库。

## 用法

```
/sync-db [文件夹路径或文件路径]
```

不传路径时默认扫描 `data/` 目录下所有 CSV/Excel 文件。

---

## 执行步骤

**第一步：检查数据库配置**

检查项目根目录是否存在 `db_config.json`：

```bash
cat db_config.json 2>/dev/null || echo "配置文件不存在"
```

如果不存在，创建配置文件（请先提供数据库信息）：
```json
{
  "host": "your-host",
  "port": 3306,
  "user": "your-user",
  "password": "your-password",
  "database": "your-database",
  "driver": "mysql+pymysql"
}
```

**第二步：安装依赖**

```bash
pip install pandas sqlalchemy pymysql openpyxl psycopg2-binary
```

**第三步：预览（dry-run）**

先预览不写入，确认数据正确：

```bash
python scripts/sync_to_db.py "$ARGUMENTS" --dry-run
```

若 `$ARGUMENTS` 为空则扫描 `data/` 目录：

```bash
python scripts/sync_to_db.py data/ --dry-run
```

**第四步：正式同步**

确认无误后执行正式同步：

```bash
python scripts/sync_to_db.py "$ARGUMENTS"
```

若路径为空：

```bash
python scripts/sync_to_db.py data/
```

---

## 自动识别的表类型

| 表名 | 匹配特征 | 去重键 |
|------|---------|--------|
| `stock_daily` | 含「股票代码」「日期」「开盘」 | code + trade_date |
| `trades` | 含「买入时间」「卖出时间」 | code + buy_time + sell_time |
| `etf_daily` | 含 date, open, close, high, low | code + trade_date |
| 文件名 | 其他格式 | 无 |

## 常用选项

```bash
# 同步单个文件
python scripts/sync_to_db.py data/4hao/raw/daily/600583.csv

# 同步整个文件夹
python scripts/sync_to_db.py data/4hao/raw/daily/

# 强制写入指定表名
python scripts/sync_to_db.py data/ --table my_table

# 遇到重复数据时替换整张表
python scripts/sync_to_db.py data/ --if-exists replace

# 直接指定数据库 URL（不用配置文件）
python scripts/sync_to_db.py data/ --url "mysql+pymysql://user:pass@host:3306/dbname"
```
