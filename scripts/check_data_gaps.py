"""检查 quant_data 数据库中所有 ETF 表的数据断档问题"""
import pymysql

conn = pymysql.connect(
    host='162.14.104.191', port=3306,
    user='root', password='mysg0806', database='quant_data'
)
cur = conn.cursor()

# 获取所有表
cur.execute('SHOW TABLES')
tables = sorted([r[0] for r in cur.fetchall()])

# 用黄金ETF（518880）作为基准交易日历（最全的ETF）
cur.execute('SELECT DATE(dt) as d, COUNT(*) as bars FROM etf_518880_5 GROUP BY d ORDER BY d')
ref_calendar = {r[0]: r[1] for r in cur.fetchall()}
ref_dates = set(ref_calendar.keys())
print(f'基准交易日历: {len(ref_dates)} 个交易日')
print(f'时间范围: {min(ref_dates)} -> {max(ref_dates)}\n')

problems = {}
ok_tables = []

for tname in tables:
    sql = f'SELECT DATE(dt) as d, COUNT(*) as bars FROM `{tname}` GROUP BY d ORDER BY d'
    cur.execute(sql)
    rows = {r[0]: r[1] for r in cur.fetchall()}
    tbl_dates = set(rows.keys())

    # 该表的有效基准日历（从该表最早日期开始才算）
    tbl_start = min(tbl_dates) if tbl_dates else None
    local_ref = {d for d in ref_dates if tbl_start and d >= tbl_start}

    missing = sorted(local_ref - tbl_dates)
    thin = {str(d): cnt for d, cnt in rows.items() if cnt < 40}  # 少于40根算不完整

    if missing or thin:
        problems[tname] = {'missing': missing, 'thin': thin}
    else:
        ok_tables.append(tname)

# 打印结果
print(f'{'='*60}')
print(f'正常表: {len(ok_tables)}/{len(tables)}')
print(f'有问题表: {len(problems)}/{len(tables)}\n')

if not problems:
    print('所有表数据完整，无断档！')
else:
    for tname, info in problems.items():
        missing = info['missing']
        thin = info['thin']
        print(f'[{tname}]')
        if missing:
            # 把连续缺失日期合并显示
            print(f'  缺失交易日 ({len(missing)} 天):')
            if len(missing) <= 10:
                for d in missing:
                    print(f'    {d}')
            else:
                for d in missing[:5]:
                    print(f'    {d}')
                print(f'    ... 共 {len(missing)} 天')
        if thin:
            print(f'  不完整交易日 (< 40根K线) ({len(thin)} 天):')
            for d, cnt in list(thin.items())[:5]:
                print(f'    {d}: {cnt} 根')
            if len(thin) > 5:
                print(f'    ... 共 {len(thin)} 天')
        print()

conn.close()
