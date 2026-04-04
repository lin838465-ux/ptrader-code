# ════════════════════════════════════════════════════════════════
# 首板低开5止损策略 - PTrade 版本
# ────────────────────────────────────────────────────────────────
# 原策略：https://www.joinquant.com/post/44901
# 作者：wywy1995 | PTrade适配：自动转换
#
# 逻辑：昨日首次涨停 → 今日低开3%-4% → 买入
# 止损：-5% 止损；11:28 / 14:50 若有利润清仓
#
# PTrade 关键 API 差异：
#   get_all_securities()  → get_Ashares(date)
#   get_extras('is_st',…) → get_stock_status(stocks,'ST', date)
#   curr_data[s].paused   → get_stock_status(stocks,'HALT', date)
#   curr_data[s].last_price → data[s].close (handle_data) / snapshot
#   curr_data[s].day_open / high_limit → get_price() 取当日字段
#   get_security_info(s).start_date → get_stock_info(s,'listed_date')
#   context.portfolio.available_cash → context.portfolio.cash
#   get_all_trade_days()  → get_all_trades_days()
#   run_daily(func,time)  → run_daily(context, func, time=time)
# ════════════════════════════════════════════════════════════════

import pandas as pd
import datetime as dt


def initialize(context):
    set_option('use_real_price', True)
    set_option('avoid_future_data', True)
    log.set_level('system', 'error')

    run_daily(context, buy,  time='09:30')
    run_daily(context, sell, time='09:35')
    run_daily(context, sell, time='11:28')
    run_daily(context, sell, time='14:50')


def handle_data(context, data):
    pass


# ─────────────────────────────────────────────────────────────
# 买入：昨日首板 + 今日低开 3%-4%
# ─────────────────────────────────────────────────────────────
def buy(context, data):
    if len(context.portfolio.positions) > 0:
        return

    yesterday = context.previous_date.strftime('%Y-%m-%d')
    today_str = context.current_dt.strftime('%Y%m%d')

    # 1. A 股池
    stock_list = get_Ashares(yesterday)

    # 2. 过滤 ST
    st_status = get_stock_status(stock_list, 'ST', today_str)
    if st_status:
        stock_list = [s for s in stock_list if not st_status.get(s, False)]

    # 3. 剔除科创板、北交所
    stock_list = [s for s in stock_list
                  if s[:2] != '68' and s[0] not in ['4', '8']]

    # 4. 剔除上市不满1年
    listed_info = get_stock_info(stock_list, 'listed_date') if stock_list else {}
    today_date = context.current_dt.date()
    stock_list = [s for s in stock_list
                  if s in listed_info
                  and listed_info[s].get('listed_date')
                  and (today_date - dt.datetime.strptime(
                      listed_info[s]['listed_date'], '%Y-%m-%d').date()).days > 250]

    # 5. 剔除停牌
    halt_status = get_stock_status(stock_list, 'HALT', today_str) if stock_list else {}
    stock_list = [s for s in stock_list if not halt_status.get(s, False)]

    if not stock_list:
        return

    # 6. 昨日涨停
    df = get_price(stock_list, end_date=yesterday, count=1,
                   fields=['close', 'high_limit'], panel=False)
    df = df.dropna()
    limit_up = df[df['close'] == df['high_limit']]['code'].tolist()
    if not limit_up:
        return

    # 7. 首板判断（前一日未涨停）
    pre_df = get_price(limit_up, end_date=yesterday, count=2,
                       fields=['close', 'high_limit'], panel=False)
    first_limit = []
    for code in limit_up:
        c = pre_df[pre_df['code'] == code]['close'].tolist()
        h = pre_df[pre_df['code'] == code]['high_limit'].tolist()
        if len(c) >= 2 and c[0] != h[0]:
            first_limit.append(code)

    if not first_limit:
        return

    # 8. 60日相对位置 <= 50%
    df_pos = get_price(first_limit, end_date=yesterday, count=60,
                       fields=['high', 'low', 'close'],
                       panel=False).dropna()
    rp = {}
    for code in first_limit:
        d = df_pos[df_pos['code'] == code]
        if len(d) < 10:
            continue
        close = d['close'].iloc[-1]
        low   = d['low'].min()
        high  = d['high'].max()
        rp[code] = (close - low) / (high - low) if high > low else 0

    first_limit = [c for c in rp if rp[c] <= 0.5]
    if not first_limit:
        return

    # 9. 今日低开 3%-4%（开盘价 / 昨收 在 0.96~0.97 之间）
    yc_df = get_price(first_limit, end_date=yesterday, count=1,
                      fields=['close'], panel=False)
    if yc_df.empty:
        return
    yc = yc_df.set_index('code')

    buy_list = []
    today_open_df = get_price(first_limit, end_date=context.current_dt.strftime('%Y-%m-%d'),
                               count=1, fields=['open'], panel=False)
    if today_open_df.empty:
        return
    today_open = today_open_df.set_index('code')

    for code in first_limit:
        if code not in yc.index or code not in today_open.index:
            continue
        open_p  = today_open.loc[code, 'open']
        close_p = yc.loc[code, 'close']
        if 0.96 <= open_p / close_p <= 0.97:
            buy_list.append(code)

    # 10. 再次确认非 ST
    if st_status:
        buy_list = [s for s in buy_list if not st_status.get(s, False)]

    if not buy_list:
        return

    cash = context.portfolio.cash / len(buy_list)
    for code in buy_list:
        order_value(code, cash)
        name_info = get_stock_name([code])
        log.info('买入：%s %s' % (name_info.get(code, code), code))


# ─────────────────────────────────────────────────────────────
# 卖出：止损 -5%；11:28 / 14:50 止盈清仓
# ─────────────────────────────────────────────────────────────
def sell(context, data):
    hold = list(context.portfolio.positions.keys())
    if not hold:
        return

    t = context.current_dt.strftime('%H:%M:%S')
    today_str = context.current_dt.strftime('%Y%m%d')
    today_date_str = context.current_dt.strftime('%Y-%m-%d')

    # 获取当前价 / 涨停价
    price_df = get_price(hold, end_date=today_date_str, count=1,
                         fields=['close', 'high_limit'], panel=False)
    if price_df.empty:
        return
    price_df = price_df.set_index('code')

    # 实盘可用 get_snapshot 获取更实时价格
    if is_trade():
        try:
            snap = get_snapshot(hold)
            for code in hold:
                if snap and code in snap:
                    price_df.loc[code, 'close'] = float(snap[code].get('last_px', price_df.loc[code, 'close']))
        except Exception:
            pass

    # 更新 ST 状态（防止盘中摘帽/加帽）
    st_status = get_stock_status(hold, 'ST', today_str) or {}

    for code in hold:
        # ST 强制卖出
        if st_status.get(code, False):
            order_target_value(code, 0)
            log.info('【ST强制卖出】%s' % code)
            continue

        pos = context.portfolio.positions.get(code)
        if pos is None or pos.closeable_amount <= 0:
            continue

        cost = pos.avg_cost
        now  = price_df.loc[code, 'close'] if code in price_df.index else cost
        high_lmt = price_df.loc[code, 'high_limit'] if code in price_df.index else now * 1.1

        # -5% 止损
        if now <= cost * 0.95:
            order_target_value(code, 0)
            name_info = get_stock_name([code])
            log.info('【-5%%止损】卖出：%s' % name_info.get(code, code))
            continue

        # 11:28 止盈：有利润且未涨停
        if t == '11:28:00' and now > cost and now < high_lmt:
            order_target_value(code, 0)
            name_info = get_stock_name([code])
            log.info('【11:28止盈】卖出：%s' % name_info.get(code, code))
            continue

        # 14:50 清仓：有利润且未涨停
        if t == '14:50:00' and now < high_lmt:
            order_target_value(code, 0)
            name_info = get_stock_name([code])
            log.info('【14:50清仓】卖出：%s' % name_info.get(code, code))
            continue
