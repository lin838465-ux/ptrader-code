# ════════════════════════════════════════════════════════════════
# 集合竞价三合一（一进二 + 首板低开 + 弱转强）- PTrade 版本
# ────────────────────────────────────────────────────────────────
# 原策略合并自：
#   https://www.joinquant.com/post/49474  集合竞价三合一
#   https://www.joinquant.com/post/44901  首板低开
#   https://www.joinquant.com/post/48523  一进二集合竞价
#   https://www.joinquant.com/post/49364  弱转强
# 2024/08/01 止损修改为跌破5日均线
#
# PTrade 关键 API 适配：
#   get_all_securities()       → get_Ashares(date)
#   get_extras('is_st',…)      → get_stock_status(stocks,'ST',date)
#   curr_data[s].paused        → get_stock_status(stocks,'HALT',date)
#   curr_data[s].last_price    → snapshot['last_px'] / get_price close
#   curr_data[s].day_open      → snapshot['open_px'] / get_price open
#   curr_data[s].high_limit    → snapshot['up_px'] / get_price high_limit
#   attribute_history()        → get_history()
#   get_valuation()            → get_fundamentals('valuation',...)
#   get_call_auction()         → get_trend_data(date,stocks=s) [交易]
#                                 开盘量/价近似 [回测]
#   get_security_info().start_date → get_stock_info(s,'listed_date')
#   get_all_trade_days()       → get_all_trades_days()
#   send_message()             → log.info()
#   context.portfolio.available_cash → context.portfolio.cash
#   run_daily(func,time)       → run_daily(context, func, time=time)
#   MarketOrderStyle()         → 直接 order_value() 市价单
# ════════════════════════════════════════════════════════════════

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta


# ─────────────────────────────────────────────────────────────
# 初始化
# ─────────────────────────────────────────────────────────────
def initialize(context):
    set_option('use_real_price', True)
    log.set_level('system', 'error')
    set_option('avoid_future_data', True)

    # 一进二 + 弱转强：9:01选股，9:26买入
    run_daily(context, get_stock_list, time='09:01')
    run_daily(context, buy,            time='09:26')
    run_daily(context, sell,           time='11:25')
    run_daily(context, sell,           time='14:50')


def handle_data(context, data):
    pass


# ─────────────────────────────────────────────────────────────
# 选股（9:01）
# ─────────────────────────────────────────────────────────────
def get_stock_list(context, data):
    date      = context.previous_date
    date_str  = _transform_date(date, 'str')
    date_1    = _get_shifted_date(date_str, -1, 'T')
    date_2    = _get_shifted_date(date_str, -2, 'T')

    initial_list = _prepare_stock_list(date_str)

    # 一进二：昨日涨停 且 前两日均未涨停
    hl_list  = _get_hl_stock(initial_list, date_str)
    hl1_list = _get_ever_hl_stock(initial_list, date_1)
    hl2_list = _get_ever_hl_stock(initial_list, date_2)
    exclude  = set(hl1_list + hl2_list)
    g.target_list = [s for s in hl_list if s not in exclude]

    # 弱转强：昨日曾涨停（未收涨停）且上上交易日未涨停
    h1_list  = _get_ever_hl_stock2(initial_list, date_str)
    exclude2 = set(_get_hl_stock(initial_list, date_1))
    g.target_list2 = [s for s in h1_list if s not in exclude2]


# ─────────────────────────────────────────────────────────────
# 买入（9:26）
# ─────────────────────────────────────────────────────────────
def buy(context, data):
    qualified_stocks = []
    gk_stocks  = []   # 一进二-高开
    dk_stocks  = []   # 首板低开
    rzq_stocks = []   # 弱转强

    date_str = context.previous_date.strftime('%Y-%m-%d')
    today_str = context.current_dt.strftime('%Y%m%d')
    today_date_str = context.current_dt.strftime('%Y-%m-%d')

    # ── 一进二（高开竞价） ──────────────────────────────────────
    for s in g.target_list:
        # 昨日成交额、均价涨幅
        prev = get_history(1, '1d', ['close', 'volume', 'money'],
                            security_list=s, fq=None, include=False)
        if prev is None or prev['money'].iloc[-1] < 5.5e8 or prev['money'].iloc[-1] > 20e8:
            continue
        avg_px_inc = prev['money'].iloc[-1] / prev['volume'].iloc[-1] / prev['close'].iloc[-1] * 1.1 - 1
        if avg_px_inc < 0.07:
            continue

        # 市值过滤：总市值 > 70亿，流通市值 < 520亿
        try:
            val = get_fundamentals(s, 'valuation',
                                   fields=['market_cap', 'circulating_market_cap'],
                                   date=context.previous_date.strftime('%Y%m%d'))
            if val is None or val.empty:
                continue
            if val['market_cap'].iloc[0] < 70 or val['circulating_market_cap'].iloc[0] > 520:
                continue
        except Exception:
            continue

        # 左压天数
        zyts = _calculate_zyts(s, context)
        vol_data = get_history(zyts, '1d', 'volume', security_list=s,
                               fq=None, include=False)
        if vol_data is None or len(vol_data) < 2:
            continue
        if vol_data['volume'].iloc[-1] <= vol_data['volume'].iloc[:-1].max() * 0.9:
            continue

        # 集合竞价数据
        auction_vol, auction_px, hl_px = _get_auction_data(s, context, prev)
        if auction_vol is None:
            continue
        if auction_vol / prev['volume'].iloc[-1] < 0.03:
            continue

        # 高开 0% ~ 6%（相对昨涨停价）
        current_ratio = auction_px / (hl_px / 1.1)
        if current_ratio <= 1 or current_ratio >= 1.06:
            continue

        gk_stocks.append(s)
        qualified_stocks.append(s)

    # ── 首板低开 ───────────────────────────────────────────────
    initial_list2 = _prepare_stock_list2(date_str)
    hl_list_dk    = _get_hl_stock(initial_list2, date_str)

    if hl_list_dk:
        # 过滤连板
        ccd = _get_continue_count_df(hl_list_dk, date_str, 10)
        lb_list    = list(ccd.index)
        stock_list = [s for s in hl_list_dk if s not in lb_list]

        # 60日相对位置
        rpd = _get_relative_position_df(stock_list, date_str, 60)
        if not rpd.empty:
            rpd = rpd[rpd['rp'] <= 0.5]
            stock_list = list(rpd.index)

        # 低开 3%-4.5%
        if stock_list:
            yc_df = get_price(stock_list, end_date=date_str, count=1,
                              fields=['close'], panel=False, fill_paused=False)
            if not yc_df.empty:
                yc = yc_df.set_index('code')
                today_open_df = get_price(stock_list, end_date=today_date_str,
                                          count=1, fields=['open'], panel=False)
                if not today_open_df.empty:
                    today_open = today_open_df.set_index('code')
                    for s in stock_list:
                        if s not in yc.index or s not in today_open.index:
                            continue
                        open_pct = today_open.loc[s, 'open'] / yc.loc[s, 'close']
                        if 0.955 <= open_pct <= 0.97:
                            prev = get_history(1, '1d', 'money', security_list=s,
                                               fq=None, include=False)
                            if prev is not None and prev['money'].iloc[-1] >= 1e8:
                                dk_stocks.append(s)
                                qualified_stocks.append(s)

    # ── 弱转强（集合竞价）──────────────────────────────────────
    for s in g.target_list2:
        # 过滤近4日涨幅 > 28%
        price_h = get_history(4, '1d', 'close', security_list=s,
                              fq=None, include=False)
        if price_h is None or len(price_h) < 4:
            continue
        inc = (price_h['close'].iloc[-1] - price_h['close'].iloc[0]) / price_h['close'].iloc[0]
        if inc > 0.28:
            continue

        # 昨日收盘 < 开盘 -5% 的过滤
        prev1 = get_history(1, '1d', ['open', 'close'], security_list=s,
                            fq=None, include=False)
        if prev1 is None or len(prev1) < 1:
            continue
        oc_ratio = (prev1['close'].iloc[-1] - prev1['open'].iloc[-1]) / prev1['open'].iloc[-1]
        if oc_ratio < -0.05:
            continue

        # 成交额、均价涨幅
        prev_mvm = get_history(1, '1d', ['close', 'volume', 'money'],
                               security_list=s, fq=None, include=False)
        if prev_mvm is None or prev_mvm['money'].iloc[-1] < 3e8 or prev_mvm['money'].iloc[-1] > 19e8:
            continue
        avg_px_inc2 = prev_mvm['money'].iloc[-1] / prev_mvm['volume'].iloc[-1] / prev_mvm['close'].iloc[-1] - 1
        if avg_px_inc2 < -0.04:
            continue

        # 市值
        try:
            val2 = get_fundamentals(s, 'valuation',
                                    fields=['market_cap', 'circulating_market_cap'],
                                    date=context.previous_date.strftime('%Y%m%d'))
            if val2 is None or val2.empty:
                continue
            if val2['market_cap'].iloc[0] < 70 or val2['circulating_market_cap'].iloc[0] > 520:
                continue
        except Exception:
            continue

        # 左压
        zyts2 = _calculate_zyts(s, context)
        vol2  = get_history(zyts2, '1d', 'volume', security_list=s,
                            fq=None, include=False)
        if vol2 is None or len(vol2) < 2:
            continue
        if vol2['volume'].iloc[-1] <= vol2['volume'].iloc[:-1].max() * 0.9:
            continue

        # 集合竞价
        auction_vol2, auction_px2, hl_px2 = _get_auction_data(s, context, prev_mvm)
        if auction_vol2 is None:
            continue
        if auction_vol2 / prev_mvm['volume'].iloc[-1] < 0.03:
            continue
        cr2 = auction_px2 / (hl_px2 / 1.1)
        if cr2 <= 0.98 or cr2 >= 1.09:
            continue

        rzq_stocks.append(s)
        qualified_stocks.append(s)

    # ── 打印 & 买入 ────────────────────────────────────────────
    if qualified_stocks:
        log.info('═' * 50)
        log.info('今日选股：' + ','.join(qualified_stocks))
        log.info('一进二：'  + ','.join(gk_stocks))
        log.info('首板低开：' + ','.join(dk_stocks))
        log.info('弱转强：'  + ','.join(rzq_stocks))
        log.info('═' * 50)
    else:
        log.info('今日无目标个股')

    if (qualified_stocks and
            context.portfolio.cash / context.portfolio.total_value > 0.3):
        snap = {}
        if is_trade():
            try:
                snap = get_snapshot(qualified_stocks) or {}
            except Exception:
                pass

        value = context.portfolio.cash / len(qualified_stocks)
        for s in qualified_stocks:
            open_px = None
            if snap and s in snap:
                open_px = float(snap[s].get('open_px', 0)) or None
            if open_px and open_px > 0:
                # 实盘：按开盘价下委托
                order_value(s, value)
            else:
                # 回测：直接市价
                order_value(s, value)
            log.info('买入 %s' % s)


# ─────────────────────────────────────────────────────────────
# 卖出（11:25 / 14:50）
# ─────────────────────────────────────────────────────────────
def sell(context, data):
    hold = list(context.portfolio.positions.keys())
    if not hold:
        return

    t = context.current_dt.strftime('%H:%M:%S')
    today_date_str = context.current_dt.strftime('%Y-%m-%d')
    today_str = context.current_dt.strftime('%Y%m%d')

    # 获取当前价和涨停价
    price_df = get_price(hold, end_date=today_date_str, count=1,
                         fields=['close', 'high_limit'], panel=False)
    if price_df.empty:
        return
    price_df = price_df.set_index('code')

    # 实盘用快照更新实时价
    if is_trade():
        try:
            snap = get_snapshot(hold) or {}
            for code in hold:
                if code in snap:
                    price_df.loc[code, 'close'] = float(snap[code].get('last_px',
                                                   price_df.loc[code, 'close']))
        except Exception:
            pass

    if t == '11:25:00':
        for s in hold:
            pos = context.portfolio.positions.get(s)
            if pos is None or pos.closeable_amount == 0:
                continue
            now      = price_df.loc[s, 'close'] if s in price_df.index else pos.avg_cost
            high_lmt = price_df.loc[s, 'high_limit'] if s in price_df.index else now * 1.1
            if now < high_lmt and now > pos.avg_cost:
                order_target_value(s, 0)
                log.info('止盈卖出 %s' % s)

    elif t == '14:50:00':
        for s in hold:
            pos = context.portfolio.positions.get(s)
            if pos is None or pos.closeable_amount == 0:
                continue
            now      = price_df.loc[s, 'close'] if s in price_df.index else pos.avg_cost
            high_lmt = price_df.loc[s, 'high_limit'] if s in price_df.index else now * 1.1

            # 5日均线止损
            close_h = get_history(4, '1d', 'close', security_list=s,
                                  fq=None, include=False)
            if close_h is not None and len(close_h) >= 4:
                ma4 = close_h['close'].mean()
                ma5 = (ma4 * 4 + now) / 5
            else:
                ma5 = pos.avg_cost

            if now < high_lmt and now > pos.avg_cost:
                order_target_value(s, 0)
                log.info('止盈卖出 %s' % s)
            elif now < ma5:
                order_target_value(s, 0)
                log.info('止损卖出（跌破5日线）%s' % s)


# ═════════════════════════════════════════════════════════════
# 辅助函数
# ═════════════════════════════════════════════════════════════

def _transform_date(date, date_type):
    if isinstance(date, str):
        str_date = date
        dt_date  = dt.datetime.strptime(date, '%Y-%m-%d')
        d_date   = dt_date.date()
    elif isinstance(date, dt.datetime):
        str_date = date.strftime('%Y-%m-%d')
        dt_date  = date
        d_date   = dt_date.date()
    elif isinstance(date, dt.date):
        str_date = date.strftime('%Y-%m-%d')
        dt_date  = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date   = date
    else:
        str_date = str(date)
        dt_date  = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date   = dt_date.date()
    dct = {'str': str_date, 'dt': dt_date, 'd': d_date}
    return dct[date_type]


def _get_shifted_date(date, days, days_type='T'):
    d_date    = _transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    if days_type == 'N':
        return str(yesterday + dt.timedelta(days + 1))
    # 交易日偏移
    all_trade_days = [str(i) for i in get_all_trades_days()]
    ystr = str(yesterday)
    if ystr in all_trade_days:
        return all_trade_days[all_trade_days.index(ystr) + days + 1]
    for i in range(100):
        last = str(yesterday - dt.timedelta(i))
        if last in all_trade_days:
            return all_trade_days[all_trade_days.index(last) + days + 1]
    return ystr


def _filter_new_stock(initial_list, date, days=50):
    d_date     = _transform_date(date, 'd')
    listed_info = get_stock_info(initial_list, 'listed_date') if initial_list else {}
    return [s for s in initial_list
            if s in listed_info
            and listed_info[s].get('listed_date')
            and (d_date - dt.datetime.strptime(
                listed_info[s]['listed_date'], '%Y-%m-%d').date()).days > days]


def _filter_new_stock2(initial_list, date, days=250):
    return _filter_new_stock(initial_list, date, days=days)


def _filter_st_stock(initial_list, date):
    date_str = _transform_date(date, 'str')
    date_8   = date_str.replace('-', '')
    if not initial_list:
        return []
    st = get_stock_status(initial_list, 'ST', date_8) or {}
    return [s for s in initial_list if not st.get(s, False)]


def _filter_kcbj_stock(initial_list):
    return [s for s in initial_list
            if s[0] not in ['4', '8'] and s[:2] != '68']


def _filter_paused_stock(initial_list, date):
    date_str = _transform_date(date, 'str')
    date_8   = date_str.replace('-', '')
    if not initial_list:
        return []
    halt = get_stock_status(initial_list, 'HALT', date_8) or {}
    return [s for s in initial_list if not halt.get(s, False)]


def _prepare_stock_list(date):
    lst = get_Ashares(date)
    lst = _filter_kcbj_stock(lst)
    lst = _filter_new_stock(lst, date)
    lst = _filter_st_stock(lst, date)
    lst = _filter_paused_stock(lst, date)
    return lst


def _prepare_stock_list2(date):
    lst = get_Ashares(date)
    lst = _filter_kcbj_stock(lst)
    lst = _filter_new_stock2(lst, date)
    lst = _filter_st_stock(lst, date)
    lst = _filter_paused_stock(lst, date)
    return lst


def _get_hl_stock(initial_list, date):
    if not initial_list:
        return []
    df = get_price(initial_list, end_date=date, count=1,
                   fields=['close', 'high_limit'],
                   panel=False, fill_paused=False).dropna()
    return list(df[df['close'] == df['high_limit']]['code'])


def _get_ever_hl_stock(initial_list, date):
    if not initial_list:
        return []
    df = get_price(initial_list, end_date=date, count=1,
                   fields=['high', 'high_limit'],
                   panel=False, fill_paused=False).dropna()
    return list(df[df['high'] == df['high_limit']]['code'])


def _get_ever_hl_stock2(initial_list, date):
    if not initial_list:
        return []
    df = get_price(initial_list, end_date=date, count=1,
                   fields=['close', 'high', 'high_limit'],
                   panel=False, fill_paused=False).dropna()
    mask = (df['high'] == df['high_limit']) & (df['close'] != df['high_limit'])
    return list(df[mask]['code'])


def _get_hl_count_df(hl_list, date, watch_days):
    if not hl_list:
        return pd.DataFrame(columns=['count', 'extreme_count'])
    df = get_price(hl_list, end_date=date, count=watch_days,
                   fields=['close', 'high_limit', 'low'],
                   panel=False, fill_paused=False)
    hl_count, ex_count = [], []
    for stock in hl_list:
        sub = df[df['code'] == stock]
        hl_count.append((sub['close'] == sub['high_limit']).sum())
        ex_count.append((sub['low'] == sub['high_limit']).sum())
    return pd.DataFrame({'count': hl_count, 'extreme_count': ex_count}, index=hl_list)


def _get_continue_count_df(hl_list, date, watch_days):
    if not hl_list:
        return pd.DataFrame()
    frames = []
    for d in range(2, watch_days + 1):
        hlc = _get_hl_count_df(hl_list, date, d)
        frames.append(hlc[hlc['count'] == d])
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames)
    ccd_list = []
    for s in set(combined.index):
        tmp = combined.loc[[s]]
        ccd_list.append(tmp.loc[[s], :].nlargest(1, 'count'))
    if not ccd_list:
        return pd.DataFrame()
    ccd = pd.concat(ccd_list).sort_values('count', ascending=False)
    return ccd


def _get_relative_position_df(stock_list, date, watch_days):
    if not stock_list:
        return pd.DataFrame(columns=['rp'])
    df = get_price(stock_list, end_date=date, count=watch_days,
                   fields=['high', 'low', 'close'],
                   panel=False, fill_paused=False).dropna()
    if df.empty:
        return pd.DataFrame(columns=['rp'])
    close = df.groupby('code').apply(lambda x: x['close'].iloc[-1])
    high  = df.groupby('code')['high'].max()
    low   = df.groupby('code')['low'].min()
    result = pd.DataFrame()
    result['rp'] = (close - low) / (high - low).replace(0, float('nan'))
    return result.dropna()


def _calculate_zyts(s, context):
    high_h = get_history(101, '1d', 'high', security_list=s,
                         fq=None, include=False)
    if high_h is None or len(high_h) < 2:
        return 10
    highs     = high_h['high']
    prev_high = highs.iloc[-1]
    zyts_0 = next((i - 1 for i, h in enumerate(highs.iloc[-3::-1], 2)
                   if h >= prev_high), 100)
    return zyts_0 + 5


def _get_auction_data(s, context, prev_vol_df):
    """
    获取集合竞价量和价。
    - 实盘（is_trade）：使用 get_trend_data
    - 回测：用开盘价和昨日量的比例近似
    """
    if is_trade():
        try:
            today_str = context.current_dt.strftime('%Y%m%d')
            td = get_trend_data(date=today_str, stocks=s)
            if not td or s not in td:
                return None, None, None
            auction_vol = td[s].get('business_amount', 0)
            auction_px  = td[s].get('hq_px', 0)
            # 获取涨停价
            yest_str = context.previous_date.strftime('%Y-%m-%d')
            hl_df = get_price(s, end_date=yest_str, count=1,
                              fields=['high_limit'], panel=False)
            hl_px = hl_df['high_limit'].iloc[0] if not hl_df.empty else auction_px * 1.1
            return auction_vol, auction_px, hl_px
        except Exception:
            return None, None, None
    else:
        # 回测：用今日开盘价和 open 的交易量（近似竞价量 = 日成交量 * 固定比例）
        try:
            today_date_str = context.current_dt.strftime('%Y-%m-%d')
            yest_str       = context.previous_date.strftime('%Y-%m-%d')
            today_ohlc = get_price(s, end_date=today_date_str, count=1,
                                   fields=['open', 'volume'], panel=False)
            hl_df = get_price(s, end_date=yest_str, count=1,
                              fields=['high_limit'], panel=False)
            if today_ohlc.empty or hl_df.empty:
                return None, None, None
            open_px = today_ohlc['open'].iloc[0]
            # 用昨日成交量代表竞价对比基准（回测近似）
            auction_vol_est = prev_vol_df['volume'].iloc[-1] * 0.05
            hl_px = hl_df['high_limit'].iloc[0]
            return auction_vol_est, open_px, hl_px
        except Exception:
            return None, None, None
