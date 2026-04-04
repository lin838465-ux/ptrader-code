# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲
# 闆嗗悎绔炰环涓夊悎涓€锛堜竴杩涗簩 + 棣栨澘浣庡紑 + 寮辫浆寮猴級- PTrade 鐗堟湰
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鍘熺瓥鐣ュ悎骞惰嚜锛?#   https://www.joinquant.com/post/49474  闆嗗悎绔炰环涓夊悎涓€
#   https://www.joinquant.com/post/44901  棣栨澘浣庡紑
#   https://www.joinquant.com/post/48523  涓€杩涗簩闆嗗悎绔炰环
#   https://www.joinquant.com/post/49364  寮辫浆寮?# 2024/08/01 姝㈡崯淇敼涓鸿穼鐮?鏃ュ潎绾?#
# PTrade 鍏抽敭 API 宸紓锛?#   context.current_dt        鈫?context.blotter.current_dt
#   context.previous_date     鈫?_get_previous_trading_day(context)
#   pos.closeable_amount      鈫?pos.enable_amount锛圱+1鍙崠锛?#   order_value(code, cash)   鈫?order(code, shares, limit_price=price)
#   order_target_value(code,0) 鈫?鍏堟鏌nable_amount鍐峯rder(..., -amount, limit_price)
#   set_option/log.set_level  鈫?鍒犻櫎锛圥Trade涓嶆敮鎸侊級
# 鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲鈺愨晲

import pandas as pd
import datetime as dt
from datetime import datetime, timedelta


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 宸ュ叿鍑芥暟
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€

def _get_previous_trading_day(context):
    """鑾峰彇鍓嶄竴浜ゆ槗鏃?""
    try:
        return get_trading_day(-1)
    except Exception:
        dt_now = context.blotter.current_dt
        return get_trading_day(dt_now)


def _same_security(code_a, code_b):
    if not code_a or not code_b:
        return False
    return str(code_a)[:6] == str(code_b)[:6]


def _get_live_price(code, direction='buy'):
    """鑾峰彇瀹炴椂蹇収浠锋牸锛堣偂绁?浣嶅皬鏁帮級"""
    try:
        snap = get_snapshot(code)
        if not snap:
            return None
        info = snap.get(code)
        if info is None:
            for k in snap:
                if k[:6] == code[:6]:
                    info = snap[k]
                    break
        if not info:
            return None
        last_px = float(info.get('last_px', 0))
        if last_px <= 0:
            return None
        if direction == 'buy':
            return round(last_px * 1.002, 2)
        else:
            return round(last_px * 0.998, 2)
    except Exception:
        return None


def _normalize_payload(payload):
    if not payload:
        return []
    if isinstance(payload, (list, tuple)):
        return list(payload)
    return [payload]


def _refresh_pending_order():
    try:
        open_orders = get_open_orders()
        if not open_orders:
            g.pending_order = {'code': None, 'side': None, 'order_id': None}
            return
        pending = open_orders[0]
        symbol = str(getattr(pending, 'symbol', '') or pending.get('symbol', ''))
        amount = float(getattr(pending, 'amount', 0) or pending.get('amount', 0))
        side = 'sell' if amount < 0 else 'buy'
        g.pending_order = {'code': symbol, 'side': side, 'order_id': getattr(pending, 'id', None)}
    except Exception:
        g.pending_order = {'code': None, 'side': None, 'order_id': None}


def _transform_date(date, date_type):
    if isinstance(date, str):
        str_date = date
        dt_date = dt.datetime.strptime(date, '%Y-%m-%d')
        d_date = dt_date.date()
    elif isinstance(date, dt.datetime):
        str_date = date.strftime('%Y-%m-%d')
        dt_date = date
        d_date = dt_date.date()
    elif isinstance(date, dt.date):
        str_date = date.strftime('%Y-%m-%d')
        dt_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = date
    else:
        str_date = str(date)
        dt_date = dt.datetime.strptime(str_date, '%Y-%m-%d')
        d_date = dt_date.date()
    dct = {'str': str_date, 'dt': dt_date, 'd': d_date}
    return dct[date_type]


def _get_shifted_date(date, days, days_type='T'):
    d_date = _transform_date(date, 'd')
    yesterday = d_date + dt.timedelta(-1)
    if days_type == 'N':
        return str(yesterday + dt.timedelta(days + 1))
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
    d_date = _transform_date(date, 'd')
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
    date_8 = date_str.replace('-', '')
    if not initial_list:
        return []
    st = get_stock_status(initial_list, 'ST', date_8) or {}
    return [s for s in initial_list if not st.get(s, False)]


def _filter_kcbj_stock(initial_list):
    return [s for s in initial_list
            if s[0] not in ['4', '8'] and s[:2] != '68']


def _filter_paused_stock(initial_list, date):
    date_str = _transform_date(date, 'str')
    date_8 = date_str.replace('-', '')
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
    high = df.groupby('code')['high'].max()
    low = df.groupby('code')['low'].min()
    result = pd.DataFrame()
    result['rp'] = (close - low) / (high - low).replace(0, float('nan'))
    return result.dropna()


def _calculate_zyts(s, context):
    high_h = get_history(101, '1d', 'high', security_list=s,
                        fq=None, include=False)
    if high_h is None or len(high_h) < 2:
        return 10
    highs = high_h['high']
    prev_high = highs.iloc[-1]
    zyts_0 = next((i - 1 for i, h in enumerate(highs.iloc[-3::-1], 2)
                   if h >= prev_high), 100)
    return zyts_0 + 5


def _get_auction_data(s, context, prev_vol_df):
    if is_trade():
        try:
            today_str = context.blotter.current_dt.strftime('%Y%m%d')
            td = get_trend_data(date=today_str, stocks=s)
            if not td or s not in td:
                return None, None, None
            auction_vol = td[s].get('business_amount', 0)
            auction_px = td[s].get('hq_px', 0)
            prev_day = _get_previous_trading_day(context)
            yest_str = prev_day.strftime('%Y-%m-%d')
            hl_df = get_price(s, end_date=yest_str, count=1,
                              fields=['high_limit'], panel=False)
            hl_px = hl_df['high_limit'].iloc[0] if not hl_df.empty else auction_px * 1.1
            return auction_vol, auction_px, hl_px
        except Exception:
            return None, None, None
    else:
        try:
            prev_day = _get_previous_trading_day(context)
            today_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')
            yest_str = prev_day.strftime('%Y-%m-%d')
            today_ohlc = get_price(s, end_date=today_date_str, count=1,
                                   fields=['open', 'volume'], panel=False)
            hl_df = get_price(s, end_date=yest_str, count=1,
                              fields=['high_limit'], panel=False)
            if today_ohlc.empty or hl_df.empty:
                return None, None, None
            open_px = today_ohlc['open'].iloc[0]
            auction_vol_est = prev_vol_df['volume'].iloc[-1] * 0.05
            hl_px = hl_df['high_limit'].iloc[0]
            return auction_vol_est, open_px, hl_px
        except Exception:
            return None, None, None


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鍒濆鍖?# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
def initialize(context):
    set_parameters(server_restart_not_do_before="1", receive_cancel_response="1")

    g.target_list = []
    g.target_list2 = []
    g.pending_order = {'code': None, 'side': None, 'order_id': None}
    g.code_name = {}

    run_daily(context, get_stock_list, time='09:01')
    run_daily(context, buy,            time='09:26')
    run_daily(context, sell,           time='11:25')
    run_daily(context, sell,           time='14:50')

    if not is_trade():
        set_commission(commission_ratio=0.0003)
        set_slippage(slippage=0.001)
        set_benchmark('000300.XBHS')

    log.info('闆嗗悎绔炰环涓夊悎涓€绛栫暐 鍒濆鍖栧畬鎴?)


def handle_data(context, data):
    pass


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 閫夎偂锛?:01锛?# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
def get_stock_list(context, data):
    prev_day = _get_previous_trading_day(context)
    date = prev_day
    date_str = _transform_date(date, 'str')
    date_1 = _get_shifted_date(date_str, -1, 'T')
    date_2 = _get_shifted_date(date_str, -2, 'T')

    initial_list = _prepare_stock_list(date_str)

    hl_list = _get_hl_stock(initial_list, date_str)
    hl1_list = _get_ever_hl_stock(initial_list, date_1)
    hl2_list = _get_ever_hl_stock(initial_list, date_2)
    exclude = set(hl1_list + hl2_list)
    g.target_list = [s for s in hl_list if s not in exclude]

    h1_list = _get_ever_hl_stock2(initial_list, date_str)
    exclude2 = set(_get_hl_stock(initial_list, date_1))
    g.target_list2 = [s for s in h1_list if s not in exclude2]


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 涔板叆锛?:26锛?# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
def buy(context, data):
    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []
    rzq_stocks = []

    prev_day = _get_previous_trading_day(context)
    date_str = prev_day.strftime('%Y-%m-%d')
    today_str = context.blotter.current_dt.strftime('%Y%m%d')
    today_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')

    pending = g.pending_order
    if pending.get('side') == 'buy' and pending.get('code'):
        log.info('[涔板叆] 宸叉湁鏈畬鎴愪拱鍗曪紝绛夊緟')
        return

    for s in g.target_list:
        prev = get_history(1, '1d', ['close', 'volume', 'money'],
                            security_list=s, fq=None, include=False)
        if prev is None or prev['money'].iloc[-1] < 5.5e8 or prev['money'].iloc[-1] > 20e8:
            continue
        avg_px_inc = prev['money'].iloc[-1] / prev['volume'].iloc[-1] / prev['close'].iloc[-1] * 1.1 - 1
        if avg_px_inc < 0.07:
            continue

        try:
            val = get_fundamentals(s, 'valuation',
                                   fields=['market_cap', 'circulating_market_cap'],
                                   date=prev_day.strftime('%Y%m%d'))
            if val is None or val.empty:
                continue
            if val['market_cap'].iloc[0] < 70 or val['circulating_market_cap'].iloc[0] > 520:
                continue
        except Exception:
            continue

        zyts = _calculate_zyts(s, context)
        vol_data = get_history(zyts, '1d', 'volume', security_list=s,
                               fq=None, include=False)
        if vol_data is None or len(vol_data) < 2:
            continue
        if vol_data['volume'].iloc[-1] <= vol_data['volume'].iloc[:-1].max() * 0.9:
            continue

        auction_vol, auction_px, hl_px = _get_auction_data(s, context, prev)
        if auction_vol is None:
            continue
        if auction_vol / prev['volume'].iloc[-1] < 0.03:
            continue

        current_ratio = auction_px / (hl_px / 1.1)
        if current_ratio <= 1 or current_ratio >= 1.06:
            continue

        gk_stocks.append(s)
        qualified_stocks.append(s)

    initial_list2 = _prepare_stock_list2(date_str)
    hl_list_dk = _get_hl_stock(initial_list2, date_str)

    if hl_list_dk:
        ccd = _get_continue_count_df(hl_list_dk, date_str, 10)
        lb_list = list(ccd.index)
        stock_list = [s for s in hl_list_dk if s not in lb_list]

        rpd = _get_relative_position_df(stock_list, date_str, 60)
        if not rpd.empty:
            rpd = rpd[rpd['rp'] <= 0.5]
            stock_list = list(rpd.index)

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
                            prev_m = get_history(1, '1d', 'money', security_list=s,
                                                fq=None, include=False)
                            if prev_m is not None and prev_m['money'].iloc[-1] >= 1e8:
                                dk_stocks.append(s)
                                qualified_stocks.append(s)

    for s in g.target_list2:
        price_h = get_history(4, '1d', 'close', security_list=s,
                              fq=None, include=False)
        if price_h is None or len(price_h) < 4:
            continue
        inc = (price_h['close'].iloc[-1] - price_h['close'].iloc[0]) / price_h['close'].iloc[0]
        if inc > 0.28:
            continue

        prev1 = get_history(1, '1d', ['open', 'close'], security_list=s,
                            fq=None, include=False)
        if prev1 is None or len(prev1) < 1:
            continue
        oc_ratio = (prev1['close'].iloc[-1] - prev1['open'].iloc[-1]) / prev1['open'].iloc[-1]
        if oc_ratio < -0.05:
            continue

        prev_mvm = get_history(1, '1d', ['close', 'volume', 'money'],
                               security_list=s, fq=None, include=False)
        if prev_mvm is None or prev_mvm['money'].iloc[-1] < 3e8 or prev_mvm['money'].iloc[-1] > 19e8:
            continue
        avg_px_inc2 = prev_mvm['money'].iloc[-1] / prev_mvm['volume'].iloc[-1] / prev_mvm['close'].iloc[-1] - 1
        if avg_px_inc2 < -0.04:
            continue

        try:
            val2 = get_fundamentals(s, 'valuation',
                                    fields=['market_cap', 'circulating_market_cap'],
                                    date=prev_day.strftime('%Y%m%d'))
            if val2 is None or val2.empty:
                continue
            if val2['market_cap'].iloc[0] < 70 or val2['circulating_market_cap'].iloc[0] > 520:
                continue
        except Exception:
            continue

        zyts2 = _calculate_zyts(s, context)
        vol2 = get_history(zyts2, '1d', 'volume', security_list=s,
                            fq=None, include=False)
        if vol2 is None or len(vol2) < 2:
            continue
        if vol2['volume'].iloc[-1] <= vol2['volume'].iloc[:-1].max() * 0.9:
            continue

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

    if qualified_stocks:
        log.info('鈺? * 50)
        log.info('浠婃棩閫夎偂锛? + ','.join(qualified_stocks))
        log.info('涓€杩涗簩锛? + ','.join(gk_stocks))
        log.info('棣栨澘浣庡紑锛? + ','.join(dk_stocks))
        log.info('寮辫浆寮猴細' + ','.join(rzq_stocks))
        log.info('鈺? * 50)
    else:
        log.info('浠婃棩鏃犵洰鏍囦釜鑲?)

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

            if is_trade():
                if open_px and open_px > 0:
                    buy_price = round(open_px * 1.002, 2)
                else:
                    buy_price = _get_live_price(s, direction='buy')
                    if not buy_price:
                        log.warning('[涔板叆] 鏃犳硶鑾峰彇浠锋牸 %s' % s)
                        continue
            else:
                try:
                    raw = get_history(1, '1d', 'close', s, fq=None, include=False)
                    if raw is None or len(raw) == 0:
                        continue
                    close_list = raw.get(s)
                    if close_list is None:
                        for k in raw:
                            if k[:6] == s[:6]:
                                close_list = raw[k]
                                break
                    if close_list is None or len(close_list) == 0:
                        continue
                    last = close_list[-1]
                    if isinstance(last, (int, float)):
                        buy_price = round(float(last) * 1.002, 2)
                    else:
                        buy_price = round(float(last[4]) * 1.002, 2)
                except Exception:
                    log.warning('[涔板叆] 鏃犳硶鑾峰彇鍥炴祴浠锋牸 %s' % s)
                    continue

            shares = (int(value / buy_price) // 100) * 100
            if shares <= 0:
                log.info('[涔板叆] 璧勯噾涓嶈冻 %s' % s)
                continue

            name_info = get_stock_name([s])
            name = name_info.get(s, s) if isinstance(name_info, dict) else s
            g.code_name[s] = name
            log.info('[涔板叆] %s 鎶ヤ环%.2f 鏁伴噺%d' % (name, buy_price, shares))
            oid = order(s, shares, limit_price=buy_price)
            if oid is not None:
                g.pending_order = {'code': s, 'side': 'buy', 'order_id': oid}


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鍗栧嚭锛?1:25 / 14:50锛?# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
def sell(context, data):
    hold = list(context.portfolio.positions.keys())
    if not hold:
        return

    t = context.blotter.current_dt.strftime('%H:%M:%S')
    today_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')
    today_str = context.blotter.current_dt.strftime('%Y%m%d')

    price_df = get_price(hold, end_date=today_date_str, count=1,
                         fields=['close', 'high_limit'], panel=False)
    if price_df.empty:
        return
    price_df = price_df.set_index('code')

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
            if pos is None:
                continue
            sell_amount = int(float(pos.enable_amount))
            if sell_amount <= 0:
                log.info('[鍗栧嚭] T+1闄愬埗 %s' % s)
                continue
            now = float(price_df.loc[s, 'close']) if s in price_df.index else float(getattr(pos, 'avg_cost', 0) or 0)
            high_lmt = float(price_df.loc[s, 'high_limit']) if s in price_df.index else now * 1.1

            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), s):
                log.info('[鍗栧嚭] 宸叉湁鏈畬鎴愬崠鍗?%s' % s)
                continue

            cost = float(getattr(pos, 'avg_cost', 0) or 0)
            if now < high_lmt and now > cost:
                name = g.code_name.get(s, s)
                if is_trade():
                    sp = _get_live_price(s, direction='sell')
                    if sp:
                        oid = order(s, -sell_amount, limit_price=sp)
                    else:
                        oid = None
                else:
                    oid = order(s, -sell_amount)
                if oid is not None:
                    g.pending_order = {'code': s, 'side': 'sell', 'order_id': oid}
                log.info('[姝㈢泩鍗栧嚭] %s' % name)

    elif t == '14:50:00':
        for s in hold:
            pos = context.portfolio.positions.get(s)
            if pos is None:
                continue
            sell_amount = int(float(pos.enable_amount))
            if sell_amount <= 0:
                log.info('[鍗栧嚭] T+1闄愬埗 %s' % s)
                continue

            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), s):
                log.info('[鍗栧嚭] 宸叉湁鏈畬鎴愬崠鍗?%s' % s)
                continue

            now = float(price_df.loc[s, 'close']) if s in price_df.index else float(getattr(pos, 'avg_cost', 0) or 0)
            high_lmt = float(price_df.loc[s, 'high_limit']) if s in price_df.index else now * 1.1
            cost = float(getattr(pos, 'avg_cost', 0) or 0)

            close_h = get_history(4, '1d', 'close', security_list=s,
                                  fq=None, include=False)
            if close_h is not None and len(close_h) >= 4:
                ma4 = close_h['close'].mean()
                ma5 = (ma4 * 4 + now) / 5
            else:
                ma5 = cost

            name = g.code_name.get(s, s)
            if now < high_lmt and now > cost:
                if is_trade():
                    sp = _get_live_price(s, direction='sell')
                    if sp:
                        oid = order(s, -sell_amount, limit_price=sp)
                    else:
                        oid = None
                else:
                    oid = order(s, -sell_amount)
                if oid is not None:
                    g.pending_order = {'code': s, 'side': 'sell', 'order_id': oid}
                log.info('[姝㈢泩鍗栧嚭] %s' % name)
            elif now < ma5:
                if is_trade():
                    sp = _get_live_price(s, direction='sell')
                    if sp:
                        oid = order(s, -sell_amount, limit_price=sp)
                    else:
                        oid = None
                else:
                    oid = order(s, -sell_amount)
                if oid is not None:
                    g.pending_order = {'code': s, 'side': 'sell', 'order_id': oid}
                log.info('[姝㈡崯鍗栧嚭锛堣穼鐮?鏃ョ嚎锛塢 %s' % name)


# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
# 鍥炶皟鍑芥暟
# 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
def on_order_response(context, order_list):
    if not is_trade():
        return
    for item in _normalize_payload(order_list):
        try:
            status = str(item.get('status', ''))
            error_info = str(item.get('error_info', '')).strip()
            if error_info:
                log.error('[濮旀墭寮傚父] %s status=%s error=%s' % (item.get('stock_code', ''), status, error_info))
            if status == '9':
                log.warning('[搴熷崟] %s' % item.get('stock_code', ''))
            if status in ['5', '6', '8', '9']:
                _refresh_pending_order()
        except Exception:
            pass


def on_trade_response(context, trade_list):
    if not is_trade():
        return
    for item in _normalize_payload(trade_list):
        try:
            stock_code = str(item.get('stock_code', ''))
            business_amount = float(item.get('business_amount', 0) or 0)
            real_type = str(item.get('real_type', '0'))
            cancel_info = str(item.get('cancel_info', '')).strip()
            if real_type == '2' or cancel_info:
                log.warning('[鎾ゅ崟鎴愪氦] %s' % stock_code)
                _refresh_pending_order()
                continue
            if business_amount <= 0:
                continue
            entrust_bs = str(item.get('entrust_bs', ''))
            if entrust_bs == '1':
                g.pending_order = {'code': None, 'side': None, 'order_id': None}
                log.info('[涔板叆鎴愪氦] %s' % stock_code)
            elif entrust_bs == '2':
                g.pending_order = {'code': None, 'side': None, 'order_id': None}
                log.info('[鍗栧嚭鎴愪氦] %s' % stock_code)
        except Exception:
            pass
