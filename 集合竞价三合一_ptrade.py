# -*- coding: utf-8 -*-
# ============================================================
# 集合竞价三合一策略 - PTrade 版本
# ============================================================
# 来源：JoinQuant 合并自4个策略
#   https://www.joinquant.com/post/49474  集合竞价三合一
#   https://www.joinquant.com/post/44901  首板低开
#   https://www.joinquant.com/post/48523  一进二集合竞价
#   https://www.joinquant.com/post/49364  弱转强
# PTrade适配：框架完全对齐 etf23轮动防过热.txt
#
# 逻辑：三个子策略并集
#   一进二 / 首板低开 / 弱转强
# 止损修改：跌破5日线
#
# JoinQuant → PTrade 核心差异：
#   context.current_dt        → context.blotter.current_dt
#   context.previous_date     → _get_previous_trading_day(context)
#   attribute_history()        → get_history()
#   get_valuation()           → get_fundamentals()
#   get_call_auction()        → get_trend_data() (实盘) / get_price()(回测)
#   order_value()             → order(code, shares, limit_price)
#   order_target_value(,0)    → order(code, -amount, limit_price)
# ============================================================

import datetime as dt
from datetime import datetime, timedelta


# ──────────────────── 工具函数 ────────────────────

def _get_previous_trading_day(context):
    """获取上一个交易日，返回字符串'YYYY-MM-DD'"""
    try:
        days = get_trade_days(count=2)
        if days is not None and len(days) >= 2:
            return str(days[-2])[:10]
        return str(days[-1])[:10] if days is not None and len(days) >= 1 else None
    except Exception:
        return (context.blotter.current_dt - dt.timedelta(days=1)).strftime('%Y-%m-%d')


def _same_security(code_a, code_b):
    if not code_a or not code_b:
        return False
    return str(code_a)[:6] == str(code_b)[:6]


def _extract_val(raw_dict, code):
    """从get_history(is_dict=True)返回中提取数据列表"""
    if not raw_dict:
        return None
    arr = raw_dict.get(code)
    if arr is None:
        for old, new in [('.SS', '.XSHG'), ('.SZ', '.XSHE'),
                         ('.XSHG', '.SS'), ('.XSHE', '.SZ')]:
            alt = code.replace(old, new)
            if alt in raw_dict:
                arr = raw_dict[alt]
                break
        if arr is None:
            code6 = code[:6]
            for k in raw_dict:
                if k[:6] == code6:
                    arr = raw_dict[k]
                    break
    if arr is None or len(arr) == 0:
        return None
    res = []
    for row in arr:
        try:
            if isinstance(row, (int, float)):
                res.append(float(row))
            elif hasattr(row, '__len__'):
                if len(row) >= 5:
                    res.append(float(row[4]))
                elif len(row) > 0:
                    res.append(float(row[-1]))
        except Exception:
            pass
    return res if res else None


def _listed_days(listed_date_str, today_date):
    """计算上市天数，兼容YYYYMMDD和YYYY-MM-DD格式"""
    try:
        s = str(listed_date_str).replace('-', '')[:8]
        ld = dt.datetime.strptime(s, '%Y%m%d').date()
        return (today_date - ld).days
    except Exception:
        return 0


def _do_sell(code, sell_amount, now=None):
    """统一卖出函数：实盘用snapshot报价，回测用当前价报价"""
    if is_trade():
        sp = _get_live_price(code, direction='sell')
        if sp:
            return order(code, -sell_amount, limit_price=sp)
        return None
    else:
        if now and now > 0:
            sell_price = round(float(now) * 0.998, 2)
        else:
            try:
                raw = get_history(1, '1d', 'close', code, fq='pre', include=True, is_dict=True)
                vals = _extract_val(raw, code)
                sell_price = round(vals[-1] * 0.998, 2) if vals else None
            except Exception:
                sell_price = None
        if sell_price:
            return order(code, -sell_amount, limit_price=sell_price)
        return order(code, -sell_amount)


def _get_live_price(code, direction='buy'):
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


# ──────────────────── 日期处理 ────────────────────

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


# ──────────────────── 过滤函数 ────────────────────

def _filter_new_stock(initial_list, date, days=50):
    d_date = _transform_date(date, 'd')
    listed_info = get_stock_info(initial_list, ['listed_date']) if initial_list else {}
    return [s for s in initial_list
            if s in listed_info
            and listed_info[s].get('listed_date')
            and _listed_days(listed_info[s]['listed_date'], d_date) > days]


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


# ──────────────────── 股票池准备 ────────────────────

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


# ──────────────────── 涨停相关 ────────────────────

def _get_hl_stock(initial_list, date):
    if not initial_list:
        return []
    df = get_price(initial_list, end_date=date, count=1,
                   fields=['close', 'high_limit'],
                   panel=False).dropna()
    return list(df[df['close'] == df['high_limit']]['code'])


def _get_ever_hl_stock(initial_list, date):
    if not initial_list:
        return []
    df = get_price(initial_list, end_date=date, count=1,
                   fields=['high', 'high_limit'],
                   panel=False).dropna()
    return list(df[df['high'] == df['high_limit']]['code'])


def _get_ever_hl_stock2(initial_list, date):
    if not initial_list:
        return []
    df = get_price(initial_list, end_date=date, count=1,
                   fields=['close', 'high', 'high_limit'],
                   panel=False).dropna()
    mask = (df['high'] == df['high_limit']) & (df['close'] != df['high_limit'])
    return list(df[mask]['code'])


def _get_hl_count_df(hl_list, date, watch_days):
    if not hl_list:
        return __import__('pandas').DataFrame(columns=['count', 'extreme_count'])
    df = get_price(hl_list, end_date=date, count=watch_days,
                   fields=['close', 'high_limit', 'low'],
                   panel=False)
    hl_count, ex_count = [], []
    for stock in hl_list:
        sub = df[df['code'] == stock]
        hl_count.append((sub['close'] == sub['high_limit']).sum())
        ex_count.append((sub['low'] == sub['high_limit']).sum())
    return __import__('pandas').DataFrame(
        {'count': hl_count, 'extreme_count': ex_count}, index=hl_list)


def _get_continue_count_df(hl_list, date, watch_days):
    if not hl_list:
        return __import__('pandas').DataFrame()
    frames = []
    for d in range(2, watch_days + 1):
        hlc = _get_hl_count_df(hl_list, date, d)
        frames.append(hlc[hlc['count'] == d])
    if not frames:
        return __import__('pandas').DataFrame()
    combined = __import__('pandas').concat(frames)
    ccd_list = []
    for s in set(combined.index):
        tmp = combined.loc[[s]]
        ccd_list.append(tmp.loc[[s], :].nlargest(1, 'count'))
    if not ccd_list:
        return __import__('pandas').DataFrame()
    ccd = __import__('pandas').concat(ccd_list).sort_values('count', ascending=False)
    return ccd


def _get_relative_position_df(stock_list, date, watch_days):
    if not stock_list:
        return __import__('pandas').DataFrame(columns=['rp'])
    df = get_price(stock_list, end_date=date, count=watch_days,
                   fields=['high', 'low', 'close'],
                   panel=False).dropna()
    if df.empty:
        return __import__('pandas').DataFrame(columns=['rp'])
    close = df.groupby('code').apply(lambda x: x['close'].iloc[-1])
    high = df.groupby('code')['high'].max()
    low = df.groupby('code')['low'].min()
    result = __import__('pandas').DataFrame()
    result['rp'] = (close - low) / (high - low).replace(0, float('nan'))
    return result.dropna()


# ──────────────────── 左压天数 ────────────────────

def _calculate_zyts(s, context):
    raw_h = get_history(101, '1d', 'high', s, fq='pre', include=False, is_dict=True)
    highs = _extract_val(raw_h, s)
    if highs is None or len(highs) < 2:
        return 10
    prev_high = highs[-1]
    zyts_0 = 100
    for i, h in enumerate(reversed(highs[:-2]), 2):
        if h >= prev_high:
            zyts_0 = i - 1
            break
    return zyts_0 + 5


# ──────────────────── 竞价数据 ────────────────────

def _get_auction_data(s, context, prev_vol=None):
    """获取竞价数据：实盘用get_trend_data，回测用开盘价近似
    参数: prev_vol — 昨日成交量(数值)
    返回: (竞价量, 竞价价格, 昨日涨停价) 或 (None, None, None)
    """
    if is_trade():
        try:
            today_str = context.blotter.current_dt.strftime('%Y%m%d')
            td = get_trend_data(date=today_str, stocks=s)
            if not td:
                return None, None, None
            # get_trend_data可能用不同后缀
            info = td.get(s)
            if info is None:
                for k in td:
                    if k[:6] == s[:6]:
                        info = td[k]
                        break
            if not info:
                return None, None, None
            auction_vol = info.get('business_amount', 0)
            auction_px = info.get('hq_px', 0)
            # 昨日涨停价
            yest_str = _get_previous_trading_day(context)
            raw_hl = get_history(1, '1d', 'high_limit', s, fq='pre', include=False, is_dict=True)
            hl_vals = _extract_val(raw_hl, s)
            hl_px = hl_vals[-1] if hl_vals else auction_px * 1.1
            return auction_vol, auction_px, hl_px
        except Exception:
            return None, None, None
    else:
        try:
            # 回测：用今日开盘价近似竞价价格，竞价量按昨日量5%估算
            raw_o = get_history(1, '1d', 'open', s, fq='pre', include=True, is_dict=True)
            open_vals = _extract_val(raw_o, s)
            raw_hl = get_history(1, '1d', 'high_limit', s, fq='pre', include=False, is_dict=True)
            hl_vals = _extract_val(raw_hl, s)
            if not open_vals or not hl_vals:
                return None, None, None
            open_px = open_vals[-1]
            hl_px = hl_vals[-1]
            auction_vol_est = prev_vol * 0.05 if prev_vol else 0
            return auction_vol_est, open_px, hl_px
        except Exception:
            return None, None, None


# ──────────────────── 初始化 ────────────────────

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

    log.info('集合竞价三合一策略 初始化完成')


def before_trading_start(context, data):
    """盘前重置状态"""
    g.pending_order = {'code': None, 'side': None, 'order_id': None}
    log.info('[盘前] 集合竞价三合一策略准备就绪')


def handle_data(context, data):
    pass


# ──────────────────── 选股（9:01） ────────────────────

def get_stock_list(context):
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


# ──────────────────── 买入（9:26） ────────────────────

def buy(context):
    qualified_stocks = []
    gk_stocks = []
    dk_stocks = []
    rzq_stocks = []

    date_str = _get_previous_trading_day(context)  # 返回 'YYYY-MM-DD'
    today_str = context.blotter.current_dt.strftime('%Y%m%d')
    today_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')

    pending = g.pending_order
    if pending.get('side') == 'buy' and pending.get('code'):
        log.info('[买入] 已有未完成买单，等待')
        return

    # ── 一进二 ──
    for s in g.target_list:
        try:
            raw_c = get_history(1, '1d', 'close', s, fq='pre', include=False, is_dict=True)
            raw_v = get_history(1, '1d', 'volume', s, fq='pre', include=False, is_dict=True)
            raw_m = get_history(1, '1d', 'money', s, fq='pre', include=False, is_dict=True)
            cl = _extract_val(raw_c, s)
            vl = _extract_val(raw_v, s)
            ml = _extract_val(raw_m, s)
            if cl is None or vl is None or ml is None:
                continue
            prev_close = cl[-1]
            prev_vol = vl[-1]
            prev_money = ml[-1]
        except Exception:
            continue
        if prev_money < 5.5e8 or prev_money > 20e8:
            continue
        avg_px_inc = prev_money / prev_vol / prev_close * 1.1 - 1
        if avg_px_inc < 0.07:
            continue

        try:
            val = get_fundamentals(s, 'valuation',
                                  fields=['market_cap', 'circulating_market_cap'],
                                  date=date_str.replace('-', ''))
            if val is None or val.empty:
                continue
            if val['market_cap'].iloc[0] < 70 or val['circulating_market_cap'].iloc[0] > 520:
                continue
        except Exception:
            continue

        zyts = _calculate_zyts(s, context)
        raw_vz = get_history(zyts, '1d', 'volume', s, fq='pre', include=False, is_dict=True)
        vols = _extract_val(raw_vz, s)
        if vols is None or len(vols) < 2:
            continue
        if vols[-1] <= max(vols[:-1]) * 0.9:
            continue

        auction_vol, auction_px, hl_px = _get_auction_data(s, context, prev_vol)
        if auction_vol is None:
            continue
        if auction_vol / prev_vol < 0.03:
            continue

        current_ratio = auction_px / (hl_px / 1.1)
        if current_ratio <= 1 or current_ratio >= 1.06:
            continue

        gk_stocks.append(s)
        qualified_stocks.append(s)

    # ── 首板低开 ──
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

        for s in stock_list:
            try:
                # 昨收
                raw_yc = get_history(1, '1d', 'close', s, fq='pre', include=False, is_dict=True)
                yc_vals = _extract_val(raw_yc, s)
                if not yc_vals:
                    continue
                yest_close = yc_vals[-1]
                # 今开
                raw_to = get_history(1, '1d', 'open', s, fq='pre', include=True, is_dict=True)
                to_vals = _extract_val(raw_to, s)
                if not to_vals:
                    continue
                today_open_px = to_vals[-1]
                open_pct = today_open_px / yest_close
                if 0.955 <= open_pct <= 0.97:
                    raw_pm = get_history(1, '1d', 'money', s, fq='pre', include=False, is_dict=True)
                    pm_vals = _extract_val(raw_pm, s)
                    if pm_vals and pm_vals[-1] >= 1e8:
                        dk_stocks.append(s)
                        qualified_stocks.append(s)
            except Exception:
                continue

    # ── 弱转强 ──
    for s in g.target_list2:
        try:
            # 4日涨幅
            raw_4c = get_history(4, '1d', 'close', s, fq='pre', include=False, is_dict=True)
            cl4 = _extract_val(raw_4c, s)
            if not cl4 or len(cl4) < 4:
                continue
            inc = (cl4[-1] - cl4[0]) / cl4[0]
            if inc > 0.28:
                continue

            # 昨日开收盘
            raw_o1 = get_history(1, '1d', 'open', s, fq='pre', include=False, is_dict=True)
            raw_c1 = get_history(1, '1d', 'close', s, fq='pre', include=False, is_dict=True)
            ol1 = _extract_val(raw_o1, s)
            cl1 = _extract_val(raw_c1, s)
            if not ol1 or not cl1:
                continue
            oc_ratio = (cl1[-1] - ol1[-1]) / ol1[-1]
            if oc_ratio < -0.05:
                continue

            # 昨日成交额/量
            raw_v1 = get_history(1, '1d', 'volume', s, fq='pre', include=False, is_dict=True)
            raw_m1 = get_history(1, '1d', 'money', s, fq='pre', include=False, is_dict=True)
            vl1 = _extract_val(raw_v1, s)
            ml1 = _extract_val(raw_m1, s)
            if not vl1 or not ml1:
                continue
            prev_money2 = ml1[-1]
            prev_vol2 = vl1[-1]
            prev_close2 = cl1[-1]
            if prev_money2 < 3e8 or prev_money2 > 19e8:
                continue
            avg_px_inc2 = prev_money2 / prev_vol2 / prev_close2 - 1
            if avg_px_inc2 < -0.04:
                continue
        except Exception:
            continue

        try:
            val2 = get_fundamentals(s, 'valuation',
                                  fields=['market_cap', 'circulating_market_cap'],
                                  date=date_str.replace('-', ''))
            if val2 is None or val2.empty:
                continue
            if val2['market_cap'].iloc[0] < 70 or val2['circulating_market_cap'].iloc[0] > 520:
                continue
        except Exception:
            continue

        zyts2 = _calculate_zyts(s, context)
        raw_vz2 = get_history(zyts2, '1d', 'volume', s, fq='pre', include=False, is_dict=True)
        vols2 = _extract_val(raw_vz2, s)
        if vols2 is None or len(vols2) < 2:
            continue
        if vols2[-1] <= max(vols2[:-1]) * 0.9:
            continue

        auction_vol2, auction_px2, hl_px2 = _get_auction_data(s, context, prev_vol2)
        if auction_vol2 is None:
            continue
        if auction_vol2 / prev_vol2 < 0.03:
            continue
        cr2 = auction_px2 / (hl_px2 / 1.1)
        if cr2 <= 0.98 or cr2 >= 1.09:
            continue

        rzq_stocks.append(s)
        qualified_stocks.append(s)

    if qualified_stocks:
        log.info('==================================================')
        log.info('今日选股：' + ','.join(qualified_stocks))
        log.info('一进二：' + ','.join(gk_stocks))
        log.info('首板低开：' + ','.join(dk_stocks))
        log.info('弱转强：' + ','.join(rzq_stocks))
        log.info('==================================================')
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

            if is_trade():
                if open_px and open_px > 0:
                    buy_price = round(open_px * 1.002, 2)
                else:
                    buy_price = _get_live_price(s, direction='buy')
                    if not buy_price:
                        log.warning('[买入] 无法获取价格 %s' % s)
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
                    log.warning('[买入] 无法获取回测价格 %s' % s)
                    continue

            shares = (int(value / buy_price) // 100) * 100
            if shares <= 0:
                log.info('[买入] 资金不足 %s' % s)
                continue

            name_info = get_stock_name([s])
            name = name_info.get(s, s) if isinstance(name_info, dict) else s
            g.code_name[s] = name
            log.info('[买入] %s 报价%.2f 数量%d' % (name, buy_price, shares))
            oid = order(s, shares, limit_price=buy_price)
            if oid is not None:
                g.pending_order = {'code': s, 'side': 'buy', 'order_id': oid}


# ──────────────────── 卖出（11:25 / 14:50） ────────────────────

def sell(context):
    hold = list(context.portfolio.positions.keys())
    if not hold:
        return

    t = context.blotter.current_dt.strftime('%H:%M:%S')
    today_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')
    today_str = context.blotter.current_dt.strftime('%Y%m%d')

    price_df = get_price(hold, end_date=today_date_str, count=1,
                         fields=['close', 'high_limit'])
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
                log.info('[卖出] T+1限制 %s' % s)
                continue
            now = float(price_df.loc[s, 'close']) if s in price_df.index else \
                float(getattr(pos, 'cost_basis', 0) or 0)
            high_lmt = float(price_df.loc[s, 'high_limit']) if s in price_df.index else now * 1.1

            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), s):
                log.info('[卖出] 已有未完成卖单 %s' % s)
                continue

            cost = float(getattr(pos, 'cost_basis', 0) or 0)
            if now < high_lmt and now > cost:
                name = g.code_name.get(s, s)
                oid = _do_sell(s, sell_amount, now)
                if oid is not None:
                    g.pending_order = {'code': s, 'side': 'sell', 'order_id': oid}
                log.info('[止盈卖出] %s' % name)

    elif t == '14:50:00':
        for s in hold:
            pos = context.portfolio.positions.get(s)
            if pos is None:
                continue
            sell_amount = int(float(pos.enable_amount))
            if sell_amount <= 0:
                log.info('[卖出] T+1限制 %s' % s)
                continue

            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), s):
                log.info('[卖出] 已有未完成卖单 %s' % s)
                continue

            now = float(price_df.loc[s, 'close']) if s in price_df.index else \
                float(getattr(pos, 'cost_basis', 0) or 0)
            high_lmt = float(price_df.loc[s, 'high_limit']) if s in price_df.index else now * 1.1
            cost = float(getattr(pos, 'cost_basis', 0) or 0)

            raw_ch = get_history(4, '1d', 'close', s, fq='pre', include=False, is_dict=True)
            ch_vals = _extract_val(raw_ch, s)
            if ch_vals and len(ch_vals) >= 4:
                ma4 = sum(ch_vals) / len(ch_vals)
                ma5 = (ma4 * 4 + now) / 5
            else:
                ma5 = cost

            name = g.code_name.get(s, s)
            if now < high_lmt and now > cost:
                oid = _do_sell(s, sell_amount, now)
                if oid is not None:
                    g.pending_order = {'code': s, 'side': 'sell', 'order_id': oid}
                log.info('[止盈卖出] %s' % name)
            elif now < ma5:
                oid = _do_sell(s, sell_amount, now)
                if oid is not None:
                    g.pending_order = {'code': s, 'side': 'sell', 'order_id': oid}
                log.info('[止损卖出（跌破5日线）] %s' % name)


# ──────────────────── 回调函数 ────────────────────

def on_order_response(context, order_list):
    if not is_trade():
        return
    for item in _normalize_payload(order_list):
        try:
            status = str(item.get('status', ''))
            error_info = str(item.get('error_info', '')).strip()
            if error_info:
                log.error('[委托异常] %s status=%s error=%s' % (item.get('stock_code', ''), status, error_info))
            if status == '9':
                log.warning('[废单] %s' % item.get('stock_code', ''))
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
                log.warning('[撤单成交] %s' % stock_code)
                _refresh_pending_order()
                continue
            if business_amount <= 0:
                continue
            entrust_bs = str(item.get('entrust_bs', ''))
            if entrust_bs == '1':
                g.pending_order = {'code': None, 'side': None, 'order_id': None}
                log.info('[买入成交] %s' % stock_code)
            elif entrust_bs == '2':
                g.pending_order = {'code': None, 'side': None, 'order_id': None}
                log.info('[卖出成交] %s' % stock_code)
        except Exception:
            pass
