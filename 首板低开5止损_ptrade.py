# ════════════════════════════════════════════════════════════════
# 首板低开5止损策略 - PTrade 版本
# ────────────────────────────────────────────────────────────────
# 原策略：https://www.joinquant.com/post/44901
# 作者：wywy1995 | PTrade适配：手动修复
#
# 逻辑：昨日首次涨停 → 今日低开3%-4% → 买入
# 止损：-5% 止损；11:28 / 14:50 若有利润清仓
#
# PTrade 关键 API 差异：
#   context.current_dt        → context.blotter.current_dt
#   context.previous_date     → _get_previous_trading_day(context)
#   pos.closeable_amount     → pos.enable_amount（T+1可卖）
#   order_value(code, cash)   → order(code, shares, limit_price=price)
#   order_target_value(code,0) → 先检查enable_amount再order(..., -amount, limit_price)
#   set_option/log.set_level → 删除（PTrade不支持）
# ════════════════════════════════════════════════════════════════

import pandas as pd
import datetime as dt


# ─────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────

def _get_previous_trading_day(context):
    """获取前一交易日"""
    try:
        return get_trading_day(-1)
    except Exception:
        dt_now = context.blotter.current_dt
        return get_trading_day(dt_now)


def _same_security(code_a, code_b):
    """判断两个代码是否同一标的（比较前6位）"""
    if not code_a or not code_b:
        return False
    return str(code_a)[:6] == str(code_b)[:6]


def _get_live_price(code, direction='buy'):
    """获取实时快照价格（股票2位小数）"""
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
    """兼容主推回调可能是单个对象或列表"""
    if not payload:
        return []
    if isinstance(payload, (list, tuple)):
        return list(payload)
    return [payload]


def _refresh_pending_order():
    """根据当前挂单刷新待成交状态"""
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


# ─────────────────────────────────────────────────────────────
# 初始化
# ─────────────────────────────────────────────────────────────
def initialize(context):
    set_parameters(server_restart_not_do_before="1", receive_cancel_response="1")

    g.pending_order = {'code': None, 'side': None, 'order_id': None}
    g.code_name = {}

    run_daily(context, buy,  time='09:30')
    run_daily(context, sell, time='09:35')
    run_daily(context, sell, time='11:28')
    run_daily(context, sell, time='14:50')

    if not is_trade():
        set_commission(commission_ratio=0.0003)
        set_slippage(slippage=0.001)
        set_benchmark('000300.XBHS')

    log.info('首板低开5止损策略 初始化完成')


def handle_data(context, data):
    pass


# ─────────────────────────────────────────────────────────────
# 买入：昨日首板 + 今日低开 3%-4%
# ─────────────────────────────────────────────────────────────
def buy(context, data):
    if len(context.portfolio.positions) > 0:
        return

    prev_day = _get_previous_trading_day(context)
    yesterday = prev_day.strftime('%Y-%m-%d')
    today_str = context.blotter.current_dt.strftime('%Y%m%d')

    pending = g.pending_order
    if pending.get('side') == 'buy' and pending.get('code'):
        log.info('[买入] 已有未完成买单，等待')
        return

    stock_list = get_Ashares(yesterday)

    st_status = get_stock_status(stock_list, 'ST', today_str)
    if st_status:
        stock_list = [s for s in stock_list if not st_status.get(s, False)]

    stock_list = [s for s in stock_list
                  if s[:2] != '68' and s[0] not in ['4', '8']]

    listed_info = get_stock_info(stock_list, 'listed_date') if stock_list else {}
    today_date = context.blotter.current_dt.date()
    stock_list = [s for s in stock_list
                  if s in listed_info
                  and listed_info[s].get('listed_date')
                  and (today_date - dt.datetime.strptime(
                      listed_info[s]['listed_date'], '%Y-%m-%d').date()).days > 250]

    halt_status = get_stock_status(stock_list, 'HALT', today_str) if stock_list else {}
    stock_list = [s for s in stock_list if not halt_status.get(s, False)]

    if not stock_list:
        return

    df = get_price(stock_list, end_date=yesterday, count=1,
                   fields=['close', 'high_limit'], panel=False)
    df = df.dropna()
    limit_up = df[df['close'] == df['high_limit']]['code'].tolist()
    if not limit_up:
        return

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

    yc_df = get_price(first_limit, end_date=yesterday, count=1,
                      fields=['close'], panel=False)
    if yc_df.empty:
        return
    yc = yc_df.set_index('code')

    buy_list = []
    today_open_df = get_price(first_limit,
                               end_date=context.blotter.current_dt.strftime('%Y-%m-%d'),
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

    if st_status:
        buy_list = [s for s in buy_list if not st_status.get(s, False)]

    if not buy_list:
        return

    cash = context.portfolio.cash / len(buy_list)
    for code in buy_list:
        if is_trade():
            buy_price = _get_live_price(code, direction='buy')
            if not buy_price:
                log.warning('[买入] 无法获取价格 %s，跳过' % code)
                continue
        else:
            try:
                raw = get_history(1, '1d', 'close', code, fq=None, include=False)
                if raw is None or len(raw) == 0:
                    continue
                close_list = raw.get(code)
                if close_list is None:
                    for k in raw:
                        if k[:6] == code[:6]:
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
                log.warning('[买入] 无法获取回测价格 %s' % code)
                continue

        shares = (int(cash / buy_price) // 100) * 100
        if shares <= 0:
            log.info('[买入] 资金不足 %s' % code)
            continue

        name_info = get_stock_name([code])
        name = name_info.get(code, code) if isinstance(name_info, dict) else code
        g.code_name[code] = name
        log.info('[买入] %s 报价%.2f 数量%d' % (name, buy_price, shares))
        oid = order(code, shares, limit_price=buy_price)
        if oid is not None:
            g.pending_order = {'code': code, 'side': 'buy', 'order_id': oid}


# ─────────────────────────────────────────────────────────────
# 卖出：止损 -5%；11:28 / 14:50 止盈清仓
# ─────────────────────────────────────────────────────────────
def sell(context, data):
    hold = list(context.portfolio.positions.keys())
    if not hold:
        return

    t = context.blotter.current_dt.strftime('%H:%M:%S')
    today_str = context.blotter.current_dt.strftime('%Y%m%d')
    today_date_str = context.blotter.current_dt.strftime('%Y-%m-%d')

    price_df = get_price(hold, end_date=today_date_str, count=1,
                         fields=['close', 'high_limit'], panel=False)
    if price_df.empty:
        return
    price_df = price_df.set_index('code')

    if is_trade():
        try:
            snap = get_snapshot(hold)
            for code in hold:
                if snap and code in snap:
                    price_df.loc[code, 'close'] = float(snap[code].get('last_px', price_df.loc[code, 'close']))
        except Exception:
            pass

    st_status = get_stock_status(hold, 'ST', today_str) or {}

    for code in hold:
        if st_status.get(code, False):
            pos = context.portfolio.positions.get(code)
            if pos:
                sell_amount = int(float(pos.enable_amount))
                if sell_amount > 0:
                    if is_trade():
                        sp = _get_live_price(code, direction='sell')
                        if sp:
                            order(code, -sell_amount, limit_price=sp)
                    else:
                        order(code, -sell_amount)
            log.info('[ST强制卖出] %s' % code)
            continue

        pos = context.portfolio.positions.get(code)
        if pos is None:
            continue
        sell_amount = int(float(pos.enable_amount))
        if sell_amount <= 0:
            log.info('[卖出] T+1限制，当日买入无法卖出 %s' % code)
            continue

        cost = float(getattr(pos, 'avg_cost', 0) or 0)
        now  = float(price_df.loc[code, 'close']) if code in price_df.index else cost
        high_lmt = float(price_df.loc[code, 'high_limit']) if code in price_df.index else now * 1.1

        if now <= cost * 0.95:
            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), code):
                log.info('[卖出] 已有未完成卖单，等待 %s' % code)
                continue
            name = g.code_name.get(code, code)
            if is_trade():
                sp = _get_live_price(code, direction='sell')
                if sp:
                    oid = order(code, -sell_amount, limit_price=sp)
                else:
                    oid = None
            else:
                oid = order(code, -sell_amount)
            if oid is not None:
                g.pending_order = {'code': code, 'side': 'sell', 'order_id': oid}
            log.info('[%s止损] 卖出：%s' % (name, code))
            continue

        if t == '11:28:00' and now > cost and now < high_lmt:
            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), code):
                continue
            name = g.code_name.get(code, code)
            if is_trade():
                sp = _get_live_price(code, direction='sell')
                if sp:
                    oid = order(code, -sell_amount, limit_price=sp)
                else:
                    oid = None
            else:
                oid = order(code, -sell_amount)
            if oid is not None:
                g.pending_order = {'code': code, 'side': 'sell', 'order_id': oid}
            log.info('[11:28止盈] 卖出：%s' % name)
            continue

        if t == '14:50:00' and now < high_lmt:
            pending = g.pending_order
            if pending.get('side') == 'sell' and _same_security(pending.get('code'), code):
                continue
            name = g.code_name.get(code, code)
            if is_trade():
                sp = _get_live_price(code, direction='sell')
                if sp:
                    oid = order(code, -sell_amount, limit_price=sp)
                else:
                    oid = None
            else:
                oid = order(code, -sell_amount)
            if oid is not None:
                g.pending_order = {'code': code, 'side': 'sell', 'order_id': oid}
            log.info('[14:50清仓] 卖出：%s' % name)
            continue


# ─────────────────────────────────────────────────────────────
# 回调函数
# ─────────────────────────────────────────────────────────────
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
