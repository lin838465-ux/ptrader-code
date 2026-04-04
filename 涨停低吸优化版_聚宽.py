# ═══════════════════════════════════════════════════════════════
# 涨停低吸优化版 - 聚宽(JoinQuant)回测版
# ─────────────────────────────────────────────────────────────
# 综合自：4号低开低吸 + 首板低开 + 集合竞价三合一
#
# 核心逻辑：
#   近5日有涨停记忆 → 昨日回调 → 今日轻微低开 → 打分选最优1只
#   盘中 +5% 止盈（立即卖）
#   14:55 才检查止损（-5%才卖，没到不卖）
#   最长持仓7天超时平仓
#
# 回测设置建议：2020-01-01 ~ 至今, 20万, 分钟级别
# ═══════════════════════════════════════════════════════════════

from jqdata import *
import numpy as np
import datetime as dt


# ─────────────────────────────────────────────────────────────
# 用户可调参数（改这里就行）
# ─────────────────────────────────────────────────────────────

CONF = {
    '每日买入只数': 1,           # 每天最多买几只，建议1
    '止盈比例': 0.05,            # 盘中达到就卖，默认+5%
    '止损比例': 0.05,            # 14:55检查，达到才卖，默认-5%
    '盘中硬止损': 0.03,          # 盘中跌超3%立即割，不等14:55（防暴跌）
    '最长持仓天数': 7,           # 超过N天强制平仓
    '低开下限': -0.09,           # 低开太多不买（-9%）
    '低开上限': 0.003,           # 高开不买（+0.3%以内可以）
    '理想低开': -0.012,          # 打分基准：低开-1.2%最佳
    '要求昨日回调': True,        # 昨天必须是阴线或下跌
    '昨日最大跌幅': -0.04,       # 昨天跌超4%不买（跌太多的第二天往往继续跌）
    '连亏暂停天数': 2,           # 连续亏损后暂停N天不买
    '偏好大市值': True,          # 大市值加分
    '主板池上限': 1500,          # 流通市值前N只
}


def initialize(context):
    set_option('use_real_price', True)
    set_option('avoid_future_data', True)
    log.set_level('system', 'error')
    set_benchmark('000300.XSHG')

    g.buy_count = CONF['每日买入只数']
    g.tp_ratio = CONF['止盈比例']
    g.sl_ratio = CONF['止损比例']
    g.max_hold_days = CONF['最长持仓天数']
    g.min_open_gap = CONF['低开下限']
    g.max_open_gap = CONF['低开上限']
    g.target_open_gap = CONF['理想低开']
    g.require_pullback = CONF['要求昨日回调']
    g.prefer_large_cap = CONF['偏好大市值']
    g.max_pool = CONF['主板池上限']

    g.intraday_sl = CONF['盘中硬止损']
    g.max_yesterday_drop = CONF['昨日最大跌幅']
    g.pause_after_loss = CONF['连亏暂停天数']

    g.hold_days = {}
    g.today_bought = 0
    g.consecutive_losses = 0     # 连续亏损次数
    g.pause_until = None         # 暂停买入直到这个日期

    run_daily(execute_buy, time='09:30')
    run_daily(check_stop_loss, time='14:55')

    log.info('涨停低吸优化版 V2 初始化完成')
    log.info('止盈: +%.1f%% | 止损: -%.1f%%(14:55) | 盘中硬止损: -%.1f%%' % (
        g.tp_ratio * 100, g.sl_ratio * 100, g.intraday_sl * 100))
    log.info('昨跌上限: %.1f%% | 连亏暂停: %d天 | 持仓上限: %d天' % (
        g.max_yesterday_drop * 100, g.pause_after_loss, g.max_hold_days))


def before_trading_start(context):
    g.today_bought = 0

    # 同步持仓天数
    positions = context.portfolio.positions
    for stock in list(g.hold_days.keys()):
        if stock not in positions or positions[stock].total_amount == 0:
            del g.hold_days[stock]
        else:
            g.hold_days[stock] += 1

    # 盘前检查：持仓股今日开盘价是否已跳空亏损超3%
    curr_data = get_current_data()
    for stock in list(g.hold_days.keys()):
        if stock not in positions or positions[stock].total_amount == 0:
            continue
        cost = positions[stock].avg_cost
        if cost <= 0:
            continue
        try:
            open_px = curr_data[stock].day_open
            if open_px > 0:
                open_ret = open_px / cost - 1
                if open_ret <= -g.intraday_sl:
                    g.gap_down_stocks = getattr(g, 'gap_down_stocks', set())
                    g.gap_down_stocks.add(stock)
                    log.info('[盘前预警] %s 开盘跳空%.2f%%，第一分钟立即割' % (stock, open_ret * 100))
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# 盘中：每分钟止盈检查
# ─────────────────────────────────────────────────────────────

def handle_data(context, data):
    """盘中：跳空立割 + 止盈 + 硬止损"""
    curr_data = get_current_data()
    positions = context.portfolio.positions

    # 跳空跌的票第一时间割（抚顺特钢-8.56%那种）
    gap_down = getattr(g, 'gap_down_stocks', set())
    if gap_down:
        for stock in list(gap_down):
            if stock in positions and positions[stock].closeable_amount > 0:
                cost = positions[stock].avg_cost
                px = curr_data[stock].last_price
                ret = px / cost - 1 if cost > 0 else 0
                order_target_value(stock, 0)
                days = g.hold_days.get(stock, 0)
                name = get_security_info(stock).display_name
                log.info('[跳空止损] %s %s | 成本:%.2f | 现价:%.2f | %.2f%% | 持%d天' % (
                    stock, name, cost, px, ret * 100, days))
                if stock in g.hold_days:
                    del g.hold_days[stock]
                g.consecutive_losses += 1
                if g.consecutive_losses >= 2:
                    g.pause_until = context.current_dt.date() + dt.timedelta(days=g.pause_after_loss + 1)
                    log.info('[连亏暂停] 连续亏损%d次，暂停买入至%s' % (g.consecutive_losses, g.pause_until))
        g.gap_down_stocks = set()

    for stock in list(positions.keys()):
        pos = positions[stock]
        if pos.total_amount <= 0 or pos.closeable_amount <= 0:
            continue

        cost = pos.avg_cost
        curr_price = curr_data[stock].last_price
        if cost <= 0 or curr_price <= 0:
            continue

        ret = curr_price / cost - 1

        # 盘中止盈：+5%
        if ret >= g.tp_ratio:
            order_target_value(stock, 0)
            days = g.hold_days.get(stock, 0)
            name = get_security_info(stock).display_name
            log.info('[盘中止盈] %s %s | 成本:%.2f | 现价:%.2f | +%.2f%% | 持%d天' % (
                stock, name, cost, curr_price, ret * 100, days))
            if stock in g.hold_days:
                del g.hold_days[stock]
            g.consecutive_losses = 0  # 盈利重置连亏计数

        # 盘中硬止损：-3%立即割（不等14:55，防暴跌扩大到-8%）
        elif ret <= -g.intraday_sl:
            order_target_value(stock, 0)
            days = g.hold_days.get(stock, 0)
            name = get_security_info(stock).display_name
            log.info('[盘中硬止损] %s %s | 成本:%.2f | 现价:%.2f | %.2f%% | 持%d天' % (
                stock, name, cost, curr_price, ret * 100, days))
            if stock in g.hold_days:
                del g.hold_days[stock]
            g.consecutive_losses += 1
            if g.consecutive_losses >= 2:
                g.pause_until = context.current_dt.date() + dt.timedelta(days=g.pause_after_loss + 1)
                log.info('[连亏暂停] 连续亏损%d次，暂停买入至%s' % (
                    g.consecutive_losses, g.pause_until))


# ─────────────────────────────────────────────────────────────
# 买入逻辑（9:30）
# ─────────────────────────────────────────────────────────────

def execute_buy(context):
    if g.today_bought >= g.buy_count:
        return

    # 已有持仓不买
    for stock, pos in context.portfolio.positions.items():
        if pos.total_amount > 0:
            return

    # 连亏暂停检查
    if g.pause_until and context.current_dt.date() <= g.pause_until:
        log.info('[连亏暂停] 暂停中，%s恢复' % g.pause_until)
        return

    # 大盘过滤
    if _is_market_crash(context):
        log.info('[大盘过滤] 沪深300跌超1.5%%，今日不买')
        return

    # 选股
    candidates = _get_candidates(context)
    if not candidates:
        log.info('[选股] 今日无符合条件标的')
        return

    # 打印前5名
    for i, item in enumerate(candidates[:5]):
        name = get_security_info(item['stock']).display_name
        log.info('[候选%d] %s %s | 低开:%.2f%% | 昨跌:%.2f%% | 涨停距:%d天 | 分:%.3f' % (
            i + 1, item['stock'], name,
            item['open_gap'] * 100, item['y_ret'] * 100,
            item['days_since_limit'], item['score']))

    # 买最优1只
    best = candidates[0]
    stock = best['stock']
    cash = context.portfolio.cash * 0.98
    if cash < 2000:
        return

    order_value(stock, cash)
    g.hold_days[stock] = 0
    g.today_bought += 1
    name = get_security_info(stock).display_name
    log.info('[买入] %s %s | 低开:%.2f%% | 得分:%.3f' % (
        stock, name, best['open_gap'] * 100, best['score']))


# ─────────────────────────────────────────────────────────────
# 14:55 止损 + 超时
# ─────────────────────────────────────────────────────────────

def check_stop_loss(context):
    """14:55检查：达到止损才卖，没到不卖"""
    curr_data = get_current_data()
    positions = context.portfolio.positions

    for stock in list(positions.keys()):
        pos = positions[stock]
        if pos.total_amount <= 0 or pos.closeable_amount <= 0:
            continue

        cost = pos.avg_cost
        curr_price = curr_data[stock].last_price
        if cost <= 0 or curr_price <= 0:
            continue

        ret = curr_price / cost - 1
        hold = g.hold_days.get(stock, 0)
        name = get_security_info(stock).display_name

        if ret <= -g.sl_ratio:
            order_target_value(stock, 0)
            log.info('[14:55止损] %s %s | %.2f%% | 持%d天' % (stock, name, ret * 100, hold))
            if stock in g.hold_days:
                del g.hold_days[stock]
            g.consecutive_losses += 1
            if g.consecutive_losses >= 2:
                g.pause_until = context.current_dt.date() + dt.timedelta(days=g.pause_after_loss + 1)
                log.info('[连亏暂停] 连续亏损%d次，暂停买入至%s' % (
                    g.consecutive_losses, g.pause_until))

        elif hold >= g.max_hold_days:
            order_target_value(stock, 0)
            log.info('[14:55超时] %s %s | %.2f%% | 持%d天' % (stock, name, ret * 100, hold))
            if stock in g.hold_days:
                del g.hold_days[stock]

        else:
            log.info('[14:55持有] %s %s | %.2f%% | 持%d天' % (stock, name, ret * 100, hold))


# ─────────────────────────────────────────────────────────────
# 选股 + 打分
# ─────────────────────────────────────────────────────────────

def _get_candidates(context):
    """选股：近5日有涨停 + 昨日回调 + 今日低开 → 打分排序"""
    yesterday = context.previous_date
    curr_data = get_current_data()

    # 1. 建主板池
    stock_pool = _build_main_board_pool(context)
    if not stock_pool:
        return []

    # 2. 批量取近8日K线
    df = get_price(stock_pool, end_date=yesterday, count=8,
                   fields=['open', 'close', 'high', 'low', 'high_limit'],
                   panel=False, fill_paused=False, skip_paused=True)
    if df is None or df.empty:
        return []

    candidates = []
    for stock in stock_pool:
        sub = df[df['code'] == stock]
        if len(sub) < 7:
            continue

        closes = sub['close'].values
        opens = sub['open'].values
        high_limits = sub['high_limit'].values

        y_close = float(closes[-1])
        y_open = float(opens[-1])
        prev_close = float(closes[-2])
        if y_close <= 0 or prev_close <= 0 or y_open <= 0:
            continue

        # ── 条件1：近5日内有涨停（close == high_limit）──
        latest_limit_idx = None
        for i in range(-6, -1):
            try:
                c = float(closes[i])
                hl = float(high_limits[i])
                if hl > 0 and abs(c - hl) < 0.01:
                    latest_limit_idx = i
            except Exception:
                continue
        if latest_limit_idx is None:
            continue
        days_since_limit = abs(latest_limit_idx + 1)

        # ── 条件2：昨日回调（但不能跌太多）──
        y_ret = y_close / prev_close - 1
        is_pullback = (y_close < y_open) or (y_close < prev_close)
        if g.require_pullback and not is_pullback:
            continue
        # 昨天跌超4%的不买（天娱数科-5.53%、文投控股-5.36%这种买了就亏）
        if y_ret < g.max_yesterday_drop:
            continue

        # ── 条件3：今日低开 ──
        try:
            open_px = curr_data[stock].day_open
        except Exception:
            continue
        if open_px <= 0:
            continue
        open_gap = open_px / y_close - 1
        if open_gap < g.min_open_gap or open_gap > g.max_open_gap:
            continue

        # ── 条件4：不是停牌/ST（双重检查，防*ST漏网）──
        try:
            if curr_data[stock].paused or curr_data[stock].is_st:
                continue
            # 名称里含ST也过滤（防*ST漏网）
            stock_name = get_security_info(stock).display_name
            if 'ST' in stock_name or 'st' in stock_name:
                continue
        except Exception:
            pass

        # ── 打分（越低越好）──
        gap_penalty = abs(open_gap - g.target_open_gap) * 100
        limit_recency_penalty = days_since_limit * 0.6
        pullback_penalty = abs(y_ret) * 8
        deep_gap_penalty = max(0, abs(open_gap) - 0.03) * 20
        positive_gap_penalty = max(0, open_gap) * 40
        y_body_ret = y_close / y_open - 1
        body_penalty = max(0, abs(y_body_ret) - 0.08) * 20

        score = (gap_penalty + limit_recency_penalty + pullback_penalty
                 + deep_gap_penalty + positive_gap_penalty + body_penalty)

        candidates.append({
            'stock': stock,
            'open_gap': open_gap,
            'y_ret': y_ret,
            'y_close': y_close,
            'is_pullback': is_pullback,
            'days_since_limit': days_since_limit,
            'score': score,
        })

    # 大市值偏好
    if g.prefer_large_cap and candidates:
        _apply_large_cap_bonus(context, candidates)

    candidates.sort(key=lambda x: x['score'])
    return candidates


def _apply_large_cap_bonus(context, candidates):
    """大市值加分"""
    codes = [item['stock'] for item in candidates]
    try:
        val_df = get_valuation(codes, context.previous_date,
                               fields=['circulating_market_cap'])
        if val_df is None or val_df.empty:
            return
        val_df = val_df.set_index('code').sort_values('circulating_market_cap', ascending=False)
        total = max(len(val_df) - 1, 1)
        cap_map = {}
        for idx, code in enumerate(val_df.index):
            cap_map[code] = (idx / float(total)) * 1.2
        for item in candidates:
            if item['stock'] in cap_map:
                item['score'] += cap_map[item['stock']]
    except Exception:
        pass


def _build_main_board_pool(context):
    """建主板大市值候选池"""
    yesterday = context.previous_date

    # 全A股
    all_stocks = get_all_securities('stock', date=yesterday).index.tolist()

    # ST过滤
    st_df = get_extras('is_st', all_stocks, start_date=yesterday, end_date=yesterday, df=True)
    st_list = st_df.columns[st_df.iloc[0]].tolist() if not st_df.empty else []

    # 主板过滤
    curr_data = get_current_data()
    pool = []
    for s in all_stocks:
        code = s[:6]
        # 只留主板
        if not code.startswith(('60', '000', '001', '002', '003', '605', '603')):
            continue
        if code.startswith(('300', '301', '688')):
            continue
        # 去ST
        if s in st_list:
            continue
        try:
            if curr_data[s].is_st:
                continue
        except Exception:
            pass
        # 去停牌
        try:
            if curr_data[s].paused:
                continue
        except Exception:
            pass
        # 去新股（上市不满250天）
        try:
            info = get_security_info(s)
            if (context.current_dt.date() - info.start_date).days < 250:
                continue
        except Exception:
            pass
        pool.append(s)

    # 按流通市值排序，取前N只
    if pool and g.prefer_large_cap:
        try:
            val_df = get_valuation(pool, yesterday,
                                   fields=['circulating_market_cap'])
            if val_df is not None and not val_df.empty:
                val_df = val_df.set_index('code').sort_values(
                    'circulating_market_cap', ascending=False)
                pool = val_df.head(g.max_pool).index.tolist()
        except Exception:
            pass

    return pool


def _is_market_crash(context):
    """沪深300当日跌超1.5%"""
    try:
        bench = attribute_history('000300.XSHG', 2, '1d', ['close'])
        if len(bench) >= 2:
            chg = (bench['close'].iloc[-1] - bench['close'].iloc[-2]) / bench['close'].iloc[-2]
            return chg < -0.015
    except Exception:
        pass
    return False
