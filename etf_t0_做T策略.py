# ═══════════════════════════════════════════════════════════════
# ETF T+0 日内做T策略 V2 - PTrade
# ─────────────────────────────────────────────────────────────
# 核心逻辑：
#   - 标的池分主力/备选，优先交易高波动ETF
#   - 买入条件：实时价 ≤ VWAP × (1 - 1%)，且有企稳迹象
#   - 卖出条件：买入价 +1% 卖1/3，+1.5% 再卖1/3，+2% 清仓
#   - 止损：买入价 -1% 全部卖出（快止损）
#   - 14:55 强制清仓，绝不留隔夜仓
#   - 趋势过滤 + 日内放量下跌过滤
#
# 盈亏比：赚1%~2% vs 亏1%，胜率>43%即可盈利
# ═══════════════════════════════════════════════════════════════

import json


# ─────────────────────────────────────────────────────────────
# ETF标的名称映射（方便日志查看）
# ─────────────────────────────────────────────────────────────

ETF_NAMES = {
    # ── 主力池：高波动，日内3%-8%，做T机会多 ──
    '513180.SS': '恒生科技ETF',         # 腾讯/阿里/美团，日均30亿+，波动3%-5%
    '513330.SS': '恒生互联网ETF',       # 港股互联网平台，波动3%-5%，弹性好
    '513050.SS': '中概互联ETF',         # 中美两地中概股，波动3%-4%
    '162411.SZ': '华宝油气ETF',         # 跟踪国际油价，波动4%-8%，最活跃
    '513100.SS': '纳斯达克ETF',         # 苹果/微软/特斯拉，日均20亿+，波动2%-4%
    '159518.SZ': '日经ETF',             # 跟踪日经225，跨境T+0
    '513310.SS': '港股科技30ETF',       # 港股科技龙头30，波动3%-5%
    # ── 备选池：中等波动，日内2%-5%，稳健型 ──
    '518880.SS': '黄金ETF(华安)',       # 跟踪Au99.99，日均50亿+，波动1%-3%
    '520500.SS': '恒生创新药ETF',       # 港股创新药龙头，波动4%-6%
    '159608.SZ': '稀有金属ETF',         # 稀土/锂/钴，周期波动3%-6%
    '159985.SZ': '豆粕ETF',             # 跟踪豆粕期货，波动3%-5%，与股市低相关
    '159981.SZ': '能源化工ETF',         # 跟踪能源化工品，波动3%-5%
    '159286.SZ': '碳中和ETF',           # 新能源相关，波动2%-4%
}


def _etf_name(code):
    """获取ETF中文名"""
    return ETF_NAMES.get(code, code)


# ─────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────

def _get_file_path(filename):
    try:
        return get_research_path() + '/' + filename
    except Exception:
        return filename


def _load_json(filename):
    try:
        path = _get_file_path(filename)
        with open(path, 'r', encoding='utf-8') as f:
            return json.loads(f.read()) or {}
    except Exception as e:
        log.warning('[文件读取失败] %s | 原因: %s' % (filename, str(e)))
        return None


def _save_json(filename, data):
    try:
        import os
        path = _get_file_path(filename)
        tmp_path = path + '.tmp'
        with open(tmp_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(data, ensure_ascii=False, indent=2))
        os.rename(tmp_path, path)
    except Exception as e:
        log.error('[硬盘写入失败] %s: %s' % (filename, str(e)))


def _create_default_config():
    config = {
        "主力池(高波动优先)": [
            "513180.SS",   # 恒生科技ETF
            "513330.SS",   # 恒生互联网ETF
            "513050.SS",   # 中概互联ETF
            "162411.SZ",   # 华宝油气ETF
            "513100.SS",   # 纳斯达克ETF
            "159518.SZ",   # 日经ETF
            "513310.SS"    # 港股科技30ETF
        ],
        "备选池(稳健补充)": [
            "518880.SS",   # 黄金ETF
            "520500.SS",   # 恒生创新药ETF
            "159608.SZ",   # 稀有金属ETF
            "159985.SZ",   # 豆粕ETF
            "159981.SZ",   # 能源化工ETF
            "159286.SZ"    # 碳中和ETF
        ],
        "启用备选池": True,
        "买入低于均线比例(%)": 1.0,
        "第一档卖出收益(%)": 1.0,
        "第二档卖出收益(%)": 1.5,
        "第三档卖出收益(%)": 2.0,
        "止损比例(%)": 1.0,
        "最大总仓位比例": 0.3,
        "单标的最大仓位比例": 0.12,
        "每日最大交易次数": 2,
        "手续费率": 0.0001,
        "最低手续费(元)": 5.0,
        "趋势判断均线天数": 5,
        "趋势判断跌幅阈值(%)": 3.0,
        "日内波动率最低要求(%)": 1.0,
        "清仓时间": "14:55",
        "开始交易时间": "09:50",
        "买入冷却分钟数": 15,
        "缩量判断比例": 0.7,
        "量能对比分钟数": 5
    }
    _save_json('etf_t0_config.json', config)
    log.info('[配置] 已创建默认配置文件 etf_t0_config.json')
    return config


def _load_config():
    data = _load_json('etf_t0_config.json')
    if data is None:
        return _create_default_config()
    return data


# ─────────────────────────────────────────────────────────────
# 初始化
# ─────────────────────────────────────────────────────────────

def initialize(context):
    config = _load_config()

    # ── 标的池：主力 + 备选 ──
    g.primary_pool = config.get("主力池(高波动优先)", [
        "513180.SS", "513330.SS", "513050.SS", "162411.SZ",
        "513100.SS", "159518.SZ", "513310.SS"
    ])
    g.secondary_pool = config.get("备选池(稳健补充)", [
        "518880.SS", "520500.SS", "159608.SZ",
        "159985.SZ", "159981.SZ", "159286.SZ"
    ])
    g.use_secondary = config.get("启用备选池", True)
    g.etf_pool = g.primary_pool + (g.secondary_pool if g.use_secondary else [])
    set_universe(g.etf_pool)

    # ── 策略参数 ──
    g.buy_dip_pct = config.get("买入低于均线比例(%)", 1.0) / 100.0
    g.sell_target_1 = config.get("第一档卖出收益(%)", 1.0) / 100.0     # +1% 卖1/3
    g.sell_target_2 = config.get("第二档卖出收益(%)", 1.5) / 100.0     # +1.5% 再卖1/3
    g.sell_target_3 = config.get("第三档卖出收益(%)", 2.0) / 100.0     # +2% 清仓
    g.stop_loss_pct = config.get("止损比例(%)", 1.0) / 100.0           # -1% 快止损
    g.max_position = config.get("最大总仓位比例", 0.3)
    g.max_single_position = config.get("单标的最大仓位比例", 0.12)
    g.max_trades_per_day = config.get("每日最大交易次数", 2)
    g.commission_rate = config.get("手续费率", 0.0001)
    g.min_commission = config.get("最低手续费(元)", 5.0)
    g.trend_ma_days = config.get("趋势判断均线天数", 5)
    g.trend_drop_threshold = config.get("趋势判断跌幅阈值(%)", 3.0) / 100.0
    g.min_intraday_vol = config.get("日内波动率最低要求(%)", 1.0) / 100.0
    g.start_trade_time = config.get("开始交易时间", "09:50")
    g.clear_time = config.get("清仓时间", "14:55")
    g.buy_cooldown_min = config.get("买入冷却分钟数", 15)

    # ── 日内状态（每日重置）──
    g.today_trades = 0
    g.today_buy_prices = {}    # {code: 买入价}
    g.today_sold_stage = {}    # {code: set(已卖出的档位)}
    g.last_buy_time = {}       # {code: datetime}
    g.today_positions = {}     # {code: 持仓数量}
    g.trend_blocked = set()
    g.prev_prices = {}         # {code: 上一分钟价格} 用于判断企稳
    g.volume_history = {}      # {code: [(cumulative_vol, price), ...]} 每分钟快照
    g.vol_shrink_ratio = config.get("缩量判断比例", 0.7)  # 近期量/前期量 < 0.7 = 缩量
    g.vol_lookback = config.get("量能对比分钟数", 5)       # 对比最近5分钟 vs 前5分钟

    # ── 定时任务 ──
    run_daily(context, _reset_daily_state, time='09:25')
    run_daily(context, _force_clear_all, time=g.clear_time)
    run_daily(context, _print_daily_summary, time='15:05')

    log.info('═══════════════════════════════════════════════')
    log.info('  ETF T+0 做T策略 V2 已初始化')
    log.info('─────────────────────────────────────────────')
    log.info('  主力池(%d只):' % len(g.primary_pool))
    for c in g.primary_pool:
        log.info('    %s %s' % (c, _etf_name(c)))
    if g.use_secondary:
        log.info('  备选池(%d只):' % len(g.secondary_pool))
        for c in g.secondary_pool:
            log.info('    %s %s' % (c, _etf_name(c)))
    log.info('─────────────────────────────────────────────')
    log.info('  买入: VWAP下 %.1f%%' % (g.buy_dip_pct * 100))
    log.info('  卖出: +%.1f%% / +%.1f%% / +%.1f%%' % (
        g.sell_target_1 * 100, g.sell_target_2 * 100, g.sell_target_3 * 100))
    log.info('  止损: -%.1f%% (快止损)' % (g.stop_loss_pct * 100))
    log.info('  仓位: 总≤%.0f%% 单≤%.0f%%' % (g.max_position * 100, g.max_single_position * 100))
    log.info('  量能: 缩量比≤%.0f%% 才买入(对比%d分钟窗口)' % (g.vol_shrink_ratio * 100, g.vol_lookback))
    log.info('  交易窗口: %s ~ %s' % (g.start_trade_time, g.clear_time))
    log.info('═══════════════════════════════════════════════')


# ─────────────────────────────────────────────────────────────
# 每日重置 + 趋势过滤
# ─────────────────────────────────────────────────────────────

def _reset_daily_state(context):
    g.today_trades = 0
    g.today_buy_prices = {}
    g.today_sold_stage = {}
    g.last_buy_time = {}
    g.today_positions = {}
    g.trend_blocked = set()
    g.prev_prices = {}
    g.volume_history = {}
    log.info('[日内重置] 状态已清空')

    for code in g.etf_pool:
        if _is_downtrend(code):
            g.trend_blocked.add(code)
            log.info('[趋势过滤] %s(%s) 近期下跌趋势，今日不开仓' % (_etf_name(code), code))

    active = len(g.etf_pool) - len(g.trend_blocked)
    log.info('[趋势过滤] 今日可交易: %d / %d' % (active, len(g.etf_pool)))


def _is_downtrend(code):
    """近N日累计跌幅超过阈值 = 下跌趋势"""
    try:
        hist = get_history(g.trend_ma_days + 1, '1d', 'close', code, fq='pre', include=False)
        if hist is None or len(hist) < g.trend_ma_days:
            return False
        closes = list(hist[code])
        if len(closes) < 2:
            return False
        change = (closes[-1] - closes[0]) / closes[0]
        return change < -g.trend_drop_threshold
    except Exception as e:
        log.warning('[趋势判断异常] %s: %s' % (code, str(e)))
        return False


# ─────────────────────────────────────────────────────────────
# 主交易逻辑（每分钟执行）
# ─────────────────────────────────────────────────────────────

def handle_data(context, data):
    now = context.blotter.current_dt
    current_time = now.strftime('%H:%M')

    if current_time < g.start_trade_time or current_time >= g.clear_time:
        return

    # 每5分钟打印一次全局监控面板
    minute = now.minute
    if minute % 5 == 0:
        _print_monitor_panel(context, now)

    # 优先扫描主力池，再扫备选池
    for code in g.etf_pool:
        if code in g.trend_blocked:
            continue
        try:
            _process_single_etf(context, code, now)
        except Exception as e:
            log.warning('[异常] %s(%s): %s' % (_etf_name(code), code, str(e)))


def _process_single_etf(context, code, now):
    """单个ETF的做T逻辑"""
    snapshot = get_snapshot(code)
    if snapshot is None or len(snapshot) == 0:
        return

    snap = snapshot.get(code, None)
    if snap is None:
        snap = snapshot.get(code.split('.')[0], None)
    if snap is None:
        return

    current_price = snap.get('last_px', 0)
    vwap = snap.get('wavg_px', 0)
    high_px = snap.get('high_px', 0)
    low_px = snap.get('low_px', 0)
    open_px = snap.get('open_px', 0)

    if current_price <= 0 or vwap <= 0:
        return

    # ── 每分钟记录累计成交量 ──
    cum_vol = snap.get('business_amount', 0)
    if code not in g.volume_history:
        g.volume_history[code] = []
    g.volume_history[code].append((cum_vol, current_price))
    if len(g.volume_history[code]) > 310:
        g.volume_history[code] = g.volume_history[code][-310:]

    # ── 计算通用指标 ──
    vwap_pct = (current_price / vwap - 1) * 100        # 偏离均价线%
    open_pct = (current_price / open_px - 1) * 100 if open_px > 0 else 0  # 涨跌幅%
    vol_status = _check_volume_shrink(code)
    short_r, day_r = _get_volume_ratio(code)
    vol_tag = {'shrinking': '缩量', 'expanding': '放量', 'neutral': '--'}.get(vol_status, '--')
    prev_price = g.prev_prices.get(code, 0)
    price_dir = '↑' if (prev_price > 0 and current_price > prev_price) else (
                '↓' if (prev_price > 0 and current_price < prev_price) else '→')
    g.prev_prices[code] = current_price
    name = _etf_name(code)

    today_holding = g.today_positions.get(code, 0)

    # ═══════════════════════════════════════
    # 有持仓 → 检查止盈/止损
    # ═══════════════════════════════════════
    if today_holding > 0 and code in g.today_buy_prices:
        buy_price = g.today_buy_prices[code]
        pnl_pct = (current_price - buy_price) / buy_price
        sold_stages = g.today_sold_stage.get(code, set())

        # ── 持仓监控日志（每分钟打印）──
        log.info('[持仓监控] %s | 价:%s%.3f | 成本:%.3f | 盈亏:%+.2f%% | VWAP偏离:%+.2f%% | %s(短:%.2f/日:%.2f) | 档:%s' % (
            name, price_dir, current_price, buy_price, pnl_pct * 100,
            vwap_pct, vol_tag, short_r, day_r, list(sold_stages)))

        # ── 快止损：-1% 立即全卖 ──
        if pnl_pct <= -g.stop_loss_pct:
            _sell_all_today(context, code, current_price,
                            '快止损 %.2f%% (买入:%.3f)' % (pnl_pct * 100, buy_price))
            return

        # ── 上涨缩量：涨不动了，提前卖出 ──
        # 盈利状态 + 价格在涨或持平 + 成交量明显萎缩 = 上涨没后劲
        if pnl_pct > 0 and vol_status == 'shrinking' and price_dir != '↓':
            remaining = g.today_positions.get(code, 0)
            if remaining >= 200:  # 至少留100股观察
                sell_amt = _round_lot(remaining // 2)
                if sell_amt >= 100:
                    log.info('[量能预警] %s 上涨缩量！涨不动了，主动减仓' % name)
                    _do_sell(context, code, sell_amt, current_price,
                             '上涨缩量减仓 +%.2f%% (短:%.2f/日:%.2f)' % (pnl_pct * 100, short_r, day_r))
                    return

        # ── 放量上涨：有后劲，先拿着 ──
        if pnl_pct > 0 and vol_status == 'expanding':
            log.info('[量能提示] %s 放量上涨中，有后劲，继续持有' % name)

        # ── 第一档：+1% 卖1/3 ──
        if pnl_pct >= g.sell_target_1 and 1 not in sold_stages:
            sell_amt = _round_lot(today_holding // 3)
            if sell_amt >= 100:
                _do_sell(context, code, sell_amt, current_price,
                         '一档止盈 +%.2f%%' % (pnl_pct * 100))
                sold_stages.add(1)
                g.today_sold_stage[code] = sold_stages

        # ── 第二档：+1.5% 再卖1/3 ──
        today_holding = g.today_positions.get(code, 0)
        if pnl_pct >= g.sell_target_2 and 2 not in sold_stages and today_holding > 0:
            sell_amt = _round_lot(today_holding // 2)
            if sell_amt >= 100:
                _do_sell(context, code, sell_amt, current_price,
                         '二档止盈 +%.2f%%' % (pnl_pct * 100))
                sold_stages.add(2)
                g.today_sold_stage[code] = sold_stages

        # ── 第三档：+2% 清仓 ──
        today_holding = g.today_positions.get(code, 0)
        if pnl_pct >= g.sell_target_3 and 3 not in sold_stages and today_holding > 0:
            _sell_all_today(context, code, current_price,
                            '三档清仓 +%.2f%%' % (pnl_pct * 100))
            sold_stages.add(3)
            g.today_sold_stage[code] = sold_stages
        return  # 有持仓时不再考虑买入

    # ═══════════════════════════════════════
    # 无持仓 → 检查买入机会
    # ═══════════════════════════════════════
    if today_holding > 0 or g.today_trades >= g.max_trades_per_day:
        return

    # 冷却检查
    last_buy = g.last_buy_time.get(code, None)
    if last_buy is not None:
        minutes_since = (now - last_buy).total_seconds() / 60.0
        if minutes_since < g.buy_cooldown_min:
            return

    # 条件1：日内有足够波动（排除横盘ETF）
    intraday_range = 0
    if high_px > 0 and low_px > 0:
        intraday_range = (high_px - low_px) / low_px
        if intraday_range < g.min_intraday_vol:
            return

    # 条件2：当前价 ≤ VWAP × (1 - buy_dip_pct)
    buy_threshold = vwap * (1 - g.buy_dip_pct)
    if current_price > buy_threshold:
        return

    # ── 进入买入候选区，打印详细日志 ──
    log.info('[买入观察] %s | 价:%s%.3f | VWAP:%.3f(%+.2f%%) | 涨跌:%+.2f%% | 振幅:%.2f%% | %s(短:%.2f/日:%.2f)' % (
        name, price_dir, current_price, vwap, vwap_pct,
        open_pct, intraday_range * 100, vol_tag, short_r, day_r))

    # 条件3：企稳确认 — 不再继续下跌
    if prev_price > 0 and current_price < prev_price:
        log.info('[买入等待] %s 价格还在跌(%.3f→%.3f)，等企稳' % (name, prev_price, current_price))
        return

    # 条件4：缩量确认 — 卖盘枯竭才接
    if vol_status == 'expanding':
        log.info('[买入拒绝] %s 放量下跌！卖盘还很猛，不接刀 (短:%.2f/日:%.2f)' % (name, short_r, day_r))
        return
    elif vol_status == 'shrinking':
        log.info('[买入信号] %s 缩量下跌！卖盘枯竭，准备买入 (短:%.2f/日:%.2f)' % (name, short_r, day_r))

    # 条件5：当日有过上涨（high > open），说明有弹性
    if open_px > 0 and high_px <= open_px:
        log.info('[买入拒绝] %s 全天没涨过，没弹性' % name)
        return

    # 仓位检查
    if not _check_position_limit(context, code):
        return

    _do_buy(context, code, current_price, vwap, now, vol_status)


# ─────────────────────────────────────────────────────────────
# 全局监控面板（每5分钟打印）
# ─────────────────────────────────────────────────────────────

def _print_monitor_panel(context, now):
    """每5分钟打印所有标的的实时状态，便于盯盘和复盘"""
    time_str = now.strftime('%H:%M')
    log.info('┌─────────────── 监控面板 %s ───────────────┐' % time_str)

    for code in g.etf_pool:
        if code in g.trend_blocked:
            continue
        try:
            snapshot = get_snapshot(code)
            if not snapshot:
                continue
            snap = snapshot.get(code, snapshot.get(code.split('.')[0], None))
            if not snap:
                continue

            px = snap.get('last_px', 0)
            vwap = snap.get('wavg_px', 0)
            open_px = snap.get('open_px', 0)
            high_px = snap.get('high_px', 0)
            low_px = snap.get('low_px', 0)
            if px <= 0 or vwap <= 0:
                continue

            vwap_pct = (px / vwap - 1) * 100
            open_pct = (px / open_px - 1) * 100 if open_px > 0 else 0
            amp = (high_px - low_px) / low_px * 100 if low_px > 0 else 0
            vol_st = _check_volume_shrink(code)
            short_r, day_r = _get_volume_ratio(code)
            vol_tag = {'shrinking': '缩量', 'expanding': '放量', 'neutral': '--'}.get(vol_st, '--')

            # 判断当前状态
            holding = g.today_positions.get(code, 0)
            if holding > 0:
                buy_px = g.today_buy_prices.get(code, px)
                pnl = (px - buy_px) / buy_px * 100
                status = '持仓%+.2f%%' % pnl
            elif px <= vwap * (1 - g.buy_dip_pct):
                status = '★接近买点'
            else:
                status = '观望'

            name = _etf_name(code)
            # 限制名称长度
            if len(name) > 8:
                name = name[:8]
            log.info('│ %s | %.3f | VWAP:%+.2f%% | 涨跌:%+.2f%% | 振幅:%.1f%% | %s(短%.1f/日%.1f) | %s' % (
                name, px, vwap_pct, open_pct, amp, vol_tag, short_r, day_r, status))
        except Exception:
            pass

    log.info('│ 今日交易: %d/%d | 现金: %.0f' % (
        g.today_trades, g.max_trades_per_day, context.portfolio.cash))
    log.info('└──────────────────────────────────────────────┘')


# ─────────────────────────────────────────────────────────────
# 量能分析：缩量/放量判断
# ─────────────────────────────────────────────────────────────
#
# 判断思路（双重对比，避免潮汐效应误判）：
#
#   1) 短期量比：最近N分钟均量 vs 前N分钟均量
#      → 反映"趋势"：量在缩还是在放
#
#   2) 全日量比：最近N分钟均量 vs 当日每分钟平均量
#      → 反映"绝对水平"：当前是偏高还是偏低
#
#   两个都说"缩量" → 真缩量，可以买
#   任一个说"放量" → 放量下跌，别接
# ─────────────────────────────────────────────────────────────

def _calc_minute_vols(code):
    """从累计成交量历史中提取每分钟增量"""
    history = g.volume_history.get(code, [])
    if len(history) < 2:
        return []
    minute_vols = []
    for i in range(1, len(history)):
        delta = history[i][0] - history[i - 1][0]
        minute_vols.append(max(delta, 0))
    return minute_vols


def _check_volume_shrink(code):
    """判断当前是缩量还是放量

    返回: 'shrinking'=缩量, 'expanding'=放量, 'neutral'=数据不足/持平
    """
    minute_vols = _calc_minute_vols(code)
    n = g.vol_lookback

    if len(minute_vols) < 2 * n:
        return 'neutral'

    recent_avg = sum(minute_vols[-n:]) / n
    earlier_avg = sum(minute_vols[-2 * n:-n]) / n

    # ── 短期量比：近N分钟 vs 前N分钟 ──
    if earlier_avg > 0:
        short_ratio = recent_avg / earlier_avg
    else:
        short_ratio = 1.0

    # ── 全日量比：近N分钟 vs 当日每分钟平均 ──
    all_avg = sum(minute_vols) / len(minute_vols) if minute_vols else 0
    if all_avg > 0:
        day_ratio = recent_avg / all_avg
    else:
        day_ratio = 1.0

    # 判断逻辑：
    # 放量 = 任一维度显著高于正常（短期>1.3 或 全日>1.5）
    # 缩量 = 两个维度都偏低（短期<0.7 且 全日<0.85）
    if short_ratio >= 1.3 or day_ratio >= 1.5:
        return 'expanding'
    elif short_ratio <= g.vol_shrink_ratio and day_ratio <= 0.85:
        return 'shrinking'
    else:
        return 'neutral'


def _get_volume_ratio(code):
    """获取量比数值（短期/全日），用于日志"""
    minute_vols = _calc_minute_vols(code)
    n = g.vol_lookback
    if len(minute_vols) < 2 * n:
        return 0, 0

    recent_avg = sum(minute_vols[-n:]) / n
    earlier_avg = sum(minute_vols[-2 * n:-n]) / n
    all_avg = sum(minute_vols) / len(minute_vols) if minute_vols else 0

    short_r = recent_avg / earlier_avg if earlier_avg > 0 else 0
    day_r = recent_avg / all_avg if all_avg > 0 else 0
    return short_r, day_r


# ─────────────────────────────────────────────────────────────
# 买卖执行
# ─────────────────────────────────────────────────────────────

def _do_buy(context, code, price, vwap, now, vol_status='neutral'):
    total_value = context.portfolio.total_value
    buy_value = total_value * g.max_single_position
    buy_amount = _round_lot(int(buy_value / price))
    if buy_amount < 100:
        return

    commission = max(buy_value * g.commission_rate, g.min_commission)
    order_id = order(code, buy_amount, limit_price=price)
    if order_id:
        g.today_buy_prices[code] = price
        g.today_positions[code] = buy_amount
        g.today_trades += 1
        g.last_buy_time[code] = now
        g.today_sold_stage[code] = set()
        short_r, day_r = _get_volume_ratio(code)
        vol_tag = '缩量' if vol_status == 'shrinking' else ('放量' if vol_status == 'expanding' else '正常')
        log.info('[买入] %s(%s) | 价:%.3f | VWAP:%.3f | 偏离:%.2f%% | 量:%d | 短期量比:%.2f 全日量比:%.2f(%s) | 费:%.1f' % (
            _etf_name(code), code, price, vwap,
            (price / vwap - 1) * 100, buy_amount, short_r, day_r, vol_tag, commission))


def _do_sell(context, code, amount, price, reason):
    if amount < 100:
        return
    order_id = order(code, -amount, limit_price=price)
    if order_id:
        g.today_positions[code] = max(g.today_positions.get(code, 0) - amount, 0)
        log.info('[卖出] %s(%s) | 价:%.3f | 量:%d | %s' % (
            _etf_name(code), code, price, amount, reason))


def _sell_all_today(context, code, price, reason):
    today_holding = g.today_positions.get(code, 0)
    if today_holding >= 100:
        _do_sell(context, code, today_holding, price, reason)


# ─────────────────────────────────────────────────────────────
# 14:55 强制清仓
# ─────────────────────────────────────────────────────────────

def _force_clear_all(context):
    cleared = False
    for code in list(g.today_positions.keys()):
        today_holding = g.today_positions.get(code, 0)
        if today_holding < 100:
            continue
        try:
            snapshot = get_snapshot(code)
            price = 0
            if snapshot and code in snapshot:
                price = snapshot[code].get('last_px', 0)

            if price <= 0:
                order_id = order(code, -today_holding)
            else:
                order_id = order(code, -today_holding, limit_price=price)

            if order_id:
                buy_price = g.today_buy_prices.get(code, price)
                pnl = (price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                log.info('[14:55清仓] %s(%s) | 价:%.3f | 量:%d | 盈亏:%.2f%%' % (
                    _etf_name(code), code, price, today_holding, pnl))
                g.today_positions[code] = 0
                cleared = True
        except Exception as e:
            log.error('[清仓异常] %s(%s): %s' % (_etf_name(code), code, str(e)))

    if not cleared:
        log.info('[14:55清仓] 今日无持仓需清理')


# ─────────────────────────────────────────────────────────────
# 仓位控制
# ─────────────────────────────────────────────────────────────

def _check_position_limit(context, code):
    total_value = context.portfolio.total_value
    if total_value <= 0:
        return False

    total_pos_value = 0
    for pos_code, pos in context.portfolio.positions.items():
        if pos.amount > 0:
            total_pos_value += pos.amount * pos.last_sale_price

    for t_code, t_amount in g.today_positions.items():
        if t_code != code and t_amount > 0:
            buy_px = g.today_buy_prices.get(t_code, 0)
            if buy_px > 0:
                total_pos_value += t_amount * buy_px

    if total_pos_value / total_value >= g.max_position:
        log.info('[仓位限制] 总仓位 %.1f%% 已达上限' % (total_pos_value / total_value * 100))
        return False

    return True


def _round_lot(amount):
    return (amount // 100) * 100


# ─────────────────────────────────────────────────────────────
# 日志汇总
# ─────────────────────────────────────────────────────────────

def _print_daily_summary(context):
    log.info('═══════════════════════════════════════════════')
    log.info('  ETF T+0 做T策略 V2 - 今日汇总')
    log.info('─────────────────────────────────────────────')
    log.info('  交易次数: %d / %d' % (g.today_trades, g.max_trades_per_day))
    log.info('  趋势屏蔽: %s' % (
        [_etf_name(c) for c in g.trend_blocked] if g.trend_blocked else '无'))

    total_pnl = 0
    trade_count = 0
    for code in g.today_buy_prices:
        buy_px = g.today_buy_prices[code]
        remaining = g.today_positions.get(code, 0)
        try:
            snapshot = get_snapshot(code)
            last_px = snapshot[code].get('last_px', buy_px) if snapshot and code in snapshot else buy_px
        except Exception:
            last_px = buy_px

        pnl = (last_px - buy_px) / buy_px * 100 if buy_px > 0 else 0
        total_pnl += pnl
        trade_count += 1
        stages = list(g.today_sold_stage.get(code, set()))
        log.info('  %s | 买:%.3f → %.3f | %+.2f%% | 档:%s | 余:%d' % (
            _etf_name(code), buy_px, last_px, pnl, stages, remaining))

    if trade_count > 0:
        log.info('  平均盈亏: %+.2f%%' % (total_pnl / trade_count))

    log.info('  账户: %.2f (现金: %.2f)' % (
        context.portfolio.total_value, context.portfolio.cash))
    log.info('═══════════════════════════════════════════════')
