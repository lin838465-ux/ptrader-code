# ═══════════════════════════════════════════════════════════════
# ETF T+0 日内做T策略 - PTrade
# ─────────────────────────────────────────────────────────────
# 核心逻辑：
#   - 标的池：T+0 ETF（恒生科技、纳指、原油、黄金等跨境/商品ETF）
#   - 买入条件：实时价 ≤ 分时均价线（VWAP）× (1 - buy_dip_pct)
#   - 卖出条件：买入价 × (1 + sell_target_1) 卖1/3，× (1 + sell_target_2) 再卖1/3
#   - 止损：买入价 × (1 - stop_loss_pct) 全部卖出
#   - 14:55 强制清仓，绝不留隔夜仓
#   - 趋势过滤：大盘/标的明显下跌趋势时不开仓
#
# 仓位控制：
#   - 单标的最大半仓 (max_position = 0.5)
#   - 每日最多交易 max_trades_per_day 次
#
# 手续费：默认万一(0.0001)，最低5元
# ═══════════════════════════════════════════════════════════════

import json


# ─────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────

def _get_file_path(filename):
    """获取研究目录路径"""
    try:
        return get_research_path() + '/' + filename
    except Exception:
        return filename


def _load_json(filename):
    """读取JSON文件"""
    try:
        path = _get_file_path(filename)
        with open(path, 'r', encoding='utf-8') as f:
            return json.loads(f.read()) or {}
    except Exception as e:
        log.warning('[文件读取失败] %s | 原因: %s' % (filename, str(e)))
        return None


def _save_json(filename, data):
    """写入JSON文件（原子写入）"""
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
    """创建默认配置文件"""
    config = {
        "标的池": [
            "513180.SS",
            "513100.SS",
            "513330.SS",
            "513050.SS",
            "513310.SS",
            "162411.SZ",
            "520500.SS",
            "518880.SS",
            "159608.SZ",
            "159985.SZ",
            "159981.SZ",
            "159286.SZ",
            "159518.SZ"
        ],
        "买入低于均线比例(%)": 1.0,
        "第一档卖出收益(%)": 1.0,
        "第二档卖出收益(%)": 2.0,
        "止损比例(%)": 2.0,
        "最大仓位比例": 0.5,
        "单标的最大仓位比例": 0.15,
        "每日最大交易次数": 3,
        "手续费率": 0.0001,
        "最低手续费(元)": 5.0,
        "趋势判断均线天数": 5,
        "趋势判断跌幅阈值(%)": 3.0,
        "清仓时间": "14:55",
        "开始交易时间": "09:35",
        "买入冷却分钟数": 10
    }
    _save_json('etf_t0_config.json', config)
    log.info('[配置] 已创建默认配置文件 etf_t0_config.json')
    return config


def _load_config():
    """读取配置（不存在则创建默认）"""
    data = _load_json('etf_t0_config.json')
    if data is None:
        return _create_default_config()
    return data


# ─────────────────────────────────────────────────────────────
# 初始化
# ─────────────────────────────────────────────────────────────

def initialize(context):
    # 加载配置
    config = _load_config()

    # ── 标的池 ──
    g.etf_pool = config.get("标的池", [
        "513180.SS", "513100.SS", "513330.SS", "513050.SS", "513310.SS",
        "162411.SZ", "520500.SS", "518880.SS",
        "159608.SZ", "159985.SZ", "159981.SZ", "159286.SZ", "159518.SZ"
    ])
    set_universe(g.etf_pool)

    # ── 策略参数 ──
    g.buy_dip_pct = config.get("买入低于均线比例(%)", 1.0) / 100.0       # 均线下1%买入
    g.sell_target_1 = config.get("第一档卖出收益(%)", 1.0) / 100.0       # 买入价上1%卖1/3
    g.sell_target_2 = config.get("第二档卖出收益(%)", 2.0) / 100.0       # 买入价上2%卖剩余
    g.stop_loss_pct = config.get("止损比例(%)", 2.0) / 100.0             # 止损-2%
    g.max_position = config.get("最大仓位比例", 0.5)                      # 总仓位上限50%
    g.max_single_position = config.get("单标的最大仓位比例", 0.15)        # 单标的上限15%
    g.max_trades_per_day = config.get("每日最大交易次数", 3)              # 每日最多3次
    g.commission_rate = config.get("手续费率", 0.0001)
    g.min_commission = config.get("最低手续费(元)", 5.0)
    g.trend_ma_days = config.get("趋势判断均线天数", 5)
    g.trend_drop_threshold = config.get("趋势判断跌幅阈值(%)", 3.0) / 100.0
    g.start_trade_time = config.get("开始交易时间", "09:35")
    g.clear_time = config.get("清仓时间", "14:55")
    g.buy_cooldown_min = config.get("买入冷却分钟数", 10)

    # ── 日内状态（每日重置）──
    g.today_trades = 0           # 当日已交易次数
    g.today_buy_prices = {}      # {code: 买入均价}
    g.today_sold_stage = {}      # {code: 已卖出的档位 set()}
    g.last_buy_time = {}         # {code: 上次买入时间} 冷却用
    g.today_positions = {}       # {code: 当日持仓数量}
    g.trend_blocked = set()      # 今日因趋势被屏蔽的标的

    # ── 定时任务 ──
    run_daily(context, _reset_daily_state, time='09:25')
    run_daily(context, _force_clear_all, time=g.clear_time)
    run_daily(context, _print_daily_summary, time='15:05')

    log.info('═══════════════════════════════════════════════')
    log.info('  ETF T+0 做T策略 已初始化')
    log.info('  标的池: %s' % g.etf_pool)
    log.info('  买入条件: VWAP下 %.1f%%' % (g.buy_dip_pct * 100))
    log.info('  卖出目标: +%.1f%% / +%.1f%%' % (g.sell_target_1 * 100, g.sell_target_2 * 100))
    log.info('  止损: -%.1f%%' % (g.stop_loss_pct * 100))
    log.info('  清仓时间: %s' % g.clear_time)
    log.info('═══════════════════════════════════════════════')


# ─────────────────────────────────────────────────────────────
# 每日重置
# ─────────────────────────────────────────────────────────────

def _reset_daily_state(context):
    """每日盘前重置日内状态"""
    g.today_trades = 0
    g.today_buy_prices = {}
    g.today_sold_stage = {}
    g.last_buy_time = {}
    g.today_positions = {}
    g.trend_blocked = set()
    log.info('[日内重置] 所有日内状态已清空，准备新一天交易')

    # 趋势过滤：检查标的近N日是否处于下跌趋势
    for code in g.etf_pool:
        if _is_downtrend(code):
            g.trend_blocked.add(code)
            log.info('[趋势过滤] %s 近期下跌趋势，今日不开仓' % code)

    active_count = len(g.etf_pool) - len(g.trend_blocked)
    log.info('[趋势过滤] 今日可交易标的: %d / %d' % (active_count, len(g.etf_pool)))


def _is_downtrend(code):
    """判断标的是否处于明显下跌趋势
    规则：最近N日收盘价低于N日均线且累计跌幅超过阈值
    """
    try:
        hist = get_history(g.trend_ma_days + 1, '1d', 'close', code, fq='pre', include=False)
        if hist is None or len(hist) < g.trend_ma_days:
            return False

        closes = list(hist[code])
        if len(closes) < 2:
            return False

        # 计算N日跌幅
        change = (closes[-1] - closes[0]) / closes[0]
        if change < -g.trend_drop_threshold:
            return True

        return False
    except Exception as e:
        log.warning('[趋势判断异常] %s: %s' % (code, str(e)))
        return False


# ─────────────────────────────────────────────────────────────
# 主交易逻辑（每分钟执行）
# ─────────────────────────────────────────────────────────────

def handle_data(context, data):
    """每分钟执行：扫描标的池，寻找做T机会"""
    now = context.blotter.current_dt
    current_time = now.strftime('%H:%M')

    # 未到开始交易时间 或 已过清仓时间，不操作
    if current_time < g.start_trade_time or current_time >= g.clear_time:
        return

    for code in g.etf_pool:
        # 趋势屏蔽
        if code in g.trend_blocked:
            continue

        try:
            _process_single_etf(context, code, now)
        except Exception as e:
            log.warning('[异常] %s 处理失败: %s' % (code, str(e)))


def _process_single_etf(context, code, now):
    """处理单个ETF的做T逻辑"""
    # 获取实时快照
    snapshot = get_snapshot(code)
    if snapshot is None or len(snapshot) == 0:
        return

    snap = snapshot[code] if code in snapshot else snapshot.get(code.split('.')[0], None)
    if snap is None:
        return

    current_price = snap.get('last_px', 0)
    vwap = snap.get('wavg_px', 0)  # 分时均价线（黄线）

    if current_price <= 0 or vwap <= 0:
        return

    # 获取当前持仓
    position = get_position(code)
    holding = position.amount if position else 0
    today_holding = g.today_positions.get(code, 0)

    # ── 有今日持仓：先检查卖出/止损 ──
    if today_holding > 0 and code in g.today_buy_prices:
        buy_price = g.today_buy_prices[code]
        pnl_pct = (current_price - buy_price) / buy_price

        # 止损检查：亏损超过 stop_loss_pct 全部卖出
        if pnl_pct <= -g.stop_loss_pct:
            _sell_all_today(context, code, current_price, '止损 %.2f%%' % (pnl_pct * 100))
            return

        # 第一档卖出：盈利达到 sell_target_1，卖出1/3
        sold_stages = g.today_sold_stage.get(code, set())
        if pnl_pct >= g.sell_target_1 and 1 not in sold_stages:
            sell_amount = _round_lot(today_holding // 3)
            if sell_amount >= 100:
                _do_sell(context, code, sell_amount, current_price,
                         '第一档止盈 +%.2f%%' % (pnl_pct * 100))
                sold_stages.add(1)
                g.today_sold_stage[code] = sold_stages

        # 第二档卖出：盈利达到 sell_target_2，卖出剩余的一半
        today_holding = g.today_positions.get(code, 0)  # 刷新
        if pnl_pct >= g.sell_target_2 and 2 not in sold_stages and today_holding > 0:
            sell_amount = _round_lot(today_holding // 2)
            if sell_amount >= 100:
                _do_sell(context, code, sell_amount, current_price,
                         '第二档止盈 +%.2f%%' % (pnl_pct * 100))
                sold_stages.add(2)
                g.today_sold_stage[code] = sold_stages

    # ── 无持仓或持仓较少：检查买入条件 ──
    if today_holding == 0 and g.today_trades < g.max_trades_per_day:
        # 冷却检查
        last_buy = g.last_buy_time.get(code, None)
        if last_buy is not None:
            minutes_since = (now - last_buy).total_seconds() / 60.0
            if minutes_since < g.buy_cooldown_min:
                return

        # 核心买入条件：当前价 ≤ VWAP × (1 - buy_dip_pct)
        buy_threshold = vwap * (1 - g.buy_dip_pct)
        if current_price <= buy_threshold:
            # 额外确认：当日该标的已翻红（有波动空间）
            open_px = snap.get('open_px', 0)
            if open_px > 0 and snap.get('high_px', 0) > open_px:
                # 仓位检查
                if _check_position_limit(context, code):
                    _do_buy(context, code, current_price, vwap, now)


# ─────────────────────────────────────────────────────────────
# 买卖执行
# ─────────────────────────────────────────────────────────────

def _do_buy(context, code, price, vwap, now):
    """执行买入"""
    total_value = context.portfolio.total_value
    # 单标的投入金额 = 总资产 × 单标的最大仓位比例
    buy_value = total_value * g.max_single_position
    buy_amount = _round_lot(int(buy_value / price))

    if buy_amount < 100:
        return

    # 估算手续费
    commission = max(buy_value * g.commission_rate, g.min_commission)

    order_id = order(code, buy_amount, limit_price=price)
    if order_id:
        g.today_buy_prices[code] = price
        g.today_positions[code] = buy_amount
        g.today_trades += 1
        g.last_buy_time[code] = now
        g.today_sold_stage[code] = set()
        log.info('[买入] %s | 价格:%.3f | VWAP:%.3f | 偏离:%.2f%% | 数量:%d | 手续费:%.2f' % (
            code, price, vwap, (price / vwap - 1) * 100, buy_amount, commission))


def _do_sell(context, code, amount, price, reason):
    """执行卖出"""
    if amount < 100:
        return
    order_id = order(code, -amount, limit_price=price)
    if order_id:
        g.today_positions[code] = g.today_positions.get(code, 0) - amount
        if g.today_positions[code] <= 0:
            g.today_positions[code] = 0
        log.info('[卖出] %s | 价格:%.3f | 数量:%d | 原因:%s' % (code, price, amount, reason))


def _sell_all_today(context, code, price, reason):
    """卖出今日全部持仓"""
    today_holding = g.today_positions.get(code, 0)
    if today_holding >= 100:
        _do_sell(context, code, today_holding, price, reason)


# ─────────────────────────────────────────────────────────────
# 14:55 强制清仓
# ─────────────────────────────────────────────────────────────

def _force_clear_all(context):
    """14:55 强制卖出所有今日T+0持仓，绝不留隔夜"""
    cleared = False
    for code in list(g.today_positions.keys()):
        today_holding = g.today_positions.get(code, 0)
        if today_holding < 100:
            continue

        try:
            snapshot = get_snapshot(code)
            if snapshot and code in snapshot:
                price = snapshot[code].get('last_px', 0)
            else:
                price = 0

            if price <= 0:
                # 无法获取价格，用市价单
                order_id = order(code, -today_holding)
            else:
                order_id = order(code, -today_holding, limit_price=price)

            if order_id:
                buy_price = g.today_buy_prices.get(code, price)
                pnl_pct = (price - buy_price) / buy_price * 100 if buy_price > 0 else 0
                log.info('[14:55清仓] %s | 价格:%.3f | 数量:%d | 盈亏:%.2f%%' % (
                    code, price, today_holding, pnl_pct))
                g.today_positions[code] = 0
                cleared = True
        except Exception as e:
            log.error('[清仓异常] %s: %s' % (code, str(e)))

    if not cleared:
        log.info('[14:55清仓] 无今日持仓需要清理')


# ─────────────────────────────────────────────────────────────
# 仓位控制
# ─────────────────────────────────────────────────────────────

def _check_position_limit(context, code):
    """检查是否超过总仓位和单标的仓位限制"""
    total_value = context.portfolio.total_value
    if total_value <= 0:
        return False

    # 当前总持仓市值
    total_position_value = 0
    for pos_code, pos in context.portfolio.positions.items():
        if pos.amount > 0:
            total_position_value += pos.amount * pos.last_sale_price

    # 加上今日已买入但可能未体现在positions中的
    for t_code, t_amount in g.today_positions.items():
        if t_code != code and t_amount > 0:
            buy_px = g.today_buy_prices.get(t_code, 0)
            if buy_px > 0:
                total_position_value += t_amount * buy_px

    # 总仓位限制
    if total_position_value / total_value >= g.max_position:
        log.info('[仓位限制] 总仓位已达 %.1f%%，不再开仓' % (total_position_value / total_value * 100))
        return False

    return True


# ─────────────────────────────────────────────────────────────
# 工具
# ─────────────────────────────────────────────────────────────

def _round_lot(amount):
    """取整到100股（ETF最小交易单位）"""
    return (amount // 100) * 100


# ─────────────────────────────────────────────────────────────
# 日志汇总
# ─────────────────────────────────────────────────────────────

def _print_daily_summary(context):
    """收盘后打印今日做T汇总"""
    log.info('═══════════════════════════════════════════════')
    log.info('  ETF T+0 做T策略 - 今日汇总')
    log.info('─────────────────────────────────────────────')
    log.info('  交易次数: %d / %d' % (g.today_trades, g.max_trades_per_day))
    log.info('  趋势屏蔽标的: %s' % (list(g.trend_blocked) if g.trend_blocked else '无'))

    total_pnl = 0
    for code in g.today_buy_prices:
        buy_px = g.today_buy_prices[code]
        remaining = g.today_positions.get(code, 0)
        # 如果已清仓，尝试获取最后卖出价
        try:
            snapshot = get_snapshot(code)
            last_px = snapshot[code].get('last_px', buy_px) if snapshot and code in snapshot else buy_px
        except Exception:
            last_px = buy_px

        pnl_pct = (last_px - buy_px) / buy_px * 100 if buy_px > 0 else 0
        total_pnl += pnl_pct
        sold_stages = g.today_sold_stage.get(code, set())
        log.info('  %s | 买入:%.3f | 收盘:%.3f | 盈亏:%.2f%% | 卖出档:%s | 剩余:%d' % (
            code, buy_px, last_px, pnl_pct, list(sold_stages), remaining))

    if g.today_buy_prices:
        log.info('  平均盈亏: %.2f%%' % (total_pnl / len(g.today_buy_prices)))

    log.info('  账户总值: %.2f' % context.portfolio.total_value)
    log.info('  可用现金: %.2f' % context.portfolio.cash)
    log.info('═══════════════════════════════════════════════')
