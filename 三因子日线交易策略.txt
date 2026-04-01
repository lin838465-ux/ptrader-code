"""
策略名称：
三因子日线交易策略
运行周期:
日线
策略流程：
盘前将中小板成分股中st、停牌、退市的股票过滤得到股票池
盘中：
1、获取市场风险溢价、市值因子、账面市值比因子三因子数据，
2、分组差值做线性回归处理，最终得到得分，选择得分高的标的调仓买入
3、每15天换仓一次
注意事项：
策略中调用的order_target_value接口的使用有场景限制，回测可以正常使用，交易谨慎使用。
回测场景下撮合是引擎计算的，因此成交之后持仓信息的更新是瞬时的，但交易场景下信息的更新依赖于柜台数据
的返回，无法做到瞬时同步，可能造成重复下单。详细原因请看帮助文档。
"""
# 导入函数库
import pandas as pd
import numpy as np
import statsmodels.api as sm
from statsmodels import regression
from decimal import Decimal


# 初始化此策略
def initialize(context):
    g.factor_params_info = {
        'total_shareholder_equity': ['balance_statement', 'total_shareholder_equity'],
        'roe': ['profit_ability', 'roe']
    }
    set_params()  # 设置策参数
    set_variables()  # 设置中间变量
    if not is_trade():
        set_backtest()  # 设置回测条件


# 设置策参数
def set_params():
    g.tc = 15  # 调仓频率
    g.yb = 63  # 样本长度
    g.N = 10  # 持仓数目
    g.NoF = 3  # 三因子模型


# 设置中间变量
def set_variables():
    g.t = 0  # 记录连续回测天数
    g.rf = 0.04  # 无风险利率
    g.if_trade = False  # 当天是否交易


# 设置回测条件
def set_backtest():
    set_limit_mode('UNLIMITED')


# 每天盘前处理
def before_trading_start(context, data):
    g.current_date = context.blotter.current_dt.strftime("%Y%m%d")
    # 2005-06-01前回测由于数据不足，不执行。
    if g.current_date < '20050601':
        g.trade_flag = False
    else:
        g.trade_flag = True

    g.rf = 0.04
    g.all_stocks = get_index_stocks('000300.XBHS', g.current_date)

    if g.t % g.tc == 0:
        # 每隔g.tc天，交易一次
        g.if_trade = True
        # 将ST、停牌、退市三种状态的股票剔除当日的股票池
        g.all_stocks = filter_stock_by_status(g.all_stocks, filter_type=["ST", "HALT", "DELISTING"], query_date=None)
    g.t += 1


# 每天交易时要做的事情
def handle_data(context, data):
    if not g.trade_flag:
        return

    if g.if_trade:
        df_scores = get_scores(g.all_stocks, str(get_trading_day(-63)), str(get_trading_day(-1)), g.rf)
        # 为每个持仓股票分配资金
        # 依打分排序，当前需要持仓的股票
        if df_scores.empty:
            stock_sort = list()
        else:
            stock_sort = df_scores.sort_values('score')['code'].tolist()
        # 把涨停状态的股票剔除
        up_limit_stock = get_limit_stock(stock_sort)['up_limit']
        # stock_sort = list(set(stock_sort)-set(up_limit_stock))
        stock_sort = [stock for stock in stock_sort if stock not in up_limit_stock]
        position_list = get_position_list(context)
        # 持仓中跌停的股票不做卖出
        limit_info = get_limit_stock(position_list)
        hold_down_limit_stock = limit_info['down_limit']
        log.info('持仓跌停股：%s' % hold_down_limit_stock)
        position_list = get_position_list(context)
        # 持仓中除了不处于前g.N且跌停不能卖的股票进行卖出
        sell_stocks = list(set(position_list) - set(stock_sort[:g.N]) - set(hold_down_limit_stock))
        # 对不在换仓列表中且飞跌停股的股票进行卖出操作
        order_stock_sell(sell_stocks)
        # 获取仍在持仓中的股票
        position_list = get_position_list(context)
        # 获取调仓买入的股票
        buy_stocks = [stock for stock in stock_sort if stock not in position_list][:(g.N - len(position_list))]
        # 仓位动态平衡的股票
        balance_stocks = list(set(buy_stocks + position_list) - set(hold_down_limit_stock))
        every_stock = context.portfolio.portfolio_value / g.N
        order_stock_balance(balance_stocks, every_stock)
        order_stock_balance(balance_stocks, every_stock)
    g.if_trade = False


# 不在换仓目标中且没有跌停的股票进行清仓操作
def order_stock_sell(sell_stocks):
    # 对于不需要持仓的股票，全仓卖出
    for stock in sell_stocks:
        order_target_value(stock, 0)


# 非跌停的换仓目标股进行仓位再平衡
def order_stock_balance(balance_stocks, every_stock):
    for stock in balance_stocks:
        order_target_value(stock, every_stock)


# 获取综合得分
def get_scores(stocks, begin, end, rf):
    try:
        length = len(stocks)
        market_cap_df = get_fundamentals(stocks, 'valuation', fields='total_value', date=begin)
        market_cap_df.dropna(inplace=True)
        if market_cap_df.empty:
            print('获取市值数据失败，股票因子评分失败')
            return pd.DataFrame()
        total_shareholder_equity_df = get_factor_values(stocks, 'total_shareholder_equity', begin, g.factor_params_info)
        total_shareholder_equity_df.dropna(inplace=True)
        if total_shareholder_equity_df.empty:
            print('获取total_shareholder_equity财务数据失败，股票因子评分失败')
            return pd.DataFrame()
        roe_df = get_factor_values(stocks, 'roe', begin, g.factor_params_info)
        roe_df.dropna(inplace=True)
        if roe_df.empty:
            print('获取roe财务数据失败，股票因子评分失败')
            return pd.DataFrame()
        df_all = pd.concat([market_cap_df, total_shareholder_equity_df, roe_df], axis=1)
        df_all.dropna(inplace=True)
        df_all['BTM'] = df_all['total_shareholder_equity'] / df_all['total_value']
        df_all = df_all.reset_index()
        S = df_all.sort_values('total_value')['index'][:int(length / 3)]
        B = df_all.sort_values('total_value')['index'][length - int(length / 3):]
        L = df_all.sort_values('BTM')['index'][:int(length / 3)]
        H = df_all.sort_values('BTM')['index'][length - int(length / 3):]
        W = df_all.sort_values('roe')['index'][:int(length / 3)]
        R = df_all.sort_values('roe')['index'][length - int(length / 3):]

        close_data = get_price(stocks, begin, end, fields='close', frequency='1d', is_dict=True)

        close_df = pd.DataFrame()
        for stock_code, stock_data in close_data.items():
            date_info = pd.to_datetime(stock_data['datetime'], format='%Y%m%d')
            close_info = stock_data['close']
            close_df[stock_code] = pd.Series(close_info, index=date_info)
        close_df.sort_index(inplace=True)
        df = np.diff(np.log(close_df), axis=0) + 0 * close_df[1:]
        SMB = df[S].T.sum() / len(S) - df[B].T.sum() / len(B)
        HML = df[H].T.sum() / len(H) - df[L].T.sum() / len(L)
        RMW = df[R].T.sum() / len(R) - df[W].T.sum() / len(W)
        dp = get_price('000300.XSHG', begin, end, '1d')['close']
        if len(dp)-len(df)>1:
            log.info('历史行情数据缺失，股票因子评分失败')
            return pd.DataFrame()
        RM = np.diff(np.log(dp)) - rf / 252
        X = pd.DataFrame({"RM": RM, "SMB": SMB, "HML": HML, "RMW": RMW})
        factor_flag = ["RM", "SMB", "HML", "RMW"][:g.NoF]
        X = X[factor_flag]
        t_scores = [0.0] * length
        for i in range(length):
            t_stock = stocks[i]
            t_r = linreg(X, df[t_stock] - rf / 252, len(factor_flag))
            t_scores[i] = t_r[0]
        scores = pd.DataFrame({'code': stocks, 'score': t_scores})
        df_scores = scores.sort_values(by='score')
        return df_scores
    except:
        print('股票因子评分失败，请检查数据')
        return pd.DataFrame()


# 获取因子值
def get_factor_values(stock_list, factor, date, factor_params_info):
    """
    获取因子值方法
    入参：
    1、股票池：stock_list
    2、因子名称：factor
    3、计算日期：date
    4、因子数据获取需要维护的信息（因子名称、表名、字段名）
    """
    data = get_fundamentals(stock_list, table=factor_params_info[factor][0], fields=factor_params_info[factor][1],
                            date=date, is_dataframe=True)
    factor_info = {}
    for stock in stock_list.copy():
        if stock not in data.index:
            continue
        factor_info[stock] = data.loc[stock, factor_params_info[factor][1]]
    if factor_info == {}:
        return pd.DataFrame()
    factor_df = pd.DataFrame.from_dict(factor_info, orient='index')
    factor_df.columns = [factor_params_info[factor][1]]
    return factor_df


# 线性回归
def linreg(x, y, columns=3):
    x = sm.add_constant(np.array(x))
    y = np.array(y)
    if len(y) > 1:
        results = regression.linear_model.OLS(y, x).fit()
        return results.params
    else:
        return [float("nan")] * (columns + 1)


# 保留小数点两位
def replace(x):
    x = Decimal(x)
    x = float(str(round(x, 2)))
    return x


# 生成昨日持仓股票列表
def get_position_list(context):
    return [
        position.sid
        for position in context.portfolio.positions.values()
        if position.amount != 0
    ]


# 日级别回测获取持仓中不能卖出的股票(涨停就不卖出)
def get_limit_stock(stock_list):
    out_info = {'up_limit': [], 'down_limit': []}
    for stock in stock_list:
        limit_status = check_limit(stock)[stock]
        if limit_status == 1:
            out_info['up_limit'].append(stock)
        elif limit_status == -1:
            out_info['down_limit'].append(stock)
    return out_info