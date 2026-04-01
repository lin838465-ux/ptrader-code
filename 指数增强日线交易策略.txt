"""
策略名称：
指数增强日线交易策略
策略流程：
盘前：
1、将沪深300成分股中st、停牌、退市的股票过滤得到股票池
2、示例用roe作为单因子选出排名第一档的股票作为目标股票池
盘中：
1、财报调仓日或者固定间隔调仓日通过线性规划的方法进行调仓，以图实现增强效果
注意事项：
策略中调用的order_target、order_target_value接口的使用有场景限制，回测可以正常使用，交易谨慎使用。
回测场景下撮合是引擎计算的，因此成交之后持仓信息的更新是瞬时的，但交易场景下信息的更新依赖于柜台数据
的返回，无法做到瞬时同步，可能造成重复下单。详细原因请看帮助文档。
"""
import pandas as pd
import numpy as np
import statsmodels.api as sm
import math
from decimal import Decimal
import datetime
from datetime import date as justdate
from scipy.optimize import minimize


# 初始化
def initialize(context):
    g.factor = 'roe'
    g.factor_params_info = {'roe': ['profit_ability', 'roe', False],  # 净资产收益率,最后布尔值为排序方式
                            'operating_revenue_grow': ['growth_ability', 'operating_revenue_grow_rate', False],  # 营收增速
                            'net_profit_grow': ['growth_ability', 'np_parent_company_cut_yoy', False],  # 扣非净利润增速
                            }
    set_params()  # 设置策参数
    set_variables()  # 设置中间变量
    is_trade_flag = is_trade()
    if is_trade_flag:
        pass
    else:
        set_backtest()  # 设置回测条件


# 设置策参数
def set_params():
    g.percent = 0.10
    g.est_interval = 80  # 记录优化区间，使用二次规划根据这个区间最优化权重
    g.lamda = 0
    g.hold_days = 60
    g.max_hold_num = 20  # 最大持仓的股票
    g.run_days = 0
    g.benchmark = '000300.SS'
    # 财报季度调仓所依据的指定日期
    g.finance_update_date_list = ['0401', '0801', '1001']


# 设置中间变量
def set_variables():
    g.init_screen = True
    g.is_update_stocks = False


# 设置回测条件
def set_backtest():
    set_limit_mode('UNLIMITED')  # 回测撮合不限制成交量


# 盘前处理
def before_trading_start(context, data):
    g.current_date = context.blotter.current_dt.strftime('%Y%m%d')
    # 2008-01-01前回测由于数据不足，不执行。
    if g.current_date < '20080101':
        g.trade_flag = False
    else:
        g.trade_flag = True
    if not g.trade_flag:
        return
    g.everyStock = 0
    if is_pub_date(g.current_date):  # 财报调仓日
        g.stocks = create_stocks()
        g.is_update_stocks = True
    elif g.init_screen:
        '''初始化一个组合，这一小段代码只会用一次'''
        g.stocks = create_stocks()
        g.is_update_stocks = True
        g.init_screen = False  # 将Flag置为False，保证下次不再运行


# 每天交易时要做的事情
def handle_data(context, data):
    if not g.trade_flag:
        return
    # 如果到公告日更新了调仓
    if g.is_update_stocks:
        stock_sort = g.stocks
        log.info('初始日或调仓日股票池')
        log.info(stock_sort)
        if not stock_sort:
            return
        previous_date = get_trading_day(-1)
        # 通过二次规划确定权重
        weight = get_weights(stock_sort, previous_date)
        stock_weight = dict(zip(stock_sort, weight))
        stocks = stock_sort
        current_hold_set = set(context.portfolio.positions.keys())
        if set(stocks) != current_hold_set:
            need_buy = set(stocks).difference(current_hold_set)
            need_sell = current_hold_set.difference(stocks)
            current_stocks = set(stocks).difference(need_buy)
            try:
                for stock in need_sell:
                    order_target(stock, 0)
                for stock in need_buy:
                    order_value(stock, context.portfolio.portfolio_value * stock_weight[stock])
                for stock in current_stocks:
                    order_target_value(stock, context.portfolio.portfolio_value * stock_weight[stock])
            except:
                pass
        g.is_update_stocks = False
        g.run_days = 0

    elif g.run_days % g.hold_days == 0:
        stocks = g.stocks
        log.info('非调仓日股票池')
        log.info(stocks)
        if not stocks:
            return
        '''这里的权重通过二次规划确定'''
        weight = get_weights(stocks, context.previous_date)
        stock_weight = dict(zip(stocks, weight))
        try:
            for stock in stocks:
                order_target_value(stock, context.portfolio.portfolio_value * stock_weight[stock])
        except:
            pass
    if context.portfolio.cash > 0:
        # 如果可用资金大于0，说明没有全仓，就是撮合单的时候出问题，所以需要重新买入，这时候重新全仓买入几个ETF
        log.info('尝试把剩余资金用完，买入ETF')
        cash = context.portfolio.cash
        order_value('510300.SS', cash / 10 * 4)
        order_value('510330.SS', cash / 10 * 3)
        order_value('510500.SS', cash / 10 * 3)
    g.run_days += 1


# 建立股票池
def create_stocks():
    g.all_stocks = get_index_stocks('000300.XBHS', g.current_date)
    for stock in g.all_stocks.copy():
        if stock[:3] == '688':
            g.all_stocks.remove(stock)
    # 将ST、停牌、退市三种状态的股票剔除当日的股票池
    g.all_stocks = filter_stock_by_status(g.all_stocks, filter_type=["ST", "HALT", "DELISTING"], query_date=None)
    return get_stocks(g.all_stocks, str(get_trading_day(-1)), g.factor)


# 获取拟持仓股票池
def get_stocks(stocks, date, factor):
    sort_type = g.factor_params_info[factor][-1]
    df = get_factor_values(stocks, factor, date, g.factor_params_info)
    df.dropna(inplace=True)
    if df.empty:
        print('%s数据获取失败，选股失败' % factor)
        return list()
    # 3倍标准差去极值
    df = winsorize(df, factor, std=3, have_negative=True)
    # z标准化
    df = standardize(df, factor, ty=2)
    # 市值中性化
    market_cap_df = get_fundamentals(stocks, 'valuation', fields='total_value', date=date)
    market_cap_df = market_cap_df[['total_value']]
    market_cap_df.dropna(inplace=True)
    if market_cap_df.empty:
        print('市值数据获取失败，选股失败')
        return list()
    df = neutralization(df, factor, market_cap_df)
    df = df.sort_values(by=factor, ascending=sort_type)
    return list(df.head(int(len(df) * g.percent)).index)


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


# 使用二次规划确定权重  
def get_weights(stocks, date):
    date = date.strftime('%Y-%m-%d')
    start_date = get_trading_day(-(g.est_interval + 1)).strftime('%Y-%m-%d')
    price_data = get_price(stocks, start_date=start_date, end_date=date, frequency='daily',
                           fields=['close'], is_dict=True)
    
    close_df = pd.DataFrame()
    for stock_code, stock_data in price_data.items():
        date_info = pd.to_datetime(stock_data['datetime'], format='%Y%m%d')
        close_info = stock_data['close']
        close_df[stock_code] = pd.Series(close_info, index=date_info)
    close_df.sort_index(inplace=True)    

    code_list = list(close_df.columns)
    df_list = []
    for stock in code_list:
        df = close_df[[stock]]
        df['change'] = 0 
        df['change'] = df[stock] / df[stock].shift(1) - 1
        df[stock] = df['change']
        df = df[[stock]]
        df.fillna(0, inplace=True)
        df_list.append(df)

    result = pd.concat(df_list, axis=1)
    index_price = get_price(g.benchmark, start_date=start_date, end_date=date, frequency='daily',
                            fields=['close'], is_dict=False)
    index_r = index_price.pct_change()
    index_r.fillna(0, inplace=True)
    weight = calculate_weight(np.array(result), np.array(index_r))
    return weight


def calculate_weight(train_returns, target_returns):
    length = len(train_returns.T)

    # 定义二次线性规划目标函数
    def objective(weights):
        return np.sum((np.dot(train_returns, weights) - target_returns) ** 2)

    # 定义约束条件
    constraints = [{'type': 'eq', 'fun': lambda weights: np.sum(weights) - 1},
                   {'type': 'ineq', 'fun': lambda weights: 0.2 - np.max(weights)}
                   ]
    # 定义权重的取值范围（可以设置最小权重和最大权重区间）
    min_weight = (1 / length) * 0.2  # 最小权重
    max_weight = (1 / length) * 5  # 最大权重
    bounds = [(min_weight, max_weight)] * train_returns.shape[1]
    # 初始化权重
    initial_weights = np.ones(train_returns.shape[1]) / train_returns.shape[1]
    # 最小化目标函数，求解权重
    result = minimize(objective, initial_weights, constraints=constraints, bounds=bounds)
    # 输出结果
    test_weights = result.x
    # print("测试集投资权重：", test_weights)
    return test_weights


# 保留小数点两位
def replace(x):
    x = Decimal(x)
    x = float(str(round(x, 2)))
    return x


# 去极值函数（3倍标准差去极值）
def winsorize(factor_data, factor, std=3, have_negative=True):
    """
    去极值函数
    factor:以股票code为index，因子值为value的Series
    std为几倍的标准差，have_negative 为布尔值，是否包括负值
    输出Series
    """
    r = factor_data[factor]
    if not have_negative:
        r = r[r >= 0]
    # 取极值
    edge_up = r.mean() + std * r.std()
    edge_low = r.mean() - std * r.std()
    r[r > edge_up] = edge_up
    r[r < edge_low] = edge_low
    r = pd.DataFrame(r)
    return r


# z－score标准化函数：
def standardize(factor_data, factor, ty=2):
    """
    s为Series数据
    ty为标准化类型:1 MinMax,2 Standard,3 maxabs
    """
    temp = factor_data[factor]
    re = 0
    if int(ty) == 1:
        re = (temp - temp.min()) / (temp.max() - temp.min())
    elif ty == 2:
        re = (temp - temp.mean()) / temp.std()
    elif ty == 3:
        re = temp / 10 ** np.ceil(np.log10(temp.abs().max()))
    return pd.DataFrame(re)


# 市值中性化函数
def neutralization(data_factor, factor, data_market_cap):
    data_market_cap['total_value2'] = 0
    data_market_cap['total_value2'] = data_market_cap['total_value'].apply(lambda a: math.log(a))
    df = pd.concat([data_factor, data_market_cap], axis=1, join='inner')
    y = df[factor]
    x = df['total_value2']
    result = sm.OLS(y, x).fit()
    result = pd.DataFrame(result.resid)
    result.columns = [factor]
    return result


# 判断当天时间是不是出财报的下一天时间
def is_pub_date(current_date):
    cur_year = current_date[:4]
    trade_dates = []
    # 按季度选股，在4.30、8.31、10.31三个时间日重新根据财务报表选择股票
    for date in g.finance_update_date_list:
        trade_dates.append(get_trading_day_by_date(cur_year+date, day=0))
    if current_date in trade_dates:
        return True
    return False