# 基准策略 + 流动性过滤

# 微盘股双低轮动策略
# 股票池: 1. 排除科创板 \ 创业板 \ ST \* ST \ 退市整理股; 
# 双低策略: 总市值 + 250天换手率
# 周期: 5天

# 流动性 近 20 交易日日均换手率低于0.7%, 日均成交金额低于 1500w

# type: ignore
def init(context):
    # 初始化函数，全局只运行一次
    set_benchmark('000300.SH')  # 设置基准收益：沪深300指数
    log.info('微盘股双低轮动策略开始运行')
    
    # 设置交易成本与规则
    set_commission(PerShare(type='stock', cost=0.0002))  # 手续费万分之二
    set_slippage(PriceSlippage(0.005))  # 双边滑点0.5%
    set_volume_limit(0.25, 0.5)  # 日级最大成交比例25%，分钟级50%
    
    # 设置策略参数
    g.hold_num = 20  # 持仓股票数量
    g.period = 5  # 调仓周期（交易日）
    g.days = 0  # 交易计数器
    g.last_trade_date = None  # 记录上次调仓日期

def before_trading(context):
    date = get_datetime().strftime('%Y-%m-%d %H:%M:%S')

def handle_bar(context, bar_dict):
    g.days += 1  # 计数交易日
    
    # 检查是否到达调仓日（每5天一次）
    if g.days % g.period != 0 and g.last_trade_date != get_datetime().date():
        return  
    
    g.last_trade_date = get_datetime().date()
    
    # 1. 获取股票池
    stock_pool = get_stock_pool(context)

    # 2. 流动性过滤
    liquid_pool = filter_by_liquidity(stock_pool, context, 
                                    min_turnover=0.8, min_volume=2e7) 
    
    # 3. 财务质量过滤
    quality_pool = filter_by_financials(liquid_pool, context)


    if len(quality_pool) == 0:
        log.warn('当日无符合条件的股票，跳过调仓')
        return
    
    # 2. 计算市值和换手率，并排序
    df_stocks = get_stock_metrics(quality_pool, context)
    if df_stocks is None or len(df_stocks) < g.hold_num:
        log.warn('可选股票数量不足，跳过调仓')
        return
    
    
    # 3. 按双低标准排序：先市值升序，再换手率升序
    df_stocks = df_stocks.sort_values(['market_cap', 'turnover_250d'], ascending=[True, True])
    target_stocks = list(df_stocks.head(g.hold_num)['symbol'])
    
    # 4. 执行调仓
    rebalance_portfolio(context, target_stocks)

def after_trading(context):
    time = get_datetime().strftime('%Y-%m-%d %H:%M:%S')


def get_stock_pool(context):
    # 获取全A股股票列表
    all_stocks = get_all_securities('stock', get_datetime()).index.tolist()
    filtered_stocks = []
    
    for stock in all_stocks:
        # 排除科创板（代码以688开头）[7](@ref)
        if stock.startswith('688'):
            continue
        
        # 排除创业板（代码以300、301开头）
        if stock.startswith('300') or stock.startswith('301'):
            continue
        
        # 排除ST/*ST股票 [7](@ref)
        name = get_security_info(stock).display_name
        if name is not None and ('ST' in name or '*ST' in name):
            continue
        
        # 排除退市整理股（通常名称含"退市"或代码以"退"结尾）
        if name is not None and ('退市' in name or '退' in name or stock.endswith('.RT')):
            continue
        
        filtered_stocks.append(stock)
    
    log.info('初步过滤后股票池数量：{}'.format(len(filtered_stocks)))
    return filtered_stocks



def filter_by_liquidity(stock_list, context, days=20, min_turnover=0.5, min_volume=1e6):
    """
    基于流动性过滤股票
    :param stock_list: 待过滤股票列表
    :param days: 考察期
    :param min_turnover: 最低日均换手率(%)
    :param min_volume: 最低日均成交金额(万元)
    """
    qualified_stocks = []
    
    for stock in stock_list:
        try:
            # 获取历史成交数据
            hist_data = history(stock, ['turnover_rate', 'turnover'], days, '1d', 
                              skip_paused=True, fq='pre', is_panel=1)
            
            if hist_data is None or len(hist_data) < days//2:  # 允许部分缺失
                continue
                
            avg_turnover = hist_data['turnover_rate'].mean()
            avg_volume = hist_data['turnover'].mean()
            
            # 流动性筛选
            if avg_turnover >= min_turnover and avg_volume >= min_volume:
                qualified_stocks.append(stock)
                
        except Exception as e:
            log.warn('流动性检查失败 {}: {}'.format(stock, e))
            continue
    
    log.info('流动性过滤后股票数量：{}'.format(len(qualified_stocks)))
    return qualified_stocks

def filter_by_financials(stock_list, context):
    """
    基于财务质量过滤股票，避免潜在退市风险
    """
    qualified_stocks = []
    
    for stock in stock_list:
        try:
            # 获取最新财务数据（示例指标，可根据需要调整）
            current_date = get_datetime()
            q = query(
                income.code, income.net_profit, balance.total_liability,
                cash_flow.net_operate_cash_flow
            ).filter(
                income.code == stock,
                income.pub_date >= current_date.replace(month=1, day=1)  # 本年财报
            )
            
            df = get_fundamentals(q)
            if df.empty:
                continue
                
            net_profit = df['net_profit'][0]
            # 排除连续亏损或财务异常的公司
            if net_profit is not None and net_profit > 0:  # 简单示例：要求盈利
                qualified_stocks.append(stock)
                
        except Exception as e:
            # 财务数据获取失败时谨慎排除
            log.warn('财务数据获取失败 {}: {}'.format(stock, e))
            continue
    
    log.info('财务质量过滤后股票数量：{}'.format(len(qualified_stocks)))
    return qualified_stocks



def get_stock_metrics(stock_list, context):
    import pandas as pd
    data_list = []
    
    for stock in stock_list:
        try:
            # 获取总市值（亿元）
            fundamental_data = get_fundamentals(
                query(valuation.market_cap).filter(valuation.symbol == stock), 
                get_datetime()
            )
            
            if fundamental_data.empty:
                continue
                
            market_cap_value = fundamental_data.iloc[0, 0] / 1e8  # 转换为亿元
            
            
            # 获取过去250日换手率数据
            turnover_data = history(
                stock, 
                ['turnover_rate'], 
                250, 
                '1d', 
                False, 
                'pre', 
                is_panel=0
            )
            
            if len(turnover_data) == 0:
                continue
                
            turnover_250d = turnover_data['turnover_rate'].mean()  # 250日平均换手率
            
            data_list.append({
                'symbol': stock,  
                'market_cap': market_cap_value,  # 现在是标量值
                'turnover_250d': turnover_250d
            })
            
        except Exception as e:
            log.warn('股票{}数据获取失败: {}'.format(stock, str(e)))
            continue
    
    if len(data_list) == 0:
        return None
        
    return pd.DataFrame(data_list)

def rebalance_portfolio(context, target_stocks):
    
    # 获取当前持仓股票列表
    current_positions = list(context.portfolio.stock_account.positions.keys())
    
    # 如果目标股票列表为空，清空所有持仓
    if len(target_stocks) == 0:
        for stock in current_positions:
            order_target(stock, 0)  # 清仓
        return
    
    # 计算每只股票应分配的资金（等权重）
    total_value = context.portfolio.total_value
    weight_per_stock = 1.0 / len(target_stocks)
    target_value_per_stock = total_value * weight_per_stock
    
    # 卖出当前持仓中不在目标列表的股票
    for stock in current_positions:
        if stock not in target_stocks:
            # 记录日志[4](@ref)
            log.info('卖出不在目标列表的股票：{}'.format(stock))
            order_target(stock, 0)  
    
    # 对目标股票进行等权重配置
    for stock in target_stocks:
        # 获取当前价格来计算应买入股数
        current_price = history(stock, ['close'], 1, '1d', skip_paused=True, 
                              fq='pre', is_panel=1)['close'][-1]
        
        if current_price > 0:
            # 计算目标股数（向下取整到100的整数倍）
            target_shares = int(target_value_per_stock / current_price)
            target_shares = target_shares // 100 * 100  # 确保是100的整数倍
            
            if target_shares > 0:
                # 获取当前持仓数量
                current_holding = 0
                if stock in context.portfolio.stock_account.positions:
                    current_holding = context.portfolio.stock_account.positions[stock].quantity
                
                # 只有当目标股数与当前持仓不同时才交易
                if target_shares != current_holding:
                    log.info('调整 {} 持仓：当前{}股，目标{}股'.format(stock, current_holding, target_shares))
                    order_target(stock, target_shares)
            else:
                # 如果计算出的股数为0，清仓该股票
                if stock in current_positions:
                    order_target(stock, 0)
                    log.info('清仓 {}，因目标股数为0'.format(stock))