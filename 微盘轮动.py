# 微盘股双低轮动策略
# 策略逻辑：每5个交易日，买入全A股中（剔除ST、科创板、退市整理股）市值最小且250日换手率最低的20只股票，等权重持仓

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
    # 盘前运行（可选：用于数据预处理）
    date = get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    # log.info('{} 盘前运行'.format(date))

def handle_bar(context, bar_dict):
    # 主交易逻辑，每交易日执行
    current_date = get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    g.days += 1  # 计数交易日
    
    # 检查是否到达调仓日（每5天一次）
    if g.days % g.period != 0 and g.last_trade_date != get_datetime().date():
        return  # 非调仓日不操作
    
    # log.info('开始调仓日选股流程')
    g.last_trade_date = get_datetime().date()
    
    # 1. 获取股票池并过滤风险股票
    stock_pool = get_stock_pool(context)
    if len(stock_pool) == 0:
        log.warn('当日无符合条件的股票，跳过调仓')
        return
    
    # 2. 计算市值和换手率，并排序
    df_stocks = get_stock_metrics(stock_pool, context)
    if df_stocks is None or len(df_stocks) < g.hold_num:
        log.warn('可选股票数量不足，跳过调仓')
        return
    
    
    # 3. 按双低标准排序：先市值升序，再换手率升序
    df_stocks = df_stocks.sort_values(['market_cap', 'turnover_250d'], ascending=[True, True])
    target_stocks = list(df_stocks.head(g.hold_num)['symbol'])
    
    # 4. 执行调仓：卖出不在目标列表的股票，买入新股票
    rebalance_portfolio(context, target_stocks)

def after_trading(context):
    # 盘后运行（可选：记录日志）
    time = get_datetime().strftime('%Y-%m-%d %H:%M:%S')
    # log.info('{} 盘后运行'.format(time))

# --- 自定义函数 ---
def get_stock_pool(context):
    """获取初始股票池并过滤风险股票"""
    # 获取全A股股票列表
    all_stocks = get_all_securities('stock', get_datetime()).index.tolist()
    filtered_stocks = []
    
    for stock in all_stocks:
        # 排除科创板（代码以688开头）[7](@ref)
        if stock.startswith('688'):
            continue
        
        # 排除ST/*ST股票 [7](@ref)
        name = get_security_info(stock).display_name
        if name is not None and ('ST' in name or '*ST' in name):
            continue
        
        # 排除退市整理股（通常名称含"退市"或代码以"退"结尾）
        if name is not None and ('退市' in name or stock.endswith('.RT')):
            continue
        
        filtered_stocks.append(stock)
    
    # log.info('过滤后股票池数量：{}'.format(len(filtered_stocks)))
    # 股票代码列表['301010.SZ']
    return filtered_stocks

def get_stock_metrics(stock_list, context):
    """计算股票的市值和250日换手率"""
    import pandas as pd
    data_list = []
    
    for stock in stock_list:
        try:
            # 获取总市值（亿元）
            fundamental_data = get_fundamentals(
                query(valuation.market_cap).filter(valuation.symbol == stock), 
                date=get_datetime()
            )
            
            if fundamental_data.empty:
                continue
                
            # 使用iloc[0, 0]或iat[0, 0]获取标量值，而不是Series
            market_cap_value = fundamental_data.iloc[0, 0] / 1e8  # 转换为亿元
            # 或者使用：market_cap_value = fundamental_data.iat[0, 0] / 1e8
            # 或者使用：market_cap_value = fundamental_data['market_cap'].iloc[0] / 1e8
            
            # log.info('股票{}市值：{:.2f}亿元'.format(stock, market_cap_value))  # 记录标量值
            
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
                
            turnover_250d = turnover_data['turnover'].mean()  # 250日平均换手率
            
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
    """调整持仓至目标股票列表，等权重分配"""
    current_positions = list(context.portfolio.stock_account.positions.keys())
    weight_per_stock = 1.0 / len(target_stocks)  # 等权重
    
    # 卖出当前持仓中不在目标列表的股票
    for stock in current_positions:
        if stock not in target_stocks:
            # log.info('卖出股票：{}'.format(stock))
            order_target_percent(stock, 0)  # 清仓
    
    # 买入目标股票，等权重分配
    for stock in target_stocks:
        # log.info('买入股票：{}，目标权重{:.2%}'.format(stock, weight_per_stock))
        order_target_percent(stock, weight_per_stock)
