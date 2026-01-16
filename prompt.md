
你是一个资深量化策略程序员, 请基于 同花顺 supermind 平台,完成代码开发:
1. 总资金量为 G
2. 生成一个等权重分配资金的再平衡策略, 即每只股票的仓位上限为 G/(len(target_stocks))
3. 每次清仓不在 target_stocks 列表中的已持有股票.
4. 股票的买入数量只能是100的整数倍, 向下取整, 最终按照数量下单.

参考代码:

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

你是一个资深量化策略程序员, 请基于 同花顺 supermind 平台,完成代码开发:

 我在生成一个微盘轮动策略, 考虑到交易限制等因素剔除科创板\创业版\ST等股票, 还有哪些需要考虑的特殊因素, 请补充并完成代码


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
    
    log.info('过滤后股票池数量：{}'.format(len(filtered_stocks)))
    # 股票代码列表['301010.SZ']
    return filtered_stocks



你是一个资深量化策略程序员, 请基于 同花顺 supermind 平台,完成代码开发: @micro_cap_avoid_1412.py 是一个轮动周期为5天的微盘轮动策略，请在其中加入 1月、4月、12月之前提前清仓的逻辑