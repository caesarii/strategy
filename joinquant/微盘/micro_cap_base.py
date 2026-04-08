# -*- coding: utf-8 -*-
"""
微盘股双低轮动策略（聚宽版）

逻辑来源：同花顺 Supermind 平台微盘双低系列（见仓库 supermind/微盘/ 下同类脚本）。
本文件为迁移至聚宽（JoinQuant）后的基座策略，便于在同一套选股规则下用聚宽回测/实盘。

与 Supermind 常见差异（本文件已按聚宽带目调整）：
- 策略入口：initialize + run_weekly 回调（聚宽）；Supermind 多为 init / handle_bar。
- 基准代码：000300.XSHG（聚宽）；Supermind 常用 000300.SH。
- 市值查询：valuation.code 过滤标的（聚宽）；Supermind 侧常为 valuation.symbol。
- 持仓：context.portfolio.positions[].total_amount（聚宽）；Supermind 多为 stock_account.positions[].quantity。

股票池: 排除科创板、创业板、ST/*ST、退市整理股；并满足 Supermind 同构条件——总市值排名最小 400、
前交易日收盘 ∈ (1, 15) 元、当日累计成交额 > 0.1 亿元、非涨跌停。
选股: 先按总市值取最小 400 并过成交额/价/涨跌停；在入池标的上对「市值 + 250 日平均换手」做同步排名（权重各 1，名次相加越小越好），再取持仓。
调仓时间: 每周五 13:00（下午 1 点，聚宽 run_weekly weekday=4）
资金与持仓: 建议初始资金 3 万元（聚宽回测/模拟中设置）；目标持仓 15 只；调仓按当前 total_value 等权。
"""

from jqdata import *


def initialize(context):
    # 初始化函数，全局只运行一次
    set_option('use_real_price', True)
    set_benchmark('000300.XSHG')
    log.info('微盘股双低轮动策略开始运行')

    g.hold_num = 15  # 持仓股票数量
    g.initial_capital = 30000  # 与聚宽回测「初始资金」保持一致（仓位按 total_value 等权）
    log.info('目标持仓{}只，建议初始资金{}元（请在回测/模拟设置中一致）'.format(g.hold_num, g.initial_capital))

    # 设置交易成本与规则（与 Supermind 示例保持一致，具体以聚宽当前 API 为准）
    set_commission(PerShare(type='stock', cost=0.0002, min_trade_cost=0.0))
    set_slippage(PriceSlippage(0.005))
    set_volume_limit(0.25, 0.5)
    # weekday: 0–4 周一～周五，4=周五；time 为 HH:MM（此处为下午 1 点）
    run_weekly(
        weekly_rebalance,
        weekday=4,
        time='13:00',
        reference_security='000300.XSHG',
    )

def before_trading_start(context):
    _ = get_datetime().strftime('%Y-%m-%d %H:%M:%S')


def weekly_rebalance(context):
    # 1. 获取股票池并过滤风险股票
    stock_pool = get_stock_pool(context)
    if len(stock_pool) == 0:
        log.warn('当日无符合条件的股票，跳过调仓')
        return
    
    # 2. 计算市值与 250 日换手，在池内做同步排名（双因子等权）
    df_stocks = get_stock_metrics(stock_pool, context)
    if df_stocks is None or len(df_stocks) < g.hold_num:
        log.warn('可选股票数量不足，跳过调仓')
        return

    df_stocks = df_stocks.dropna(subset=['market_cap', 'turnover_250d'])
    if len(df_stocks) < g.hold_num:
        log.warn('有效市值/换手数据不足，跳过调仓')
        return

    # 双低：市值越小、换手越低 → 升序排名；同步得分 = w_cap * rk_cap + w_to * rk_turnover
    rk_cap = df_stocks['market_cap'].rank(method='average', ascending=True)
    rk_to = df_stocks['turnover_250d'].rank(method='average', ascending=True)
    sync = _SYNC_RANK_W_CAP * rk_cap + _SYNC_RANK_W_TO * rk_to
    df_stocks = df_stocks.assign(_sync_score=sync).sort_values('_sync_score', ascending=True)
    target_stocks = list(df_stocks.head(g.hold_num)['symbol'])

    # 3. 执行调仓
    rebalance_portfolio(context, target_stocks)

def after_trading_end(context):
    _ = get_datetime().strftime('%Y-%m-%d %H:%M:%S')


# 与 Supermind 截图一致：0.1 亿元；前收 (1, 15)；市值最小 400；当日成交额与涨跌停过滤
_MIN_DAILY_MONEY = 1e7  # 0.1 亿 = 1e7 元
_PREV_CLOSE_MIN = 1.0
_PREV_CLOSE_MAX = 15.0
_CAP_SMALLEST_N = 400
_FUND_BATCH = 800
# 入池后同步排名权重（市值因子、换手率因子均为 1）
_SYNC_RANK_W_CAP = 1.0
_SYNC_RANK_W_TO = 1.0


def get_stock_pool(context):
    dt = context.current_dt
    all_stocks = get_all_securities('stock', dt.date()).index.tolist()
    filtered_stocks = []

    for stock in all_stocks:
        if stock.startswith('688'):
            continue
        if stock.startswith('300') or stock.startswith('301'):
            continue
        name = get_security_info(stock).display_name
        if name is not None and ('ST' in name or '*ST' in name):
            continue
        if name is not None and ('退市' in name or '退' in name or stock.endswith('.RT')):
            continue
        filtered_stocks.append(stock)

    if not filtered_stocks:
        log.warn('基础过滤后股票池为空')
        return []

    # 总市值排名最小 400（在基础池内以市值升序取前 400）
    cap_pairs = []
    for i in range(0, len(filtered_stocks), _FUND_BATCH):
        chunk = filtered_stocks[i : i + _FUND_BATCH]
        fdf = get_fundamentals(
            query(valuation.code, valuation.market_cap).filter(valuation.code.in_(chunk)),
            date=dt,
        )
        if fdf is None or fdf.empty:
            continue
        for _, row in fdf.iterrows():
            code = row['code']
            cap = row['market_cap']
            try:
                c = float(cap)
            except (TypeError, ValueError):
                continue
            if c > 0:
                cap_pairs.append((code, c))
    if not cap_pairs:
        log.warn('无法取得市值数据，股票池为空')
        return []
    cap_pairs.sort(key=lambda x: x[1])
    cap_candidates = [p[0] for p in cap_pairs[:_CAP_SMALLEST_N]]

    cd = get_current_data()
    result = []
    for stock in cap_candidates:
        try:
            bar = cd[stock]
            if bar.paused or bar.last_price <= 0:
                continue

            money = getattr(bar, 'money', None)
            if money is None:
                money = 0
            try:
                money = float(money)
            except (TypeError, ValueError):
                money = 0
            if money < _MIN_DAILY_MONEY:
                continue

            if bar.high_limit > 0 and bar.last_price >= bar.high_limit - 1e-8:
                continue
            if bar.low_limit > 0 and bar.last_price <= bar.low_limit + 1e-8:
                continue

            ph = attribute_history(
                stock,
                1,
                '1d',
                ['close'],
                skip_paused=True,
                df=True,
                fq='pre',
            )
            if ph is None or ph.empty:
                continue
            prev_close = float(ph['close'].iloc[-1])
            if not (_PREV_CLOSE_MIN < prev_close < _PREV_CLOSE_MAX):
                continue

            result.append(stock)
        except Exception:
            continue

    log.info(
        '股票池：基础{}只 → 市值最小{}候选{}只 → 成交额/价/涨跌停后{}只'.format(
            len(filtered_stocks),
            _CAP_SMALLEST_N,
            len(cap_candidates),
            len(result),
        )
    )
    return result

def get_stock_metrics(stock_list, context):
    import pandas as pd
    data_list = []
    
    for stock in stock_list:
        try:
            # 获取总市值（亿元）
            fundamental_data = get_fundamentals(
                query(valuation.market_cap).filter(valuation.code == stock),
                get_datetime(),
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
    current_positions = [
        s for s in context.portfolio.positions
        if context.portfolio.positions[s].total_amount > 0
    ]
    
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
                if stock in context.portfolio.positions:
                    current_holding = context.portfolio.positions[stock].total_amount
                
                # 只有当目标股数与当前持仓不同时才交易
                if target_shares != current_holding:
                    log.info('调整 {} 持仓：当前{}股，目标{}股'.format(stock, current_holding, target_shares))
                    order_target(stock, target_shares) 
            else:
                # 如果计算出的股数为0，清仓该股票
                if stock in current_positions:
                    order_target(stock, 0) 
                    log.info('清仓 {}，因目标股数为0'.format(stock)) 