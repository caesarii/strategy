# -*- coding: utf-8 -*-
"""
微盘股双低轮动策略（聚宽版）

股票池: 排除科创板、创业板、ST/*ST、退市整理股；并满足 Supermind 同构条件——总市值排名最小 400、
前交易日收盘 ∈ (1, 15) 元（日线不复权）、上一完整交易日成交额 > 0.1 亿元、调仓时刻非涨跌停。
选股: 先按总市值取最小 400 并过成交额/价/涨跌停；在入池标的上对「市值 + 250 日日均成交量（换手率代理）」做同步排名（权重各 1），再取持仓。
调仓时间: 每周四开盘（run_weekly weekday=3, time=open）；回测请在聚宽中选择「日线」频率，勿用分钟线（显著加快）。
资金与持仓: 建议初始资金 3 万元（聚宽回测/模拟中设置）；目标持仓 15 只；调仓按当前 total_value 等权；下单为限价单，价格取前一完整交易日收盘价（不复权，与入池价口径一致）。
日志: `g.verbose_log` 控制选股侧调试信息；`log.set_level('order','error')` 关闭平台订单 INFO；调仓结束输出当前持仓明细 CSV（调仓前股数、目标股数、本次操作：买入/卖出/不变）。
"""

import csv
from io import StringIO

from jqdata import *


def initialize(context):
    # 初始化函数，全局只运行一次
    set_option('use_real_price', True)
    set_benchmark('000300.XSHG')
    # 仅保留订单 error；隐藏「订单已委托」「order StockOrder…trade price」等系统 INFO
    log.set_level('order', 'error')

    g.hold_num = 15  # 持仓股票数量
    g.initial_capital = 30000  # 与聚宽回测「初始资金」保持一致（仓位按 total_value 等权）
    # True：初始化/股票池/选股失败与跳过原因等；False：仅打印调仓买卖明细
    g.verbose_log = False

    if g.verbose_log:
        log.info('微盘股双低轮动策略开始运行')
        log.info(
            '目标持仓{}只，建议初始资金{}元（回测/模拟请一致）'.format(
                g.hold_num,
                g.initial_capital,
            )
        )

    # 聚宽 API：OrderCost + set_order_cost（非 Supermind 的 PerShare）
    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,  # 卖出印花税千一
            open_commission=0.0002,
            close_commission=0.0002,  # 买卖佣金万二
            min_commission=0,  # 原策略无最低佣金；若平台报错可改为 5c
        ),
        type='stock',
    )
    set_slippage(PriceRelatedSlippage(0.005), type='stock')  # 按成交价 0.5% 比例滑点
    # 聚宽无 Supermind 的 set_volume_limit；成交比例由回测撮合规则约束，拆单需自行在下单逻辑中实现
    # 日线回测：weekday 3=周四；time='open' 与「每天」频率对齐（勿再填 09:30，否则易误用分钟回测）
    run_weekly(
        weekly_rebalance,
        weekday=3,
        time='open',
        reference_security='000300.XSHG',
    )

def before_trading_start(context):
    pass  # 聚宽无全局 get_datetime()，需用 context.current_dt


def weekly_rebalance(context):
    # 1. 获取股票池并过滤风险股票
    stock_pool = get_stock_pool(context)
    if len(stock_pool) == 0:
        if g.verbose_log:
            log.warn('当日无符合条件的股票，跳过调仓')
        return

    # 2. 计算市值与 250 日换手，在池内做同步排名（双因子等权）
    df_stocks = get_stock_metrics(stock_pool, context)
    if df_stocks is None or len(df_stocks) < g.hold_num:
        if g.verbose_log:
            log.warn('可选股票数量不足，跳过调仓')
        return

    df_stocks = df_stocks.dropna(subset=['market_cap', 'turnover_250d'])
    if len(df_stocks) < g.hold_num:
        if g.verbose_log:
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
    pass


# 与 Supermind 截图一致：0.1 亿元；前收 (1, 15)；市值最小 400；当日成交额与涨跌停过滤
_MIN_DAILY_MONEY = 1e7  # 0.1 亿 = 1e7 元
_PREV_CLOSE_MIN = 1.0
_PREV_CLOSE_MAX = 15.0
_CAP_SMALLEST_N = 400
_FUND_BATCH = 800
# 入池后同步排名权重（市值因子、换手率因子均为 1）
_SYNC_RANK_W_CAP = 1.0
_SYNC_RANK_W_TO = 1.0


def _prev_trading_day_close_unadjusted(code):
    """前一完整交易日收盘价（不复权），与 get_stock_pool 中价格区间过滤一致。"""
    h = attribute_history(
        code, 1, '1d', ['close'], skip_paused=True, df=True, fq='none',
    )
    if h is None or h.empty:
        return 0.0
    return float(h['close'].iloc[-1])


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
        if g.verbose_log:
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
        if g.verbose_log:
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

            # 成交额：用「上一完整交易日」日线 money；开盘附近当日累计常远小于全天，不宜与 0.1 亿直接比。
            hist = attribute_history(
                stock,
                1,
                '1d',
                ['close', 'money'],
                skip_paused=True,
                df=True,
                fq='none',
            )
            if hist is None or hist.empty:
                continue
            day_money = float(hist['money'].iloc[-1])
            if day_money < _MIN_DAILY_MONEY:
                continue
            # 前收区间与大众行情「收盘价」一致，用不复权
            prev_close = float(hist['close'].iloc[-1])
            if not (_PREV_CLOSE_MIN < prev_close < _PREV_CLOSE_MAX):
                continue

            if bar.high_limit > 0 and bar.last_price >= bar.high_limit - 1e-8:
                continue
            if bar.low_limit > 0 and bar.last_price <= bar.low_limit + 1e-8:
                continue

            result.append(stock)
        except Exception:
            continue

    if g.verbose_log:
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
                date=context.current_dt,
            )
            
            if fundamental_data.empty:
                continue
                
            market_cap_value = fundamental_data.iloc[0, 0] / 1e8  # 转换为亿元
            
            
            # 聚宽无 Supermind 的 history(..., is_panel=)；日线亦无便捷 250 日换手率序列。
            # 用 250 交易日日均成交量作「换手/流动性」代理，列名仍为 turnover_250d 以供同步排名。
            vol_df = attribute_history(
                stock,
                250,
                '1d',
                ['volume'],
                skip_paused=True,
                df=True,
                fq='pre',
            )
            if vol_df is None or vol_df.empty or len(vol_df) < 50:
                continue
            turnover_250d = float(vol_df['volume'].mean())
            
            data_list.append({
                'symbol': stock,  
                'market_cap': market_cap_value,  # 现在是标量值
                'turnover_250d': turnover_250d
            })
            
        except Exception as e:
            if g.verbose_log:
                log.warn('股票{}数据获取失败: {}'.format(stock, str(e)))
            continue

    if len(data_list) == 0:
        return None

    return pd.DataFrame(data_list)

def rebalance_portfolio(context, target_stocks):
    holdings_before = {}
    for s in context.portfolio.positions:
        amt = int(context.portfolio.positions[s].total_amount)
        if amt > 0:
            holdings_before[s] = amt

    cdpx = get_current_data()
    # 每项: [代码, 名称, 当前股数, 目标股数, 本次操作, 参考价, 变动股数, 备注]
    summary_rows = []

    def _stock_display(code):
        inf = get_security_info(code)
        return inf.display_name if inf and getattr(inf, 'display_name', None) else code

    def _ref_price(code):
        p = _prev_trading_day_close_unadjusted(code)
        if p > 0:
            return p
        bar = cdpx[code]
        p = float(bar.last_price) if bar and bar.last_price and bar.last_price > 0 else 0.0
        if p <= 0:
            h1 = attribute_history(
                code, 1, '1d', ['close'], skip_paused=True, df=True, fq='pre',
            )
            if h1 is not None and not h1.empty:
                p = float(h1['close'].iloc[-1])
        return p

    def _order_style(code):
        px = _ref_price(code)
        if px <= 0:
            return None
        return LimitOrderStyle(round(px, 2))

    def _append_row(stock, cur, tgt, action, px, delta, note):
        summary_rows.append(
            [
                stock,
                _stock_display(stock),
                cur,
                tgt,
                action,
                round(px, 3) if px and px > 0 else '',
                delta if delta is not None else '',
                note,
            ]
        )

    def _emit_rebalance_summary():
        if not summary_rows:
            return
        ts = context.current_dt.strftime('%Y-%m-%d %H:%M:%S')
        buf = StringIO()
        w = csv.writer(buf, lineterminator='\n')
        w.writerow(
            [
                '调仓时间',
                '代码',
                '名称',
                '当前股数',
                '目标股数',
                '本次操作',
                '参考价',
                '变动股数',
                '备注',
            ]
        )
        for row in summary_rows:
            w.writerow([ts] + row)
        log.info('持仓明细CSV\n{}'.format(buf.getvalue().rstrip('\n')))

    if len(target_stocks) == 0:
        for stock in sorted(holdings_before.keys()):
            q = holdings_before[stock]
            px = _ref_price(stock)
            _append_row(stock, q, 0, '卖出', px, -q, '清仓')
            sty = _order_style(stock)
            if sty is not None:
                order_target(stock, 0, sty)
            else:
                order_target(stock, 0)
        _emit_rebalance_summary()
        return

    total_value = context.portfolio.total_value
    weight_per_stock = 1.0 / len(target_stocks)
    target_value_per_stock = total_value * weight_per_stock

    target_qty = {}
    for stock in target_stocks:
        px = _ref_price(stock)
        if px > 0:
            t = int(target_value_per_stock / px)
            target_qty[stock] = t // 100 * 100
        else:
            target_qty[stock] = None

    for stock in sorted(s for s in holdings_before if s not in target_stocks):
        q = holdings_before[stock]
        px = _ref_price(stock)
        _append_row(stock, q, 0, '卖出', px, -q, '调出')
        sty = _order_style(stock)
        if sty is not None:
            order_target(stock, 0, sty)
        else:
            order_target(stock, 0)

    for stock in target_stocks:
        cur = holdings_before.get(stock, 0)
        px = _ref_price(stock)
        tgt = target_qty.get(stock)

        if tgt is None:
            _append_row(stock, cur, '', '不变', px, '', '无参考价')
            continue

        if tgt > 0:
            if tgt > cur:
                action = '买入'
            elif tgt < cur:
                action = '卖出'
            else:
                action = '不变'
            delta = tgt - cur
            note = '新进' if cur == 0 and tgt > 0 else ''
            _append_row(stock, cur, tgt, action, px, delta, note)
            if tgt != cur:
                sty = _order_style(stock)
                if sty is not None:
                    order_target(stock, tgt, sty)
                else:
                    order_target(stock, tgt)
        else:
            if cur > 0:
                _append_row(stock, cur, 0, '卖出', px, -cur, '不足一手')
                sty = _order_style(stock)
                if sty is not None:
                    order_target(stock, 0, sty)
                else:
                    order_target(stock, 0)
            else:
                _append_row(stock, 0, 0, '不变', px, 0, '不足一手')

    _emit_rebalance_summary()