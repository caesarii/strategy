#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
微盘股“双低”轮动策略（聚宽 JoinQuant 版）

参考 `joinquant/微盘/micro_cap_base.py` 的策略骨架：
- 股票池过滤：排除科创板、创业板、ST/*ST、退市整理股；并限制 size 因子最小 N；成交额/价格/涨跌停过滤
- 调仓：每周三收盘后等权调仓；限价单参考调仓日收盘价（不复权）

本文件实现“双低”的因子为：
- 市值因子：`size`（越小越好）
- 流动性因子：`liquidity`（越小越好）
"""

import csv
from io import StringIO

from jqdata import *
from jqfactor import get_factor_values  # type: ignore


def initialize(context):
    set_option('use_real_price', True)
    set_benchmark('000300.XSHG')
    log.set_level('order', 'error')

    g.hold_num = 15
    g.initial_capital = 30000
    g.verbose_log = False

    set_order_cost(
        OrderCost(
            open_tax=0,
            close_tax=0.001,
            open_commission=0.0002,
            close_commission=0.0002,
            min_commission=0,
        ),
        type='stock',
    )
    set_slippage(PriceRelatedSlippage(0.005), type='stock')

    run_weekly(
        weekly_rebalance,
        weekday=2,  # 周三
        time='close',
        reference_security='000300.XSHG',
    )


def weekly_rebalance(context):
    stock_pool = get_stock_pool(context)
    if not stock_pool:
        if g.verbose_log:
            log.warn('当日无符合条件的股票，跳过调仓')
        return

    df_stocks = get_stock_metrics(stock_pool, context)
    if df_stocks is None or len(df_stocks) < g.hold_num:
        if g.verbose_log:
            log.warn('可选股票数量不足，跳过调仓')
        return

    df_stocks = df_stocks.dropna(subset=['size', 'liquidity'])
    if len(df_stocks) < g.hold_num:
        if g.verbose_log:
            log.warn('有效市值/流动性数据不足，跳过调仓')
        return

    # 双低：size 越小越好、liquidity 越小越好
    rk_cap = df_stocks['size'].rank(method='average', ascending=True)
    rk_liq = df_stocks['liquidity'].rank(method='average', ascending=True)
    sync = _SYNC_RANK_W_CAP * rk_cap + _SYNC_RANK_W_LIQ * rk_liq
    df_stocks = df_stocks.assign(_sync_score=sync).sort_values('_sync_score', ascending=True)

    target_stocks = list(df_stocks.head(g.hold_num)['symbol'])
    rebalance_portfolio(context, target_stocks)


def after_trading_end(context):
    pass


# 与 `micro_cap_base.py` 对齐的过滤条件
_MIN_DAILY_MONEY = 1e7  # 0.1 亿
_PREV_CLOSE_MIN = 1.0
_PREV_CLOSE_MAX = 30.0
_CAP_SMALLEST_N = 400
_FUND_BATCH = 800

# 同步排名权重
_SYNC_RANK_W_CAP = 1.0
_SYNC_RANK_W_LIQ = 1.0

def _rebalance_day_close_money_unadjusted(code):
    hist = attribute_history(
        code,
        1,
        '1d',
        ['close', 'money'],
        skip_paused=True,
        df=True,
        fq='none',
    )
    if hist is None or hist.empty:
        return None, None
    row = hist.iloc[-1]
    try:
        return float(row['close']), float(row['money'])
    except (TypeError, ValueError, KeyError):
        return None, None


def get_stock_pool(context):
    dt = context.current_dt
    all_stocks = get_all_securities('stock', dt.date()).index.tolist()
    filtered_stocks = []

    min_list_days = 120
    for stock in all_stocks:
        if stock.startswith('688'):
            continue
        if stock.startswith('300') or stock.startswith('301'):
            continue
        if stock.startswith('8') or stock.startswith('920'):
            continue

        info = get_security_info(stock)
        name = info.display_name
        if name is not None and ('ST' in name or '*ST' in name):
            continue
        if name is not None and ('退市' in name or '退' in name or stock.endswith('.RT')):
            continue

        if (dt.date() - info.start_date).days < min_list_days:
            continue

        filtered_stocks.append(stock)

    if not filtered_stocks:
        if g.verbose_log:
            log.warn('基础过滤后股票池为空')
        return []

    # 纯因子：size 最小 N（基础池内按 size 升序取前 N）
    size_pairs = []
    for i in range(0, len(filtered_stocks), _FUND_BATCH):
        chunk = filtered_stocks[i : i + _FUND_BATCH]
        fv = get_factor_values(chunk, ['size'], end_date=dt.date(), count=1)
        size_df = fv.get('size')
        if size_df is None or size_df.empty:
            continue
        row = size_df.iloc[-1]
        for code, v in row.to_dict().items():
            try:
                s = float(v)
            except (TypeError, ValueError):
                continue
            size_pairs.append((code, s))
    if not size_pairs:
        if g.verbose_log:
            log.warn('无法取得 size 因子数据，股票池为空')
        return []

    size_pairs.sort(key=lambda x: x[1])
    cap_candidates = [p[0] for p in size_pairs[:_CAP_SMALLEST_N]]

    cd = get_current_data()
    result = []
    for stock in cap_candidates:
        try:
            bar = cd[stock]
            if bar.paused or bar.last_price <= 0:
                continue

            day_close, day_money = _rebalance_day_close_money_unadjusted(stock)
            if day_close is None or day_money is None:
                continue
            if day_money < _MIN_DAILY_MONEY:
                continue
            if not (_PREV_CLOSE_MIN < day_close < _PREV_CLOSE_MAX):
                continue

            # 调仓时刻过滤涨跌停
            if bar.high_limit > 0 and bar.last_price >= bar.high_limit - 1e-8:
                continue
            if bar.low_limit > 0 and bar.last_price <= bar.low_limit + 1e-8:
                continue

            result.append(stock)
        except Exception:
            continue

    if g.verbose_log:
        log.info(
            '股票池：基础{}只 → size最小{}候选{}只 → 成交额/价/涨跌停后{}只'.format(
                len(filtered_stocks),
                _CAP_SMALLEST_N,
                len(cap_candidates),
                len(result),
            )
        )
    return result


def get_stock_metrics(stock_list, context):
    import pandas as pd

    dt = context.current_dt.date()
    factor_names = ['size', 'liquidity']
    fv = get_factor_values(stock_list, factor_names, end_date=dt, count=1)
    size_df = fv.get('size')
    liq_df = fv.get('liquidity')
    if size_df is None or liq_df is None or size_df.empty or liq_df.empty:
        return None

    data_list = []
    for stock in stock_list:
        size_v = size_df.iloc[-1].get(stock)
        liq_v = liq_df.iloc[-1].get(stock)
        if size_v is None or liq_v is None:
            continue
        data_list.append({'symbol': stock, 'size': float(size_v), 'liquidity': float(liq_v)})

    if not data_list:
        return None
    return pd.DataFrame(data_list)


def rebalance_portfolio(context, target_stocks):
    holdings_before = {}
    for s in context.portfolio.positions:
        amt = int(context.portfolio.positions[s].total_amount)
        if amt > 0:
            holdings_before[s] = amt

    cdpx = get_current_data()
    summary_rows = []

    def _stock_display(code):
        inf = get_security_info(code)
        return inf.display_name if inf and getattr(inf, 'display_name', None) else code

    def _ref_price(code):
        p, _ = _rebalance_day_close_money_unadjusted(code)
        p = p if p is not None else 0.0
        if p > 0:
            return p
        bar = cdpx[code]
        p = float(bar.last_price) if bar and bar.last_price and bar.last_price > 0 else 0.0
        if p <= 0:
            h1 = attribute_history(code, 1, '1d', ['close'], skip_paused=True, df=True, fq='pre')
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
        w.writerow(['调仓时间', '代码', '名称', '当前股数', '目标股数', '本次操作', '参考价', '变动股数', '备注'])
        for row in summary_rows:
            w.writerow([ts] + row)
        log.info('持仓明细CSV\n{}'.format(buf.getvalue().rstrip('\n')))

    if not target_stocks:
        for stock in sorted(holdings_before.keys()):
            q = holdings_before[stock]
            px = _ref_price(stock)
            _append_row(stock, q, 0, '卖出', px, -q, '清仓')
            sty = _order_style(stock)
            order_target(stock, 0, sty) if sty is not None else order_target(stock, 0)
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

    # 卖出：不在目标池的持仓
    for stock in sorted(s for s in holdings_before if s not in target_stocks):
        q = holdings_before[stock]
        px = _ref_price(stock)
        _append_row(stock, q, 0, '卖出', px, -q, '调出')
        sty = _order_style(stock)
        order_target(stock, 0, sty) if sty is not None else order_target(stock, 0)

    # 买入/调整：目标池
    for stock in target_stocks:
        cur = holdings_before.get(stock, 0)
        px = _ref_price(stock)
        tgt = target_qty.get(stock)

        if tgt is None:
            _append_row(stock, cur, '', '不变', px, '', '无参考价')
            continue

        if tgt > 0:
            action = '买入' if tgt > cur else ('卖出' if tgt < cur else '不变')
            delta = tgt - cur
            note = '新进' if cur == 0 and tgt > 0 else ''
            _append_row(stock, cur, tgt, action, px, delta, note)
            if tgt != cur:
                sty = _order_style(stock)
                order_target(stock, tgt, sty) if sty is not None else order_target(stock, tgt)
        else:
            if cur > 0:
                _append_row(stock, cur, 0, '卖出', px, -cur, '不足一手')
                sty = _order_style(stock)
                order_target(stock, 0, sty) if sty is not None else order_target(stock, 0)
            else:
                _append_row(stock, 0, 0, '不变', px, 0, '不足一手')

    _emit_rebalance_summary()
