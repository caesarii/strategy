# 导入函数库
from jqdata import *


class WyckoffSpringDetector:
    """威科夫 Spring 假突破信号识别器"""

    def __init__(self, config=None):
        config = config or {}
        self.lookback_period = config.get('lookback_period', 20)
        self.recovery_period = config.get('recovery_period', 3)
        self.volume_threshold = config.get('volume_threshold', 1.2)

    def analyze(self, candles):
        if not candles or len(candles) < self.lookback_period + self.recovery_period:
            return {'error': 'K线数据不足，至少需要 %d 根K线' % (self.lookback_period + self.recovery_period)}

        current_candle = candles[-1]
        start_idx = -(self.lookback_period + self.recovery_period)
        end_idx = -self.recovery_period
        previous_candles = candles[start_idx:end_idx]

        support_level = min(c['low'] for c in previous_candles)

        test_range = candles[-self.recovery_period:]
        has_broken_support = False
        break_index = -1

        for idx, candle in enumerate(test_range):
            if candle['low'] < support_level:
                has_broken_support = True
                break_index = idx
                break

        is_recovered = current_candle['close'] > support_level

        if has_broken_support and is_recovered:
            break_candle = test_range[break_index]
            avg_volume = self._calculate_avg_volume(candles[-20:-self.recovery_period])

            is_low_volume_spring = break_candle.get('volume', 0) < avg_volume
            signal_type = "Type 2 (Low Supply)" if is_low_volume_spring else "Type 1 (Shakeout)"

            return {
                'symbol': current_candle.get('symbol', 'Unknown'),
                'type': 'WYCKOFF_SPRING',
                'classification': signal_type,
                'supportLevel': self._format_price(support_level),
                'breakPrice': self._format_price(break_candle['low']),
                'recoveryPrice': self._format_price(current_candle['close']),
                'breakVolume': break_candle.get('volume', 0),
                'avgVolume': round(avg_volume),
                'volumeRatio': "%.2f" % (break_candle.get('volume', 0) / avg_volume) if avg_volume > 0 else "0.00",
                'confidence': 'High' if is_low_volume_spring else 'Medium',
                'timestamp': current_candle.get('timestamp', 0),
            }

        return None

    def _calculate_avg_volume(self, candles):
        if not candles:
            return 0
        return sum(c.get('volume', 0) for c in candles) / len(candles)

    def _format_price(self, price):
        if isinstance(price, (int, float)):
            return round(price * 100) / 100
        return price


def joinquant_bars_to_candles(bars, symbol):
    """将 get_bars 返回的结构化数组转为 WyckoffSpringDetector.analyze 所需 K 线列表。"""
    if bars is None or len(bars) == 0:
        return []
    names = bars.dtype.names or ()
    candles = []
    for i in range(len(bars)):
        ts = 0
        if 'date' in names:
            d = bars['date'][i]
            if hasattr(d, 'strftime'):
                ts = d.strftime('%Y-%m-%d')
            elif hasattr(d, 'timestamp'):
                ts = int(d.timestamp())
        row = {
            'open': float(bars['open'][i]),
            'high': float(bars['high'][i]),
            'low': float(bars['low'][i]),
            'close': float(bars['close'][i]),
            'volume': float(bars['volume'][i]),
            'timestamp': ts,
            'symbol': symbol,
        }
        candles.append(row)
    return candles


def detect_spring_on_contract(security, lookback_period=20, recovery_period=3):
    need = lookback_period + recovery_period + 5
    bars = get_bars(
        security,
        count=need,
        unit='1d',
        fields=['date', 'open', 'high', 'low', 'close', 'volume'],
        include_now=False,
    )
    candles = joinquant_bars_to_candles(bars, security)
    detector = WyckoffSpringDetector({
        'lookback_period': lookback_period,
        'recovery_period': recovery_period,
    })
    return detector.analyze(candles)

## 初始化函数，设定基准等等
def initialize(context):
    # 设定沪深300作为基准
    set_benchmark('000300.XSHG')
    # 开启动态复权模式(真实价格)
    set_option('use_real_price', True)
    # 过滤掉order系列API产生的比error级别低的log
    # log.set_level('order', 'error')
    # 输出内容到日志 log.info()
    log.info('初始函数开始运行且全局只运行一次')
    g.last_spring_signal = None  # Wyckoff Spring 最近一次检测结果（market_open 更新）

    ### 期货相关设定 ###
    # 设定账户为金融账户
    set_subportfolios([SubPortfolioConfig(cash=context.portfolio.starting_cash, type='index_futures')])
    # 期货类每笔交易时的手续费是：买入时万分之0.23,卖出时万分之0.23,平今仓为万分之23
    set_order_cost(OrderCost(open_commission=0.000023, close_commission=0.000023,close_today_commission=0.0023), type='index_futures')
    # 设定保证金比例
    set_option('futures_margin_rate', 0.15)

    # 设置期货交易的滑点
    set_slippage(StepRelatedSlippage(2))
    # 运行函数（reference_security为运行时间的参考标的；传入的标的只做种类区分，因此传入'IF8888.CCFX'或'IH1602.CCFX'是一样的）
    # 注意：before_open/open/close/after_close等相对时间不可用于有夜盘的交易品种，有夜盘的交易品种请指定绝对时间（如9：30）
      # 开盘前运行
    run_daily( before_market_open, time='09:00', reference_security='IF8888.CCFX')
      # 开盘时运行
    run_daily( market_open, time='09:30', reference_security='IF8888.CCFX')
      # 收盘后运行
    run_daily( after_market_close, time='15:30', reference_security='IF8888.CCFX')


## 开盘前运行函数
def before_market_open(context):
    # 输出运行时间
    log.info('函数运行时间(before_market_open)：'+str(context.current_dt.time()))

    # 给微信发送消息（添加模拟交易，并绑定微信生效）
    # send_message('美好的一天~')

    ## 获取要操作的股票(g.为全局变量)
      # 获取当月沪深300指数期货合约
    g.IF_current_month = get_future_contracts('IF')[0]
      # 获取下季沪深300指数期货合约
    g.IF_next_quarter = get_future_contracts('IF')[2]

## 开盘时运行函数
def market_open(context):
    log.info('函数运行时间(market_open):'+str(context.current_dt.time()))

    ## 交易

    # 当月合约
    IF_current_month = g.IF_current_month

    # Wyckoff Spring 信号（对当月连续合约日线，仅记录；可与下方价差逻辑组合使用）
    spring_result = detect_spring_on_contract(IF_current_month)
    if spring_result and 'error' in spring_result:
        log.warning('Spring 检测: %s' % spring_result['error'])
    elif spring_result:
        log.info(
            'Spring | %s | %s | 支撑=%s 收回=%s 置信=%s'
            % (
                spring_result.get('classification'),
                IF_current_month,
                spring_result.get('supportLevel'),
                spring_result.get('recoveryPrice'),
                spring_result.get('confidence'),
            )
        )
        g.last_spring_signal = spring_result
    else:
        g.last_spring_signal = None
    # 下季合约
    IF_next_quarter = g.IF_next_quarter

    # 合约列表
    # 当月合约价格
    IF_current_month_close = get_bars(IF_current_month, count=1, unit='1d', fields=['close'])["close"]
    # 下季合约价格
    # IF_next_quarter_close = hist[IF_next_quarter][0]
    IF_next_quarter_close = get_bars(IF_next_quarter, count=1, unit='1d', fields=['close'])["close"]
    print(IF_current_month_close)
    print(IF_next_quarter_close)
    # 计算差值
    CZ = IF_current_month_close - IF_next_quarter_close

    # 获取当月合约交割日期
    end_data = get_CCFX_end_date(IF_current_month)

    # 判断差值大于80，且空仓，则做空当月合约、做多下季合约；当月合约交割日当天不开仓
    if (CZ > 80):
        if (context.current_dt.date() == end_data):
            # return
            pass
        else:
            if (len(context.portfolio.short_positions) == 0) and (len(context.portfolio.long_positions) == 0):
                log.info('开仓---差值：', CZ)
                # 做空1手当月合约
                order(IF_current_month, 1, side='short')
                # 做多1手下季合约
                order(IF_next_quarter, 1, side='long')
    # 如有持仓，并且基差缩小至70内，则平仓
    if (CZ < 70):
        if(len(context.portfolio.short_positions) > 0) and (len(context.portfolio.long_positions) > 0):
            log.info('平仓---差值：', CZ)
            # 平仓当月合约
            order_target(IF_current_month, 0, side='short')
            # 平仓下季合约
            order_target(IF_next_quarter, 0, side='long')

## 收盘后运行函数
def after_market_close(context):
    log.info(str('函数运行时间(after_market_close):'+str(context.current_dt.time())))
    # 得到当天所有成交记录
    trades = get_trades()
    for _trade in trades.values():
        log.info('成交记录：'+str(_trade))
    log.info('一天结束')
    log.info('##############################################################')

########################## 获取期货合约信息，请保留 #################################
# 获取金融期货合约到期日
def get_CCFX_end_date(future_code):
    # 获取金融期货合约到期日
    return get_security_info(future_code).end_date


########################## 自动移仓换月函数 #################################
def position_auto_switch(context,pindex=0,switch_func=None, callback=None):
    """
    期货自动移仓换月。默认使用市价单进行开平仓。
    :param context: 上下文对象
    :param pindex: 子仓对象
    :param switch_func: 用户自定义的移仓换月函数.
        函数原型必须满足：func(context, pindex, previous_dominant_future_position, current_dominant_future_symbol)
    :param callback: 移仓换月完成后的回调函数。
        函数原型必须满足：func(context, pindex, previous_dominant_future_position, current_dominant_future_symbol)
    :return: 发生移仓换月的标的。类型为列表。
    """
    import re
    subportfolio = context.subportfolios[pindex]
    symbols = set(subportfolio.long_positions.keys()) | set(subportfolio.short_positions.keys())
    switch_result = []
    for symbol in symbols:
        match = re.match(r"(?P<underlying_symbol>[A-Z]{1,})", symbol)
        if not match:
            raise ValueError("未知期货标的：{}".format(symbol))
        else:
            dominant = get_dominant_future(match.groupdict()["underlying_symbol"])
            cur = get_current_data()
            symbol_last_price = cur[symbol].last_price
            dominant_last_price = cur[dominant].last_price
            if dominant > symbol:
                for positions_ in (subportfolio.long_positions, subportfolio.short_positions):
                    if symbol not in positions_.keys():
                        continue
                    else :
                        p = positions_[symbol]

                    if switch_func is not None:
                        switch_func(context, pindex, p, dominant)
                    else:
                        amount = p.total_amount
                        # 跌停不能开空和平多，涨停不能开多和平空。
                        if p.side == "long":
                            symbol_low_limit = cur[symbol].low_limit
                            dominant_high_limit = cur[dominant].high_limit
                            if symbol_last_price <= symbol_low_limit:
                                log.warning("标的{}跌停，无法平仓。移仓换月取消。".format(symbol))
                                continue
                            elif dominant_last_price >= dominant_high_limit:
                                log.warning("标的{}涨停，无法开仓。移仓换月取消。".format(symbol))
                                continue
                            else:
                                log.info("进行移仓换月：({0},long) -> ({1},long)".format(symbol, dominant))
                                order_target(symbol,0,side='long')
                                order_target(dominant,amount,side='long')
                                switch_result.append({"before": symbol, "after":dominant, "side": "long"})
                            if callback:
                                callback(context, pindex, p, dominant)
                        if p.side == "short":
                            symbol_high_limit = cur[symbol].high_limit
                            dominant_low_limit = cur[dominant].low_limit
                            if symbol_last_price >= symbol_high_limit:
                                log.warning("标的{}涨停，无法平仓。移仓换月取消。".format(symbol))
                                continue
                            elif dominant_last_price <= dominant_low_limit:
                                log.warning("标的{}跌停，无法开仓。移仓换月取消。".format(symbol))
                                continue
                            else:
                                log.info("进行移仓换月：({0},short) -> ({1},short)".format(symbol, dominant))
                                order_target(symbol,0,side='short')
                                order_target(dominant,amount,side='short')
                                switch_result.append({"before": symbol, "after": dominant, "side": "short"})
                                if callback:
                                    callback(context, pindex, p, dominant)
    return switch_result