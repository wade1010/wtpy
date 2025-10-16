import pandas as pd
from datetime import datetime, timedelta
from tools.datetime_utils import DatetimeUtils
from wtpy import BaseCtaStrategy
from wtpy import CtaContext
import numpy as np
from wtpy.CodeHelper import CodeHelper
from wtpy.WtDataDefs import WtNpKline
from wtpy.wrapper import WtDataHelper


class StraTrendFlow(BaseCtaStrategy):

    def __init__(self, name: str, code: str, barCnt: int, period: str = "m3", profit_target: float = 0.02, stop_loss: float = 0.01):
        """
        多时间框架趋势策略
        @name: 策略名称
        @code: 交易品种代码
        @barCnt: K线数量
        @period: 主要周期，默认3分钟
        @profit_target: 止盈比例，默认2%
        @stop_loss: 止损比例，默认1%
        """
        BaseCtaStrategy.__init__(self, name)

        self.__period__ = period
        self.__bar_cnt__ = barCnt
        self.__code__ = code
        self.__profit_target__ = profit_target
        self.__stop_loss__ = stop_loss

        # 存储不同时间框架的K线数据
        self.week_kline: WtNpKline | None = None

        # 记录开仓价格用于止盈止损
        self.__entry_price__ = 0.0
        self.__last_signal__ = 0  # 1为多头信号，-1为空头信号，0为无信号

    def on_init(self, context: CtaContext):
        code = self.__code__  # 品种代码
        # 准备不同时间框架的K线数据
        context.stra_prepare_bars(code, self.__period__, self.__bar_cnt__, isMain=True)  # 3分钟K线
        # isMain只有一个，确定后就不能修改，这个再传True，返回空
        # np_bars2 = context.stra_get_bars(theCode, 'd2', self.__bar_cnt__, isMain=True)
        context.stra_prepare_bars(code, "d1", self.__bar_cnt__, isMain=False)  # 日线
        context.stra_sub_ticks(code)
        # context.stra_sub_ticks(code) # 会触发on_tick
        # context.stra_sub_bar_events(code, self.__period__)# 这个有作用
        # context.stra_sub_bar_events(code, 'd1') #这个不起作用
        context.stra_log_text("多时间框架趋势策略已初始化")

        # 加载周线数据
        dtHelper = WtDataHelper()
        comm_id = CodeHelper.stdCodeToStdCommID(code)
        try:
            self.week_kline = dtHelper.read_dsb_bars(f'../storage/his/week/{comm_id}_HOT.dsb')
            if self.week_kline is not None and len(self.week_kline) > 0:
                date_obj = pd.to_datetime(self.week_kline.bartimes[-1], format="%Y%m%d%H%M%S", errors="coerce")
                context.stra_log_text(f"周线数据加载成功，长度: {len(self.week_kline)}, 最新日期: {date_obj.strftime('%Y-%m-%d')}")
                # 获取当前时间
                now = datetime.now()
                current_weekday = now.weekday()  # 0=周一, 1=周二, ..., 6=周日
                # 判断当前是否为周末（周六=5, 周日=6）
                is_weekend = current_weekday >= 5
                if is_weekend:
                    # 如果是周末，判断date_obj是不是本周一
                    # 计算本周一的日期
                    days_since_monday = current_weekday
                    kline_week_dt = now - timedelta(days=days_since_monday)
                else:
                    # 如果不是周末，判断date_obj是不是上周一
                    # 计算上周一的日期
                    days_since_monday = current_weekday
                    kline_week_dt = now - timedelta(days=days_since_monday + 7)
                if date_obj.date() != kline_week_dt.date():
                    raise RuntimeError(f"DSB文件的最后一根周线日期为 {date_obj.date()}，需要的周线日期 {kline_week_dt.date()}，所以需要更新周线")
            else:
                context.stra_log_text("周线数据加载失败或为空")
        except Exception as e:
            context.stra_log_text(f"周线数据加载异常: {str(e)}")
            self.week_kline = None

    def on_tick(self, context: CtaContext, stdCode: str, newTick: dict):
        pass

    def calculate_ma(self, closes: np.ndarray, period: int) -> float:
        """
        计算移动平均线
        @closes: 收盘价数组
        @period: 周期
        @return: 移动平均线值
        """
        if len(closes) < period:
            return 0.0
        return float(np.mean(closes[-period:]))

    def check_weekly_trend(self) -> bool:
        """
        检查周线趋势：5、10、20均线都在上涨
        @return: True表示周线趋势向上
        """
        if self.week_kline is None or len(self.week_kline.closes) < 20:
            return False

        closes = self.week_kline.closes

        # 计算当前和前一根K线的均线值
        ma5_current = self.calculate_ma(closes, 5)
        ma10_current = self.calculate_ma(closes, 10)
        ma20_current = self.calculate_ma(closes, 20)

        ma5_previous = self.calculate_ma(closes[:-1], 5)
        ma10_previous = self.calculate_ma(closes[:-1], 10)
        ma20_previous = self.calculate_ma(closes[:-1], 20)

        # 检查所有均线都在上涨
        return (ma5_current > ma5_previous and
                ma10_current > ma10_previous and
                ma20_current > ma20_previous)

    def check_daily_trend(self, daily_closes: np.ndarray) -> bool:
        """
        检查日线趋势：10均线在上涨
        @daily_closes: 日线收盘价数组
        @return: True表示日线趋势向上
        """
        if len(daily_closes) < 10:
            return False

        ma10_current = self.calculate_ma(daily_closes, 10)
        ma10_previous = self.calculate_ma(daily_closes[:-1], 10)

        return ma10_current > ma10_previous

    def check_minute_signal(self, minute_closes: np.ndarray) -> int:
        """
        检查3分钟K线信号
        @minute_closes: 3分钟收盘价数组
        @return: 1为多头信号，-1为空头信号，0为无信号
        """
        if len(minute_closes) < 20:
            return 0

        ma5 = self.calculate_ma(minute_closes, 5)
        ma10 = self.calculate_ma(minute_closes, 10)
        ma20 = self.calculate_ma(minute_closes, 20)

        # 多头信号：5均线 > 10均线 > 20均线
        if ma5 > ma10 > ma20:
            return 1
        # 空头信号：5均线 < 10均线 < 20均线
        elif ma5 < ma10 < ma20:
            return -1
        else:
            return 0

    def check_profit_loss(self, current_price: float, position: float) -> bool:
        """
        检查止盈止损条件
        @current_price: 当前价格
        @position: 当前仓位
        @return: True表示需要平仓
        """
        if position == 0 or self.__entry_price__ == 0:
            return False

        # 计算盈亏比例
        if position > 0:  # 多头仓位
            profit_ratio = (current_price - self.__entry_price__) / self.__entry_price__
        else:  # 空头仓位
            profit_ratio = (self.__entry_price__ - current_price) / self.__entry_price__

        # 止盈或止损
        return profit_ratio >= self.__profit_target__ or profit_ratio <= -self.__stop_loss__

    def on_calculate(self, context: CtaContext):
        now = DatetimeUtils.wt_make_time(context.stra_get_date(), context.stra_get_time())
        code = self.__code__

        theCode = code
        # 获取当前价格
        current_price = context.stra_get_price(theCode)
        context.stra_log_text(f"当前K线时间为 {now}，价格为 {current_price}")
        if current_price <= 0:
            return

        # 获取3分钟K线数据
        minute_bars = context.stra_get_bars(theCode, self.__period__, self.__bar_cnt__, isMain=True)
        if minute_bars is None or len(minute_bars) < 20:
            return

        # 获取日线数据
        daily_bars = context.stra_get_bars(theCode, "d1", 50, isMain=False)
        if daily_bars is None or len(daily_bars.closes) < 10:
            return
        context.stra_log_text(f"当前K线时间为 {minute_bars.bartimes[-1]}，分钟级别：-3根close为{minute_bars.closes[-3]},-2根close为{minute_bars.closes[-2]},-1根close为{minute_bars.closes[-1]}|||"
                              f"日线级别：-3根close为{daily_bars.closes[-3]},-2根close为{daily_bars.closes[-2]},-1根close为{daily_bars.closes[-1]},-1根time为{daily_bars.bartimes[-1]}")
        # return
        # 获取当前仓位
        curPos = context.stra_get_position(code)

        # 检查止盈止损
        if curPos != 0 and self.check_profit_loss(current_price, curPos):
            if curPos > 0:
                context.stra_exit_long(code, abs(curPos), 'profit_stop_loss')
                context.stra_log_text(f"多头止盈止损平仓，价格: {current_price:.2f}, 开仓价: {self.__entry_price__:.2f}")
            else:
                context.stra_exit_short(code, abs(curPos), 'profit_stop_loss')
                context.stra_log_text(f"空头止盈止损平仓，价格: {current_price:.2f}, 开仓价: {self.__entry_price__:.2f}")

            self.__entry_price__ = 0.0
            self.__last_signal__ = 0
            return

        # 检查各时间框架的趋势条件
        weekly_bullish = self.check_weekly_trend()
        daily_bullish = self.check_daily_trend(daily_bars.closes)
        minute_signal = self.check_minute_signal(minute_bars.closes)

        # 记录信号状态（用于调试）
        context.stra_log_text(f"价格: {current_price:.2f}, 周线趋势: {weekly_bullish}, 日线趋势: {daily_bullish}, 3分钟信号: {minute_signal}, 当前仓位: {curPos}")

        # 开仓逻辑
        if curPos == 0:
            # 多头开仓条件：周线5/10/20均线上涨 + 日线10均线上涨 + 3分钟5>10>20
            if weekly_bullish and daily_bullish and minute_signal == 1:
                context.stra_enter_long(code, 1, 'multi_timeframe_long')
                self.__entry_price__ = current_price
                self.__last_signal__ = 1
                context.stra_log_text(f"多头开仓，价格: {current_price:.2f}")
                return

            # 空头开仓条件：所有条件相反
            if not weekly_bullish and not daily_bullish and minute_signal == -1:
                context.stra_enter_short(code, 1, 'multi_timeframe_short')
                self.__entry_price__ = current_price
                self.__last_signal__ = -1
                context.stra_log_text(f"空头开仓，价格: {current_price:.2f}")
                return

        # 平仓逻辑（信号反转时）
        elif curPos > 0:
            # 多头平仓：趋势转弱或出现空头信号
            if not weekly_bullish or not daily_bullish or minute_signal == -1:
                context.stra_exit_long(code, abs(curPos), 'trend_reversal')
                context.stra_log_text(f"多头趋势反转平仓，价格: {current_price:.2f}")
                self.__entry_price__ = 0.0
                self.__last_signal__ = 0
                return

        elif curPos < 0:
            # 空头平仓：趋势转强或出现多头信号
            if weekly_bullish or daily_bullish or minute_signal == 1:
                context.stra_exit_short(code, abs(curPos), 'trend_reversal')
                context.stra_log_text(f"空头趋势反转平仓，价格: {current_price:.2f}")
                self.__entry_price__ = 0.0
                self.__last_signal__ = 0
                return
