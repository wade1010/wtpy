import pandas as pd
from tools.datetime_utils import DatetimeUtils
from wtpy import BaseCtaStrategy
from wtpy import CtaContext
import numpy as np
from wtpy.CodeHelper import CodeHelper
from wtpy.WtDataDefs import WtNpKline
from wtpy.wrapper import WtDataHelper


class StraTrendFlow(BaseCtaStrategy):

    def __init__(self, name: str, code: str, barCnt: int, period: str, days: int, k1: float, k2: float, isForStk: bool = False):
        BaseCtaStrategy.__init__(self, name)

        self.__days__ = days
        self.__k1__ = k1
        self.__k2__ = k2

        self.__period__ = period
        self.__bar_cnt__ = barCnt
        self.__code__ = code

        self.__is_stk__ = isForStk
        self.week_kline: WtNpKline | None = None

    def on_init(self, context: CtaContext):
        code = self.__code__  # 品种代码
        if self.__is_stk__:
            code = code + "-"  # 如果是股票代码，后面加上一个+/-，+表示后复权，-表示前复权

        # 这里演示了品种信息获取的接口
        # pInfo = context.stra_get_comminfo(code)
        # print(pInfo)

        context.stra_prepare_bars(code, self.__period__, self.__bar_cnt__, isMain=True)
        context.stra_sub_ticks(code)
        context.stra_log_text("DualThrust inited")

        # 读取存储的数据
        self.xxx = context.user_load_data('xxx', 1)

        # 加载周线
        dtHelper = WtDataHelper()  # type:WtDataHelper

        comm_id = CodeHelper.stdCodeToStdCommID(code)
        self.week_kline = dtHelper.read_dsb_bars(f'../storage/his/week/{comm_id}_HOT.dsb')  # type:WtNpKline
        date_obj = pd.to_datetime(self.week_kline.bartimes[-1], format="%Y%m%d%H%M%S", errors="coerce")
        print(f"周线长度为 {len(self.week_kline)} ，最近一根K线日期为：{date_obj.strftime('%Y-%m-%d')}")

    def on_tick(self, context: CtaContext, stdCode: str, newTick: dict):
        # print(newTick)
        pass

    def on_calculate(self, context: CtaContext):
        now = DatetimeUtils.wt_make_time(context.stra_get_date(), context.stra_get_time())
        code = self.__code__  # 品种代码

        trdUnit = 1
        if self.__is_stk__:
            trdUnit = 100

        # 读取最近50条1分钟线(dataframe对象)
        theCode = code
        if self.__is_stk__:
            theCode = theCode + "-"  # 如果是股票代码，后面加上一个+/-，+表示后复权，-表示前复权

        print(now, self.__period__, context.stra_get_price(theCode))

        np_bars = context.stra_get_bars(theCode, self.__period__, self.__bar_cnt__, isMain=True)
        # isMain只有一个，确定后就不能修改，这个再传True，返回空
        # np_bars2 = context.stra_get_bars(theCode, 'd2', self.__bar_cnt__, isMain=True)

        # 把策略参数读进来，作为临时变量，方便引用
        days = self.__days__
        k1 = self.__k1__
        k2 = self.__k2__

        # 平仓价序列、最高价序列、最低价序列
        closes = np_bars.closes
        highs = np_bars.highs
        lows = np_bars.lows

        # 读取days天之前到上一个交易日位置的数据
        hh = np.amax(highs[-days:-1])
        hc = np.amax(closes[-days:-1])
        ll = np.amin(lows[-days:-1])
        lc = np.amin(closes[-days:-1])

        # 读取今天的开盘价、最高价和最低价
        # lastBar = df_bars.get_last_bar()
        openpx = np_bars.opens[-1]
        highpx = np_bars.highs[-1]
        lowpx = np_bars.lows[-1]

        '''
        !!!!!这里是重点
        1、首先根据最后一条K线的时间，计算当前的日期
        2、根据当前的日期，对日线进行切片,并截取所需条数
        3、最后在最终切片内计算所需数据
        '''

        # 确定上轨和下轨
        upper_bound = openpx + k1 * max(hh - lc, hc - ll)
        lower_bound = openpx - k2 * max(hh - lc, hc - ll)

        # 读取当前仓位
        curPos = context.stra_get_position(code) / trdUnit

        if curPos == 0:
            if highpx >= upper_bound:
                context.stra_enter_long(code, 1 * trdUnit, 'enterlong')
                # context.stra_log_text(f"向上突破{highpx:.2f}>={upper_bound:.2f}，多仓进场")
                # 修改并保存
                self.xxx = 1
                context.user_save_data('xxx', self.xxx)
                return

            if lowpx <= lower_bound and not self.__is_stk__:
                context.stra_enter_short(code, 1 * trdUnit, 'entershort')
                # context.stra_log_text(f"向下突破{lowpx:.2f}<={lower_bound:.2f}，空仓进场")
                return
        elif curPos > 0:
            if lowpx <= lower_bound:
                context.stra_exit_long(code, 1 * trdUnit, 'exitlong')
                # context.stra_log_text(f"向下突破{lowpx:.2f}<={lower_bound:.2f}，多仓出场")
                # raise Exception("except on purpose")
                return
        else:
            if highpx >= upper_bound and not self.__is_stk__:
                context.stra_exit_short(code, 1 * trdUnit, 'exitshort')
                # context.stra_log_text(f"向上突破{highpx:.2f}>={upper_bound:.2f}，空仓出场")
                return
