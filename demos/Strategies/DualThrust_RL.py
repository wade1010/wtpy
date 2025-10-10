from wtpy import BaseCtaStrategy
from wtpy import CtaContext
import numpy as np


class StraDualThrustRL(BaseCtaStrategy):
    """
    实盘测试使用
    """

    def __init__(self, name: str, code: str, barCnt: int, period: str, days: int, k1: float, k2: float, isForStk: bool = False):
        BaseCtaStrategy.__init__(self, name)

        self.__days__ = days
        self.__k1__ = k1
        self.__k2__ = k2

        self.__period__ = period
        self.__bar_cnt__ = barCnt
        self.__code__ = code

        self.__is_stk__ = isForStk

    def on_init(self, context: CtaContext):
        code = self.__code__  # 品种代码
        if self.__is_stk__:
            code = code + "-"  # 如果是股票代码，后面加上一个+/-，+表示后复权，-表示前复权
        context.stra_prepare_bars(code, self.__period__, self.__bar_cnt__, isMain=True)
        context.stra_sub_ticks(code)
        context.stra_log_text("DualThrust inited")
        # 读取存储的数据
        self.xxx = context.user_load_data('xxx', 1)

    def on_tick(self, context: CtaContext, stdCode: str, newTick: dict):
        # 配置需要打印的字段和对应的中文名称
        print_config = {
            'price': '最新价',
            # 'action_date': '行情日期',
            'action_time': '行情时间',
            # 'open': '开盘价',
            # 'high': '最高价',
            # 'low': '最低价',
            # 'upper_limit': '涨停价',
            # 'lower_limit': '跌停价',
            # 'pre_close': '昨收盘',
            # 'pre_settle': '昨结算',
            'total_volume': '总成交量',
            'volume': '瞬时成交量',
            'total_turnover': '总成交额',
            'turn_over': '瞬时成交额',
            'open_interest': '持仓量',
            'diff_interest': '持仓变化',
            'bid_price_0': '买一价',
            'bid_qty_0': '买一量',
            'ask_price_0': '卖一价',
            'ask_qty_0': '卖一量'
        }

        # 构建日志消息
        log_parts = []
        for key, name in print_config.items():
            if key in newTick:
                value = newTick[key]
                # 对特定字段进行格式化处理
                if key in ['action_date', 'trading_date']:
                    value = str(value)
                elif key == 'action_time':
                    value = f"{str(value)[:2]}:{str(value)[2:4]}:{str(value)[4:6]}"
                elif key in ['total_turnover', 'turn_over']:
                    value = f"{value / 10000:.2f}万"  # 转换为万元单位
                log_parts.append(f"{name}:{value}")

        # 打印日志
        print(f"Tick行情 [{newTick.get('code').decode('utf-8')}] {' | '.join(log_parts)}")

    def on_calculate(self, context: CtaContext):
        print(f"on_calculate {context.stra_get_time()},{context.stra_get_price(self.__code__)}")
