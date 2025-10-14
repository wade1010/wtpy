from wtpy.WtCoreDefs import WTSBarStruct, WTSTickStruct
from wtpy.wrapper import WtDataHelper
import pandas as pd
from tqdm import tqdm
import os
from tqsdk import TqApi, TqAuth
import datetime
from tqsdk import TqSim, TqBacktest

# 环境变量和配置管理
try:
    from dotenv import load_dotenv

    load_dotenv()  # 加载.env文件
except ImportError:
    print("未安装python-dotenv包，将直接从系统环境变量读取配置")


def get_tq_credentials():
    """从环境变量获取天勤账号信息"""
    username = os.getenv('TQ_USERNAME')
    password = os.getenv('TQ_PASSWORD')

    if not username or not password:
        raise ValueError(
            "未找到天勤账号信息！\n"
            "请设置环境变量 TQ_USERNAME 和 TQ_PASSWORD，\n"
            "或创建 .env 文件并填入账号信息。\n"
            "参考 env.example 文件格式。"
        )

    return username, password


class Ifeed(object):
    def __init__(self):
        self.dthelper = WtDataHelper()
        self.period_map = {"m1": "min1", "m5": "min5", "d": "day", "tick": "ticks"}
        self.frequency_map = {
            "m1": "1m",
            "m5": "5m",
            "d": "1d",
        }

    def get_tick(self, symbol, start_date=None, end_date=None):
        return

    def get_bar(self, symbol, frequency, start_date=None, end_date=None):
        return

    def parse_code(self, code):
        items = code.split(".")
        return items[0], items[1], items[2]

    def code_std(self, stdCode: str):
        stdCode = stdCode.upper()
        items = stdCode.split(".")
        exchg = self.exchgStdToRQ(items[0])
        if len(items) == 2:
            # 简单股票代码，格式如SSE.600000
            return items[1] + "." + exchg
        elif items[1] in ["IDX", "ETF", "STK", "OPT"]:
            # 标准股票代码，格式如SSE.IDX.000001
            return items[2] + "." + exchg
        elif len(items) == 3:
            # 标准期货代码，格式如CFFEX.IF.2103
            if items[2] != 'HOT':
                return ''.join(items[1:])
            else:
                return items[1] + "88"

    def cover_d_bar(self, df):
        count = len(df)
        BUFFER = WTSBarStruct * count
        buffer = BUFFER()
        for index, row in tqdm(df.iterrows()):
            curBar = buffer[index]
            curBar.date = int(row["date"])
            curBar.open = float(row["open"])
            curBar.high = float(row["high"])
            curBar.low = float(row["low"])
            curBar.close = float(row["close"])
            curBar.volume = float(row["volume"])
            curBar.turnover = float(row["turnover"])
            curBar.open_interest = float(row["open_interest"])
        return buffer

    def cover_m_bar(self, df):
        count = len(df)
        BUFFER = WTSBarStruct * count
        buffer = BUFFER()
        for index, row in tqdm(df.iterrows()):
            curBar = buffer[index]
            curBar.time = (int(row["date"]) - 19900000) * 10000 + int(row["time"])
            curBar.open = float(row["open"])
            curBar.high = float(row["high"])
            curBar.low = float(row["low"])
            curBar.close = float(row["close"])
            curBar.volume = float(row["volume"])
            curBar.turnover = float(row["turnover"])
            curBar.open_interest = float(row["open_interest"])
        return buffer

    def cover_tick(self, df):
        count = len(df)
        BUFFER = WTSTickStruct * count
        buffer = BUFFER()
        for index, row in tqdm(df.iterrows()):
            curTick = buffer[index]
            curTick.exchg = bytes(row["exchg"], 'utf-8')
            curTick.code = bytes(row["code"], 'utf-8')
            curTick.price = float(row["price"])
            curTick.open = float(row["open"])
            curTick.high = float(row["high"])
            curTick.low = float(row["low"])
            curTick.settle_price = float(row["settle_price"])
            curTick.total_volume = float(row["total_volume"])
            curTick.volume = float(row["volume"])
            curTick.total_turnover = float(row["total_turnover"])
            curTick.turn_over = float(row["turn_over"])
            curTick.open_interest = float(row["open_interest"])
            curTick.diff_interest = float(row["diff_interest"])
            curTick.trading_date = int(row["trading_date"])
            curTick.action_date = int(row["action_date"])
            curTick.action_time = int(int(row["action_time"]) / 1000)
            curTick.pre_close = float(row["pre_close"])
            curTick.pre_settle = float(row["pre_settle"])
            curTick.pre_interest = float(0.0)
            # 设置买卖盘数据 - 直接设置对应的字段而不是元组索引
            for x in range(0, 5):
                # 设置买价和买量
                setattr(curTick, f"bid_price_{x}", float(row["bid_" + str(x + 1)]))
                setattr(curTick, f"bid_qty_{x}", float(row["bid_qty_" + str(x + 1)]))
                # 设置卖价和卖量
                setattr(curTick, f"ask_price_{x}", float(row["ask_" + str(x + 1)]))
                setattr(curTick, f"ask_qty_{x}", float(row["ask_qty_" + str(x + 1)]))
        return buffer

    def bar_df_to_dsb(self, df, dsb_file, period):
        if "d" in period:
            buffer = self.cover_d_bar(df)
        elif "m" in period:
            buffer = self.cover_m_bar(df)
        self.dthelper.store_bars(barFile=dsb_file, firstBar=buffer, count=len(buffer), period=period)

    def tick_df_to_dsb(self, df, dsb_file):
        buffer = self.cover_tick(df)
        self.dthelper.store_ticks(tickFile=dsb_file, firstTick=buffer, count=len(buffer))

    # 新下的数据会覆盖旧的数据
    def store_bin_bar(self, storage_path, code, start_date=None, end_date=None, frequency="1m", col_map=None):
        df = self.get_bar(code, start_date, end_date, frequency)
        period = self.period_map[frequency]
        save_path = os.path.join(storage_path, "bin", period)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        dsb_path = os.path.join(save_path, f"{code}_{frequency}.dsb")
        self.bar_df_to_dsb(df, dsb_path, frequency)

    def store_bin_tick(self, storage_path, code, start_date=None, end_date=None, col_map=None):
        df = self.get_tick(code, start_date, end_date)
        save_path = os.path.join(storage_path, "bin", "ticks")
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        g = df.groupby("trading_date")
        for trading_date, g_df in g:
            g_df = g_df.reset_index()
            dsb_path = os.path.join(save_path, f"{code}_tick_{trading_date}.dsb")
            self.tick_df_to_dsb(g_df, dsb_path)

    # 除了转换为dsb格式，还会按照his的格式进行存储
    def store_his_bar(self, storage_path, code, start_date=None, end_date=None, frequency="1m", skip_saved=False):
        exchange, pid, month = self.parse_code(code)
        if frequency not in self.frequency_map.keys():
            print("周期只能为m1、m5或d,回测或实盘中会自动拼接")
        period = self.period_map[frequency]
        save_path = os.path.join(storage_path, "his", period, exchange)
        if not os.path.exists(save_path):
            os.makedirs(save_path)
        if exchange == "CZCE":
            month = month[-3:]
        if "HOT" == month:
            dsb_name = f"{exchange}.{pid}_HOT.dsb"
        else:
            dsb_name = f"{pid}{month}.dsb"
        dsb_path = os.path.join(save_path, dsb_name)
        if skip_saved:
            saved_list = os.listdir(save_path)
            if dsb_name in saved_list:
                print(f"重复数据，跳过{dsb_name}")
                return
        df = self.get_bar(code, start_date, end_date, frequency)
        self.bar_df_to_dsb(df, dsb_path, frequency)

    def store_his_tick(self, storage_path, code, start_date=None, end_date=None, skip_saved=False):
        exchange, pid, month = self.parse_code(code)
        # 分天下载，避免内存超出
        for _date in pd.date_range(start_date, end_date):
            # 判断是不是周日，如果是就跳过
            if _date.weekday() == 6:  # 0为周一，6为周日
                continue
            t_day = _date.strftime('%Y%m%d')
            save_path = os.path.join(storage_path, "his", "ticks", exchange, t_day)
            if not os.path.exists(save_path):
                os.makedirs(save_path)
            dsb_name = f"{pid}{month}.dsb"
            if skip_saved:
                saved_list = os.listdir(save_path)
                if dsb_name in saved_list:
                    print(f"重复数据，跳过{dsb_name}")
                    continue
            # 转换日期格式
            t_day = datetime.datetime.strptime(t_day, "%Y%m%d")
            start_dt = datetime.datetime.combine(t_day, datetime.time.min)
            end_dt = datetime.datetime.combine(t_day, datetime.time.max)

            df = self.get_tick(code, start_dt, end_dt)
            if (df is None) or (df.empty):
                print(f"{_date}:{code}没有数据")
                continue
            if exchange == "CZCE":
                month = month[-3:]
            dsb_path = os.path.join(save_path, f"{pid}{month}.dsb")
            self.tick_df_to_dsb(df, dsb_path)


class TqFeed(Ifeed):
    def __init__(self, user=None, passwd=None):
        super().__init__()
        self.tqauth = TqAuth(user, passwd)
        # 列映射关系
        self.bar_col_map = {
            "date": "date",
            "time": "time",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
            "amount": "turnover",
            "open_interest": "open_interest"
        }

        self.tick_col_map = {
            "code": "code",
            "exchg": "exchg",
            "last_price": "price",
            "open": "open",
            "highest": "high",
            "lowest": "low",
            "volume": "total_volume",
            "volume_delta": "volume",
            "amount": "total_turnover",
            "amount_delta": "turn_over",
            "open_interest": "open_interest",
            "open_interest_delta": "diff_interest",
            "trading_day": "trading_date",
            "datetime": "action_date",
            "time": "action_time",
            "pre_close": "pre_close",
            "settlement": "settle_price",
            "pre_settlement": "pre_settle"
        }

        # 添加买卖盘数据映射
        for i in range(1, 6):
            self.tick_col_map[f"ask_price{i}"] = f"ask_{i}"
            self.tick_col_map[f"bid_price{i}"] = f"bid_{i}"
            self.tick_col_map[f"ask_volume{i}"] = f"ask_qty_{i}"
            self.tick_col_map[f"bid_volume{i}"] = f"bid_qty_{i}"

    def get_tick(self, code, start_date=None, end_date=None):
        """获取Tick数据"""
        symbol = self.code_std(code)
        exchange, pid, month = self.parse_code(code)

        # 准备回测参数
        if start_date is None or end_date is None:
            return None

        # 使用字典存储所有tick数据，以datetime为key去重
        all_ticks = {}

        # 从end_date开始，逐步往前回测
        current_datetime = end_date

        print(f"开始获取 {symbol} 从 {start_date} 到 {end_date} 的Tick历史数据...")

        while current_datetime.date() >= start_date.date():
            # 当前时间的回测参数
            backtest_start = current_datetime
            backtest_end = backtest_start + datetime.timedelta(days=8)

            print(f"正在获取 {current_datetime} 之前的10000个Tick数据...")

            try:
                # 使用回测模式获取当前时间段的历史数据
                api = TqApi(
                    account=TqSim(),
                    auth=self.tqauth,
                    backtest=TqBacktest(start_dt=backtest_start, end_dt=backtest_end),
                    disable_print=True
                )

                try:
                    # 获取Tick数据，每次最多10000条
                    ticks_data = api.get_tick_serial(symbol=symbol, data_length=10000)

                    api.wait_update()

                    # 当前批次的tick数据
                    tick_list = []
                    for i in range(len(ticks_data)):
                        tick_data = ticks_data.iloc[i]
                        if tick_data['datetime'] > 0:  # 有效数据
                            # 转换时间戳（纳秒转秒）
                            timestamp_ns = tick_data['datetime']
                            timestamp_s = timestamp_ns / 1e9
                            dt = datetime.datetime.fromtimestamp(timestamp_s)

                            # 构建tick字典
                            tick_dict = {
                                'datetime': dt,
                                'exchg': exchange,
                                'code': pid + month,
                                'price': float(tick_data.get('last_price', 0)),
                                'open': float(tick_data.get('open', 0)),
                                'high': float(tick_data.get('highest', 0)),
                                'low': float(tick_data.get('lowest', 0)),
                                'total_volume': float(tick_data.get('volume', 0)),
                                'volume': 0,  # 增量，稍后计算
                                'total_turnover': float(tick_data.get('amount', 0)),
                                'turn_over': 0,  # 增量，稍后计算
                                'open_interest': float(tick_data.get('open_interest', 0)),
                                'diff_interest': 0,  # 增量，稍后计算
                                # 'trading_date': tick_data.get('trading_day', '').replace('-', ''),
                                'trading_date': dt.strftime('%Y%m%d'),  # todo 暂时使用这个替换
                                'action_date': dt.strftime('%Y%m%d'),
                                'action_time': dt.strftime('%H%M%S%f'),
                                'pre_close': float(tick_data.get('pre_close', 0)),
                                'pre_settle': float(tick_data.get('pre_settlement', 0)),
                                'settle_price': float(tick_data.get('settlement', 0)),
                            }

                            # 添加买卖盘数据
                            for j in range(1, 6):
                                tick_dict[f'bid_{j}'] = float(tick_data.get(f'bid_price{j}', 0))
                                tick_dict[f'ask_{j}'] = float(tick_data.get(f'ask_price{j}', 0))
                                tick_dict[f'bid_qty_{j}'] = float(tick_data.get(f'bid_volume{j}', 0))
                                tick_dict[f'ask_qty_{j}'] = float(tick_data.get(f'ask_volume{j}', 0))

                            tick_list.append(tick_dict)

                    # 将当前批次的数据加入到all_ticks中（用时间去重）
                    for tick_dict in tick_list:
                        dt_key = tick_dict['datetime']
                        # 只保留在请求时间范围内的数据
                        if start_date <= dt_key <= end_date:
                            all_ticks[dt_key] = tick_dict

                    print(f"{current_datetime} 获取到 {len(tick_list)} 条Tick数据，当前共 {len(all_ticks)} 条Tick数据")

                    # 检查最早的数据时间，判断是否需要继续往前获取
                    if tick_list:
                        # 找到最早的时间
                        earliest_time = min(tick['datetime'] for tick in tick_list)

                        # 如果最早的数据时间已经小于等于start_date的开始时间，说明已经获取了足够的数据
                        if earliest_time <= start_date:
                            print(f"已获取到 {start_date} 的数据，停止回测")
                            break

                        # 更新current_datetime
                        current_datetime = earliest_time - datetime.timedelta(seconds=1)
                    else:
                        # 如果当前时间段没有数据，往前推一天
                        current_datetime = current_datetime - datetime.timedelta(days=1)
                        print(f"当前时间段没有数据，往前推一天到 {current_datetime}")

                finally:
                    # 确保关闭API连接
                    api.close()

            except Exception as e:
                print(f"获取 {current_datetime} 的Tick数据失败: {e}")
                current_datetime = current_datetime - datetime.timedelta(days=1)

        # 将all_ticks转换为按时间排序的DataFrame
        if not all_ticks:
            return None

        # 按时间排序
        sorted_times = sorted(all_ticks.keys())
        sorted_ticks = [all_ticks[dt] for dt in sorted_times]

        # 转换为DataFrame
        df = pd.DataFrame(sorted_ticks)

        # 计算增量值
        df['volume'] = df['total_volume'].diff().fillna(0).astype(float)
        df['turn_over'] = df['total_turnover'].diff().fillna(0).astype(float)
        df['diff_interest'] = df['open_interest'].diff().fillna(0).astype(float)

        return df

    def get_bar(self, code, start_date=None, end_date=None, frequency="1m"):
        """获取K线数据"""
        if frequency not in self.frequency_map.keys():
            print("周期只能为m1、m5或d，回测或实盘中会自动拼接")
            return None

        symbol = self.code_std(code)

        # 准备回测参数
        if start_date is None or end_date is None:
            return None

        # 转换日期格式
        if isinstance(start_date, str):
            start_dt = datetime.datetime.strptime(start_date, "%Y%m%d")
        elif isinstance(start_date, datetime.datetime):
            start_dt = start_date
        else:
            start_dt = datetime.datetime.combine(start_date, datetime.time.min)

        if isinstance(end_date, str):
            end_dt = datetime.datetime.strptime(end_date, "%Y%m%d")
        elif isinstance(end_date, datetime.datetime):
            end_dt = end_date
        else:
            end_dt = datetime.datetime.combine(end_date, datetime.time.max)

        # 转换周期格式为天勤接受的格式并计算duration_seconds参数
        if frequency == "m1":
            duration_seconds = 60
        elif frequency == "m5":
            duration_seconds = 300
        elif frequency == "d":
            duration_seconds = 86400
        else:
            print(f"不支持的周期: {frequency}")
            return None

        # 存储所有K线数据
        all_bars = []
        existing_datetimes = set()  # 用于去重

        # 从end_date开始，逐步往前回测
        current_end = end_dt

        print(f"开始获取 {symbol} 从 {start_dt} 到 {end_dt} 的历史K线数据...")

        while current_end.date() >= start_dt.date():
            # 当前时间的回测参数
            backtest_start = current_end
            backtest_end = backtest_start + datetime.timedelta(days=8)

            print(f"正在获取 {current_end} 的K线数据...")

            try:
                # 使用回测模式获取历史数据
                api = TqApi(
                    account=TqSim(),
                    auth=self.tqauth,
                    backtest=TqBacktest(start_dt=backtest_start, end_dt=backtest_end),
                    disable_print=True
                )

                try:
                    # 获取K线数据
                    klines = api.get_kline_serial(
                        symbol=symbol,
                        duration_seconds=duration_seconds,
                        data_length=10000
                    )

                    api.wait_update()

                    # 收集数据
                    current_data = []
                    for i in range(len(klines)):
                        if klines.iloc[i]['datetime'] > 0:  # 有效数据
                            # 转换时间戳（纳秒转秒）
                            timestamp_ns = klines.iloc[i]['datetime']
                            timestamp_s = timestamp_ns / 1e9
                            dt = datetime.datetime.fromtimestamp(timestamp_s)

                            # 去重检查 - 如果这个时间点已经存在，跳过
                            if dt in existing_datetimes:
                                continue

                            # 过滤日期范围
                            if start_dt <= dt <= end_dt:
                                # print(11)
                                bar_dict = {
                                    'datetime': dt,
                                    'date': dt.strftime('%Y%m%d'),
                                    'time': dt.strftime('%H%M') if "m" in frequency else "0000",
                                    'open': float(klines.iloc[i]['open']),
                                    'high': float(klines.iloc[i]['high']),
                                    'low': float(klines.iloc[i]['low']),
                                    'close': float(klines.iloc[i]['close']),
                                    'volume': float(klines.iloc[i]['volume']),
                                    'turnover': float(klines.iloc[i]['amount']) if 'amount' in klines.iloc[i] else float(klines.iloc[i]['volume'] * klines.iloc[i]['close']),
                                    'open_interest': float(klines.iloc[i]['open_oi']),
                                    "diff_interest": float(klines.iloc[i].get('close_oi', 0) - klines.iloc[i].get('open_oi', 0))
                                }

                                existing_datetimes.add(dt)  # 添加到去重集合
                                current_data.append(bar_dict)

                    # 将本次收集的数据加入到所有数据中
                    all_bars.extend(current_data)
                    print(f"实际获取 {len(current_data)} 条K线数据，总计 {len(all_bars)} 条")
                    # 检查是否满足终止条件
                    if len(current_data) <= 0:
                        print(f"实际获取数量为0，表明已获取全部可用数据")
                        break

                    # 找到本次获取数据中最老的时间
                    if current_data:
                        oldest_datetime = min(item['datetime'] for item in current_data)

                        # 检查是否已经到达起始日期
                        if oldest_datetime.date() <= start_dt.date():
                            print(f"已到达起始日期{start_dt}，停止查询")
                            break

                        # 设置下一次查询的结束时间
                        current_end = oldest_datetime - datetime.timedelta(seconds=1)
                    else:
                        # 如果没有数据，往前推一天
                        current_end = current_end - datetime.timedelta(days=1)

                finally:
                    # 确保关闭API连接
                    api.close()

            except Exception as e:
                print(f"获取 {current_end} 的K线数据失败: {e}")
                current_end = current_end - datetime.timedelta(days=1)

        # 将收集的数据转换为DataFrame并按时间排序
        if not all_bars:
            return None

        df = pd.DataFrame(all_bars)
        df = df.sort_values(by='datetime')

        return df

    def code_std(self, stdCode: str):
        items = stdCode.split(".")
        exchg = items[0]
        if len(items) == 2 and exchg in ['SSE', 'SZSE']:
            # 简单股票代码，格式如SSE.600000
            return stdCode
        elif items[1] in ["IDX", "ETF", "STK", "OPT"]:
            # 标准股票代码，格式如SSE.IDX.000001
            return exchg + "." + items[2]
        elif len(items) == 3 and exchg in ["SHFE", "CFFEX", "DCE", "CZCE", "INE", "GFEX"]:
            # 标准期货代码，格式如CFFEX.IF.2103
            if items[2] != 'HOT':
                return exchg + '.' + items[1] + items[2]
            else:
                return "KQ.m@" + exchg + '.' + items[1]
        else:
            return stdCode


if __name__ == '__main__':
    try:
        username, password = get_tq_credentials()
        feed = TqFeed(username, password)
        print(f"天勤账号认证成功: {username}")
    except ValueError as e:
        print(f"配置错误: {e}")
        exit(1)
    except Exception as e:
        print(f"天勤认证失败: {e}")
        exit(1)

    # 数据存储的目录
    storage_path = "./storage"

    # 下载期货数据示例
    feed.store_his_bar(storage_path, "SHFE.ni.HOT", start_date="20211225", end_date="20220101", frequency="m1", skip_saved=False)
    feed.store_his_tick(storage_path, "SHFE.au.2512", start_date="20250901", end_date="20251013", skip_saved=False)

    # 读取dsb数据
    dtHelper = WtDataHelper()
    dtHelper.dump_bars(binFolder="../storage/his/min1/SHFE/", csvFolder="min1_csv")
    dtHelper.dump_ticks(binFolder="./storage/his/ticks/SHFE/20250901/", csvFolder="ticks_csv")
